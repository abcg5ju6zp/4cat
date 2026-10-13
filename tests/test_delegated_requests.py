"""
Tests for the durable delegated request machinery

These cover the persistence-free logic of backend/lib/proxied_requests.py:
input-order sequence allocation, kwargs serialization/hook references,
response/error summaries, recovery reconstruction, and the in-memory
compatibility path used when durable delegation is disabled.
"""
import json
import logging

import pytest
import requests

from backend.lib.proxied_requests import (
    DelegatedRequestHandler,
    FailedProxiedRequest,
    RecoveredProxiedRequest,
    RecoveredRequestError,
    QueuedRequest,
    InFlightRequest,
    SEQ_GAP,
    HOOK_REFS_MARKER,
    REQUEST_SUCCEEDED,
    REQUEST_FAILED,
    RESULT_RESPONSE,
    RESULT_ERROR,
    next_append_seqs,
    insert_seq,
    renorm_seqs,
    hook_reference,
    serializable_kwargs,
    resolve_hook_ref,
    summarize_response,
    summarize_error,
    rehydrate_error,
    remove_proxy_credentials,
)

STATUS_QUEUED = DelegatedRequestHandler.REQUEST_STATUS_QUEUED
STATUS_WAITING = DelegatedRequestHandler.REQUEST_STATUS_WAITING_FOR_YIELD


class FakeConfig:
    """Minimal config reader for handler construction"""

    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None):
        return self.values.get(key, default)


@pytest.fixture
def handler():
    config = FakeConfig({
        "proxies.urls": ["__localhost__"],
        "proxies.durable-delegation": False,
        "proxies.cooloff": 0,
        "proxies.concurrent-overall": 5,
        "proxies.concurrent-host": 2,
        "proxies.allow-localhost-fallback": True,
        "proxies.healthcheck-url": "",
        "proxies.healthcheck-timeout": 1,
    })
    return DelegatedRequestHandler(logging.getLogger("test-delegator"), config)


# ---------------------------------------------------------------------------
# input order / sequences
# ---------------------------------------------------------------------------

def test_next_append_seqs_evenly_spaced_and_monotonic():
    new_next, seqs = next_append_seqs(0.0, 3)
    assert seqs == [SEQ_GAP, 2 * SEQ_GAP, 3 * SEQ_GAP]
    assert new_next == 3 * SEQ_GAP

    new_next, more = next_append_seqs(new_next, 2)
    assert more == [4 * SEQ_GAP, 5 * SEQ_GAP]
    assert len(set(seqs + more)) == 5
    assert seqs + more == sorted(seqs + more)


def test_insert_seq_head_middle_tail_and_ordering():
    # head inserts take the midpoint between the virtual origin and the head
    assert insert_seq(None, SEQ_GAP) == SEQ_GAP / 2
    middle = insert_seq(0.0, SEQ_GAP)
    assert 0.0 < middle < SEQ_GAP
    assert insert_seq(SEQ_GAP, None) == 2 * SEQ_GAP

    # repeated head inserts stay ordered before the previous head (descending)
    previous = SEQ_GAP
    values = []
    for _ in range(5):
        previous = insert_seq(None, previous)
        values.append(previous)
    assert values == sorted(values, reverse=True)
    assert len(set(values)) == len(values)


def test_insert_seq_raises_when_gap_exhausted():
    with pytest.raises(ValueError):
        insert_seq(1.0, 1.0 + 1e-12)


def test_renorm_seqs_restores_gaps():
    sequences = renorm_seqs(3)
    assert sequences == [SEQ_GAP, 2 * SEQ_GAP, 3 * SEQ_GAP]
    assert all(sequences[i + 1] - sequences[i] == SEQ_GAP for i in range(len(sequences) - 1))


# ---------------------------------------------------------------------------
# kwargs and hook references
# ---------------------------------------------------------------------------

def example_response_hook(response):
    return response


def test_serializable_kwargs_keeps_json_values_and_drops_other():
    kwargs = serializable_kwargs({
        "headers": {"User-Agent": "x"},
        "timeout": 20,
        "stream": True,
        "verify": False,
        "file": object(),
    })
    assert kwargs["headers"] == {"User-Agent": "x"}
    assert kwargs["timeout"] == 20
    assert kwargs["stream"] is True
    assert kwargs["verify"] is False
    assert "file" not in kwargs


def test_hook_reference_for_module_level_function_roundtrips():
    ref = hook_reference(example_response_hook)
    assert ref == {"module": __name__, "qualname": "example_response_hook"}
    assert resolve_hook_ref(ref) is example_response_hook


def test_hook_reference_for_lambda_is_none():
    assert hook_reference(lambda r: r) is None


def test_hook_reference_for_bound_method_needs_binder():
    class ExampleProcessor:
        def stream_url(self, response):
            return response

    processor = ExampleProcessor()
    ref = hook_reference(processor.stream_url)
    assert ref is not None and ref.get("bind") == "processor"
    # binder-required refs are not globally resolvable
    assert resolve_hook_ref(ref) is None


def test_serializable_kwargs_records_importable_hooks():
    kwargs = serializable_kwargs({"hooks": {"response": example_response_hook}})
    refs = kwargs["hooks"][HOOK_REFS_MARKER]["response"]
    assert len(refs) == 1
    assert resolve_hook_ref(refs[0]) is example_response_hook


def test_kwargs_from_stored_rebinds_processor_hooks(handler):
    sentinel = object()

    def binder(ref):
        if ref.get("bind") == "processor":
            return sentinel
        return None

    binding = handler.bind_batch(
        "test-queue-rebind", "test-processor", "dataset-1", 42, hook_binder=binder
    )
    assert binding.durable is False

    stored = {
        "timeout": 20,
        "hooks": {
            HOOK_REFS_MARKER: {
                "response": [
                    {"bind": "processor", "module": "mod", "qualname": "Processor.stream_url"},
                    {"module": __name__, "qualname": "example_response_hook"},
                ]
            }
        },
    }
    kwargs = handler._kwargs_from_stored(stored, binding)
    assert kwargs["timeout"] == 20
    hooks = kwargs["hooks"]["response"]
    assert hooks == [sentinel, example_response_hook]

    # original-url injection is added on top when a concrete URL is restored;
    # rebuild with a hook binder returning a recording callable
    called = {}

    def recording(resp, *args, **kwargs):
        called.update(kwargs)

    binding.hook_binder = lambda ref: recording
    metadata = handler._metadata_from_row({
        "id": 100,
        "seq": 2048,
        "url": "https://example.com/other",
        "kwargs": stored,
        "status": "queued",
        "attempt_no": 0,
        "result_kind": None,
        "response_summary": {},
    }, binding)
    metadata.kwargs["hooks"]["response"][0](None)
    assert called["fourcat_original_url"] == "https://example.com/other"


# ---------------------------------------------------------------------------
# summaries and recovery reconstruction
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self):
        self.status_code = 200
        self.reason = "OK"
        self.url = "https://example.com/final"
        self.headers = {
            "content-type": "text/html",
            "content-length": "42",
            "x-secret": "do-not-store",
        }


def test_summarize_response_whitelists_headers_and_strips_proxy_credentials():
    summary = summarize_response(FakeResponse(), "http://user:secret@proxy.example:8080")
    assert summary["status_code"] == 200
    assert summary["reason"] == "OK"
    assert summary["url"] == "https://example.com/final"
    assert summary["headers"] == {"content-type": "text/html", "content-length": "42"}
    assert "secret" not in summary["proxy"]
    assert summary["proxy"] == "http://proxy.example:8080"


def test_summarize_and_rehydrate_known_exception():
    error = requests.exceptions.Timeout("request timed out")
    summary = summarize_error(error, None)
    restored = rehydrate_error(summary)
    assert isinstance(restored, requests.exceptions.Timeout)
    assert "timed out" in str(restored)


def test_rehydrate_unknown_exception_falls_back_to_recovered_error():
    restored = rehydrate_error({
        "error_class": "SomeTotallyMadeUpError",
        "error_module": "nonexistent.module",
        "error_message": "boom",
    })
    assert isinstance(restored, RecoveredRequestError)
    assert restored.error_class == "SomeTotallyMadeUpError"
    assert "boom" in str(restored)


def test_metadata_from_succeeded_row_is_recovered_response(handler):
    binding = handler.bind_batch("test-queue-ok", "test-processor", "dataset-2", 7)
    summary = {
        "status_code": 200,
        "reason": "OK",
        "url": "https://example.com",
        "headers": {"content-type": "image/png"},
        "proxy": "__localhost__",
    }
    metadata = handler._metadata_from_row({
        "id": 3,
        "seq": 1024,
        "url": "https://example.com",
        "kwargs": {},
        "status": REQUEST_SUCCEEDED,
        "attempt_no": 1,
        "result_kind": RESULT_RESPONSE,
        "response_summary": summary,
    }, binding)

    assert metadata.status == STATUS_WAITING
    assert isinstance(metadata.proxied.result, RecoveredProxiedRequest)
    assert metadata.proxied.result.recovered is True
    assert metadata.proxied.result.status_code == 200
    assert metadata.proxied.result.headers.get("content-type") == "image/png"
    # a recovered response has no body
    assert metadata.proxied.result.content == b""


def test_metadata_from_failed_row_is_failed_proxied_request(handler):
    binding = handler.bind_batch("test-queue-fail", "test-processor", "dataset-3", 8)
    metadata = handler._metadata_from_row({
        "id": 4,
        "seq": 1024,
        "url": "https://example.com/missing",
        "kwargs": {},
        "status": REQUEST_FAILED,
        "attempt_no": 2,
        "result_kind": RESULT_ERROR,
        "response_summary": {
            "error_class": "ConnectionError",
            "error_module": "builtins",
            "error_message": "connection refused",
            "proxy": None,
        },
    }, binding)

    assert metadata.status == STATUS_WAITING
    result = metadata.proxied.result
    assert type(result) is FailedProxiedRequest
    assert isinstance(result.context, ConnectionError)
    assert "refused" in str(result.context)


def test_metadata_from_queued_row_preserves_order_fields(handler):
    binding = handler.bind_batch("test-queue-queued", "test-processor", "dataset-4", 9)
    metadata = handler._metadata_from_row({
        "id": 5,
        "seq": 4096,
        "url": "https://example.com",
        "kwargs": {"timeout": 5},
        "status": "queued",
        "attempt_no": 0,
        "result_kind": None,
        "response_summary": {},
    }, binding)
    assert metadata.status == STATUS_QUEUED
    assert metadata.row_id == 5
    assert metadata.seq == 4096
    assert metadata.kwargs["timeout"] == 5
    assert metadata.durable is True


# ---------------------------------------------------------------------------
# in-memory compatibility path (durable delegation disabled)
# ---------------------------------------------------------------------------

def test_memory_queue_enqueue_length_and_halt(handler):
    queue_name = "test-processor-dataset-5-job1"
    handler.add_urls(["https://a", "https://b", "https://c"], queue_name)
    assert handler.get_queue_length(queue_name) == 3
    assert handler.get_queue_length("_") == 3

    # halt cancels/removes queued entries without touching the network
    handler.halt_and_wait(queue_name)
    assert handler.get_queue_length(queue_name) == 0
    assert queue_name not in handler.queue
    assert queue_name not in handler.bindings


def test_memory_queue_bind_is_non_durable_and_skips_are_silent(handler):
    queue_name = "test-processor-dataset-6-job2"
    binding = handler.bind_batch(queue_name, "test-processor", "dataset-6", 11)
    assert binding.durable is False

    # a delivered-url skip placeholder is removed without being yielded
    placeholder = QueuedRequest(
        url="https://already-delivered",
        kwargs={},
        status=STATUS_QUEUED,
        index=0,
        durable=True,
        skip=True,
    )
    handler.queue[queue_name] = [placeholder]
    assert handler.get_queue_length(queue_name) == 0
    handler.manage_requests()
    assert placeholder not in handler.queue[queue_name]
    assert list(handler.get_results(queue_name)) == []


def test_inflight_request_carries_attempt_number_for_fencing():
    inflight = InFlightRequest(request=None, created=1.0, proxy=None,
                               url="https://example.com", index=0, attempt_no=3)
    assert inflight.attempt_no == 3
    assert inflight.result is None


def test_remove_proxy_credentials_without_credentials_unchanged():
    assert remove_proxy_credentials("http://proxy.example:8080") == "http://proxy.example:8080"
    assert remove_proxy_credentials(None) is None
    assert json.dumps({"x": 1})  # sanity: json import used elsewhere too
