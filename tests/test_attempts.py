"""
Tests for the per-claim attempt machinery (common/lib/attempt.py) and the
attempt-aware job operations (common/lib/job.py).

The Database is mocked: what we verify here is the state machine, the claim
binding and the conditional terminal writes that arbitrate late workers.
"""
import json
import re
import types
from unittest.mock import MagicMock

import pytest

from common.lib.attempt import (
    Attempt,
    AttemptStatus,
    AttemptPhase,
    ATTEMPTS_DIRNAME,
    TERMINAL_STATUSES,
    build_input_signature,
)
from common.lib.exceptions import JobClaimedException
from common.lib.job import Job


@pytest.fixture
def db():
    """In-memory-ish mock database that records every write."""
    instance = MagicMock()
    # default: writes affect one row
    instance.update.return_value = 1
    instance.delete.return_value = 1
    instance.execute.return_value = 1
    instance.insert.return_value = 1
    return instance


def make_job_data():
    return {
        "id": 42,
        "jobtype": "test-processor",
        "remote_id": "abc123",
        "queue_id": "test-processor",
        "details": "{}",
        "timestamp": 1000,
        "timestamp_after": 0,
        "timestamp_lastclaimed": 0,
        "timestamp_claimed": 0,
        "attempts": 0,
        "interval": 0,
        "attempt_id": "",
    }


# ---------------------------------------------------------------------------
# Attempt state machine
# ---------------------------------------------------------------------------

def test_attempt_create_persists_row(db):
    attempt = Attempt.create("test-processor", "abc123", db)
    assert re.fullmatch(r"[0-9a-f]{32}", attempt.id)
    assert attempt.status == AttemptStatus.RUNNING.value
    assert attempt.phase == AttemptPhase.CLAIMED.value
    assert not attempt.is_terminal

    inserted = db.insert.call_args.args[1]
    assert inserted["attempt_id"] == attempt.id
    assert inserted["jobtype"] == "test-processor"
    assert inserted["remote_id"] == "abc123"
    assert inserted["phase"] == "claimed"


def test_attempt_phase_only_moves_forward(db):
    attempt = Attempt.create("test-processor", "abc123", db)
    attempt.transition(phase=AttemptPhase.PROCESSING)
    attempt.transition(phase=AttemptPhase.VALIDATED)
    db.update.reset_mock()
    with pytest.raises(ValueError):
        attempt.transition(phase=AttemptPhase.CLAIMED)
    db.update.assert_not_called()
    assert attempt.phase == AttemptPhase.VALIDATED.value


def test_attempt_terminal_finalize_sets_finished_at(db):
    attempt = Attempt.create("t", "r", db)
    assert attempt.finalize(AttemptStatus.COMMITTED) is True
    assert attempt.is_terminal and attempt.is_committed
    query, replacements = db.execute.call_args.args
    assert "UPDATE job_attempts SET status = %s" in query
    assert "status NOT IN %s" in query  # never flips a terminal row
    assert replacements[0] == "committed"
    assert isinstance(replacements[-1], tuple)

    with pytest.raises(ValueError):
        attempt.finalize(AttemptStatus.RUNNING)


def test_attempt_failed_is_non_recoverable_validated_is(db):
    failed = Attempt.create("t", "r", db)
    failed.transition(phase=AttemptPhase.PROCESSING)
    assert not failed.can_resume_without_reprocessing

    ok = Attempt.create("t", "r2", db)
    ok.transition(phase=AttemptPhase.VALIDATED)
    assert ok.can_resume_without_reprocessing

    assert AttemptStatus.SUPERSEDED.value in TERMINAL_STATUSES


def test_attempt_json_fields_roundtrip(db):
    attempt = Attempt.create("t", "r", db)
    signature = {"parent": {"num_rows": 7, "mtime": 12345}}
    children = [{"jobtype": "next", "remote_id": "child"}]
    attempt.transition(input_signature=signature, pending_children=children)
    assert attempt.get_input_signature() == signature
    assert attempt.get_pending_children() == children

    stored_sig = db.update.call_args.kwargs["data"]["input_signature"]
    json.loads(stored_sig)  # must be JSON text, not a dict


def test_attempt_get_open_filters_terminal(db):
    db.fetchone.return_value = None
    assert Attempt.get_open("t", "r", db) is None
    query = db.fetchone.call_args.args[0]
    assert "status NOT IN %s" in query
    assert tuple(TERMINAL_STATUSES) == db.fetchone.call_args.args[1][2]


def test_attempt_dir_layout_under_path_data(db, tmp_path):
    modules = types.SimpleNamespace(config=types.SimpleNamespace(get=lambda key: tmp_path))
    attempt = Attempt.create("t", "r", db, modules=modules)
    attempt.ensure_dirs()
    root = tmp_path / ATTEMPTS_DIRNAME / attempt.id
    assert root.is_dir()
    assert attempt.result_dir == root / "result"
    assert attempt.files_dir == root / "files"
    assert attempt.staging_dir == root / "staging"
    assert attempt.quarantine_dir == root / "quarantine"
    for sub in ("result", "files", "staging", "quarantine"):
        assert (root / sub).is_dir()

    # candidate paths stay inside the attempt area
    logical = tmp_path / "query-abc.csv"
    assert attempt.candidate_result_path(logical) == root / "result" / "query-abc.csv"
    assert attempt.candidate_files_path("datasetkey") == root / "files" / "folder_datasetkey"

    # staging names are unique, mirroring the legacy -staging[-N] scheme
    first = attempt.candidate_staging_path("x-staging")
    second = attempt.candidate_staging_path("x-staging")
    assert first != second and first.is_dir() and second.is_dir()


def test_attempt_manifest_file_mirrors_db(db, tmp_path):
    modules = types.SimpleNamespace(config=types.SimpleNamespace(get=lambda key: tmp_path))
    attempt = Attempt.create("t", "r", db, modules=modules)
    manifest = {"results": [{"path": "result/q.csv", "sha256": "deadbeef"}]}
    attempt.transition(manifest=manifest)
    assert json.loads(attempt.manifest_path.read_text()) == manifest
    assert attempt.load_manifest_file() == manifest


# ---------------------------------------------------------------------------
# Input dataset version signature
# ---------------------------------------------------------------------------

def make_input_dataset(tmp_path, key="parentkey", num_rows=10, is_finished=True,
                       mtime=1000000, size=42, result_file="q-parentkey.csv", exists=True):
    path = tmp_path / result_file
    if exists:
        import os
        path.write_text("x" * size)
        os.utime(path, (mtime, mtime))
    data = {
        "is_finished": is_finished,
        "num_rows": num_rows,
        "timestamp_finished": 2000000,
        "result_file": result_file,
    }
    return types.SimpleNamespace(key=key, data=data, get_results_path=lambda: path)


def test_input_signature_fields_and_stability(db, tmp_path):
    ds = make_input_dataset(tmp_path)
    sig1 = build_input_signature([ds, ds], parameters={"a": 1})
    sig2 = build_input_signature([ds], parameters={"a": 1})
    assert sig1 == sig2  # de-duplicated, stable
    entry = sig1["datasets"]["parentkey"]
    assert set(entry.keys()) == {
        "key", "is_finished", "num_rows", "timestamp_finished", "result_file", "size", "mtime"
    }
    assert entry["num_rows"] == 10 and entry["mtime"] == 1000000
    # JSON-serialisable and key-sorted
    encoded = json.dumps(sig1, sort_keys=True)
    assert json.loads(encoded)["parameters"] == {"a": 1}


def test_input_signature_missing_file(db, tmp_path):
    ds = make_input_dataset(tmp_path, exists=False)
    entry = build_input_signature([ds])["datasets"]["parentkey"]
    assert entry["size"] is None and entry["mtime"] is None


@pytest.mark.parametrize("mutate", ["num_rows", "is_finished", "mtime", "size", "result_file", "parameters"])
def test_verify_inputs_detects_each_change(db, tmp_path, mutate):
    ds = make_input_dataset(tmp_path)
    attempt = Attempt.create("t", "r", db)
    attempt.set_inputs([ds], parameters={"n": 1})

    if mutate == "num_rows":
        ds.data["num_rows"] = 11
    elif mutate == "is_finished":
        ds.data["is_finished"] = False
    elif mutate == "mtime":
        import os
        os.utime(ds.get_results_path(), (2000000, 2000000))
    elif mutate == "size":
        ds.get_results_path().write_text("different-length-content")
    elif mutate == "result_file":
        ds.data["result_file"] = "q-parentkey-v2.csv"
    elif mutate == "parameters":
        pass  # passed differently below

    parameters = {"n": 2} if mutate == "parameters" else {"n": 1}
    ok, changed = attempt.verify_inputs([ds], parameters=parameters)
    assert not ok
    assert changed  # human-readable difference reported


def test_verify_inputs_passes_unchanged(db, tmp_path):
    ds = make_input_dataset(tmp_path)
    attempt = Attempt.create("t", "r", db)
    attempt.set_inputs([ds], parameters={"n": 1})
    ok, changed = attempt.verify_inputs([ds], parameters={"n": 1})
    assert ok and changed == []


# ---------------------------------------------------------------------------
# Job claim binding and conditional terminal writes
# ---------------------------------------------------------------------------

def test_job_claim_stamps_attempt_id_in_conditional_update(db):
    job = Job(make_job_data(), database=db)
    job.claim(attempt_id="attempt-1")

    query, replacements = db.execute.call_args.args
    assert "attempt_id = %s" in query
    assert "timestamp_claimed = 0" in query
    assert replacements[2] == "attempt-1"
    assert tuple(replacements[3:]) == ("test-processor", "abc123")
    assert job.data["attempt_id"] == "attempt-1"
    assert job.is_claimed


def test_job_lost_claim_raises_and_keeps_attempt_id_clean(db):
    db.execute.return_value = 0
    job = Job(make_job_data(), database=db)
    with pytest.raises(JobClaimedException):
        job.claim(attempt_id="attempt-1")
    assert job.data["attempt_id"] == ""
    assert not job.is_claimed


def test_job_finish_conditional_attempt_id(db):
    job = Job(make_job_data(), database=db)
    assert job.finish(attempt_id="attempt-1") is True
    where = db.delete.call_args.kwargs["where"]
    assert where["attempt_id"] == "attempt-1"
    assert job.is_finished

    db.delete.return_value = 0
    job2 = Job(make_job_data(), database=db)
    assert job2.finish(attempt_id="attempt-new") is False
    assert not job2.is_finished  # late worker learns it lost arbitration


def test_job_release_and_park_conditional(db):
    job = Job(make_job_data(), database=db)
    assert job.release(delay=10, attempt_id="attempt-1") is True
    assert db.update.call_args.kwargs["where"]["attempt_id"] == "attempt-1"

    db.update.return_value = 0
    assert job.park(attempt_id="attempt-stale") is False
    assert not job.is_parked

    db.update.return_value = 1
    assert job.park(attempt_id="attempt-1") is True
    assert job.is_parked
    assert db.update.call_args.kwargs["data"]["timestamp_claimed"] == Job.STATUS_PARKED
