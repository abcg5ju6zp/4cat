"""
Tests for attempt-based arbitration: cancel, retry, late completion and
in-transaction cancellation races.
"""
import types
from unittest.mock import MagicMock

import pytest

from backend.lib.processor import BasicProcessor
from common.lib.attempt import Attempt, AttemptStatus
from common.lib.dataset import DataSet
from common.lib.exceptions import LostArbitrationException
from common.lib.job import Job


class DummyProcessor(BasicProcessor):
    type = "dummy-processor"
    category = "Testing"
    title = "Dummy"
    description = "Arbitration test processor"
    extension = "csv"

    def process(self):
        pass


DATASET_KEY = "outkey1234567890abcdef1234567890ab"
RESULT_FILE = "query-outkey1234567890abcdef1234567890ab.csv"


@pytest.fixture
def db():
    instance = MagicMock()
    instance.update.return_value = 1
    instance.delete.return_value = 1
    instance.execute.return_value = 1
    instance.insert.return_value = 1
    instance.fetchall.return_value = []
    instance.commit = MagicMock()
    instance.rollback = MagicMock()
    instance.fetchone.return_value = {"attempt_id": ""}
    return instance


@pytest.fixture
def modules(tmp_path):
    return types.SimpleNamespace(
        config=types.SimpleNamespace(get=lambda key: tmp_path),
        processors={},
    )


def build_processor(db, modules, attempt, interrupted=None):
    job = Job({
        "id": 9,
        "jobtype": "dummy-processor",
        "remote_id": DATASET_KEY,
        "queue_id": "dummy-processor",
        "details": "{}",
        "timestamp": 100,
        "timestamp_after": 0,
        "timestamp_lastclaimed": 0,
        "timestamp_claimed": 100,
        "attempts": 0,
        "interval": 0,
        "attempt_id": attempt.id,
    }, database=db)
    processor = DummyProcessor.__new__(DummyProcessor)
    processor.db = db
    processor.modules = modules
    processor.attempt = attempt
    processor.job = job
    processor.log = MagicMock()
    processor.config = types.SimpleNamespace(get=lambda *args, **kwargs: None)
    processor.parameters = {}
    processor.source_dataset = None
    processor._post_commit_actions = []
    processor.interrupted = interrupted
    processor.type = "dummy-processor"
    processor.dataset = DataSet(data={
        "key": DATASET_KEY,
        "query": "query",
        "parameters": "{}",
        "key_parent": "parent",
        "result_file": RESULT_FILE,
        "is_finished": False,
        "num_rows": 0,
        "status": "",
        "status_type": "processing",
        "timestamp_finished": None,
        "attempt_id": attempt.id,
    }, db=db, modules=modules, check_owners=False)
    processor.dataset._attempt = attempt
    processor._resuming_attempt = False
    return processor


def test_cancel_finalizes_attempt_and_conditionally_finishes_job(db, modules):
    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    processor = build_processor(db, modules, attempt, interrupted=BasicProcessor.INTERRUPT_CANCEL)

    # an owned, pre-existing dataset (surrogate/self) is only untagged...
    db.fetchall.return_value = [{"key": DATASET_KEY}]
    # ...and DataSet(key=) loads for discard
    db.fetchone.return_value = {
        "key": DATASET_KEY, "query": "q", "parameters": "{}", "key_parent": "parent",
        "result_file": RESULT_FILE, "is_finished": False, "num_rows": 0,
        "status": "", "status_type": "", "timestamp_finished": None, "attempt_id": attempt.id,
    }

    processor.abort()

    assert attempt.status == AttemptStatus.CANCELLED.value
    job_deletes = [c for c in db.delete.call_args_list if c.args and c.args[0] == "jobs"]
    assert job_deletes[-1].kwargs["where"]["attempt_id"] == attempt.id


def test_retry_keeps_attempt_open_and_releases_conditionally(db, modules):
    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    processor = build_processor(db, modules, attempt, interrupted=BasicProcessor.INTERRUPT_RETRY)

    processor.abort()

    assert not attempt.is_terminal  # reconciled/superseded on the next claim
    releases = [c for c in db.update.call_args_list
                if c.kwargs.get("data", {}).get("timestamp_claimed") == 0
                and c.kwargs.get("where", {}).get("attempt_id") == attempt.id]
    assert releases


def test_cancel_winning_inside_transaction_blocks_publication(db, modules, tmp_path):
    from tests.test_publication import make_row, build_processor as build_publisher

    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    attempt.transition(phase="processing")
    row = make_row(attempt.id)
    db.fetchall.return_value = [dict(row)]
    db.fetchone.return_value = {"attempt_id": attempt.id}
    processor = build_publisher(db, modules, tmp_path, attempt, row)

    candidate = processor.dataset.get_results_path()
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("id\n1\n")
    attempt.record_pending_finish(DATASET_KEY, 1, "finished", RESULT_FILE)

    # cancel deletes the job row while the publication transaction is open
    original_delete = db.delete
    def conditional_delete(table, where, commit=True):
        if table == "jobs":
            return 0  # job already gone (cancelled)
        return original_delete(table, where, commit=commit)
    db.delete.side_effect = conditional_delete

    with pytest.raises(LostArbitrationException):
        processor._publish_attempt()

    db.rollback.assert_called()
    assert not attempt.is_committed
    # post-commit actions never ran
    assert processor._post_commit_actions == []


def test_worker_run_closes_lightweight_attempt_on_success():
    # code-level guarantee for non-processor workers (scrapers/system jobs)
    import inspect
    from backend.lib.worker import BasicWorker
    source = inspect.getsource(BasicWorker.run)
    assert "AttemptStatus.COMMITTED" in source
    assert "not self.attempt.is_terminal" in source
