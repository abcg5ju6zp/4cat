"""
Integration tests for BasicProcessor._publish_attempt(): validation,
atomic file move, single-transaction DB publication, conditional job
termination and post-commit actions.

Postgres is mocked; the filesystem is a real tmp directory.
"""
import types
from unittest.mock import MagicMock

import pytest

from backend.lib.processor import BasicProcessor
from common.lib.attempt import Attempt, AttemptStatus, AttemptPhase
from common.lib.dataset import DataSet
from common.lib.exceptions import LostArbitrationException
from common.lib.job import Job


class DummyProcessor(BasicProcessor):
    type = "dummy-processor"
    category = "Testing"
    title = "Dummy"
    description = "Publication test processor"
    extension = "csv"

    def process(self):
        pass


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
    instance.fetchone.return_value = None
    return instance


@pytest.fixture
def modules(tmp_path):
    return types.SimpleNamespace(
        config=types.SimpleNamespace(get=lambda key: tmp_path),
        processors={},
    )


DATASET_KEY = "outkey1234567890abcdef1234567890ab"
RESULT_FILE = "query-outkey1234567890abcdef1234567890ab.csv"


def make_row(attempt_id):
    return {
        "key": DATASET_KEY,
        "query": "query",
        "parameters": "{}",
        "key_parent": "parentkey",
        "result_file": RESULT_FILE,
        "is_finished": False,
        "num_rows": 0,
        "status": "",
        "status_type": "processing",
        "timestamp_finished": None,
        "attempt_id": attempt_id,
    }


def build_processor(db, modules, tmp_path, attempt, row):
    job = Job({
        "id": 7,
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
    processor.interrupted = False
    processor.type = "dummy-processor"
    processor.dataset = DataSet(data=dict(row), db=db, modules=modules, check_owners=False)
    processor.dataset._attempt = attempt
    return processor


def test_full_publication_file_move_transaction_and_actions(db, modules, tmp_path):
    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    attempt.transition(phase=AttemptPhase.PROCESSING)
    row = make_row(attempt.id)
    db.fetchall.return_value = [dict(row)]
    db.fetchone.return_value = {"attempt_id": attempt.id}

    processor = build_processor(db, modules, tmp_path, attempt, row)

    # processor wrote its candidate in the isolated area
    candidate = processor.dataset.get_results_path()
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("id,body\n1,alpha\n2,beta\n")
    attempt.record_pending_finish(DATASET_KEY, 2, "finished", RESULT_FILE)

    actions = []
    processor.defer_after_commit(lambda: actions.append("done"))

    processor._publish_attempt()

    logical = tmp_path / RESULT_FILE
    assert logical.read_text() == "id,body\n1,alpha\n2,beta\n"
    assert not candidate.exists()

    # terminal dataset state committed with finished metadata
    terminal_updates = [
        c for c in db.update.call_args_list
        if c.kwargs.get("where", {}).get("key") == DATASET_KEY
        and c.kwargs.get("data", {}).get("is_finished") is True
    ]
    assert terminal_updates and terminal_updates[-1].kwargs["commit"] is False

    # tags cleared, children flushed and attempt committed in the txn
    assert db.commit.call_count >= 1
    assert attempt.status == AttemptStatus.COMMITTED.value
    assert attempt.phase == AttemptPhase.JOB_FINISHED.value

    # job deleted conditionally inside the transaction
    job_deletes = [c for c in db.delete.call_args_list if c.args and c.args[0] == "jobs"]
    assert job_deletes[-1].kwargs["where"]["attempt_id"] == attempt.id
    assert job_deletes[-1].kwargs["commit"] is False

    # post-commit side effect ran exactly once, after commit
    assert actions == ["done"]
    assert processor.dataset._attempt is None
    assert processor.dataset.data["attempt_id"] == ""


def test_no_output_publication_requires_no_file(db, modules, tmp_path):
    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    attempt.transition(phase=AttemptPhase.PROCESSING)
    row = make_row(attempt.id)
    db.fetchall.return_value = [dict(row)]
    db.fetchone.return_value = {"attempt_id": attempt.id}
    processor = build_processor(db, modules, tmp_path, attempt, row)

    attempt.record_pending_finish(DATASET_KEY, 0, "empty", RESULT_FILE)
    processor._publish_attempt()

    assert attempt.status == AttemptStatus.COMMITTED.value
    assert not (tmp_path / RESULT_FILE).exists()
    terminal = [
        c for c in db.update.call_args_list
        if c.kwargs.get("where", {}).get("key") == DATASET_KEY
        and c.kwargs.get("data", {}).get("status_type") == "empty"
    ]
    assert terminal


def test_late_worker_ownership_mismatch_blocks_publication(db, modules, tmp_path):
    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    attempt.transition(phase=AttemptPhase.PROCESSING)
    row = make_row(attempt.id)
    db.fetchall.return_value = [dict(row)]
    db.fetchone.return_value = {"attempt_id": "newer-attempt"}  # pre-check fails
    processor = build_processor(db, modules, tmp_path, attempt, row)

    candidate = processor.dataset.get_results_path()
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("id\n1\n")
    attempt.record_pending_finish(DATASET_KEY, 1, "finished", RESULT_FILE)

    with pytest.raises(LostArbitrationException):
        processor._publish_attempt()

    # published path untouched; candidate kept; nothing committed; no actions
    assert not (tmp_path / RESULT_FILE).exists()
    assert candidate.exists()
    db.commit.assert_not_called()
    assert attempt.status != AttemptStatus.COMMITTED.value


def test_db_failure_after_file_move_rolls_back_for_reconciliation(db, modules, tmp_path):
    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    attempt.transition(phase=AttemptPhase.PROCESSING)
    row = make_row(attempt.id)
    db.fetchall.return_value = [dict(row)]
    db.fetchone.return_value = {"attempt_id": attempt.id}
    processor = build_processor(db, modules, tmp_path, attempt, row)

    candidate = processor.dataset.get_results_path()
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("id,body\n1,alpha\n2,beta\n")
    attempt.record_pending_finish(DATASET_KEY, 2, "finished", RESULT_FILE)

    # fail the terminal dataset update inside the transaction
    def update_side_effect(table, data, where=None, commit=True):
        if table == "datasets" and data.get("is_finished") is True:
            raise RuntimeError("simulated db outage")
        return 1
    db.update.side_effect = update_side_effect

    with pytest.raises(RuntimeError, match="simulated db outage"):
        processor._publish_attempt()

    db.rollback.assert_called()
    assert not attempt.is_committed
    assert attempt.phase == AttemptPhase.FILES_PUBLISHED.value
    # file already moved: reconciliation can finish from here without reprocessing
    assert (tmp_path / RESULT_FILE).read_text() == "id,body\n1,alpha\n2,beta\n"
