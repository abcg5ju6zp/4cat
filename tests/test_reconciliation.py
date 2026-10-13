"""
Tests for crash reconciliation: adopting a validated/published prior attempt
(resume without reprocessing) and superseding one that died mid-processing.
"""
import types
from unittest.mock import MagicMock

import pytest

from backend.lib.processor import BasicProcessor
from common.lib.attempt import Attempt, AttemptStatus, AttemptPhase
from common.lib.dataset import DataSet
from common.lib.result_validation import build_candidate_manifest_entry
from common.lib.job import Job


class DummyProcessor(BasicProcessor):
    type = "dummy-processor"
    category = "Testing"
    title = "Dummy"
    description = "Reconciliation test processor"
    extension = "csv"

    def process(self):
        raise AssertionError("process() must not run during reconciliation resume")


DATASET_KEY = "outkey1234567890abcdef1234567890ab"
RESULT_FILE = "query-outkey1234567890abcdef1234567890ab.csv"
CONTENT = "id,body\n1,alpha\n2,beta\n"


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
    return instance


@pytest.fixture
def modules(tmp_path):
    return types.SimpleNamespace(
        config=types.SimpleNamespace(get=lambda key: tmp_path),
        processors={},
    )


def make_row(attempt_id):
    return {
        "key": DATASET_KEY,
        "query": "query",
        "parameters": "{}",
        "key_parent": "",
        "result_file": RESULT_FILE,
        "is_finished": False,
        "num_rows": 0,
        "status": "",
        "status_type": "processing",
        "timestamp_finished": None,
        "attempt_id": attempt_id,
    }


def make_job_data(attempt_id):
    return {
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
        "attempt_id": attempt_id,
    }


def build_processor(db, modules, attempt):
    job = Job(make_job_data(attempt.id), database=db)
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
    processor.dataset = DataSet(data=make_row(attempt.id), db=db, modules=modules, check_owners=False)
    processor.dataset._attempt = attempt
    processor._resuming_attempt = False
    return processor


def wire_fetchone(db, prior, current_attempt_id):
    def fetchone(query, replacements=None):
        if "FROM job_attempts" in query:
            return prior.data  # same row dict: finalize() mutations are visible
        return {"attempt_id": current_attempt_id}
    db.fetchone.side_effect = fetchone


def test_validated_crash_adopts_prior_and_publishes_without_process(db, modules, tmp_path):
    prior = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    prior.record_pending_finish(DATASET_KEY, 2, "finished", RESULT_FILE)

    # candidate written and validated before the worker died
    candidate = prior.root_dir / "result" / RESULT_FILE
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text(CONTENT)
    entry = build_candidate_manifest_entry(candidate, prior.root_dir, num_rows=2)
    prior.transition(phase=AttemptPhase.VALIDATED, status=AttemptStatus.PUBLISHING,
                     manifest={**prior.get_manifest(), "results": [entry]})

    # a new claim happened after restart
    current = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    wire_fetchone(db, prior, prior.id)
    db.fetchall.return_value = [make_row(prior.id)]
    processor = build_processor(db, modules, current)

    processor._reconcile_attempt()

    assert processor.attempt.id == prior.id
    assert processor._resuming_attempt is True
    assert processor.job.data["attempt_id"] == prior.id
    assert current.status == AttemptStatus.SUPERSEDED.value

    # resume: only publication phases; gate re-validates and moves the file
    processor._publish_attempt()
    assert prior.status == AttemptStatus.COMMITTED.value
    assert prior.phase == AttemptPhase.JOB_FINISHED.value
    assert (tmp_path / RESULT_FILE).read_text() == CONTENT
    assert not candidate.exists()
    db.commit.assert_called()


def test_files_published_crash_completes_from_published_file(db, modules, tmp_path):
    prior = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    prior.record_pending_finish(DATASET_KEY, 2, "finished", RESULT_FILE)

    candidate = prior.root_dir / "result" / RESULT_FILE
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text(CONTENT)
    entry = build_candidate_manifest_entry(candidate, prior.root_dir, num_rows=2)
    prior.transition(phase=AttemptPhase.VALIDATED, status=AttemptStatus.PUBLISHING,
                     manifest={**prior.get_manifest(), "results": [entry]})

    # crash AFTER moving the file but BEFORE the db transaction
    logical = tmp_path / RESULT_FILE
    logical.write_text(CONTENT)
    prior.transition(phase=AttemptPhase.FILES_PUBLISHED)

    current = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    wire_fetchone(db, prior, prior.id)
    db.fetchall.return_value = [make_row(prior.id)]
    processor = build_processor(db, modules, current)
    processor._reconcile_attempt()
    assert processor._resuming_attempt is True

    processor._publish_attempt()
    assert prior.is_committed
    assert logical.read_text() == CONTENT  # untouched, identity proven by hash
    # no duplicate child/job side effects: gate deletes job exactly once in txn
    job_deletes = [c for c in db.delete.call_args_list if c.args and c.args[0] == "jobs"]
    assert len(job_deletes) == 1


def test_files_published_with_tampered_published_file_rejected(db, modules, tmp_path):
    prior = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    prior.record_pending_finish(DATASET_KEY, 2, "finished", RESULT_FILE)

    candidate = prior.root_dir / "result" / RESULT_FILE
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text(CONTENT)
    entry = build_candidate_manifest_entry(candidate, prior.root_dir, num_rows=2)
    prior.transition(phase=AttemptPhase.VALIDATED, status=AttemptStatus.PUBLISHING,
                     manifest={**prior.get_manifest(), "results": [entry]})

    tmp_path.joinpath(RESULT_FILE).write_text("id,body\n1,alpha\n")  # changed bytes
    candidate.unlink()  # the original worker moved (then somehow bytes changed)
    prior.transition(phase=AttemptPhase.FILES_PUBLISHED)

    current = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    wire_fetchone(db, prior, prior.id)
    db.fetchall.return_value = [make_row(prior.id)]
    processor = build_processor(db, modules, current)
    processor._reconcile_attempt()

    from common.lib.result_validation import ResultValidationError
    with pytest.raises(ResultValidationError, match="differs from the validated"):
        processor._publish_attempt()
    assert not prior.is_committed


def test_superseded_append_candidate_cannot_duplicate_rows(db, modules, tmp_path):
    # prior attempt appended some rows to its candidate then died
    prior = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    prior.transition(phase=AttemptPhase.PROCESSING)
    prior_candidate = prior.root_dir / "result" / RESULT_FILE
    prior_candidate.parent.mkdir(parents=True, exist_ok=True)
    prior_candidate.write_text("id\n1\n2\n")

    current = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    wire_fetchone(db, prior, current.id)
    db.fetchone.return_value = None
    processor = build_processor(db, modules, current)
    processor._reconcile_attempt()

    # new attempt's candidate is a different, empty file: append-mode writes
    # start from zero and the prior half file can never be published
    current_candidate = processor.dataset.get_results_path()
    assert current_candidate != prior_candidate
    assert not current_candidate.exists()
    current_candidate.parent.mkdir(parents=True, exist_ok=True)
    with current_candidate.open("a") as out:  # even append mode starts empty
        out.write("id\n1\n2\n")

    db.fetchall.return_value = [make_row(current.id)]
    db.fetchone.side_effect = None
    db.fetchone.return_value = {"attempt_id": current.id}
    current.record_pending_finish(DATASET_KEY, 2, "finished", RESULT_FILE)
    processor._publish_attempt()

    published = tmp_path / RESULT_FILE
    assert published.read_text() == "id\n1\n2\n"  # exactly one run, no duplicated rows
    assert prior_candidate.read_text() == "id\n1\n2\n"  # old half candidate kept for GC


def test_processing_crash_supersedes_and_runs_fresh(db, modules, tmp_path):
    prior = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    prior.transition(phase=AttemptPhase.PROCESSING)

    current = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    wire_fetchone(db, prior, current.id)
    processor = build_processor(db, modules, current)

    processor._reconcile_attempt()

    assert processor.attempt.id == current.id
    assert processor._resuming_attempt is False
    assert prior.status == AttemptStatus.SUPERSEDED.value
    # no claim re-stamping onto the dead attempt
    adopt_updates = [c for c in db.update.call_args_list
                     if c.kwargs.get("data") == {"attempt_id": prior.id}]
    assert adopt_updates == []
