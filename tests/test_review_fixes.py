"""
Regression tests for independent Review R1 findings:
F1 - processors calling self.job.finish() inside process() must not break
     atomic publication (Tumblr/url_titles/tiktok pattern)
F2 - terminal writes are attempt-conditioned; late workers cannot affect a
     newer attempt
F4 - annotation cleanup is bounded to the attempt's time window
recurring publication resets rather than deletes the job row
"""
import inspect
import types
from unittest.mock import MagicMock

import pytest

from backend.lib.processor import BasicProcessor, _AttemptBoundJob
from common.lib.attempt import Attempt, AttemptStatus, AttemptPhase
from common.lib.dataset import DataSet


DATASET_KEY = "outkey1234567890abcdef1234567890ab"
RESULT_FILE = "query-outkey1234567890abcdef1234567890ab.csv"


class StatefulDB:
    """Minimal stateful database: jobs rows actually disappear on delete"""

    def __init__(self, job_row, owned_rows):
        self.jobs = {(job_row["jobtype"], job_row["remote_id"]): dict(job_row)}
        self.owned_rows = owned_rows
        self.insert = MagicMock(return_value=1)
        self.attempt_execute = MagicMock(return_value=1)

    def fetchone(self, query, replacements=None):
        if "FROM jobs" in query:
            return dict(self.jobs.get((replacements[0], str(replacements[1]))))
        if "FROM job_attempts" in query:
            return None
        return None

    def fetchall(self, query, replacements=None):
        if "FROM datasets WHERE attempt_id" in query:
            return [dict(row) for row in self.owned_rows]
        return []

    def execute(self, query, replacements=None, commit=True, **kwargs):
        if query.strip().startswith("SELECT pg_advisory"):
            return 1
        if "UPDATE job_attempts" in query:
            return self.attempt_execute(query, replacements, commit)
        return 1

    def update(self, table, data=None, where=None, commit=True, **kwargs):
        if table == "jobs":
            key = (where["jobtype"], where["remote_id"])
            current = self.jobs.get(key)
            if current is None:
                return 0
            if where.get("attempt_id") is not None and current["attempt_id"] != where["attempt_id"]:
                return 0
            current.update(data)
            return 1
        return 1

    def delete(self, table, where=None, commit=True, **kwargs):
        if table == "jobs":
            key = (where["jobtype"], where["remote_id"])
            current = self.jobs.get(key)
            if current is None:
                return 0
            if where.get("attempt_id") is not None and current["attempt_id"] != where["attempt_id"]:
                return 0
            self.jobs.pop(key)
            return 1
        return 1

    def commit(self):
        pass

    def rollback(self):
        pass


@pytest.fixture
def modules(tmp_path):
    return types.SimpleNamespace(
        config=types.SimpleNamespace(get=lambda key: tmp_path),
        processors={},
    )


def make_row(attempt_id, interval=0):
    return {
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
        "attempt_id": attempt_id,
    }


def make_job_data(attempt_id, interval=0):
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
        "interval": interval,
        "attempt_id": attempt_id,
    }


class FinishInsideProcess(BasicProcessor):
    type = "dummy-processor"
    category = "Testing"
    title = "Dummy"
    description = "Finishes its job inside process() like search_tumblr/url_titles"
    extension = "csv"

    def process(self):
        # exact pattern from search_tumblr.get_items / url_titles
        assert self.job.finish() is True
        candidate = self.dataset.get_results_path()
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_text("id,body\n1,alpha\n2,beta\n")
        self.dataset.finish(2)


def build(db, modules, tmp_path, attempt, interval=0):
    processor = FinishInsideProcess.__new__(FinishInsideProcess)
    processor.db = db
    processor.modules = modules
    processor.attempt = attempt
    processor.log = MagicMock()
    processor.config = types.SimpleNamespace(get=lambda *args, **kwargs: None)
    processor.parameters = {}
    processor.source_dataset = None
    processor._post_commit_actions = []
    processor.interrupted = False
    processor.type = "dummy-processor"
    processor.is_running_in_preset = False
    processor._resuming_attempt = False
    processor._defer_job_finish = False
    processor._job_finish_requested = False

    from common.lib.job import Job
    raw_job = Job(make_job_data(attempt.id, interval=interval), database=db)
    processor.job = _AttemptBoundJob(raw_job, processor)
    processor.dataset = DataSet(data=make_row(attempt.id), db=db, modules=modules, check_owners=False)
    processor.dataset._attempt = attempt
    return processor


def test_processor_finish_inside_process_is_deferred_and_publishes(modules, tmp_path):
    """F1: self.job.finish() in process() must not delete the row pre-publish"""
    # use the stateful DB for this one
    attempt = Attempt.create("dummy-processor", DATASET_KEY, MagicMock(), modules=modules)
    state = StatefulDB(make_job_data(attempt.id), [make_row(attempt.id)])
    # rebind attempt to the stateful DB
    attempt.db = state
    processor = build(state, modules, tmp_path, attempt)

    # emulate the _run_processor body section
    attempt.transition(phase=AttemptPhase.PROCESSING)
    processor._defer_job_finish = True
    try:
        processor.process()
        processor.after_process()
    finally:
        processor._defer_job_finish = False

    # job row survived the in-process finish call
    assert ("dummy-processor", DATASET_KEY) in state.jobs
    assert processor._job_finish_requested is True

    processor._publish_attempt()

    # publication deleted the row exactly once, and the result is published
    assert ("dummy-processor", DATASET_KEY) not in state.jobs
    assert (tmp_path / RESULT_FILE).read_text() == "id,body\n1,alpha\n2,beta\n"
    assert attempt.is_committed


def test_proxy_real_mode_finish_is_attempt_conditional(modules, tmp_path):
    attempt = Attempt.create("dummy-processor", DATASET_KEY, MagicMock(), modules=modules)
    state = StatefulDB(make_job_data(attempt.id), [])
    processor = build(state, modules, tmp_path, attempt)

    # real mode (not inside process body): conditional finish
    assert processor.job.finish() is True
    assert ("dummy-processor", DATASET_KEY) not in state.jobs
    # deleting again affects no row
    assert processor.job.finish() is False


def test_finalize_never_flips_a_committed_attempt(modules, tmp_path):
    """F2: a late worker cannot rewrite committed/cancelled terminal state"""
    db = MagicMock()
    db.execute.return_value = 1
    db.update.return_value = 1
    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    attempt.transition(phase=AttemptPhase.DB_COMMITTED)
    assert attempt.finalize(AttemptStatus.COMMITTED) is True

    # someone else finalised the row concurrently (0 rows affected)
    db.execute.return_value = 0
    db.fetchone.return_value = {"status": AttemptStatus.COMMITTED.value}
    assert attempt.finalize(AttemptStatus.FAILED, error="late crash") is False
    assert attempt.status == AttemptStatus.COMMITTED.value


def test_annotation_cleanup_bounded_to_attempt_window(modules, tmp_path):
    """F4: clean_up_on_error only deletes annotations created this attempt"""
    attempt = Attempt.create("dummy-processor", DATASET_KEY, MagicMock(), modules=modules)
    state = MagicMock()
    state.update.return_value = 1
    state.delete.return_value = 1
    attempt.db = state

    processor = build(state, modules, tmp_path, attempt)
    processor.manager = MagicMock()
    processor.flush_proxied_requests = MagicMock()
    # no candidate written, so the file/folder cleanup branches are no-ops
    processor.clean_up_on_error()

    delete_calls = [c for c in state.execute.call_args_list if "DELETE FROM annotations" in c.args[0]]
    assert delete_calls
    query, replacements = delete_calls[0].args
    assert "timestamp >= %s" in query and "timestamp <= %s" in query
    assert replacements[1] == attempt.data["timestamp_created"]
    assert replacements[2] == attempt.data["heartbeat_at"] + 600


def test_recurring_publication_resets_job_instead_of_delete(modules, tmp_path):
    db = MagicMock()
    db.update.return_value = 1
    db.delete.return_value = 1
    db.execute.return_value = 1
    attempt = Attempt.create("dummy-processor", DATASET_KEY, db, modules=modules)
    attempt.transition(phase="processing")
    row = make_row(attempt.id)
    job_data = make_job_data(attempt.id, interval=3600)

    db.fetchall.return_value = [dict(row)]
    db.fetchone.return_value = {"attempt_id": attempt.id}

    from tests.test_publication import build_processor
    # build_processor hardcodes interval 0; reuse its pieces but swap the job
    processor = build_processor(db, modules, tmp_path, attempt, row)
    from common.lib.job import Job
    recurring = Job(job_data, database=db)
    processor.job = recurring

    candidate = processor.dataset.get_results_path()
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_text("id,body\n1,alpha\n")
    attempt.record_pending_finish(DATASET_KEY, 1, "finished", RESULT_FILE)

    processor._publish_attempt()

    # reset branch: timestamp_claimed=0 and attempts=0 in one conditional update
    job_updates = [
        c for c in db.update.call_args_list
        if c.kwargs.get("where", {}).get("jobtype") == "dummy-processor"
        and "timestamp_claimed" in c.kwargs.get("data", {})
    ]
    assert job_updates and job_updates[-1].kwargs["data"]["attempt_id"] == ""
    assert attempt.is_committed


def test_bind_happens_after_source_wait_release():
    """F3: the release(wait) branch must precede dataset bind/tagging"""
    source = inspect.getsource(BasicProcessor._run_processor)
    release_pos = source.find("not self.source_dataset.is_finished() and not self.is_running_in_preset")
    bind_pos = source.find("self.dataset.bind_attempt(")
    assert release_pos != -1 and bind_pos != -1 and release_pos < bind_pos
