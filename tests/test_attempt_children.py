"""
Tests for isolation of child datasets and child jobs within an attempt:
jobs are buffered (not claimable) until publication, tagged children stay
hidden, and failed attempts discard only what they created.
"""
import types
from unittest.mock import MagicMock

import pytest

from common.lib.attempt import Attempt
from common.lib.dataset import DataSet
from common.lib.job import Job
from common.lib.queue import JobQueue


@pytest.fixture
def db():
    instance = MagicMock()
    instance.update.return_value = 1
    instance.insert.return_value = 1
    instance.fetchall.return_value = []
    return instance


@pytest.fixture
def modules(tmp_path):
    return types.SimpleNamespace(
        config=types.SimpleNamespace(get=lambda key: tmp_path),
        processors={},
    )


def test_add_job_buffered_inside_active_attempt(db, tmp_path, modules):
    attempt = Attempt.create("parent-processor", "parentkey", db, modules=modules)
    queue = JobQueue(logger=MagicMock(), database=db)
    child = types.SimpleNamespace(key="childkey", data={"attempt_id": attempt.id})

    with DataSet.attempt_context(attempt):
        job = queue.add_job("child-processor", details={"a": 1}, dataset=child)

    assert isinstance(job, Job)
    jobs_inserts = [c for c in db.insert.call_args_list if c.args[0] == "jobs"]
    assert jobs_inserts == []  # no jobs row: buffered, hence not claimable
    buffered = attempt.get_pending_children()
    assert len(buffered) == 1
    assert buffered[0]["jobtype"] == "child-processor"
    assert buffered[0]["remote_id"] == "childkey"
    assert buffered[0]["timestamp_claimed"] == 0


def test_add_job_outside_context_inserts_normally(db, tmp_path, modules):
    queue = JobQueue(logger=MagicMock(), database=db)
    foreign = types.SimpleNamespace(key="fk", data={"attempt_id": "other-attempt"})
    queue.add_job("child-processor", dataset=foreign)

    # foreign-tagged dataset is not buffered
    db.insert.assert_called_once()
    assert db.insert.call_args.args[0] == "jobs"
    assert db.insert.call_args.args[1]["remote_id"] == "fk"

    db.reset_mock()
    plain = types.SimpleNamespace(key="pk", data={"attempt_id": ""})
    queue.add_job("child-processor", dataset=plain)
    db.insert.assert_called_once()


def test_flush_pending_children_inserts_in_caller_transaction(db, tmp_path, modules):
    attempt = Attempt.create("parent-processor", "parentkey", db, modules=modules)
    queue = JobQueue(logger=MagicMock(), database=db)
    child = types.SimpleNamespace(key="childkey", data={"attempt_id": attempt.id})
    with DataSet.attempt_context(attempt):
        queue.add_job("child-processor", dataset=child)

    inserted = attempt.flush_pending_children(commit=False)
    assert inserted == 1
    # every buffered row is inserted without an intermediate commit
    call = db.insert.call_args
    assert call.args[0] == "jobs"
    assert call.kwargs["safe"] is True
    assert call.kwargs["commit"] is False
    # buffer cleared as part of the same transaction
    assert db.update.call_args.kwargs["commit"] is False
    assert attempt.get_pending_children() == []


def test_publish_tags_cleared_in_one_update(db, tmp_path, modules):
    attempt = Attempt.create("p", "parent", db, modules=modules)
    attempt.publish_dataset_tags(commit=False)
    call = db.update.call_args
    assert call.kwargs["data"] == {"attempt_id": ""}
    assert call.kwargs["where"] == {"attempt_id": attempt.id}
    assert call.kwargs["commit"] is False


def test_discard_owned_datasets_deletes_new_untags_preexisting(db, tmp_path, modules, monkeypatch):
    attempt = Attempt.create("p", "parent", db, modules=modules)
    # attempt created 'newchild'; surrogate row pre-existed and was merely bound
    attempt.transition(manifest={"new_datasets": ["newchild"]})

    def fetchall(query, replacements=None):
        if "FROM datasets WHERE attempt_id" in query:
            return [{"key": "newchild"}, {"key": "surrogate"}]
        return []
    db.fetchall.side_effect = fetchall

    deleted = []
    monkeypatch.setattr(DataSet, "delete", lambda self: deleted.append(self.key))
    # DataSet(key=...) constructor fetchone per dataset
    def fetchone(query, replacements=None):
        return {
            "key": replacements[0],
            "query": "q",
            "parameters": "{}",
            "key_parent": "parent",
            "result_file": "q-%s.csv" % replacements[0],
            "is_finished": False,
            "num_rows": 0,
            "status": "",
            "status_type": "",
            "timestamp_finished": None,
            "attempt_id": attempt.id,
        }
    db.fetchone.side_effect = fetchone

    result = attempt.discard_owned_datasets()

    assert result == ["newchild"]
    assert deleted == ["newchild"]
    # pre-existing surrogate is untagged, never deleted
    untag = [c for c in db.update.call_args_list if c.kwargs.get("where", {}).get("key") == "surrogate"]
    assert untag and untag[-1].kwargs["data"] == {"attempt_id": ""}


def test_get_children_hides_tagged_rows(db, tmp_path, modules):
    db.fetchall.return_value = []
    row = {
        "key": "parent",
        "query": "q",
        "parameters": "{}",
        "key_parent": "",
        "result_file": "q-parent.csv",
        "is_finished": True,
        "num_rows": 3,
        "status": "",
        "status_type": "finished",
        "timestamp_finished": 1,
        "attempt_id": "",
    }
    parent = DataSet(data=row, db=db, modules=modules, check_owners=False)
    parent.get_children()
    query = db.fetchall.call_args.args[0]
    assert "attempt_id = ''" in query
