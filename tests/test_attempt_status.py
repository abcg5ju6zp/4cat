"""
Tests for the attempt status payload exposed by check-query/check-processors.
"""
from unittest.mock import MagicMock

from common.lib.attempt import attempt_status_payload


def make_db(attempt_row, job_row=None):
    db = MagicMock()

    def fetchone(query, replacements=None):
        if "FROM job_attempts" in query:
            return attempt_row
        if "FROM jobs" in query:
            return job_row
        return None

    db.fetchone.side_effect = fetchone
    return db


def base_row(status, phase, error="", finished=None):
    return {
        "attempt_id": "a" * 32,
        "jobtype": "dummy",
        "remote_id": "dk",
        "status": status,
        "phase": phase,
        "error": error,
        "heartbeat_at": 123,
        "timestamp_updated": 456,
        "timestamp_finished": finished,
    }


def test_no_attempt_returns_none():
    db = make_db(None)
    assert attempt_status_payload(db, "dk") is None


def test_running_attempt_not_recoverable():
    db = make_db(base_row("running", "processing"), job_row={"timestamp_claimed": 100})
    payload = attempt_status_payload(db, "dk")
    assert payload["status"] == "running"
    assert payload["phase"] == "processing"
    assert payload["recoverable"] is False
    assert payload["parked"] is False
    assert payload["attempt_id"] == "a" * 32


def test_validated_attempt_is_recoverable_without_reprocessing():
    db = make_db(base_row("publishing", "validated"), job_row={"timestamp_claimed": 100})
    payload = attempt_status_payload(db, "dk")
    assert payload["recoverable"] is True


def test_parked_failed_attempt_reports_waiting_retry():
    db = make_db(base_row("failed", "processing", error="boom"), job_row={"timestamp_claimed": -1})
    payload = attempt_status_payload(db, "dk")
    assert payload["parked"] is True
    assert payload["recoverable"] is True
    assert payload["error"] == "boom"


def test_committed_attempt_is_terminal():
    db = make_db(base_row("committed", "job_finished", finished=789),
                 job_row=None)  # job row deleted on success
    payload = attempt_status_payload(db, "dk")
    assert payload["recoverable"] is False
    assert payload["parked"] is False
    assert payload["finished_at"] == 789
