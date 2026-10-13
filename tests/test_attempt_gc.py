"""
Tests for safe garbage collection of isolated attempt directories
(TempFileCleaner.gc_attempt_directories).
"""
from unittest.mock import MagicMock

from backend.workers.cleanup_tempfiles import gc_attempt_directories
from common.lib.attempt import ATTEMPTS_DIRNAME

NOW = 10_000_000
GRACE = 7 * 86400


def make_db(rows, referenced=None):
    """rows: {attempt_id: {"status": ..., "timestamp_finished": ...}}"""
    referenced = referenced or set()
    db = MagicMock()

    def fetchone(query, replacements=None):
        if "FROM job_attempts" in query:
            return rows.get(replacements[0])
        if "FROM datasets" in query or "FROM jobs" in query:
            return {"num": 1 if replacements[0] in referenced else 0}
        return None

    db.fetchone.side_effect = fetchone
    return db


def make_attempt_dir(path_data, attempt_id):
    directory = path_data / ATTEMPTS_DIRNAME / attempt_id
    directory.mkdir(parents=True)
    (directory / "result").mkdir()
    (directory / "result" / "q.csv").write_text("x")
    return directory


def test_gc_matrix(tmp_path):
    rows = {
        "a" * 32: {"status": "failed", "timestamp_finished": NOW - GRACE - 10},
        "b" * 32: {"status": "failed", "timestamp_finished": NOW - GRACE + 10_000},
        "c" * 32: {"status": "cancelled", "timestamp_finished": 0},
        "d" * 32: {"status": "superseded", "timestamp_finished": NOW - GRACE * 2},
        "e" * 32: {"status": "committed", "timestamp_finished": NOW - GRACE * 2},
        "f" * 32: {"status": "committed", "timestamp_finished": NOW},
        "g" * 32: {"status": "running", "timestamp_finished": None},
        "h" * 32: {"status": "publishing", "timestamp_finished": NOW - GRACE * 10},
        "i" * 32: {"status": "failed", "timestamp_finished": NOW - GRACE * 2},  # still referenced
    }
    for key in rows:
        make_attempt_dir(tmp_path, key)
    orphan = make_attempt_dir(tmp_path, "0" * 32)  # no DB row

    db = make_db(rows, referenced={"i" * 32})
    tracked = {}

    deleted = gc_attempt_directories(tmp_path, db, tracked, days_to_keep=7, now=NOW)

    assert set(deleted) == {"a" * 32, "c" * 32, "d" * 32, "e" * 32}
    remaining = {p.name for p in (tmp_path / ATTEMPTS_DIRNAME).iterdir()}
    assert {"b" * 32, "f" * 32, "g" * 32, "h" * 32, "i" * 32, "0" * 32} <= remaining
    # orphan was first seen: tracked, not yet deleted
    assert orphan.exists()
    assert "%s/%s" % (ATTEMPTS_DIRNAME, "0" * 32) in tracked


def test_orphan_deleted_after_tracking_expires(tmp_path):
    make_attempt_dir(tmp_path, "1" * 32)
    db = make_db({})
    tracked = {"%s/%s" % (ATTEMPTS_DIRNAME, "1" * 32): NOW - 1}

    deleted = gc_attempt_directories(tmp_path, db, tracked, days_to_keep=7, now=NOW)

    assert deleted == ["1" * 32]
    assert not ((tmp_path / ATTEMPTS_DIRNAME / ("1" * 32)).exists())
    assert tracked == {}


def test_published_results_and_other_roots_never_touched(tmp_path):
    # a real published result file at PATH_DATA level
    published = tmp_path / "query-public1234567890123456789012345678.csv"
    published.write_text("keep me")

    rows = {"2" * 32: {"status": "failed", "timestamp_finished": NOW - GRACE * 2}}
    make_attempt_dir(tmp_path, "2" * 32)
    db = make_db(rows)

    gc_attempt_directories(tmp_path, db, {}, days_to_keep=7, now=NOW)

    assert published.read_text() == "keep me"
    assert not ((tmp_path / ATTEMPTS_DIRNAME / ("2" * 32)).exists())
