"""
Tests for attempt-aware DataSet path redirection and stale-result quarantine.

Datasets are built from raw rows with check_owners=False so no real database
or module collector is needed; PATH_DATA points at a tmp directory.
"""
import types
from unittest.mock import MagicMock

import pytest

from common.lib.attempt import Attempt, ATTEMPTS_DIRNAME
from common.lib.dataset import DataSet


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


def make_dataset(tmp_path, modules, db, key="datasetkey", result_file="query-datasetkey.csv",
                 is_finished=False, attempt_id=""):
    row = {
        "key": key,
        "query": "query",
        "parameters": "{}",
        "key_parent": "",
        "result_file": result_file,
        "is_finished": is_finished,
        "num_rows": 0,
        "status": "",
        "status_type": "",
        "timestamp_finished": None,
        "attempt_id": attempt_id,
    }
    return DataSet(data=row, db=db, modules=modules, check_owners=False)


def test_unbound_paths_are_published_paths(tmp_path, modules, db):
    ds = make_dataset(tmp_path, modules, db)
    assert ds.get_results_path() == tmp_path / "query-datasetkey.csv"
    assert ds.get_results_folder_path() == tmp_path / "folder_datasetkey"
    assert ds.get_log_path() == tmp_path / "query-datasetkey.log"
    assert ds._bound_attempt() is None


def test_bound_paths_redirect_into_attempt_area(tmp_path, modules, db):
    attempt = Attempt.create("t", "datasetkey", db, modules=modules)
    ds = make_dataset(tmp_path, modules, db)
    ds.bind_attempt(attempt)

    root = tmp_path / ATTEMPTS_DIRNAME / attempt.id
    assert ds.get_results_path() == root / "result" / "query-datasetkey.csv"
    assert ds.get_results_folder_path() == root / "files" / "folder_datasetkey"
    # log stays at the published location
    assert ds.get_log_path() == tmp_path / "query-datasetkey.log"

    # ownership stamped on the row
    assert ds.data["attempt_id"] == attempt.id
    db.update.assert_any_call("datasets", data={"attempt_id": attempt.id}, where={"key": ds.key})


def test_bound_staging_area_isolated_and_disposable(tmp_path, modules, db):
    attempt = Attempt.create("t", "datasetkey", db, modules=modules)
    ds = make_dataset(tmp_path, modules, db)
    ds.bind_attempt(attempt)

    staging = ds.get_staging_area()
    root = tmp_path / ATTEMPTS_DIRNAME / attempt.id
    assert staging.parent == root / "staging"
    assert staging.is_dir()
    assert staging in ds.disposable_files

    (staging / "scratch.txt").write_text("x")
    ds.remove_disposable_files()
    assert not staging.exists()
    # candidate/published files are not touched by disposable cleanup
    assert root.exists() and tmp_path.exists()


def test_bind_quarantines_stale_half_results(tmp_path, modules, db):
    # legacy half-written result + attached folder at the published paths
    stale = tmp_path / "query-datasetkey.csv"
    stale.write_text("id,body\n1,partial")
    stale_folder = tmp_path / "folder_datasetkey"
    stale_folder.mkdir()
    (stale_folder / "asset.bin").write_text("blob")

    attempt = Attempt.create("t", "datasetkey", db, modules=modules)
    ds = make_dataset(tmp_path, modules, db)
    ds.bind_attempt(attempt)

    # published paths expose nothing until publication
    assert not stale.exists()
    assert not stale_folder.exists()

    quarantined = attempt.get_manifest()["quarantined"]["datasetkey"]
    kinds = {item["kind"] for item in quarantined}
    assert kinds == {"result", "folder"}

    q_root = tmp_path / ATTEMPTS_DIRNAME / attempt.id / "quarantine"
    assert (q_root / "query-datasetkey.csv").read_text() == "id,body\n1,partial"
    assert (q_root / "folder_datasetkey" / "asset.bin").read_text() == "blob"

    # the quarantine copy must not be served as a candidate
    assert ds.get_results_path() == tmp_path / ATTEMPTS_DIRNAME / attempt.id / "result" / stale.name


def test_bind_finished_dataset_does_not_touch_result(tmp_path, modules, db):
    published = tmp_path / "query-datasetkey.csv"
    published.write_text("id,body\n1,ok\n")
    ds = make_dataset(tmp_path, modules, db, is_finished=True)
    attempt = Attempt.create("t", "datasetkey", db, modules=modules)
    ds.bind_attempt(attempt)
    assert published.exists()
    # finished rows are never claimed into an attempt or quarantined
    assert ds._bound_attempt() is None
    assert ds.data["attempt_id"] == ""
    assert attempt.get_manifest() == {}


def test_context_redirects_only_owned_loaded_rows(tmp_path, modules, db):
    attempt = Attempt.create("t", "datasetkey", db, modules=modules)
    owned = make_dataset(tmp_path, modules, db, key="ownedkey", result_file="query-ownedkey.csv",
                         attempt_id=attempt.id)
    foreign = make_dataset(tmp_path, modules, db, key="foreignkey", result_file="query-foreignkey.csv",
                           attempt_id="some-other-attempt")

    with DataSet.attempt_context(attempt):
        assert owned._bound_attempt() is attempt
        assert owned.get_results_path() == (
            tmp_path / ATTEMPTS_DIRNAME / attempt.id / "result" / "query-ownedkey.csv"
        )
        assert foreign._bound_attempt() is None
        assert foreign.get_results_path() == tmp_path / "query-foreignkey.csv"

    # after the context, rows are no longer redirected
    assert owned._bound_attempt() is None
    assert owned.get_results_path() == tmp_path / "query-ownedkey.csv"


def test_new_dataset_created_in_context_is_tagged_and_redirected(tmp_path, modules, db):
    attempt = Attempt.create("t", "parentkey", db, modules=modules)
    # fresh insert: fetchone returns None twice (key lookup), reserve loop etc.
    db.fetchone.side_effect = [None, None]

    with DataSet.attempt_context(attempt):
        created = DataSet(
            parameters={"hello": "world"},
            db=db,
            type="example-processor",
            parent="parentkey",
            extension="csv",
            modules=modules,
            check_owners=False,
        )

    dataset_inserts = [c for c in db.insert.call_args_list if c.args[0] == "datasets"]
    assert dataset_inserts, "expected a datasets row insert"
    inserted = dataset_inserts[-1].kwargs["data"]
    assert inserted["attempt_id"] == attempt.id
    assert created.data["attempt_id"] == attempt.id
    root = tmp_path / ATTEMPTS_DIRNAME / attempt.id
    assert created.get_results_path().parent == root / "result"
