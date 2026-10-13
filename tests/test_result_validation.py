"""
Tests for the pre-publication validation gate (common/lib/result_validation.py).
"""
import json
import zipfile

import pytest

from common.lib.result_validation import (
    ResultValidationError,
    validate_candidate,
    build_candidate_manifest_entry,
    describe_files_folder,
    verify_manifest_files,
    hash_file,
)


def write_csv(path, rows, header=("id", "body")):
    lines = [",".join(header)]
    lines.extend(",".join(str(v) for v in row) for row in rows)
    path.write_text("\n".join(lines) + "\n")


def test_valid_csv_rows_counted(tmp_path):
    path = tmp_path / "r.csv"
    write_csv(path, [(1, "a"), (2, "b"), (3, "c")])
    assert validate_candidate(path, num_rows=3) == 3


def test_csv_row_count_mismatch_rejected(tmp_path):
    path = tmp_path / "r.csv"
    write_csv(path, [(1, "a"), (2, "b")])
    with pytest.raises(ResultValidationError, match="declares 3 rows"):
        validate_candidate(path, num_rows=3)


def test_csv_ragged_row_rejected(tmp_path):
    path = tmp_path / "r.csv"
    path.write_text("id,body\n1,a,extra\n")
    with pytest.raises(ResultValidationError, match="more value"):
        validate_candidate(path, num_rows=1)


def test_csv_truncated_header_rejected(tmp_path):
    path = tmp_path / "r.csv"
    path.write_bytes(b"\n")
    with pytest.raises(ResultValidationError, match="no header row|empty"):
        validate_candidate(path, num_rows=0)


def test_valid_ndjson_lines_counted(tmp_path):
    path = tmp_path / "r.ndjson"
    path.write_text(json.dumps({"id": 1}) + "\n" + json.dumps({"id": 2}) + "\n")
    assert validate_candidate(path, num_rows=2) == 2


def test_bad_ndjson_line_rejected(tmp_path):
    path = tmp_path / "r.ndjson"
    path.write_text('{"id": 1}\n{not json}\n')
    with pytest.raises(ResultValidationError, match="not valid JSON"):
        validate_candidate(path, num_rows=2)


def test_valid_zip_integrity_and_entries(tmp_path):
    path = tmp_path / "r.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("a.txt", "aaa")
        archive.writestr("b.txt", "bbb")
    assert validate_candidate(path, num_rows=2, count_archive_entries=True) == 2


def test_zip_count_mismatch_only_when_entries_semantics_enabled(tmp_path):
    # custom producer: one video, many frame files (see video_frames.py)
    path = tmp_path / "r.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("vid1/frame-001.jpg", "x")
        archive.writestr("vid1/frame-002.jpg", "y")
    assert validate_candidate(path, num_rows=1, count_archive_entries=False) == 2
    with pytest.raises(ResultValidationError, match="declares 1 rows"):
        validate_candidate(path, num_rows=1, count_archive_entries=True)


def test_corrupt_zip_rejected(tmp_path):
    path = tmp_path / "r.zip"
    path.write_bytes(b"not really a zip")
    with pytest.raises(ResultValidationError, match="not a valid zip"):
        validate_candidate(path, num_rows=1)


def test_missing_and_empty_candidate_rejected(tmp_path):
    missing = tmp_path / "nope.csv"
    with pytest.raises(ResultValidationError, match="does not exist"):
        validate_candidate(missing, num_rows=1)

    empty = tmp_path / "empty.csv"
    empty.write_text("")
    with pytest.raises(ResultValidationError):
        validate_candidate(empty, num_rows=0)


def test_other_formats_pass_with_structure_check(tmp_path):
    path = tmp_path / "report.html"
    path.write_text("<html><body>ok</body></html>")
    assert validate_candidate(path, num_rows=1) == 1


def test_manifest_entry_and_verification(tmp_path):
    root = tmp_path / "attempt"
    result_dir = root / "result"
    result_dir.mkdir(parents=True)
    candidate = result_dir / "r.csv"
    write_csv(candidate, [(1, "a"), (2, "b")])

    entry = build_candidate_manifest_entry(candidate, root, num_rows=2)
    assert entry["path"] == "result/r.csv"
    assert entry["size"] == candidate.stat().st_size
    assert entry["sha256"] == hash_file(candidate)
    assert entry["entries"] == 2

    files_dir = root / "files" / "folder_dk"
    files_dir.mkdir(parents=True)
    (files_dir / "asset.bin").write_text("blob")
    folder = describe_files_folder(files_dir, root)
    assert folder["files"] == ["files/folder_dk/asset.bin"]

    # intact manifest verifies; tampered size is detected
    verify_manifest_files({"results": [entry], "folders": [folder]}, root)
    entry["size"] += 1
    with pytest.raises(ResultValidationError, match="changed size"):
        verify_manifest_files({"results": [entry], "folders": []}, root)

    # missing referenced resource detected
    (files_dir / "asset.bin").unlink()
    with pytest.raises(ResultValidationError, match="resource"):
        verify_manifest_files({"results": [], "folders": [folder]}, root)
