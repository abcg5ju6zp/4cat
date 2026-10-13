"""
Validation gate for staged processor results

Candidates are written into an attempt's isolated area while a processor
runs. Before they are atomically published they must be proven complete:
the file format must be intact, the number of entries must match the row
count the processor declared, and the manifest's referenced resources must
all be present. Nothing is moved to a published path until this passes.
"""
import csv
import hashlib
import json
import os
import zipfile

from pathlib import Path

from common.lib.helpers import NullAwareTextIOWrapper


class ResultValidationError(Exception):
	"""
	Raised when a staged candidate fails its pre-publication validation
	"""
	pass


def hash_file(path, algorithm="sha256", block_size=1024 * 1024):
	"""
	Hash a file without loading it into memory

	:param Path path:  File to hash
	:return str:  Hex digest
	"""
	digest = hashlib.new(algorithm)
	with path.open("rb") as infile:
		while True:
			block = infile.read(block_size)
			if not block:
				break
			digest.update(block)
	return digest.hexdigest()


def count_csv_entries(path):
	"""
	Count data rows and validate the structure of a staged CSV

	:raises ResultValidationError:  Unreadable header, ragged rows (extra
	values beyond the declared columns), or undecodable content
	:return int:  Number of data rows
	"""
	with path.open("rb") as raw:
		wrapped = NullAwareTextIOWrapper(raw, encoding="utf-8")
		try:
			reader = csv.DictReader(wrapped)
			if not reader.fieldnames:
				raise ResultValidationError("CSV candidate %s has no header row" % path.name)

			count = 0
			for row in reader:
				if None in row:
					extra = len(row[None])
					raise ResultValidationError(
						"CSV candidate %s line %i has %i more value(s) than its header"
						% (path.name, reader.line_num, extra)
					)
				count += 1
		except csv.Error as e:
			raise ResultValidationError("CSV candidate %s cannot be parsed: %s" % (path.name, e)) from None

	return count


def count_ndjson_entries(path):
	"""
	Count lines and validate every line of a staged NDJSON file

	:raises ResultValidationError:  A line is not valid JSON
	:return int:  Number of JSON lines
	"""
	count = 0
	with path.open("r", encoding="utf-8") as infile:
		for line_number, line in enumerate(infile, start=1):
			if not line.strip():
				continue
			try:
				json.loads(line)
			except json.JSONDecodeError as e:
				raise ResultValidationError(
					"NDJSON candidate %s line %i is not valid JSON: %s" % (path.name, line_number, e)
				) from None
			count += 1
	return count


def inspect_zip(path):
	"""
	Validate a staged archive and enumerate its members

	:raises ResultValidationError:  Not a zip, corrupt member, or unreadable
	:return list[str]:  Member file names (directory entries excluded)
	"""
	if not zipfile.is_zipfile(path):
		raise ResultValidationError("Archive candidate %s is not a valid zip file" % path.name)

	with zipfile.ZipFile(path, "r") as archive:
		bad = archive.testzip()
		if bad is not None:
			raise ResultValidationError("Archive candidate %s is corrupt at member %s" % (path.name, bad))
		try:
			members = archive.namelist()
		except Exception as e:
			raise ResultValidationError("Archive candidate %s cannot list members: %s" % (path.name, e))

	return [name for name in members if not name.endswith("/")]


def validate_candidate(path, num_rows, count_archive_entries=False):
	"""
	Validate a single staged result file

	:param Path path:  Candidate result file in the attempt area
	:param int num_rows:  Row/item count the processor declared
	:param bool count_archive_entries:  For zip archives produced by the
	framework helpers, each member corresponds to one item; custom archives
	(e.g. frame folders per video) only get integrity validation.
	:return int:  Observed entry count (member count when applicable)
	:raises ResultValidationError:  Any structural or count mismatch
	"""
	if not path.exists():
		raise ResultValidationError("Candidate result file %s does not exist" % path.name)
	if not path.is_file():
		raise ResultValidationError("Candidate result path %s is not a file" % path.name)
	if path.stat().st_size == 0:
		raise ResultValidationError("Candidate result file %s is empty" % path.name)

	suffix = path.suffix.lower()
	if suffix == ".csv":
		observed = count_csv_entries(path)
	elif suffix == ".ndjson":
		observed = count_ndjson_entries(path)
	elif suffix == ".zip":
		members = inspect_zip(path)
		observed = len(members)
		if not count_archive_entries:
			# integrity proven; member/item count semantics are processor
			# specific, so do not compare against num_rows
			return observed
	else:
		# other formats (html, svg, png, gexf, ...): integrity is existence
		# and non-emptiness; structure checking is format-specific and left
		# to the producing processor
		return 1

	if observed != num_rows:
		raise ResultValidationError(
			"Candidate %s declares %i rows but contains %i validated entries"
			% (path.name, num_rows, observed)
		)
	return observed


def build_candidate_manifest_entry(path, attempt_root, num_rows=None, count_archive_entries=False):
	"""
	Validate a candidate and describe it for the attempt manifest

	:param Path path:  Candidate file
	:param Path attempt_root:  Attempt root for storing relative paths
	:param int num_rows:  Declared rows (None skips count comparison)
	:param bool count_archive_entries:  See validate_candidate
	:return dict:  Manifest entry with relative path, size, sha256 and entries
	"""
	entries = validate_candidate(path, num_rows=num_rows, count_archive_entries=count_archive_entries) \
		if num_rows is not None else 1
	return {
		"path": str(Path(path).resolve().relative_to(attempt_root.resolve())),
		"size": path.stat().st_size,
		"sha256": hash_file(path),
		"entries": entries,
		"declared_rows": num_rows,
	}


def describe_files_folder(folder, attempt_root):
	"""
	List every file in a candidate's `folder_<key>` resources directory

	:return dict|None:  {"path": rel, "files": [rel paths]} or None when the
	directory does not exist (no attached resources for this dataset)
	"""
	if not folder.exists():
		return None

	files = []
	for path in sorted(folder.rglob("*")):
		if path.is_file():
			files.append(str(path.resolve().relative_to(attempt_root.resolve())))
	return {"path": str(folder.resolve().relative_to(attempt_root.resolve())), "files": files}


def verify_manifest_files(manifest, attempt_root):
	"""
	Confirm every file recorded in a manifest still exists with matching size

	Used during reconciliation before the database publication phase.

	:raises ResultValidationError:  A referenced file is missing or changed
	"""
	for entry in manifest.get("results", []):
		path = attempt_root.joinpath(entry["path"])
		if not path.is_file():
			raise ResultValidationError("Manifest result %s is missing" % entry["path"])
		if path.stat().st_size != entry["size"]:
			raise ResultValidationError("Manifest result %s changed size" % entry["path"])

	for folder in manifest.get("folders", []):
		if folder is None:
			continue
		for relative in folder["files"]:
			if not attempt_root.joinpath(relative).is_file():
				raise ResultValidationError("Manifest resource %s is missing" % relative)


def fsync_path(path):
	"""
	Flush a written file and its directory so a rename/replace is durable
	"""
	for target in (path, path.parent):
		descriptor = os.open(target, os.O_RDONLY)
		try:
			os.fsync(descriptor)
		finally:
			os.close(descriptor)
