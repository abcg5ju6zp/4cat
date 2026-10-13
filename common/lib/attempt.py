"""
Per-claim attempt identity for jobs

Every time a job is claimed, an Attempt row is created with a globally unique
attempt ID. Processors use the attempt to stage their results in an isolated
directory, validate them and publish them atomically. All terminal state
writes are conditioned on the attempt still being the current owner of the
job, which arbitrates cancellation, worker restarts, duplicate claims and
late completions.

Attempt state lives in the `job_attempts` table; candidate files live under
`PATH_DATA/.attempts/<attempt_id>/`.
"""
import enum
import json
import os
import shutil
import socket
import threading
import time
import uuid

from pathlib import Path

#: Root directory (relative to PATH_DATA) holding all attempt areas
ATTEMPTS_DIRNAME = ".attempts"


class AttemptStatus(enum.Enum):
	"""
	Lifecycle status of an attempt
	"""
	#: Worker is (or was, possibly crashed) running the job
	RUNNING = "running"
	#: Candidate validated, publication in progress
	PUBLISHING = "publishing"
	#: Publication committed; the attempt completed successfully
	COMMITTED = "committed"
	#: Processing or validation failed; candidate is unpublished
	FAILED = "failed"
	#: The job was cancelled
	CANCELLED = "cancelled"
	#: A newer attempt superseded this one before it published
	SUPERSEDED = "superseded"


class AttemptPhase(enum.Enum):
	"""
	Durable processing phases, ordered. Each transition is persisted so a
	reclaimed job can be reconciled and resumed at the first missing phase.
	"""
	CLAIMED = "claimed"
	PROCESSING = "processing"
	VALIDATED = "validated"
	FILES_PUBLISHED = "files_published"
	DB_COMMITTED = "db_committed"
	JOB_FINISHED = "job_finished"


#: Terminal phases after which processing must never be re-executed
PHASE_ORDER = [p.value for p in AttemptPhase]


def phase_at_least(phase, minimum):
	"""Whether a phase value is at least as far as `minimum`"""
	return PHASE_ORDER.index(phase) >= PHASE_ORDER.index(
		minimum.value if isinstance(minimum, AttemptPhase) else minimum
	)

#: Statuses that represent a finished attempt
TERMINAL_STATUSES = {
	AttemptStatus.COMMITTED.value,
	AttemptStatus.FAILED.value,
	AttemptStatus.CANCELLED.value,
	AttemptStatus.SUPERSEDED.value,
}


def current_owner_identifier():
	"""
	Identify the process/thread owning an attempt

	Used for diagnostics and liveness heuristics; correctness never depends on
	this value alone (restart reconciliation is driven by job re-claim).

	:return str: host:pid:thread identifier
	"""
	return "%s:%s:%s" % (socket.gethostname(), os.getpid(), threading.get_ident())


def attempt_status_payload(database, remote_id):
	"""
	Build the queryable attempt status object for a dataset/job remote ID

	Used by the status APIs so users can see running/publishing/failed
	attempts, whether a crash can resume past processing, and whether a job
	is parked waiting for a restart.

	:return dict|None:  None when no attempt exists for the remote ID
	"""
	row = database.fetchone(
		"SELECT * FROM job_attempts WHERE remote_id = %s ORDER BY timestamp_created DESC LIMIT 1",
		(str(remote_id),),
	)
	if not row:
		return None

	job = database.fetchone(
		"SELECT timestamp_claimed FROM jobs WHERE jobtype = %s AND remote_id = %s",
		(row["jobtype"], str(remote_id)),
	)
	parked = bool(job and job["timestamp_claimed"] == -1)
	terminal = row["status"] in TERMINAL_STATUSES
	recoverable = (not terminal and phase_at_least(row["phase"], AttemptPhase.VALIDATED)) \
		or (parked and row["status"] == AttemptStatus.FAILED.value)

	return {
		"attempt_id": row["attempt_id"],
		"status": row["status"],
		"phase": row["phase"],
		"error": row.get("error", ""),
		"recoverable": recoverable,
		"parked": parked,
		"heartbeat_at": row.get("heartbeat_at"),
		"updated_at": row.get("timestamp_updated"),
		"finished_at": row.get("timestamp_finished"),
	}


def describe_input_dataset(dataset):
	"""
	Build a version signature for an input dataset

	The signature captures everything that could change a processor's input:
	completion state, row count, finished timestamp, reserved result file
	name, and size/mtime of the published result file. Input datasets are
	never bound to the running attempt, so their result path is the published
	one.

	:param DataSet dataset:  Input/source dataset
	:return dict:  JSON-serialisable signature
	"""
	path = dataset.get_results_path()
	try:
		stat = path.stat()
		size, mtime = stat.st_size, int(stat.st_mtime)
	except OSError:
		size, mtime = None, None

	return {
		"key": dataset.key,
		"is_finished": bool(dataset.data.get("is_finished", False)),
		"num_rows": dataset.data.get("num_rows", 0),
		"timestamp_finished": dataset.data.get("timestamp_finished"),
		"result_file": dataset.data.get("result_file", ""),
		"size": size,
		"mtime": mtime,
	}


def build_input_signature(datasets, parameters=None):
	"""
	Assemble the full input signature for an attempt

	:param Iterable[DataSet] datasets:  Input datasets (de-duplicated by key)
	:param parameters:  Processor parameters snapshot
	:return dict:  {"datasets": {key: signature}, "parameters": snapshot}
	"""
	signatures = {}
	for dataset in datasets or []:
		if dataset is None or dataset.key in signatures:
			continue
		signatures[dataset.key] = describe_input_dataset(dataset)

	return {"datasets": signatures, "parameters": parameters}


class Attempt:
	"""
	A single claim/processing attempt of a job

	Do not instantiate directly; use `Attempt.create()` or one of the
	`Attempt.get_*()` constructors.
	"""

	#: Database row for this attempt (column -> value)
	data = None

	db = None
	modules = None

	#: Lazily initialised root of the isolated attempt area
	_root = None

	def __init__(self, data, database=None, modules=None):
		self.data = data
		self.db = database
		self.modules = modules

	@property
	def id(self):
		return self.data["attempt_id"]

	@property
	def jobtype(self):
		return self.data["jobtype"]

	@property
	def remote_id(self):
		return str(self.data["remote_id"])

	@property
	def status(self):
		return self.data["status"]

	@property
	def phase(self):
		return self.data["phase"]

	@property
	def is_terminal(self):
		return self.data["status"] in TERMINAL_STATUSES

	@property
	def is_committed(self):
		return self.data["status"] == AttemptStatus.COMMITTED.value

	@property
	def can_resume_without_reprocessing(self):
		"""
		Whether the attempt reached a checkpoint from which publication can be
		resumed without re-running the processor's external fetch/processing
		"""
		return self.data["phase"] in (
			AttemptPhase.VALIDATED.value,
			AttemptPhase.FILES_PUBLISHED.value,
			AttemptPhase.DB_COMMITTED.value,
			AttemptPhase.JOB_FINISHED.value,
		)

	@classmethod
	def create(cls, jobtype, remote_id, database, modules=None, owner=None):
		"""
		Persist a new attempt for a freshly claimed job

		The corresponding jobs row is stamped separately and atomically with
		the claim UPDATE (see Job.claim()); if the claim is lost the caller
		should remove the never-used row with `delete_record()`.

		:param str jobtype:  Job type
		:param str remote_id:  Job remote ID (e.g. dataset key)
		:param database:  Database handler
		:param modules:  Module catalog (used to resolve PATH_DATA)
		:param str owner:  Owner identifier; generated if omitted
		:return Attempt:
		"""
		now = int(time.time())
		data = {
			"attempt_id": uuid.uuid4().hex,
			"jobtype": jobtype,
			"remote_id": str(remote_id),
			"status": AttemptStatus.RUNNING.value,
			"phase": AttemptPhase.CLAIMED.value,
			"input_signature": "{}",
			"manifest": "{}",
			"pending_children": "[]",
			"error": "",
			"owner": owner or current_owner_identifier(),
			"heartbeat_at": now,
			"timestamp_created": now,
			"timestamp_updated": now,
			"timestamp_finished": None,
		}
		database.insert("job_attempts", data)
		return cls(data, database=database, modules=modules)

	@classmethod
	def get(cls, attempt_id, database, modules=None):
		"""
		Load an attempt by its ID

		:return Attempt|None:
		"""
		row = database.fetchone("SELECT * FROM job_attempts WHERE attempt_id = %s", (attempt_id,))
		return cls(row, database=database, modules=modules) if row else None

	@classmethod
	def get_latest(cls, jobtype, remote_id, database, modules=None):
		"""
		Get the most recent attempt for a job, regardless of status

		:return Attempt|None:
		"""
		row = database.fetchone(
			"SELECT * FROM job_attempts WHERE jobtype = %s AND remote_id = %s "
			"ORDER BY timestamp_created DESC LIMIT 1",
			(jobtype, str(remote_id)),
		)
		return cls(row, database=database, modules=modules) if row else None

	@classmethod
	def get_prior_open(cls, jobtype, remote_id, exclude_attempt_id, database, modules=None):
		"""
		Get the most recent non-terminal attempt for a job other than the
		current one (used for reconciliation after a restart/duplicate claim)

		:return Attempt|None:
		"""
		row = database.fetchone(
			"SELECT * FROM job_attempts WHERE jobtype = %s AND remote_id = %s "
			"AND status NOT IN %s AND attempt_id <> %s "
			"ORDER BY timestamp_created DESC LIMIT 1",
			(jobtype, str(remote_id), tuple(TERMINAL_STATUSES), exclude_attempt_id),
		)
		return cls(row, database=database, modules=modules) if row else None

	@classmethod
	def get_open(cls, jobtype, remote_id, database, modules=None):
		"""
		Get the most recent non-terminal attempt for a job, if any

		Reconciliation uses this after a restart/duplicate claim: an open
		attempt means the previous owner never reached a terminal state.

		:return Attempt|None:
		"""
		row = database.fetchone(
			"SELECT * FROM job_attempts WHERE jobtype = %s AND remote_id = %s AND status NOT IN %s "
			"ORDER BY timestamp_created DESC LIMIT 1",
			(jobtype, str(remote_id), tuple(TERMINAL_STATUSES)),
		)
		return cls(row, database=database, modules=modules) if row else None

	def transition(self, phase=None, status=None, error=None, manifest=None,
				   input_signature=None, pending_children=None, heartbeat=False, commit=True):
		"""
		Advance attempt state and persist it

		Phase may only move forward. `manifest`, `input_signature` and
		`pending_children` accept Python objects and are stored as JSON.

		:param AttemptPhase phase:  New phase (must be later than current)
		:param AttemptStatus status:  New status
		:param str error:  Error message
		:param manifest:  Manifest object to JSON-encode
		:param input_signature:  Input signature object to JSON-encode
		:param pending_children:  Pending child job list to JSON-encode
		:param bool heartbeat:  Also refresh the heartbeat timestamp
		:param bool commit:  Whether to commit the UPDATE (set False inside a
		  publication transaction)
		:return bool:  Whether a row was updated (0 means the attempt row is
		  gone, which should not normally happen)
		"""
		update = {"timestamp_updated": int(time.time())}
		if heartbeat:
			update["heartbeat_at"] = int(time.time())

		if phase is not None:
			new_phase = phase.value if isinstance(phase, AttemptPhase) else phase
			if PHASE_ORDER.index(new_phase) < PHASE_ORDER.index(self.data["phase"]):
				raise ValueError("Cannot move attempt %s backwards from %s to %s" % (
					self.id, self.data["phase"], new_phase))
			update["phase"] = new_phase
			self.data["phase"] = new_phase

		if status is not None:
			new_status = status.value if isinstance(status, AttemptStatus) else status
			update["status"] = new_status
			self.data["status"] = new_status
			if new_status in TERMINAL_STATUSES:
				update["timestamp_finished"] = int(time.time())

		if error is not None:
			update["error"] = error
			self.data["error"] = error

		if manifest is not None:
			encoded = json.dumps(manifest, sort_keys=True)
			update["manifest"] = encoded
			self.data["manifest"] = encoded
			self._write_manifest_file(manifest)

		if input_signature is not None:
			encoded = json.dumps(input_signature, sort_keys=True)
			update["input_signature"] = encoded
			self.data["input_signature"] = encoded

		if pending_children is not None:
			encoded = json.dumps(pending_children, sort_keys=True)
			update["pending_children"] = encoded
			self.data["pending_children"] = encoded

		updated = self.db.update("job_attempts", data=update, where={"attempt_id": self.id}, commit=commit)
		return (updated or 0) > 0

	def touch_heartbeat(self, commit=True):
		"""Refresh the owner heartbeat"""
		return self.transition(heartbeat=True, commit=commit)

	def finalize(self, status, error=None, commit=True):
		"""
		Move the attempt to a terminal status

		The UPDATE is conditional on the row not already being terminal, so a
		late worker cannot flip a committed attempt to failed/cancelled. A
		return value of False means the arbitration was lost and the caller
		must not perform any follow-up side effects.

		:param AttemptStatus status:  Terminal status
		:return bool:  Whether this call moved the row to the terminal status
		"""
		if status not in (AttemptStatus.COMMITTED, AttemptStatus.FAILED,
						  AttemptStatus.CANCELLED, AttemptStatus.SUPERSEDED):
			raise ValueError("Cannot finalise attempt with non-terminal status %s" % status)

		new_status = status.value
		if self.data["status"] in TERMINAL_STATUSES:
			# already final: never allow flipping
			return self.data["status"] == new_status

		now = int(time.time())
		affected = self.db.execute(
			"UPDATE job_attempts SET status = %s, timestamp_updated = %s, timestamp_finished = %s, "
			"error = COALESCE(%s, error) WHERE attempt_id = %s AND status NOT IN %s",
			(new_status, now, now, error, self.id, tuple(TERMINAL_STATUSES)),
			commit=commit,
		)
		if not affected:
			# someone else finalised first; refresh local state
			row = self.db.fetchone("SELECT status FROM job_attempts WHERE attempt_id = %s", (self.id,))
			if row:
				self.data["status"] = row["status"]
			return False

		self.data["status"] = new_status
		self.data["timestamp_finished"] = now
		self.data["timestamp_updated"] = now
		if error is not None:
			self.data["error"] = error
		return True

	# ------------------------------------------------------------------
	# Serialised state
	# ------------------------------------------------------------------

	def get_input_signature(self):
		try:
			return json.loads(self.data["input_signature"])
		except (TypeError, json.JSONDecodeError):
			return {}

	def get_manifest(self):
		try:
			return json.loads(self.data["manifest"])
		except (TypeError, json.JSONDecodeError):
			return {}

	def get_pending_children(self):
		try:
			return json.loads(self.data["pending_children"])
		except (TypeError, json.JSONDecodeError):
			return []

	def set_inputs(self, datasets, parameters=None, commit=True):
		"""
		Record the version signature of the datasets this attempt reads

		:return dict:  The stored signature
		"""
		signature = build_input_signature(datasets, parameters=parameters)
		self.transition(input_signature=signature, commit=commit)
		return signature

	def verify_inputs(self, datasets, parameters=None):
		"""
		Check the inputs have not changed since the attempt started

		:return tuple:  (ok, changed) where changed lists human-readable
		differences (e.g. "parent.num_rows: 10 -> 8")
		"""
		current = build_input_signature(datasets, parameters=parameters)
		original = self.get_input_signature()
		changed = []

		original_datasets = original.get("datasets", {})
		if set(original_datasets.keys()) != set(current["datasets"].keys()):
			changed.append("input dataset set changed: %s -> %s" % (
				sorted(original_datasets.keys()), sorted(current["datasets"].keys())))

		for key, then in original_datasets.items():
			now = current["datasets"].get(key)
			if now is None:
				continue
			for field in ("is_finished", "num_rows", "timestamp_finished", "result_file", "size", "mtime"):
				if then.get(field) != now.get(field):
					changed.append("%s.%s: %r -> %r" % (key[:8], field, then.get(field), now.get(field)))

		if "parameters" in original and original.get("parameters") != current.get("parameters"):
			changed.append("parameters changed during the attempt")

		return (not changed), changed

	def record_quarantine(self, dataset_key, items):
		"""
		Record files/folders moved off the published paths when the attempt
		began (stale half-results). Quarantined items are never published.
		"""
		manifest = self.get_manifest()
		quarantined = manifest.setdefault("quarantined", {})
		quarantined.setdefault(dataset_key, []).extend(items)
		self.transition(manifest=manifest)

	def record_pending_finish(self, dataset_key, num_rows, status_type, result_file=""):
		"""
		Record a dataset's intended terminal state (deferred DataSet.finish)
		"""
		manifest = self.get_manifest()
		manifest.setdefault("pending_finishes", {})[dataset_key] = {
			"num_rows": num_rows,
			"status_type": status_type,
			"result_file": result_file,
		}
		self.transition(manifest=manifest)

	def get_pending_finishes(self):
		return self.get_manifest().get("pending_finishes", {})

	def mark_result_consumed(self, dataset_key):
		"""Record that an owned dataset's candidate was intentionally removed"""
		manifest = self.get_manifest()
		consumed = set(manifest.get("consumed_results", []))
		consumed.add(dataset_key)
		manifest["consumed_results"] = sorted(consumed)
		self.transition(manifest=manifest)

	def mark_archive_entries(self, dataset_key):
		"""
		Mark that an owned dataset's zip candidate has one member per row
		(framework archive helpers), enabling entry-count validation
		"""
		manifest = self.get_manifest()
		keys = set(manifest.get("count_archive_entries", []))
		keys.add(dataset_key)
		manifest["count_archive_entries"] = sorted(keys)
		self.transition(manifest=manifest)

	def note_new_dataset(self, key):
		"""
		Record that this attempt created (inserted) a dataset row

		Such rows are fully owned by the attempt: they are discarded if the
		attempt fails/cancels. Rows merely bound (e.g. a preset's attach
		surrogate) pre-date the attempt and are only untagged, not deleted.
		"""
		manifest = self.get_manifest()
		keys = set(manifest.get("new_datasets", []))
		keys.add(key)
		manifest["new_datasets"] = sorted(keys)
		self.transition(manifest=manifest)

	def flush_pending_children(self, commit=True):
		"""
		Insert all buffered child jobs into the jobs table

		Called inside the publication transaction so child jobs become
		claimable at the same moment the producing dataset finishes. Duplicate
		(type, remote_id) rows are ignored.

		:return int:  Number of child job rows inserted
		"""
		children = self.get_pending_children()
		for child in children:
			self.db.insert("jobs", child, safe=True, constraints=("jobtype", "remote_id"), commit=False)

		if children:
			self.transition(pending_children=[], commit=commit)
		return len(children)

	def publish_dataset_tags(self, commit=True):
		"""
		Clear attempt ownership tags so all datasets of this attempt become
		visible (part of the publication transaction)
		"""
		return self.db.update("datasets", data={"attempt_id": ""}, where={"attempt_id": self.id}, commit=commit)

	def get_owned_dataset_keys(self):
		rows = self.db.fetchall("SELECT key FROM datasets WHERE attempt_id = %s", (self.id,))
		return [row["key"] for row in rows]

	def discard_owned_datasets(self):
		"""
		Remove unpublished datasets owned by a failed/cancelled attempt

		Dataset rows created by the attempt are fully deleted (with their
		candidates, owners and annotations); pre-existing rows that were only
		bound (e.g. attach surrogates) are untagged and left in place.

		:return list:  Keys that were fully deleted
		"""
		lazy_dataset = __import__("common.lib.dataset", fromlist=["DataSet"]).DataSet
		new_keys = set(self.get_manifest().get("new_datasets", []))
		deleted = []

		for key in self.get_owned_dataset_keys():
			if key in new_keys:
				try:
					with lazy_dataset.attempt_context(self):
						lazy_dataset(key=key, db=self.db, modules=self.modules).delete()
					deleted.append(key)
				except Exception:
					# never let cleanup itself crash the failure path
					continue
			else:
				self.db.update("datasets", data={"attempt_id": ""}, where={"key": key, "attempt_id": self.id})

		# nothing left buffered should survive
		self.transition(pending_children=[])
		return deleted

	def add_pending_child(self, job):
		"""
		Buffer a child job that should only become claimable at publication

		:param dict job:  Row-like dict with the jobs columns to insert later
		:return list:  Updated pending child list
		"""
		children = self.get_pending_children()
		children.append(job)
		self.transition(pending_children=children)
		return children

	# ------------------------------------------------------------------
	# Isolated file area
	# ------------------------------------------------------------------

	def get_path_data(self):
		if self.modules is None:
			raise RuntimeError("Attempt %s cannot resolve PATH_DATA without modules" % self.id)
		return Path(self.modules.config.get("PATH_DATA"))

	@property
	def root_dir(self):
		if self._root is None:
			self._root = self.get_path_data().joinpath(ATTEMPTS_DIRNAME, self.id)
		return self._root

	@property
	def result_dir(self):
		return self.root_dir.joinpath("result")

	@property
	def files_dir(self):
		return self.root_dir.joinpath("files")

	@property
	def staging_dir(self):
		return self.root_dir.joinpath("staging")

	@property
	def quarantine_dir(self):
		return self.root_dir.joinpath("quarantine")

	@property
	def manifest_path(self):
		return self.root_dir.joinpath("manifest.json")

	def ensure_dirs(self):
		"""Create the isolated area layout. Idempotent."""
		for directory in (self.result_dir, self.files_dir, self.staging_dir, self.quarantine_dir):
			directory.mkdir(parents=True, exist_ok=True)
		return self.root_dir

	def candidate_result_path(self, logical_result_path):
		"""
		Candidate location for a dataset's logical result file

		:param Path logical_result_path:  The dataset's published result path
		:return Path:  Path inside the attempt's result directory
		"""
		return self.result_dir.joinpath(Path(logical_result_path).name)

	def candidate_files_path(self, dataset_key):
		"""
		Candidate location for a dataset's `folder_<key>` resources directory
		"""
		return self.files_dir.joinpath("folder_" + dataset_key)

	def candidate_staging_path(self, name):
		"""
		Isolated staging area with a unique name (mirrors DataSet's legacy
		`<result>-staging[-N]` naming inside the attempt's staging directory)
		"""
		base = self.staging_dir.joinpath(name)
		path = base
		index = 1
		while True:
			try:
				path.mkdir(parents=True, exist_ok=False)
				break
			except OSError as e:
				if getattr(e, "errno", None) is not None and e.errno == 17:
					index += 1
					path = self.staging_dir.joinpath("%s-%s" % (name, index))
					continue
				raise
		return path

	def _write_manifest_file(self, manifest):
		try:
			self.root_dir.mkdir(parents=True, exist_ok=True)
			tmp = self.manifest_path.with_suffix(".json.tmp")
			with tmp.open("w", encoding="utf-8") as out:
				json.dump(manifest, out, indent=2, sort_keys=True)
			os.replace(tmp, self.manifest_path)
		except (OSError, ValueError):
			# manifest file is a convenience/debug copy; the DB row is
			# authoritative, so never fail an attempt over this
			pass

	def load_manifest_file(self):
		"""
		Load the on-disk manifest (used by reconciliation/GC); fall back to
		the DB copy.
		"""
		try:
			return json.loads(self.manifest_path.read_text(encoding="utf-8"))
		except (OSError, ValueError):
			return self.get_manifest()

	def dispose_dir(self):
		"""
		Remove the whole isolated area (used by safe GC only)
		"""
		shutil.rmtree(self.root_dir, ignore_errors=True)

	def delete_record(self):
		"""
		Delete a never-current attempt row

		Only valid for an attempt whose claim was lost before it stamped a
		jobs row; finished attempts are kept as history.
		"""
		self.db.delete("job_attempts", where={"attempt_id": self.id})
