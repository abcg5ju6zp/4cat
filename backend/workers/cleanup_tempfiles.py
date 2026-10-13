"""
Delete old datasets
"""
import shutil
import re
import json
import time
from datetime import datetime
from pathlib import Path

from backend.lib.worker import BasicWorker
from common.lib.attempt import ATTEMPTS_DIRNAME, AttemptStatus
from common.lib.dataset import DataSet
from common.lib.exceptions import WorkerInterruptedException, DataSetException

#: terminal attempt statuses whose isolated directories may be garbage
#: collected once they pass the grace period
GC_TERMINAL_STATUSES = (
    AttemptStatus.COMMITTED.value,
    AttemptStatus.FAILED.value,
    AttemptStatus.CANCELLED.value,
    AttemptStatus.SUPERSEDED.value,
)


def _attempt_is_referenced(db, attempt_id):
	"""
	Whether any dataset row or job row still carries an attempt tag
	"""
	datasets = db.fetchone("SELECT COUNT(*) AS num FROM datasets WHERE attempt_id = %s", (attempt_id,))
	jobs = db.fetchone("SELECT COUNT(*) AS num FROM jobs WHERE attempt_id = %s", (attempt_id,))
	return ((datasets["num"] if datasets else 0) + (jobs["num"] if jobs else 0)) > 0


def gc_attempt_directories(path_data, db, tracked_files, days_to_keep=7, log=None, now=None):
	"""
	Garbage collect isolated attempt areas

	Deletes only directories of terminal (committed/failed/cancelled/
	superseded) attempts whose finish time is older than the grace period and
	that are no longer referenced by any dataset or job. Orphan directories
	without an attempt row follow the same tracked-grace flow as legacy
	orphaned result files. Running/publishing attempts and anything outside
	the attempt area are never touched.

	:param Path path_data:  4CAT data path
	:param tracked_files:  Tracking dict (mutated in place)
	:param int days_to_keep:  Grace period
	:return list:  Names of deleted directories
	"""
	root = Path(path_data).joinpath(ATTEMPTS_DIRNAME)
	if not root.is_dir():
		return []

	now = now if now is not None else time.time()
	grace_seconds = days_to_keep * 86400
	deleted = []

	for directory in sorted(root.glob("*")):
		if not directory.is_dir() or not re.fullmatch(r"[0-9a-f]{32}", directory.name):
			continue

		tracking_key = "%s/%s" % (ATTEMPTS_DIRNAME, directory.name)
		row = db.fetchone(
			"SELECT status, timestamp_finished FROM job_attempts WHERE attempt_id = %s",
			(directory.name,),
		)

		may_delete = False
		if row is None:
			# orphan directory (row deleted or never created): track first
			if tracking_key not in tracked_files:
				tracked_files[tracking_key] = now + grace_seconds
				if log is not None:
					log.info("Unknown attempt directory %s, marking for deletion" % directory)
			elif tracked_files[tracking_key] < now:
				may_delete = True
		elif row["status"] in GC_TERMINAL_STATUSES:
			finished_at = row.get("timestamp_finished") or 0
			if finished_at + grace_seconds < now:
				if _attempt_is_referenced(db, directory.name):
					if log is not None:
						log.info("Keeping attempt directory %s: still referenced by datasets/jobs" % directory)
				else:
					may_delete = True
		# running/publishing attempts are never collected

		if may_delete:
			shutil.rmtree(directory, ignore_errors=True)
			tracked_files.pop(tracking_key, None)
			deleted.append(directory.name)
			if log is not None:
				log.info("Deleted expired attempt directory %s" % directory)

	return deleted


class TempFileCleaner(BasicWorker):
    """
    Clean up discarded temporary files

    If 4CAT crashes while processing something, it may result in staging
    folders that are never cleaned up. This worker checks for finished
    datasets with staging area folders and cleans them up.

    Also cleans up orphaned result files for datasets that no longer exist.
    """
    type = "clean-temp-files"
    max_workers = 1

    # Use tracking file to delay deletion of files that may still be in use
    days_to_keep = 7

    @classmethod
    def ensure_job(cls, config=None):
        """
        Ensure that the temp file cleaner is always running

        This is used to ensure that the temp file cleaner is always running, and
        if it is not, it will be started by the WorkerManager.

        :return:  Job parameters for the worker
        """
        return {"remote_id": "localhost", "interval": 10800}

    def work(self):
        """
        Go through result files, and for each one check if it should still
        exist
        :return:
        """
        # Load tracking file
        tracking_file = self.config.get('PATH_DATA').joinpath(".temp_file_cleaner")
        if not tracking_file.exists():
            tracked_files = {}
        else:
            tracked_files = json.loads(tracking_file.read_text())

        # Get 4CAT paths to avoid if they are mapped inside PATH_DATA
        fourcat_paths = [self.config.get(p) for p in self.config.get_all_setting_names() if p.startswith('PATH_')]

        result_files = Path(self.config.get('PATH_DATA')).glob("*")
        for file in result_files:
            if file.stem.startswith("."):
                # skip hidden files
                continue

            if self.interrupted:
                tracking_file.write_text(json.dumps(tracked_files))
                raise WorkerInterruptedException("Interrupted while cleaning up orphaned result files")
            
            # Check if the file is inside any of the 4CAT paths
            if any(file == fourcat_path or file.is_relative_to(fourcat_path) for fourcat_path in fourcat_paths):
                continue

            # the key of the dataset files belong to can be extracted from the
            # file name in a predictable way.
            possible_keys = re.findall(r"[abcdef0-9]{32}", file.stem)
            if not possible_keys:
                self.log.warning("File %s does not seem to be a result file - clean up manually" % file)
                continue

            # if for whatever reason there are multiple hashes in the filename,
            # the key would always be the last one
            key = possible_keys.pop()

            try:
                dataset = DataSet(key=key, db=self.db, modules=self.modules)
            except DataSetException:
                # the dataset has been deleted since, but the result file still
                # exists - should be safe to clean up
                if file.name not in tracked_files:
                    self.log.info(f"No matching dataset with key {key} for file {file}; marking for deletion")
                    tracked_files[file.name] = datetime.now().timestamp() + (self.days_to_keep * 86400)
                elif tracked_files[file.name] < datetime.now().timestamp():
                    self.log.info(f"File {file} marked for deletion since {datetime.fromtimestamp(tracked_files[file.name]).strftime('%Y-%m-%d %H:%M:%S')}, deleting file")
                    if file.is_dir():
                        try:
                            shutil.rmtree(file)
                        except PermissionError:
                            self.log.info(f"Folder {file} does not belong to a dataset but cannot be deleted (no "
                                          f"permissions), skipping")

                    else:
                        try:
                            file.unlink()
                        except FileNotFoundError:
                            # the file has been deleted since
                            pass

                    # Remove from tracking
                    del tracked_files[file.name]

                continue

            # never interpret the attempt area root itself as a result file
            if file.name == ATTEMPTS_DIRNAME:
                continue

            if file.is_dir() and "-staging" in file.stem and dataset.is_finished():
                # staging area exists but dataset is marked as finished
                # if the dataset is finished, the staging area should have been
                # compressed into a zip file, or deleted, so this is also safe
                # to clean up
                if file.name not in tracked_files:
                    self.log.info("Dataset %s is finished, but staging area remains at %s, marking for deletion" % (dataset.key, str(file)))
                    tracked_files[file.name] = datetime.now().timestamp() + (self.days_to_keep * 86400)
                elif tracked_files[file.name] < datetime.now().timestamp():
                    self.log.info("Dataset %s is finished, but staging area remains at %s, deleting folder" % (dataset.key, str(file)))
                    shutil.rmtree(file)

        # Garbage collect isolated attempt areas (terminal + grace +
        # unreferenced only)
        gc_attempt_directories(
            self.config.get('PATH_DATA'),
            self.db,
            tracked_files,
            days_to_keep=self.days_to_keep,
            log=self.log,
        )

        # Update tracked files
        tracking_file.write_text(json.dumps(tracked_files))

        self.job.finish()