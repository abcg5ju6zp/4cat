# Atomic job attempts: bind a stable attempt identity to each job claim, so
# processor results can be staged in isolation, validated and published
# atomically. See common/lib/attempt.py.
# - jobs.attempt_id:     attempt currently holding the claim
# - datasets.attempt_id: attempt that created an unpublished child dataset row
# - job_attempts:        per-attempt state, phase checkpoints and manifests
import sys
import os

from pathlib import Path

sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), "../.."))
from common.lib.database import Database
from common.lib.logger import Logger

import configparser  # noqa: E402

log = Logger(output=True)
ini = configparser.ConfigParser()
ini.read(Path(__file__).parent.parent.parent.resolve().joinpath("config/config.ini"))
db_config = ini["DATABASE"]

db = Database(
    logger=log,
    dbname=db_config["db_name"],
    user=db_config["db_user"],
    password=db_config["db_password"],
    host=db_config["db_host"],
    port=db_config["db_port"],
    appname="4cat-migrate",
)


def has_column(table, column):
	return db.fetchone(
		"SELECT COUNT(*) AS num FROM information_schema.columns "
		"WHERE table_name = %s AND column_name = %s",
		(table, column),
	)["num"] > 0


print("  Checking for `attempt_id` column in jobs table...")
if has_column("jobs", "attempt_id"):
	print("    jobs table already has column 'attempt_id'")
else:
	print("    Adding column 'attempt_id' to jobs table...")
	db.execute("ALTER TABLE jobs ADD attempt_id TEXT DEFAULT ''")

print("  Checking for `attempt_id` column in datasets table...")
if has_column("datasets", "attempt_id"):
	print("    datasets table already has column 'attempt_id'")
else:
	print("    Adding column 'attempt_id' to datasets table...")
	db.execute("ALTER TABLE datasets ADD attempt_id TEXT DEFAULT ''")

print("  Creating `job_attempts` table...")
db.execute("""
CREATE TABLE IF NOT EXISTS job_attempts (
  attempt_id          TEXT PRIMARY KEY,
  jobtype             text DEFAULT 'misc',
  remote_id           text,
  status              text DEFAULT 'running',
  phase               text DEFAULT 'claimed',
  input_signature     text DEFAULT '{}',
  manifest            text DEFAULT '{}',
  pending_children    text DEFAULT '[]',
  error               text DEFAULT '',
  owner               text DEFAULT '',
  heartbeat_at        integer DEFAULT 0,
  timestamp_created   integer,
  timestamp_updated   integer,
  timestamp_finished  integer DEFAULT NULL
);
""")

print("  Creating indexes on `job_attempts`...")
db.execute("CREATE INDEX IF NOT EXISTS job_attempts_job ON job_attempts (jobtype, remote_id)")
db.execute("CREATE INDEX IF NOT EXISTS job_attempts_status ON job_attempts (status)")

print("  - done!")
