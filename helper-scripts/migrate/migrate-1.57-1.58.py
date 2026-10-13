"""
Migration 1.57 -> 1.58

Adds tables that durably track request batches delegated to the proxy pool:
delegated_batches, delegated_requests and delegated_attempts. These replace the
process-memory-only state of DelegatedRequestHandler, allowing a restarted or
reassigned worker to take over a batch without re-requesting URLs that already
have a confirmable result or delivering a result twice.
"""
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

print("  Creating durable delegation tables if needed...")
db.execute(
    """
    CREATE TABLE IF NOT EXISTS delegated_batches (
      key               TEXT PRIMARY KEY,
      processor_type    TEXT NOT NULL DEFAULT '',
      dataset_key       TEXT NOT NULL DEFAULT '',
      job_id            BIGINT DEFAULT NULL,
      status            TEXT NOT NULL DEFAULT 'active',
      owner_id          TEXT DEFAULT NULL,
      owner_generation  INTEGER NOT NULL DEFAULT 0,
      lease_expires     DOUBLE PRECISION NOT NULL DEFAULT 0,
      next_seq          DOUBLE PRECISION NOT NULL DEFAULT 0,
      expires_at        DOUBLE PRECISION NOT NULL DEFAULT 0,
      created_at        DOUBLE PRECISION NOT NULL,
      updated_at        DOUBLE PRECISION NOT NULL
    );
    """
)
db.execute(
    "CREATE INDEX IF NOT EXISTS delegated_batches_status ON delegated_batches (status, expires_at)"
)

db.execute(
    """
    CREATE TABLE IF NOT EXISTS delegated_requests (
      id                BIGSERIAL PRIMARY KEY,
      batch_key         TEXT NOT NULL REFERENCES delegated_batches (key) ON DELETE CASCADE,
      seq               DOUBLE PRECISION NOT NULL,
      url               TEXT NOT NULL,
      kwargs            JSONB NOT NULL DEFAULT '{}'::jsonb,
      status            TEXT NOT NULL DEFAULT 'queued',
      claimer           TEXT DEFAULT NULL,
      attempt_no        INTEGER NOT NULL DEFAULT 0,
      lease_expires     DOUBLE PRECISION NOT NULL DEFAULT 0,
      result_kind       TEXT DEFAULT NULL,
      response_summary  JSONB NOT NULL DEFAULT '{}'::jsonb,
      delivered         BOOLEAN NOT NULL DEFAULT FALSE,
      delivered_at      DOUBLE PRECISION NOT NULL DEFAULT 0,
      created_at        DOUBLE PRECISION NOT NULL,
      updated_at        DOUBLE PRECISION NOT NULL
    );
    """
)
db.execute(
    "CREATE UNIQUE INDEX IF NOT EXISTS delegated_requests_batch_seq ON delegated_requests (batch_key, seq)"
)
db.execute(
    "CREATE INDEX IF NOT EXISTS delegated_requests_delivery ON delegated_requests (batch_key, delivered, seq)"
)
db.execute(
    "CREATE INDEX IF NOT EXISTS delegated_requests_claim ON delegated_requests (status, lease_expires)"
)

db.execute(
    """
    CREATE TABLE IF NOT EXISTS delegated_attempts (
      id                BIGSERIAL PRIMARY KEY,
      request_id        BIGINT NOT NULL REFERENCES delegated_requests (id) ON DELETE CASCADE,
      attempt_no        INTEGER NOT NULL,
      proxy             TEXT DEFAULT NULL,
      outcome           TEXT NOT NULL,
      summary           JSONB NOT NULL DEFAULT '{}'::jsonb,
      started_at        DOUBLE PRECISION NOT NULL,
      finished_at       DOUBLE PRECISION NOT NULL DEFAULT 0
    );
    """
)
db.execute(
    "CREATE UNIQUE INDEX IF NOT EXISTS delegated_attempts_request_attempt ON delegated_attempts (request_id, attempt_no)"
)

print("  - done!")
