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

# Per-integration scoped access tokens. One series per integration, with one
# row per secret generation so tokens can be rotated with an overlap window.
# Legacy access_tokens table is deliberately left untouched.
print("  Creating table 'access_token_series'...")
db.execute("""
    CREATE TABLE IF NOT EXISTS access_token_series (
      token_id          TEXT UNIQUE PRIMARY KEY,
      name              TEXT NOT NULL,
      label             TEXT NOT NULL,
      scopes            JSONB DEFAULT '[]',
      dataset_access    TEXT DEFAULT 'all',
      dataset_keys      JSONB DEFAULT '[]',
      auth_snapshot     JSONB DEFAULT '{}',
      source_tags       JSONB DEFAULT '[]',
      expires           INTEGER DEFAULT 0,
      revoked           BOOLEAN DEFAULT FALSE,
      timestamp_created INTEGER DEFAULT 0,
      timestamp_rotated INTEGER DEFAULT 0,
      UNIQUE(name, label)
    );
""")

db.execute("""
    CREATE INDEX IF NOT EXISTS access_token_series_name_idx
      ON access_token_series (name)
""")

print("  Creating table 'access_token_generations'...")
db.execute("""
    CREATE TABLE IF NOT EXISTS access_token_generations (
      token_id          TEXT NOT NULL REFERENCES access_token_series(token_id) ON DELETE CASCADE,
      generation        INTEGER NOT NULL,
      token_hash        TEXT NOT NULL,
      request_nonce     TEXT DEFAULT '',
      timestamp_created INTEGER DEFAULT 0,
      overlap_until     INTEGER DEFAULT 0,
      revoked           BOOLEAN DEFAULT FALSE,
      last_used         INTEGER DEFAULT 0,
      calls             INTEGER DEFAULT 0,
      PRIMARY KEY (token_id, generation),
      UNIQUE(token_hash)
    );
""")

# idempotency anchor: a repeated create/rotate with the same nonce converges
# to the generation created on the first attempt
db.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS access_token_generations_nonce_idx
      ON access_token_generations (request_nonce)
      WHERE request_nonce <> ''
""")

print("  - done!")
