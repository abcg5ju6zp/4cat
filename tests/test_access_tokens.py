"""
Tests for the scoped access token logic in common.lib.access_tokens

Only depends on the standard library: database drivers and the web stack are
stubbed where needed.
"""
import re
from pathlib import Path

import pytest

# create_series recognises unique violations by exception class name, so a
# driver-independent stand-in named like psycopg2's exception hierarchy is
# sufficient (psycopg2 raises e.g. UniqueViolation, a subclass).
class IntegrityError(Exception):
	pass


class UniqueViolation(IntegrityError):
	pass

from common.lib import access_tokens as at  # noqa: E402

PATH_ROOT = Path(__file__).parent.parent.resolve()


# ---------------------------------------------------------------------------
# token presentation
# ---------------------------------------------------------------------------

def test_presented_token_roundtrip():
	token_id = at.generate_token_id()
	secret = at.generate_secret()
	assert re.fullmatch(r"[0-9a-f]{16}", token_id)
	assert re.fullmatch(r"[0-9a-f]{64}", secret)

	presented = at.present_token(token_id, secret)
	assert at.parse_presented_token(presented) == (token_id, secret)
	# stored form is never the plaintext secret
	assert at.hash_secret(secret) != secret
	assert at.hash_secret(secret) == at.hash_secret(secret)


def test_legacy_and_malformed_tokens_are_not_new_style():
	legacy = "a" * 64
	assert at.is_legacy_token(legacy)
	assert at.parse_presented_token(legacy) is None
	for value in ("", None, "nodot", "a.b", "abc." + "d" * 64, " "):
		assert at.parse_presented_token(value) is None


# ---------------------------------------------------------------------------
# pure generation evaluation
# ---------------------------------------------------------------------------

NOW = 1_000_000


def _series(**overrides):
	base = {"revoked": False, "expires": NOW + 100, "source_tags": ["research"],
			"current_generation": 2}
	base.update(overrides)
	return base


def _generation(**overrides):
	base = {"generation": 2, "revoked": False, "overlap_until": 0}
	base.update(overrides)
	return base


_USER = {"is_deactivated": False, "tags": ["research", "other"]}


@pytest.mark.parametrize("series,generation,user_row,expected,reason", [
	(_series(), _generation(), _USER, True, "current"),
	(_series(), _generation(generation=1, overlap_until=NOW + 50), _USER, True, "overlap"),
	(_series(), _generation(generation=1, overlap_until=NOW - 1), _USER, False, "overlap_ended"),
	(_series(), _generation(), {"is_deactivated": True, "tags": ["research"]}, False, "user_deactivated"),
	(_series(), _generation(), {"is_deactivated": False, "tags": ["other"]}, False, "source_removed"),
	(_series(), _generation(), None, False, "user_missing"),
	(_series(revoked=True), _generation(), _USER, False, "series_revoked"),
	(_series(), _generation(revoked=True), _USER, False, "generation_revoked"),
	(_series(expires=NOW - 1), _generation(), _USER, False, "expired"),
	# expiry 0 means the series never expires
	(_series(expires=0), _generation(), _USER, True, "current"),
	# extra tags gained later do not matter; only removed sources do
	(_series(source_tags=[]), _generation(), _USER, True, "current"),
])
def test_evaluate_generation(series, generation, user_row, expected, reason):
	ok, actual_reason = at.evaluate_generation(series, generation, user_row, now=NOW)
	assert ok is expected
	if expected:
		assert actual_reason == reason


def test_derive_series_status():
	assert at.derive_series_status({"revoked": True}, now=NOW) == "revoked"
	assert at.derive_series_status({"revoked": False, "expires": NOW - 1, "overlap_until": 0},
								  now=NOW) == "expired"
	assert at.derive_series_status({"revoked": False, "expires": NOW + 1, "overlap_until": NOW + 5},
								   now=NOW) == "overlap"
	assert at.derive_series_status({"revoked": False, "expires": NOW + 1, "overlap_until": 0},
								   now=NOW) == "active"
	assert at.derive_series_status({"revoked": False, "expires": 0, "overlap_until": 0},
								   now=NOW) == "active"


# ---------------------------------------------------------------------------
# input normalisation
# ---------------------------------------------------------------------------

class _FakeConfig:
	def __init__(self, values):
		self.values = values

	def get(self, key, *args, **kwargs):
		return self.values.get(key, False)


def test_normalise_label():
	assert at.normalise_label("  hello ") == "hello"
	with pytest.raises(at.AccessTokenError):
		at.normalise_label("")
	with pytest.raises(at.AccessTokenError):
		at.normalise_label("x" * (at.MAX_LABEL_LENGTH + 1))


def test_normalise_scopes_prerequisites():
	config = _FakeConfig({"privileges.can_create_dataset": True, "privileges.can_run_processors": False})

	result = at.normalise_scopes(["dataset:read", "dataset:create", "dataset:create"], config)
	assert result == ["dataset:read", "dataset:create"]

	with pytest.raises(at.AccessTokenError) as exc:
		at.normalise_scopes(["dataset:process"], config)
	assert exc.value.status == 403

	with pytest.raises(at.AccessTokenError):
		at.normalise_scopes([], config)
	with pytest.raises(at.AccessTokenError):
		at.normalise_scopes(["not:a:scope"], config)


def test_normalise_dataset_access():
	assert at.normalise_dataset_access("all", []) == ("all", [])
	assert at.normalise_dataset_access("listed", ["a", "a", "b"]) == ("listed", ["a", "a", "b"])
	with pytest.raises(at.AccessTokenError):
		at.normalise_dataset_access("listed", [])
	with pytest.raises(at.AccessTokenError):
		at.normalise_dataset_access("bogus", [])


def test_normalise_expiry():
	now = 1_000_000_000
	assert at.normalise_expiry("never", now=now) == 0
	assert at.normalise_expiry("30", now=now) == now + 30 * 86400
	assert at.normalise_expiry(None, now=now) == now + at.DEFAULT_EXPIRY_DAYS * 86400
	with pytest.raises(at.AccessTokenError):
		at.normalise_expiry("custom", "", now=now)
	with pytest.raises(at.AccessTokenError):
		at.normalise_expiry("custom", "2000-01-01", now=now)
	future = at.normalise_expiry("custom", "2999-01-01", now=now)
	assert future > now
	with pytest.raises(at.AccessTokenError):
		at.normalise_expiry("13", now=now)


def test_normalise_overlap():
	assert at.normalise_overlap("") == 0
	assert at.normalise_overlap(3600) == 3600
	with pytest.raises(at.AccessTokenError):
		at.normalise_overlap(-1)
	with pytest.raises(at.AccessTokenError):
		at.normalise_overlap(at.MAX_OVERLAP + 1)


# ---------------------------------------------------------------------------
# endpoint mapping and scope gate
# ---------------------------------------------------------------------------

def test_scope_registry_consistency():
	# every mapped endpoint belongs to exactly one scope
	for scope in at.SCOPES:
		assert scope["id"] in at.SCOPE_BY_ID
	assert len(at.ENDPOINT_SCOPES) == len(set(at.ENDPOINT_SCOPES))
	assert at.token_allows_endpoint(["dataset:read"], "toolapi.api_version")
	assert not at.token_allows_endpoint(["dataset:read"], "toolapi.queue_dataset")
	assert not at.token_allows_endpoint([], "toolapi.api_version")
	# public endpoints are never mapped as scoped
	assert not (at.PUBLIC_API_ENDPOINTS & set(at.ENDPOINT_SCOPES))


def test_all_login_required_api_endpoints_have_scope_mapping():
	"""Fail-closed mapping must cover every login-requiring API endpoint."""
	blueprints = {
		"toolapi": PATH_ROOT / "webtool" / "views" / "api_tool.py",
		"standalone": PATH_ROOT / "webtool" / "views" / "api_standalone.py",
	}
	for blueprint, path in blueprints.items():
		lines = path.read_text(encoding="utf-8").splitlines()
		for i, line in enumerate(lines):
			match = re.match(r"def (\w+)\(", line.strip())
			if not match:
				continue
			# walk backwards through the decorator block
			j = i - 1
			login_required_endpoint = False
			while j >= 0 and (lines[j].startswith("@") or not lines[j].strip()):
				if "@login_required" in lines[j]:
					login_required_endpoint = True
				j -= 1
			if login_required_endpoint:
				endpoint = f"{blueprint}.{match.group(1)}"
				assert endpoint in at.KNOWN_API_ENDPOINTS, (
					f"{endpoint} requires login but has no scope mapping and is not listed public"
				)


# ---------------------------------------------------------------------------
# dataset boundary
# ---------------------------------------------------------------------------

class _FakeDataset:
	def __init__(self, key, owners=None, viewers=None):
		self.key = key
		self.owners = owners or set()
		self.viewers = viewers or set()

	def is_accessible_by(self, username, role="owner"):
		if role == "owner":
			return username in self.owners
		return username in self.owners | self.viewers


def test_dataset_boundary_modes():
	dataset_owned = _FakeDataset("k1", owners={"bob"})
	dataset_other = _FakeDataset("k2", owners={"alice"})
	dataset_viewer = _FakeDataset("kv", owners={"alice"}, viewers={"bob"})

	all_context = {"dataset_access": "all", "username": "bob", "dataset_keys": []}
	owned_context = {"dataset_access": "owned", "username": "bob", "dataset_keys": []}
	listed_context = {"dataset_access": "listed", "username": "bob",
					  "dataset_keys": ["k1", "k2"]}
	listed_viewer_context = {"dataset_access": "listed", "username": "bob",
							 "dataset_keys": ["kv"]}

	assert at.token_dataset_access_allowed(all_context, dataset_other)
	assert at.token_dataset_access_allowed(owned_context, dataset_owned)
	assert not at.token_dataset_access_allowed(owned_context, dataset_other)
	# viewer-only access does not satisfy the owned boundary
	assert not at.token_dataset_access_allowed(owned_context, dataset_viewer)
	# whitelisted but ownership/view removed after issuance -> denied
	assert not at.token_dataset_access_allowed(listed_context, dataset_other)
	assert not at.token_dataset_access_allowed(listed_context, _FakeDataset("k3"))
	assert at.token_dataset_access_allowed(listed_context, dataset_owned)
	# listed boundary accepts any current role, including viewer
	assert at.token_dataset_access_allowed(listed_viewer_context, dataset_viewer)


def test_every_user_keyed_dataset_site_enforces_boundary():
	"""
	Each API view function that resolves a dataset by a request-supplied key
	must invoke the token boundary check, in addition to scope gating.
	Derived datasets (children/parents of an already checked dataset) and
	newly created datasets are exempt.
	"""
	view_path = PATH_ROOT / "webtool" / "views" / "api_tool.py"
	blocks = re.split(r"(?m)^(?=def )", view_path.read_text(encoding="utf-8"))
	checked = 0
	for block in blocks:
		for line in block.splitlines():
			if "DataSet(key=" not in line:
				continue
			if "dataset.key_parent" in line or "original_key" in line:
				continue
			assert ("enforce_dataset_boundary" in block
					or "token_dataset_access_allowed" in block), (
				f"dataset resolution missing token boundary check:\n{line.strip()}"
			)
			checked += 1
	# guard against the scan silently matching nothing
	assert checked >= 16


# ---------------------------------------------------------------------------
# create_series persistence and nonce convergence
# ---------------------------------------------------------------------------

class _FakeDB:
	"""Minimal database stand-in for create_series success/conflict paths."""
	def __init__(self, conflict=False, existing_nonce=None, label_exists=False):
		self.rows = {"access_token_series": [], "access_token_generations": []}
		self.commits = 0
		self.rollbacks_count = 0
		self.conflict = conflict
		self.existing_nonce = existing_nonce
		self.label_exists = label_exists

	def insert(self, table, data, commit=True):
		if self.conflict and table == "access_token_series":
			raise UniqueViolation("duplicate key value")
		self.rows[table].append(dict(data))

	def commit(self):
		self.commits += 1

	def rollback(self):
		self.rollbacks_count += 1

	def fetchone(self, query, args=None):
		if "FROM access_token_generations" in query:
			return dict(self.existing_nonce) if self.existing_nonce else None
		if "FROM access_token_series" in query:
			return {"token_id": "existing"} if self.label_exists else None
		return None


_SNAPSHOT = {"privileges": {"privileges.can_create_dataset": True}, "is_admin": False,
			 "tags": ["research"]}


def test_create_series_persists_only_hashes_and_returns_presented_once():
	db = _FakeDB()
	result = at.create_series(db, "bob", "collector", ["dataset:create"], "owned", [],
							  _SNAPSHOT, expires=123, nonce="nonce-1", now=42)

	assert result["converged"] is False
	assert result["generation"] == 1
	token_id, secret = at.parse_presented_token(result["presented"])
	assert token_id == result["token_id"]

	series_row = db.rows["access_token_series"][0]
	gen_row = db.rows["access_token_generations"][0]

	# no plaintext secret anywhere; stored value is the hash
	assert result["presented"] not in json_dumps(series_row)
	assert result["presented"] not in json_dumps(gen_row)
	assert gen_row["token_hash"] == at.hash_secret(secret)
	assert gen_row["request_nonce"] == "nonce-1"
	assert gen_row["generation"] == 1
	assert series_row["name"] == "bob"
	assert db.commits == 1


def test_create_series_same_nonce_converges_without_new_secret():
	existing = {"name": "bob", "token_id": "tid-existing", "generation": 1}
	db = _FakeDB(conflict=True, existing_nonce=existing)
	result = at.create_series(db, "bob", "collector", ["dataset:read"], "all", [],
							  _SNAPSHOT, expires=0, nonce="nonce-x", now=42)

	assert result["converged"] is True
	assert result["presented"] is None
	assert result["token_id"] == "tid-existing"
	assert result["generation"] == 1
	# no rows inserted, exactly one rollback
	assert not any(db.rows.values())
	assert db.rollbacks_count == 1


def test_create_series_duplicate_label_is_an_error():
	db = _FakeDB(conflict=True, label_exists=True)
	with pytest.raises(at.AccessTokenError) as exc:
		at.create_series(db, "bob", "collector", ["dataset:read"], "all", [],
						 _SNAPSHOT, expires=0, nonce="nonce-y", now=42)
	assert exc.value.status == 409


def json_dumps(value):
	import json
	return json.dumps(value)
