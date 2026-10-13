"""
Scoped access tokens

Per-integration API access token series. Each series has a human-chosen name,
an explicit set of operation scopes, a dataset access boundary, an expiry and
one or more secret generations (for rotation). Secrets are stored hashed and
only ever shown once, when a generation is created. Legacy plaintext tokens
live in the unrelated ``access_tokens`` table and are handled by the views.

This module only depends on the standard library so its decision logic (token
parsing, generation evaluation, scope mapping, status derivation) can be unit
tested without a web stack or database driver. Flask-specific wrappers live in
``webtool.lib.access_tokens``.
"""
import hashlib
import json
import re
import secrets
import time

# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

TOKEN_ID_BYTES = 8   # 16 hex characters, public series identifier
SECRET_BYTES = 32    # 64 hex characters, the authenticatable secret

# a legacy token is a bare 64-character sha256 hexdigest; new tokens always
# contain a dot separating the series id from the secret
_LEGACY_TOKEN_RE = re.compile(r"^[0-9a-f]{64}$")
_NEW_TOKEN_PART_RE = re.compile(r"^[0-9a-f]+$")

MAX_LABEL_LENGTH = 100

DEFAULT_EXPIRY_DAYS = 365
EXPIRY_PRESETS_DAYS = (30, 90, 180, 365)

MAX_OVERLAP = 30 * 86400
OVERLAP_PRESETS = (0, 3600, 86400, 7 * 86400)

DATASET_ACCESS_ALL = "all"
DATASET_ACCESS_OWNED = "owned"
DATASET_ACCESS_LISTED = "listed"
DATASET_ACCESS_MODES = (DATASET_ACCESS_ALL, DATASET_ACCESS_OWNED, DATASET_ACCESS_LISTED)

# operation scopes. ``prerequisites`` are privileges the requesting user must
# hold *at issuance time* for the scope to be grantable; ``endpoints`` are the
# Flask endpoint names (blueprint.function) the scope may call afterwards.
SCOPES = (
	{
		"id": "dataset:read",
		"label": "Read datasets",
		"description": "Read dataset status, metadata, results and exports, and manage personal favourites.",
		"prerequisites": (),
		"endpoints": frozenset((
			"toolapi.api_version",
			"toolapi.api_status",
			"toolapi.collect_available_processors",
			"toolapi.get_processor_options",
			"toolapi.check_processor",
			"toolapi.dataset_items",
			"toolapi.export_packed_dataset",
			"toolapi.toggle_favourite",
		)),
	},
	{
		"id": "dataset:create",
		"label": "Create and collect datasets",
		"description": "Queue searches, import datasets and inspect the search queue.",
		"prerequisites": ("privileges.can_create_dataset",),
		"endpoints": frozenset((
			"toolapi.datasource_form",
			"toolapi.import_dataset",
			"toolapi.queue_dataset",
			"toolapi.check_dataset",
			"toolapi.check_search_queue",
		)),
	},
	{
		"id": "dataset:process",
		"label": "Run processors",
		"description": "Queue processors, convert datasets, rename labels and cancel running queries; "
					   "also enables the standalone processor API.",
		"prerequisites": ("privileges.can_run_processors",),
		"endpoints": frozenset((
			"toolapi.queue_processor",
			"toolapi.convert_dataset",
			"toolapi.edit_dataset_label",
			"toolapi.nuke_dataset",
			"standalone.process_standalone",
		)),
	},
	{
		"id": "dataset:manage",
		"label": "Manage and share datasets",
		"description": "Delete datasets, change owners and visibility, and erase stored datasource credentials.",
		"prerequisites": (),
		"endpoints": frozenset((
			"toolapi.delete_dataset",
			"toolapi.add_dataset_owner",
			"toolapi.remove_dataset_owner",
			"toolapi.toggle_private",
			"toolapi.erase_credentials",
			"toolapi.remove_tag",
		)),
	},
)

SCOPE_BY_ID = {scope["id"]: scope for scope in SCOPES}
SCOPE_IDS = frozenset(SCOPE_BY_ID)

# endpoint name -> scope required to call it with a scoped token
ENDPOINT_SCOPES = {
	endpoint: scope["id"]
	for scope in SCOPES
	for endpoint in scope["endpoints"]
}

# API endpoints that do not require a login; a token never needs a scope for
# those
PUBLIC_API_ENDPOINTS = frozenset((
	"toolapi.openapi_overview",
	"toolapi.openapi_specification",
	"toolapi.list_datasources",
	"standalone.get_standalone_processors",
))

# login-requiring endpoints that a scoped token may nevertheless never call
# (fail-closed: they are not part of any scope). Tokens must not be able to
# mint other tokens.
TOKEN_FORBIDDEN_ENDPOINTS = frozenset((
	"toolapi.request_token",
))

# every login-requiring endpoint of the two API blueprints must have a scope
# mapping or be explicitly public/forbidden; used as a fail-closed
# completeness check
KNOWN_API_ENDPOINTS = ENDPOINT_SCOPES.keys() | PUBLIC_API_ENDPOINTS | TOKEN_FORBIDDEN_ENDPOINTS


class AccessTokenError(ValueError):
	"""
	Raised when a token management operation is invalid

	The HTTP status code suitable for the operation is attached.
	"""
	def __init__(self, message, status=400):
		super().__init__(message)
		self.status = status


# ---------------------------------------------------------------------------
# secret generation and parsing
# ---------------------------------------------------------------------------

def generate_token_id():
	"""Generate a new public series identifier."""
	return secrets.token_hex(TOKEN_ID_BYTES)


def generate_secret():
	"""Generate a new high-entropy token secret."""
	return secrets.token_hex(SECRET_BYTES)


def hash_secret(secret):
	"""Return the stored form of a token secret."""
	return hashlib.sha256(secret.encode("ascii")).hexdigest()


def present_token(token_id, secret):
	"""Compose the authenticatable string handed to the user once."""
	return token_id + "." + secret


def parse_presented_token(value):
	"""
	Split a presented token into (token_id, secret)

	Returns ``None`` for legacy (or otherwise malformed) tokens; those are
	authenticated against the legacy ``access_tokens`` table instead.
	"""
	if not value or "." not in value:
		return None

	token_id, secret = value.split(".", 1)
	if len(token_id) != TOKEN_ID_BYTES * 2 or len(secret) != SECRET_BYTES * 2:
		return None
	if not _NEW_TOKEN_PART_RE.fullmatch(token_id) or not _NEW_TOKEN_PART_RE.fullmatch(secret):
		return None

	return token_id, secret


def is_legacy_token(value):
	"""Whether a presented value looks like a legacy bare token."""
	return bool(value) and bool(_LEGACY_TOKEN_RE.fullmatch(value))


# ---------------------------------------------------------------------------
# input normalisation
# ---------------------------------------------------------------------------

def normalise_label(label):
	label = (label or "").strip()
	if not label or len(label) > MAX_LABEL_LENGTH:
		raise AccessTokenError("Please provide a name of at most %d characters for this token." % MAX_LABEL_LENGTH)
	return label


def normalise_scopes(requested_scopes, config):
	"""
	Validate requested scope ids and re-check issuance prerequisites server-side

	:param requested_scopes: raw scope ids from the request
	:param config: a request-aware config wrapper for the *current* user
	:return list: cleaned scope ids
	"""
	if not requested_scopes:
		raise AccessTokenError("Select at least one scope for the token.")

	if isinstance(requested_scopes, str):
		requested_scopes = [requested_scopes]

	scopes = []
	for scope_id in requested_scopes:
		scope = SCOPE_BY_ID.get(scope_id)
		if not scope:
			raise AccessTokenError("Unknown scope '%s'." % scope_id)
		for prerequisite in scope["prerequisites"]:
			# 'config' is the current user's live request wrapper
			if not config.get(prerequisite):
				raise AccessTokenError("Your account does not currently hold the privileges required for scope "
									   "'%s'." % scope_id, status=403)
		scopes.append(scope_id)

	# de-duplicate while preserving registry order
	return [scope["id"] for scope in SCOPES if scope["id"] in scopes]


def normalise_dataset_access(mode, keys=None):
	if mode not in DATASET_ACCESS_MODES:
		raise AccessTokenError("Unknown dataset access boundary.")
	keys = keys or []
	if mode == DATASET_ACCESS_LISTED and not keys:
		raise AccessTokenError("List at least one dataset key for the selected dataset boundary.")
	return mode, list(keys)


def normalise_expiry(choice, custom_date=None, now=None):
	"""
	Resolve the expiry form input to a unix timestamp (0 = never)
	"""
	now = int(now if now is not None else time.time())

	if choice == "never":
		return 0

	if choice in ("", None, "preset"):
		choice = str(DEFAULT_EXPIRY_DAYS)

	if choice == "custom":
		if not custom_date:
			raise AccessTokenError("Provide an expiry date.")
		try:
			# browsers submit YYYY-MM-DD; expire at the end of that day
			expires = time.mktime(time.strptime(custom_date.strip() + " 23:59:59", "%Y-%m-%d %H:%M:%S"))
		except ValueError:
			raise AccessTokenError("Expiry date is not a valid date.")
		if expires <= now:
			raise AccessTokenError("Expiry date must be in the future.")
		return int(expires)

	try:
		days = int(choice)
	except ValueError:
		raise AccessTokenError("Invalid expiry choice.")
	if days not in EXPIRY_PRESETS_DAYS:
		raise AccessTokenError("Invalid expiry choice.")
	return now + days * 86400


def normalise_overlap(overlap):
	"""Resolve the rotation overlap window to seconds within the allowed range."""
	if overlap in ("", None):
		return 0
	try:
		overlap = int(overlap)
	except (TypeError, ValueError):
		raise AccessTokenError("Invalid overlap period.")
	if overlap < 0 or overlap > MAX_OVERLAP:
		raise AccessTokenError("Overlap period must be between 0 and %d seconds." % MAX_OVERLAP)
	return overlap


# ---------------------------------------------------------------------------
# snapshots and pure decision logic
# ---------------------------------------------------------------------------

def build_snapshot(user, fourcat_config):
	"""
	Capture the authorisation snapshot stored with a new series

	Contains the user's resolved ``privileges.*`` values, their admin flag and
	the set of tags serving as the privilege source at issuance time.
	"""
	from common.config_manager import ConfigWrapper
	from common.lib.config_definition import config_definition

	# a wrapper without an HTTP request, so proxy-header tag overrides cannot
	# sneak into the snapshot
	resolver = ConfigWrapper(fourcat_config, user=user)
	# resolve every defined privilege explicitly: get_all() only yields names
	# persisted in the settings table, missing defaults otherwise
	privilege_keys = [key for key in config_definition if key.startswith("privileges.")]
	privileges = {key: resolver.get(key) for key in privilege_keys}
	return {
		"privileges": privileges,
		"is_admin": bool(user.is_admin),
		"tags": list(user.data.get("tags") or []),
	}


def evaluate_generation(series, generation, user_row, now=None):
	"""
	Decide whether a presented generation may authenticate

	Pure function over row-like dicts. Returns ``(ok, reason)``.

	:param dict series: access_token_series row plus ``current_generation``
	:param dict generation: the matched access_token_generations row
	:param dict user_row: users row (needs ``is_deactivated`` and ``tags``)
	"""
	now = int(now if now is not None else time.time())

	if not user_row:
		return False, "user_missing"
	if user_row.get("is_deactivated"):
		return False, "user_deactivated"
	if series.get("revoked"):
		return False, "series_revoked"
	if generation.get("revoked"):
		return False, "generation_revoked"

	expires = int(series.get("expires") or 0)
	if expires and expires <= now:
		return False, "expired"

	source_tags = set(series.get("source_tags") or [])
	current_tags = set(user_row.get("tags") or [])
	if not source_tags.issubset(current_tags):
		return False, "source_removed"

	if int(generation.get("generation")) >= int(series.get("current_generation")):
		return True, "current"
	if int(generation.get("overlap_until") or 0) > now:
		return True, "overlap"
	return False, "overlap_ended"


def derive_series_status(series, now=None):
	"""
	Derive the displayed status of a series row

	``series`` needs revoked/expires and an ``overlap_until`` aggregate (0 if
	none of the older generations is still overlapping).
	"""
	now = int(now if now is not None else time.time())
	if series.get("revoked"):
		return "revoked"
	expires = int(series.get("expires") or 0)
	if expires and expires <= now:
		return "expired"
	if int(series.get("overlap_until") or 0) > now:
		return "overlap"
	return "active"


# ---------------------------------------------------------------------------
# database operations
# ---------------------------------------------------------------------------

def _generation_by_nonce(db, nonce):
	"""Look up the generation previously created with a given request nonce."""
	if not nonce:
		return None
	return db.fetchone(
		"SELECT s.name AS name, s.token_id AS token_id, g.generation AS generation "
		"FROM access_token_generations g JOIN access_token_series s USING (token_id) "
		"WHERE g.request_nonce = %s",
		(nonce,))


def create_series(db, username, label, scopes, dataset_access, dataset_keys, snapshot,
				  expires, nonce, now=None):
	"""
	Create a new token series with its first generation

	Returns a dict ``{"presented", "token_id", "generation", "converged"}``.
	On a repeated request with the same nonce (duplicate submit, lost response,
	process restart) it converges to the existing generation and returns
	``presented=None`` and ``converged=True``; a second usable secret is never
	created.
	"""
	now = int(now if now is not None else time.time())
	token_id = generate_token_id()
	secret = generate_secret()

	series_row = {
		"token_id": token_id,
		"name": username,
		"label": label,
		"scopes": json.dumps(scopes),
		"dataset_access": dataset_access,
		"dataset_keys": json.dumps(dataset_keys),
		"auth_snapshot": json.dumps(snapshot),
		"source_tags": json.dumps(snapshot.get("tags", [])),
		"expires": expires,
		"revoked": False,
		"timestamp_created": now,
		"timestamp_rotated": now,
	}
	generation_row = {
		"token_id": token_id,
		"generation": 1,
		"token_hash": hash_secret(secret),
		"request_nonce": nonce or "",
		"timestamp_created": now,
		"overlap_until": 0,
		"revoked": False,
		"last_used": 0,
		"calls": 0,
	}

	try:
		db.insert("access_token_series", series_row, commit=False)
		db.insert("access_token_generations", generation_row, commit=False)
		db.commit()
	except Exception as exc:
		# always release the aborted transaction first so the shared
		# connection stays usable
		db.rollback()
		# unique violations (label/nonce) converge below; psycopg2 raises a
		# subclass such as UniqueViolation, so check the whole MRO by name
		# rather than importing the driver
		if not any(base.__name__ == "IntegrityError" for base in type(exc).__mro__):
			raise
		# the request may already have been committed before the response was
		# lost: converge to the generation anchored by its nonce
		existing = _generation_by_nonce(db, nonce)
		if existing and existing["name"] == username:
			return {
				"presented": None,
				"token_id": existing["token_id"],
				"generation": existing["generation"],
				"converged": True,
			}
		# otherwise the (name, label) uniqueness was violated
		if db.fetchone("SELECT token_id FROM access_token_series WHERE name = %s AND label = %s",
					   (username, label)):
			raise AccessTokenError("You already have a token named '%s'." % label, status=409)
		# a nonce collision with another user/series (negligible, nonces are
		# 128-bit random): surface as a client error instead of a 500
		raise AccessTokenError("Token creation conflicts with an existing request; "
							   "please try again.", status=409)

	return {
		"presented": present_token(token_id, secret),
		"token_id": token_id,
		"generation": 1,
		"converged": False,
	}


def rotate_series(db, token_id, username, overlap, nonce, now=None):
	"""
	Create the next generation for a series inside one locked transaction

	The previous current generation keeps working until ``overlap`` seconds
	have passed (overlap 0 retires it immediately). Same-nonce retries converge
	to the generation created by the first attempt without a new secret.

	The row lock and the numbering are deliberately *separate statements*:
	under READ COMMITTED a concurrent rotation that committed while we waited
	for the lock is visible to the second statement (a fresh snapshot), so
	different nonces still receive distinct generation numbers.
	"""
	now = int(now if now is not None else time.time())
	nonce = nonce or ""
	secret = generate_secret()
	series = None

	cursor = db.get_cursor()
	try:
		cursor.execute(
			"SELECT token_id, expires, revoked FROM access_token_series "
			"WHERE token_id = %s AND name = %s FOR UPDATE",
			(token_id, username))
		series = cursor.fetchone()

		if series and not series["revoked"] and not (
				int(series["expires"] or 0) and int(series["expires"]) <= now):
			# fresh statement after lock acquisition: sees generations
			# committed by a rotation that held the lock just before us
			cursor.execute(
				"""
				INSERT INTO access_token_generations
					(token_id, generation, token_hash, request_nonce, timestamp_created, overlap_until)
				SELECT %s, COALESCE(MAX(generation), 0) + 1, %s, %s, %s, 0
				FROM access_token_generations
				WHERE token_id = %s
				ON CONFLICT (request_nonce) WHERE request_nonce <> '' DO NOTHING
				RETURNING generation
				""",
				(token_id, hash_secret(secret), nonce, now, token_id))
			row = cursor.fetchone()

			if row and row.get("generation"):
				generation = row["generation"]
				if overlap:
					# retire the previous current generation after the window
					cursor.execute(
						"UPDATE access_token_generations SET overlap_until = %s "
						"WHERE token_id = %s AND generation = %s - 1",
						(now + overlap, token_id, generation))
				cursor.execute(
					"UPDATE access_token_series SET timestamp_rotated = %s WHERE token_id = %s",
					(now, token_id))
				db.commit()
				cursor.close()
				return {
					"presented": present_token(token_id, secret),
					"token_id": token_id,
					"generation": generation,
					"converged": False,
				}

		db.rollback()
		cursor.close()
	except Exception as exc:
		db.rollback()
		cursor.close()
		if not any(base.__name__ == "IntegrityError" for base in type(exc).__mro__):
			raise

	# empty RETURNING (nonce conflict) or raised unique violation: a first
	# attempt with this nonce already created a generation - converge to it
	existing = _generation_by_nonce(db, nonce)
	if existing and existing["token_id"] == token_id and existing["name"] == username:
		return {
			"presented": None,
			"token_id": token_id,
			"generation": existing["generation"],
			"converged": True,
		}

	if not series:
		raise AccessTokenError("Token not found.", status=404)
	if series["revoked"]:
		raise AccessTokenError("This token has been revoked.", status=403)
	if int(series["expires"] or 0) and int(series["expires"]) <= now:
		raise AccessTokenError("This token has expired.", status=403)
	# nonce collided with another series/user
	raise AccessTokenError("Token rotation request conflicts with an existing request.",
						   status=409)


def revoke_series(db, token_id, username):
	"""Revoke a series and all of its generations. Other series are untouched."""
	updated = db.execute(
		"UPDATE access_token_series SET revoked = TRUE WHERE token_id = %s AND name = %s",
		(token_id, username))
	if not updated:
		raise AccessTokenError("Token not found.", status=404)
	db.execute(
		"UPDATE access_token_generations SET revoked = TRUE WHERE token_id = %s",
		(token_id,))
	return True


def delete_legacy_token(db, username):
	"""Delete (revoke) a user's legacy plaintext token."""
	return db.delete("access_tokens", where={"name": username})


def authenticate(db, presented, now=None):
	"""
	Authenticate a presented token against the scoped token tables

	Returns a dict with the user row and token context, or ``None`` if the
	token is unknown or its generation may not authenticate.
	"""
	now = int(now if now is not None else time.time())
	parsed = parse_presented_token(presented)
	if not parsed:
		return None
	token_id, secret = parsed

	row = db.fetchone(
		"""
		SELECT u.*,
		       s.scopes AS tok_scopes,
		       s.dataset_access AS tok_dataset_access,
		       s.dataset_keys AS tok_dataset_keys,
		       s.auth_snapshot AS tok_snapshot,
		       s.source_tags AS tok_source_tags,
		       s.expires AS tok_expires,
		       s.revoked AS tok_series_revoked,
		       g.generation AS tok_generation,
		       g.revoked AS tok_generation_revoked,
		       g.overlap_until AS tok_overlap_until,
		       (SELECT COALESCE(MAX(generation), 0) FROM access_token_generations
		        WHERE token_id = s.token_id) AS tok_current_generation
		FROM access_token_series s
		JOIN access_token_generations g ON g.token_id = s.token_id
		JOIN users u ON u.name = s.name
		WHERE s.token_id = %s AND g.token_hash = %s
		""",
		(token_id, hash_secret(secret)))
	if not row:
		return None

	series = {
		"revoked": row["tok_series_revoked"],
		"expires": row["tok_expires"],
		"source_tags": row["tok_source_tags"],
		"current_generation": row["tok_current_generation"],
	}
	generation = {
		"generation": row["tok_generation"],
		"revoked": row["tok_generation_revoked"],
		"overlap_until": row["tok_overlap_until"],
	}
	user_row = {"is_deactivated": row.get("is_deactivated"), "tags": row.get("tags") or []}

	ok, reason = evaluate_generation(series, generation, user_row, now=now)
	if not ok:
		return None

	# usage tracking must never break authentication; roll back any aborted
	# state so the shared connection stays usable for the request
	try:
		db.execute(
			"UPDATE access_token_generations SET calls = calls + 1, last_used = %s "
			"WHERE token_id = %s AND generation = %s",
			(now, token_id, row["tok_generation"]))
	except Exception:
		try:
			db.rollback()
		except Exception:
			pass

	context = {
		"token_id": token_id,
		"generation": row["tok_generation"],
		"scopes": list(row["tok_scopes"] or []),
		"dataset_access": row["tok_dataset_access"],
		"dataset_keys": list(row["tok_dataset_keys"] or []),
		"snapshot": row["tok_snapshot"] or {},
		"reason": reason,
	}
	return {"user_row": row, "context": context}


def list_user_series(db, username, now=None):
	"""
	Return all token series for a user with aggregated generation metadata

	Each result carries ``current_generation``, ``overlap_until`` (latest
	retirement timestamp among older generations), ``last_used`` (max across
	generations), total ``calls`` and a derived ``status``.
	"""
	now = int(now if now is not None else time.time())
	series_rows = db.fetchall(
		"""
		SELECT s.*,
		       (SELECT COALESCE(MAX(generation), 0) FROM access_token_generations g
		        WHERE g.token_id = s.token_id) AS current_generation,
		       (SELECT COALESCE(MAX(overlap_until), 0) FROM access_token_generations g
		        WHERE g.token_id = s.token_id
		          AND g.generation < (SELECT COALESCE(MAX(generation), 0)
		                              FROM access_token_generations WHERE token_id = s.token_id))
		        AS overlap_until,
		       (SELECT COALESCE(MAX(last_used), 0) FROM access_token_generations g
		        WHERE g.token_id = s.token_id) AS last_used,
		       (SELECT COALESCE(SUM(calls), 0) FROM access_token_generations g
		        WHERE g.token_id = s.token_id) AS calls
		FROM access_token_series s
		WHERE s.name = %s
		ORDER BY s.timestamp_created
		""",
		(username,))

	series = []
	for row in series_rows:
		row["status"] = derive_series_status(row, now=now)
		series.append(row)
	return series


# ---------------------------------------------------------------------------
# request-time authorisation (pure parts)
# ---------------------------------------------------------------------------

def token_allows_endpoint(scopes, endpoint):
	"""Whether the given scopes grant access to an endpoint."""
	required = ENDPOINT_SCOPES.get(endpoint)
	return required is not None and required in (scopes or [])


def token_dataset_access_allowed(context, dataset, username=None):
	"""
	Check a dataset against the token's dataset access boundary

	For ``all`` the endpoint's ordinary owner/admin checks still apply; this
	function only enforces the additional boundary.
	"""
	mode = context.get("dataset_access")
	username = username or context.get("username")

	if mode == DATASET_ACCESS_ALL:
		return True
	if mode == DATASET_ACCESS_OWNED:
		return dataset.is_accessible_by(username, role="owner")
	if mode == DATASET_ACCESS_LISTED:
		if dataset.key not in (context.get("dataset_keys") or []):
			return False
		# the user may have been removed from the dataset after issuance;
		# any role (owner or viewer) still counts
		return dataset.is_accessible_by(username, role=None)
	return False
