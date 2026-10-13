"""
Scoped access tokens - web layer

Re-exports the framework-independent logic from ``common.lib.access_tokens``
and adds the Flask-specific pieces: the frozen-snapshot config wrapper, the
token-authenticated user class, and blueprint hooks for scope enforcement and
dataset boundary checks.
"""
from common.lib.access_tokens import *  # noqa: F401,F403
from common.lib.access_tokens import (  # noqa: F401
	AccessTokenError,
	ENDPOINT_SCOPES,
	KNOWN_API_ENDPOINTS,
	PUBLIC_API_ENDPOINTS,
	SCOPES,
	SCOPE_BY_ID,
	authenticate,
	build_snapshot,
	create_series,
	delete_legacy_token,
	derive_series_status,
	evaluate_generation,
	generate_secret,
	generate_token_id,
	hash_secret,
	list_user_series,
	normalise_dataset_access,
	normalise_expiry,
	normalise_label,
	normalise_overlap,
	normalise_scopes,
	parse_presented_token,
	present_token,
	revoke_series,
	rotate_series,
	token_allows_endpoint,
	token_dataset_access_allowed,
)

from common.config_manager import ConfigWrapper
from common.lib.user import User


class TokenConfigWrapper(ConfigWrapper):
	"""
	Config wrapper that serves frozen privilege values for a token request

	All ``privileges.*`` keys resolve from the issuance snapshot, so later
	promotions of the user account cannot widen an existing token. Non-
	privilege settings keep resolving live.
	"""
	def __init__(self, config, user=None, snapshot=None, request=None):
		super().__init__(config, user=user, request=request)
		self.token_privileges = dict((snapshot or {}).get("privileges") or {})

	def get(self, *args, **kwargs):
		if args and isinstance(args[0], str) and args[0].startswith("privileges."):
			# unknown privilege keys (e.g. added by a later 4CAT version) must
			# not silently widen an old token through live resolution
			return self.token_privileges.get(args[0], False)
		return super().get(*args, **kwargs)

	def get_all(self, *args, **kwargs):
		values = dict(super().get_all(*args, **kwargs))
		values.update(self.token_privileges)
		return values


class TokenUser(User):
	"""
	User authenticated through a scoped token

	Carries the token context and freezes ``is_admin`` to the snapshot value.
	"""
	api_token = None
	_frozen_is_admin = False

	def __init__(self, db, data, token_context, config=None):
		# authenticate() after construction, like the legacy token path, so a
		# token API call does not refresh the user's last-seen timestamp
		super().__init__(db, data, authenticated=False, config=config)
		self.api_token = token_context
		self._frozen_is_admin = bool((token_context.get("snapshot") or {}).get("is_admin"))
		self.authenticate()

	@property
	def is_admin(self):
		return self._frozen_is_admin


def enforce_token_scope():
	"""
	API blueprint ``before_request`` hook: fail-closed scope enforcement

	Session users and legacy tokens are unaffected. Public API endpoints are
	open to everyone. Scoped tokens may only reach endpoints mapped to one of
	their scopes.
	"""
	from flask import request
	from flask_login import current_user

	token_context = getattr(current_user, "api_token", None)
	if not token_context:
		return None

	endpoint = request.endpoint or ""
	if endpoint in PUBLIC_API_ENDPOINTS:
		return None

	if token_allows_endpoint(token_context["scopes"], endpoint):
		return None

	from webtool.lib.helpers import error
	return error(403, error="This access token is not permitted to use this API endpoint.")


API_BLUEPRINTS = frozenset(("toolapi", "standalone"))


def enforce_api_only_token():
	"""
	Application-wide ``before_request`` hook: scoped tokens authenticate only
	for API requests. On any other blueprint (HTML pages, downloads, admin)
	they are refused, so token scopes and dataset boundaries cannot be
	bypassed through parallel interactive routes. Legacy tokens and session
	users are unaffected.
	"""
	from flask import request
	from flask_login import current_user

	token_context = getattr(current_user, "api_token", None)
	if not token_context:
		return None

	if request.blueprint in API_BLUEPRINTS or (request.endpoint or "") == "static":
		return None

	from webtool.lib.helpers import error
	return error(403, error="This access token can only be used with the API.")


def enforce_dataset_boundary(dataset):
	"""
	Return a 403 response if the current scoped token's boundary excludes the
	dataset, otherwise ``None``. Session users and legacy tokens pass.
	"""
	from flask_login import current_user

	token_context = getattr(current_user, "api_token", None)
	if not token_context:
		return None

	if token_dataset_access_allowed(token_context, dataset, username=current_user.get_id()):
		return None

	from webtool.lib.helpers import error
	return error(403, error="This access token is not allowed to access this dataset.")
