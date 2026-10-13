from requests_futures.sessions import FuturesSession
from concurrent.futures import ThreadPoolExecutor
import threading
from functools import wraps

import importlib
import inspect
import json
import os
import socket
import time
import uuid
import urllib3
import ural
import requests

from collections import namedtuple
from asyncio import CancelledError as asyncioCancelledError
from concurrent.futures import CancelledError as futureCancelledError


# Sequences use a fixed gap so position inserts can take the midpoint between
# two neighbouring requests. They are stored as doubles, so midpoints are
# fractions; when a gap gets too small to halve again the batch is re-sequenced.
SEQ_GAP = 1024.0
SEQ_EPSILON = 1e-6

# delegation batch statuses
BATCH_ACTIVE = "active"
BATCH_COMPLETED = "completed"
BATCH_CANCELLED = "cancelled"
BATCH_EXPIRED = "expired"

# delegated request statuses
REQUEST_QUEUED = "queued"
REQUEST_CLAIMED = "claimed"
REQUEST_RUNNING = "running"
REQUEST_SUCCEEDED = "succeeded"
REQUEST_FAILED = "failed"
REQUEST_CANCELLED = "cancelled"
REQUEST_EXPIRED = "expired"

# kinds of confirmable results
RESULT_RESPONSE = "response"
RESULT_ERROR = "error"

# attempt outcomes
ATTEMPT_SUCCEEDED = "succeeded"
ATTEMPT_REQUEST_ERROR = "request_error"
ATTEMPT_PROXY_ERROR = "proxy_error"
ATTEMPT_CANCELLED = "cancelled"
ATTEMPT_ABANDONED = "abandoned"
ATTEMPT_EXPIRED = "expired"

# response headers that are worth persisting in a response summary
SUMMARY_HEADERS = ("content-type", "content-length", "location", "content-disposition")

# marker for request kwargs holding serializable response-hook references
HOOK_REFS_MARKER = "__4cat_hook_refs__"


def synchronized_method(func):
    """
    A decorator to synchronize access to a method using a lock.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)

    return wrapper


def remove_proxy_credentials(proxy_url):
    """
    Remove the username and password from a proxy URL

    Proxy URLs can contain a username and password, as in
    `http://user:password@host:port`. Processors may write the proxy they
    were given to dataset logs or results that users can see, and the backend
    log may be forwarded elsewhere (e.g. to Slack), so use this on any proxy
    URL that is passed on to processors or written to a log.

    :param str|None proxy_url:  Proxy URL
    :return str|None:  The same URL, without anything before the last `@`
    """
    if not proxy_url:
        return proxy_url

    scheme, separator, rest = proxy_url.partition("://")
    if not separator:
        scheme, rest = "", proxy_url

    return scheme + separator + rest.rpartition("@")[2]


class FailedProxiedRequest:
    """
    A delegated request that has failed for whatever reason

    The failure context (usually the exception) is stored in the `context`
    property. We also keep track of the proxy URL that serviced the request so
    downstream consumers can make informed retry decisions. Any username and
    password are removed from that URL.
    """

    context = None
    proxy_url = None

    def __init__(self, context=None, proxy_url=None):
        self.context = context
        self.proxy_url = remove_proxy_credentials(proxy_url)


class RecoveredProxiedRequest:
    """
    Stand-in for a response reconstructed from a persisted response summary

    A response body (in particular one streamed to disk by a response hook)
    cannot be brought back after a backend restart, so a request that has a
    confirmable successful result but was never delivered is handed to the
    takeover processor as this lightweight object instead of being re-requested.
    It carries the bits of the response that were persisted: status code,
    reason, final URL and a small header whitelist. `recovered` is always True
    so processors can tell the stand-in apart from a live response.
    """

    recovered = True

    def __init__(self, summary=None):
        self.summary = summary or {}
        self.status_code = self.summary.get("status_code")
        self.reason = self.summary.get("reason") or ""
        self.url = self.summary.get("url") or ""
        self.headers = requests.structures.CaseInsensitiveDict(self.summary.get("headers") or {})
        self.page_title = ""
        self.history = []
        self.content = b""
        self.text = ""
        self.ok = isinstance(self.status_code, int) and 200 <= self.status_code < 300
        self._4cat_proxy = self.summary.get("proxy")


class RecoveredRequestError(Exception):
    """
    Stand-in for an exception raised by a previous attempt

    Reconstructed from the persisted error class name and message; the
    original class name is available as `error_class`.
    """

    def __init__(self, message, error_class=None):
        super().__init__(message)
        self.error_class = error_class


class NoProxiesAvailableError(Exception):
    """
    Raised when all proxies are unhealthy and localhost fallback is disabled
    """
    pass


class DelegatedRequestConflict(Exception):
    """
    Raised when a durable batch is actively owned by another live backend
    """
    pass


def next_append_seqs(current_next, count):
    """
    Allocate `count` append sequences from a batch's sequence counter

    :param float current_next:  Current value of next_seq
    :param int count:  Number of sequences to allocate
    :return tuple:  (new counter value, list of allocated sequences in order)
    """
    allocated = [current_next + SEQ_GAP * (i + 1) for i in range(count)]
    return current_next + SEQ_GAP * count, allocated


def insert_seq(lower, upper):
    """
    Return the sequence to use for a request inserted between two neighbours

    :param float|None lower:  Sequence of the request before the insertion
        point, or None at the head of the batch
    :param float|None upper:  Sequence of the request after the insertion
        point, or None at the tail of the batch
    :return float:
    :raises ValueError:  When the neighbours are too close to fit another
        sequence in between and the batch needs re-sequencing
    """
    if lower is None and upper is None:
        return SEQ_GAP
    elif lower is None:
        # head of the batch: take the midpoint between the virtual origin (0)
        # and the first request
        candidate = upper / 2.0
    elif upper is None:
        candidate = lower + SEQ_GAP
    else:
        candidate = (lower + upper) / 2.0

    if (lower is not None and abs(candidate - lower) <= SEQ_EPSILON) or \
            (upper is not None and abs(upper - candidate) <= SEQ_EPSILON):
        raise ValueError("Not enough room between sequences to insert another one")

    return candidate


def renorm_seqs(count):
    """
    Return fresh evenly spaced sequences for `count` requests

    :param int count:  Number of requests
    :return list[float]:
    """
    return [SEQ_GAP * (i + 1) for i in range(count)]


def hook_reference(callback):
    """
    Produce a persistable reference for a response hook, if possible

    Module-level functions, class/static methods and bound methods of a
    processor instance can be referenced. Bound instance methods need the
    processor's hook binder to be re-attached after takeover; lambdas, locals
    and partials cannot be referenced and return None.

    :param callable callback:
    :return dict|None:
    """
    if isinstance(callback, (staticmethod, classmethod)):
        callback = callback.__func__

    if inspect.ismethod(callback):
        func = callback.__func__
        if isinstance(callback.__self__, type):
            return {"module": func.__module__, "qualname": func.__qualname__}
        return {
            "module": func.__module__,
            "qualname": ".".join((type(callback.__self__).__qualname__, func.__name__)),
            "bind": "processor",
        }

    if inspect.isfunction(callback) and "<locals>" not in (callback.__qualname__ or ""):
        return {"module": callback.__module__, "qualname": callback.__qualname__}

    return None


def serializable_kwargs(kwargs):
    """
    Reduce requests.get() kwargs to JSON-persistable values

    Response hooks are callables and cannot be stored as such; importable ones
    are stored as references instead. Other values that JSON cannot encode are
    dropped.

    :param dict kwargs:
    :return dict:
    """
    safe = {}
    for key, value in (kwargs or {}).items():
        if key == "hooks" and isinstance(value, dict):
            refs = {}
            for event, hook_value in value.items():
                hook_list = hook_value if isinstance(hook_value, list) else [hook_value]
                event_refs = [ref for hook in hook_list if (ref := hook_reference(hook))]
                if event_refs:
                    refs[event] = event_refs
            if refs:
                safe[key] = {HOOK_REFS_MARKER: refs}
            continue

        try:
            json.dumps(value)
        except (TypeError, ValueError):
            continue
        safe[key] = value

    return safe


def resolve_hook_ref(ref):
    """
    Resolve a hook reference to a (module-level) callable

    References that need to be bound to a processor instance return None here;
    the processor supplies a binder for those.

    :param dict ref:
    :return callable|None:
    """
    if ref.get("bind") == "processor":
        return None
    try:
        obj = importlib.import_module(ref.get("module"))
        for part in (ref.get("qualname") or "").split("."):
            if not part:
                return None
            obj = getattr(obj, part)
        return obj if callable(obj) else None
    except (ImportError, AttributeError, TypeError):
        return None


def summarize_response(response, proxy_url=None):
    """
    Build a persistable summary of a successful response

    :param requests.Response response:
    :param str|None proxy_url:
    :return dict:
    """
    headers = getattr(response, "headers", None) or {}
    summary = {
        "status_code": getattr(response, "status_code", None),
        "reason": getattr(response, "reason", None),
        "url": getattr(response, "url", None),
        "headers": {header: headers[header] for header in SUMMARY_HEADERS if header in headers},
        "proxy": remove_proxy_credentials(proxy_url),
    }
    return summary


def summarize_error(exc, proxy_url=None):
    """
    Build a persistable summary of a failed attempt's exception

    :param Exception exc:
    :param str|None proxy_url:
    :return dict:
    """
    return {
        "error_class": type(exc).__name__,
        "error_module": type(exc).__module__,
        "error_message": str(exc)[:2000],
        "proxy": remove_proxy_credentials(proxy_url),
    }


def rehydrate_error(summary):
    """
    Reconstruct the best possible exception from an error summary

    The original exception class is returned when it is importable and can be
    constructed from a message; otherwise a RecoveredRequestError carrying the
    original class name is used.

    :param dict summary:
    :return Exception:
    """
    summary = summary or {}
    message = summary.get("error_message") or ""
    class_name = summary.get("error_class")
    module_name = summary.get("error_module")

    if class_name and module_name:
        try:
            obj = importlib.import_module(module_name)
            for part in class_name.split("."):
                obj = getattr(obj, part)
            if isinstance(obj, type) and issubclass(obj, Exception):
                try:
                    return obj(message)
                except TypeError:
                    return RecoveredRequestError(message, class_name)
        except (ImportError, AttributeError, TypeError):
            pass

    return RecoveredRequestError(message, class_name)


class QueuedRequest:
    """
    One URL in a delegation queue, with durable bookkeeping metadata
    """

    def __init__(self, url, kwargs, status, index, durable=False, row_id=None, seq=None,
                 skip=False, stored_kwargs=None):
        self.url = url
        self.kwargs = kwargs
        self.status = status
        self.index = index
        self.durable = durable
        self.row_id = row_id
        self.seq = seq
        # skip placeholders represent URLs already delivered in an earlier
        # attempt; they are removed silently and never yielded
        self.skip = skip
        # persistable kwargs, computed before in-memory hook wrapping
        self.stored_kwargs = stored_kwargs if stored_kwargs is not None else {}
        self.proxied = None


class InFlightRequest:
    """
    Bookkeeping for a request that has been started (an external attempt)
    """

    def __init__(self, request, created, proxy, url, index, attempt_no=0):
        self.request = request
        self.created = created
        self.result = None
        self.proxy = proxy
        self.url = url
        self.index = index
        self.attempt_no = attempt_no


class _BatchBinding:
    """
    In-process bookkeeping for one durable delegation batch

    A batch is owned by one processor job (its key includes the job id).
    """

    def __init__(self, key, processor_type, dataset_key, job_id, durable, hook_binder=None):
        self.key = key
        self.processor_type = processor_type
        self.dataset_key = dataset_key
        self.job_id = job_id
        # durable is downgraded to False when the database cannot be used
        self.durable = durable
        self.hook_binder = hook_binder
        self.generation = 0
        # URLs currently present in the in-memory queue and not delivered
        self.url_rows = {}
        # URLs already delivered (or explicitly discarded) in this batch
        self.delivered_urls = set()


class ProxyStatus:
    """
    An enum of possible statuses of a SophisticatedFuturesProxy
    """

    AVAILABLE = 3
    CLAIMED = 4
    RUNNING = 5
    COOLING_OFF = 6


class SophisticatedFuturesProxy:
    """
    A proxy that can be used in combination with the DelegatedRequestHandler

    This keeps track of cooloffs, etc, to ensure that any individual proxy does
    not request more often than it should. This is a separate class because of
    an additional piece of logic that allows this cooloff to be kept track of
    on a per-hostname basis. This is useful because rate limits are typically
    enforced per site, so we can have (figuratively) unlimited concurrent
    request as long as each is on a separate hostname, but need to be more
    careful when requesting from a single host.
    """

    log = None
    looping = True

    COOLOFF = 0
    MAX_CONCURRENT_OVERALL = 0
    MAX_CONCURRENT_PER_HOST = 0

    def __init__(
        self,
        url,
        log=None,
        cooloff=3,
        concurrent_overall=5,
        concurrent_host=2,
        healthcheck_url=None,
        healthcheck_timeout=5,
    ):
        self.proxy_url = url
        self.hostnames = {}
        self.log = log
        self.healthcheck_url = healthcheck_url
        self.healthcheck_timeout = healthcheck_timeout

        self.COOLOFF = cooloff
        self.MAX_CONCURRENT_OVERALL = concurrent_overall
        self.MAX_CONCURRENT_PER_HOST = concurrent_host

    def probe(self):
        """
        Probe proxy health via a known-good endpoint.

        :return bool: True if probe succeeds, False otherwise.
        """
        if not self.healthcheck_url:
            return False

        proxy_definition = (
            {"http": self.proxy_url, "https": self.proxy_url}
            if self.proxy_url != DelegatedRequestHandler.PROXY_LOCALHOST
            else None
        )

        try:
            response = requests.get(
                self.healthcheck_url,
                timeout=self.healthcheck_timeout,
                proxies=proxy_definition,
            )
            return response.ok
        except requests.exceptions.RequestException:
            return False

    def know_hostname(self, url):
        """
        Make sure the hostname is known to this proxy

        This means that we can now keep track of some per-hostname statistics
        for this hostname. If the hostname is not known yet, the statistics are
        re-initialised.

        :param str url:  URL with host name to keep stats for. Case-insensitive.
        :param str:  The host name, as parsed for internal use
        """
        hostname = ural.get_hostname(url).lower()
        if hostname not in self.hostnames:
            self.hostnames[hostname] = namedtuple(
                "HostnameForProxiedRequests", ("running",)
            )
            self.hostnames[hostname].running = []

        return hostname

    def has_active_requests(self):
        """
        Check if this proxy has any truly active requests.

        Active means CLAIMED or RUNNING status - not COOLING_OFF.
        COOLING_OFF requests are essentially finished and waiting to be released,
        so they shouldn't prevent proxy removal.

        :return bool: True if proxy has requests that are claimed or running
        """
        for metadata in self.hostnames.values():
            for request in metadata.running:
                if request.status in (ProxyStatus.CLAIMED, ProxyStatus.RUNNING):
                    return True
        return False

    def release_cooled_off(self):
        """
        Release proxies that have finished cooling off.

        Proxies cool off for a certain amount of time after starting a request.
        This method removes cooled off requests, so that new ones may fill
        their slot.
        """
        for hostname, metadata in self.hostnames.copy().items():
            for request in metadata.running:
                if (
                    request.status == ProxyStatus.COOLING_OFF
                    and request.timestamp_finished < time.time() - self.COOLOFF
                ):
                    self.log.debug(
                        f"Releasing proxy {remove_proxy_credentials(self.proxy_url)} for host name {hostname}"
                    )
                    self.hostnames[hostname].running.remove(request)

                    # get rid of hostnames with no running or cooling off
                    # requests, else this might grow indefinitely
                    if len(self.hostnames[hostname].running) == 0:
                        del self.hostnames[hostname]

    def claim_for(self, url):
        """
        Try claiming a slot in this proxy for the given URL

        Whether a slot is available depends both on the overall concurrency
        limit, and the per-hostname limit. If both are not maxed out, fill
        the slot and return the proxy object.

        :param str url:  URL to proxy a request for.
        :return: `False` if no proxy is available, or the
        `SophisticatedFuturesProxy` object if one is.
        """
        self.release_cooled_off()
        hostname = self.know_hostname(url)

        total_running = sum([len(m.running) for h, m in self.hostnames.items()])
        if total_running >= self.MAX_CONCURRENT_OVERALL:
            return False

        if len(self.hostnames[hostname].running) < self.MAX_CONCURRENT_PER_HOST:
            request = namedtuple(
                "ProxiedRequest",
                ("url", "status", "timestamp_started", "timestamp_finished"),
            )
            request.url = url
            request.status = ProxyStatus.CLAIMED
            request.timestamp_started = 0
            request.timestamp_finished = 0
            self.hostnames[hostname].running.append(request)
            self.log.debug(
                f"Claiming proxy {remove_proxy_credentials(self.proxy_url)} for host name {hostname} ({len(self.hostnames[hostname].running)} of {self.MAX_CONCURRENT_PER_HOST} for host)"
            )
            return self
        else:
            return False

    def mark_request_started(self, url):
        """
        Mark a request for a URL as started

        This updates the status for the related slot. If no matching slot
        exists that is waiting for a request to start running, a `ValueError`
        is raised.

        :param str url:  URL of the proxied request.
        """
        hostname = self.know_hostname(url)

        for i, metadata in enumerate(self.hostnames[hostname].running):
            if metadata.status == ProxyStatus.CLAIMED and metadata.url == url:
                self.hostnames[hostname].running[i].status = ProxyStatus.RUNNING
                self.hostnames[hostname].running[i].timestamp_started = time.time()
                return

        raise ValueError(f"No proxy is waiting for a request with URL {url} to start!")

    def mark_request_finished(self, url):
        """
        Mark a request for a URL as finished

        This updates the status for the related slot. If no matching slot
        exists that is waiting for a request to finish, a `ValueError`
        is raised. After this, the proxy will be marked as cooling off, and is
        released after cooling off is completed.

        :param str url:  URL of the proxied request.
        """
        hostname = self.know_hostname(url)

        for i, metadata in enumerate(self.hostnames[hostname].running):
            if metadata.status == ProxyStatus.RUNNING and metadata.url == url:
                self.hostnames[hostname].running[i].timestamp_finished = time.time()
                self.hostnames[hostname].running[i].status = ProxyStatus.COOLING_OFF
                return

        raise ValueError(f"No proxy is currently running a request for URL {url}!")


class DelegatedRequestHandler:
    queue = {}
    session = None
    proxy_pool = {}
    proxy_settings = {}
    halted = set()
    log = None
    index = 0

    # some magic values
    REQUEST_STATUS_QUEUED = 0
    REQUEST_STATUS_STARTED = 1
    REQUEST_STATUS_WAITING_FOR_YIELD = 2
    PROXY_LOCALHOST = "__localhost__"

    def __init__(self, log, config, database=None):
        pool = ThreadPoolExecutor()
        self.session = FuturesSession(executor=pool)
        self.log = log
        self.config = config
        self.lock = threading.RLock()

        # durable delegation
        # a database may be injected (e.g. in tests); otherwise a dedicated
        # connection is created lazily from configuration
        self._db = database
        self._db_unavailable = False
        self.instance_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.bindings = {}
        self._last_sweep = 0

        # Proxy health tracking
        self.proxy_health = {}
        self.proxy_warnings_logged = set()
        self.all_proxies_failed_logged = False

        # Track proxies that should be removed but have active requests
        self.proxies_pending_removal = set()

        self.refresh_settings(config)

    @synchronized_method
    def refresh_settings(self, config):
        """
        Load proxy settings

        This is done on demand, so that we do not need a persistent
        configuration reader, which could make things complicated in
        thread-based contexts.

        Initializes proxy pool or updates if URLs have changed, preserving proxies with active requests.

        :param config:  Configuration reader
        """
        # Load new settings
        new_proxy_settings = {
            k: config.get(k) for k in ("proxies.urls", "proxies.cooloff", "proxies.concurrent-overall",
                                            "proxies.concurrent-host", "proxies.allow-localhost-fallback")
        }

        # Probe target for proxy health checks; defaults to local frontend robots endpoint
        healthcheck_url = config.get("proxies.healthcheck-url")
        if not healthcheck_url:
            server_name = config.get("flask.server_name")
            if server_name:
                scheme = "https" if config.get("flask.https") else "http"
                healthcheck_url = f"{scheme}://{server_name}/robots.txt"
        new_proxy_settings["proxies.healthcheck-url"] = healthcheck_url
        new_proxy_settings["proxies.healthcheck-timeout"] = config.get("proxies.healthcheck-timeout") or 5

        # durable delegation settings
        new_proxy_settings["proxies.durable-delegation"] = self._config_bool(
            config.get("proxies.durable-delegation"), True)
        new_proxy_settings["proxies.owner-lease"] = config.get("proxies.owner-lease") or 300
        new_proxy_settings["proxies.request-lease"] = config.get("proxies.request-lease") or 300
        new_proxy_settings["proxies.batch-expiry"] = config.get("proxies.batch-expiry") or 604800
        new_proxy_settings["proxies.record-retention"] = config.get("proxies.record-retention") or 2592000

        # Normalize empty config to localhost
        if not new_proxy_settings["proxies.urls"]:
            self.log.warning(
                "No proxies configured (proxies.urls is empty or None). "
                "Using localhost (direct connection) for all requests. "
                "Set Proxy URLs in setting to [\"__localhost__\"] to disable this warning."
            )
            new_proxy_settings["proxies.urls"] = [self.PROXY_LOCALHOST]

        # Check if proxy URLs have changed
        proxy_urls_changed = (not hasattr(self, 'proxy_settings') or
                             self.proxy_settings.get("proxies.urls") != new_proxy_settings["proxies.urls"])

        # Update settings
        self.proxy_settings = new_proxy_settings

        # Reset proxy health tracking when settings are refreshed
        num_proxies_restored = len([p for p in self.proxy_health if not self.proxy_health[p]])
        self.proxy_health = {}
        self.proxy_warnings_logged = set()
        self.all_proxies_failed_logged = False

        if num_proxies_restored > 0:
            self.log.debug(f"Proxy health reset: {num_proxies_restored} proxies restored")

        # Initialize or update proxy pool
        if not self.proxy_pool:
            # First initialization
            self._initialize_proxy_pool()
        elif proxy_urls_changed:
            # Settings changed - update pool
            self._update_proxy_pool()

    @staticmethod
    def _config_bool(value, default=False):
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).lower() in ("1", "true", "yes", "on")

    def _database(self):
        """
        Return the database connection used for durable delegation, if any

        A dedicated connection is created lazily from configuration. If it
        cannot be created, durability stays disabled for this backend and no
        error is raised: request delegation continues fully in memory.
        """
        if not self.proxy_settings.get("proxies.durable-delegation", True):
            return None
        if self._db is not None:
            return self._db
        if self._db_unavailable:
            return None

        try:
            from common.lib.database import Database

            config = self.config
            def _cfg(name):
                value = getattr(config, name, None)
                return value if value is not None else config.get(name)

            self._db = Database(
                logger=self.log,
                appname="4cat-delegator",
                dbname=_cfg("DB_NAME"),
                user=_cfg("DB_USER"),
                password=_cfg("DB_PASSWORD"),
                host=_cfg("DB_HOST"),
                port=_cfg("DB_PORT"),
            )
        except Exception as e:
            self.log.warning(f"Durable delegation is unavailable, continuing with in-memory request queues: {e}")
            self._db_unavailable = True

        return self._db

    def add_urls(self, urls, queue_name="_", position=-1, **kwargs):
        """
        Add URLs to the request queue

        :param urls:  An iterable of URLs.
        :param queue_name:  Queue name to add to.
        :param position:  Where in queue to insert; -1 adds to end of queue
        :param kwargs: Other keyword arguments will be passed on to
        `requests.get()`
        """
        with self.lock:
            if queue_name in self.halted or "_" in self.halted:
                # do not add URLs while delegator is shutting down
                return

            binding = self.bindings.get(queue_name)
            if binding is None or not binding.durable or self._database() is None:
                self._add_urls_memory(urls, queue_name, position, kwargs)
            else:
                self._add_urls_durable(urls, queue_name, position, kwargs, binding)

        self.manage_requests()

    def _inject_original_url_hook(self, per_kwargs, url):
        """
        Wrap response hooks so the original request URL is injected

        The requests library resolves redirects, so without this hooks cannot
        tell which queued URL a response belongs to.
        """
        hooks = per_kwargs.get("hooks")
        if not hooks or "response" not in hooks:
            return

        hooks = {**hooks}
        original_hook = hooks["response"]

        def wrap(h, original_url):
            def _wrapped(resp, *a, **k):
                # Inject FourCAT-specific original URL in kwargs once
                if "fourcat_original_url" not in k:
                    k["fourcat_original_url"] = original_url
                return h(resp, *a, **k)
            return _wrapped

        if isinstance(original_hook, list):
            hooks["response"] = [wrap(h, url) for h in original_hook]
        else:
            hooks["response"] = wrap(original_hook, url)

        per_kwargs["hooks"] = hooks

    def _make_metadata(self, url, kwargs, durable=False, row_id=None, seq=None,
                       skip=False, stored_kwargs=None):
        """
        Construct a QueuedRequest, taking a per-URL copy of the kwargs
        """
        per_kwargs = {**(kwargs or {})}
        self._inject_original_url_hook(per_kwargs, url)
        metadata = QueuedRequest(
            url=url,
            kwargs=per_kwargs,
            status=self.REQUEST_STATUS_QUEUED,
            index=self.index,
            durable=durable,
            row_id=row_id,
            seq=seq,
            skip=skip,
            stored_kwargs=stored_kwargs or {},
        )
        self.index += 1
        return metadata

    def _add_urls_memory(self, urls, queue_name, position, kwargs):
        """
        Original, purely in-memory enqueue path
        """
        if queue_name not in self.queue:
            self.queue[queue_name] = []

        for i, url in enumerate(urls):
            metadata = self._make_metadata(url, kwargs)
            if position == -1:
                self.queue[queue_name].append(metadata)
            else:
                self.queue[queue_name].insert(position + i, metadata)

    def _add_urls_durable(self, urls, queue_name, position, kwargs, binding):
        """
        Enqueue path for durable batches

        URLs already delivered in this batch become silent skip placeholders;
        URLs with an unfinished existing row are merged (mainly to re-attach
        non-persistable kwargs such as hooks); genuinely new URLs are persisted
        in input order.
        """
        db = self._database()
        queue = self.queue.setdefault(queue_name, [])
        now = time.time()
        expiry = self.proxy_settings.get("proxies.batch-expiry", 604800)

        fresh = []
        for i, url in enumerate(urls):
            existing = binding.url_rows.get(url)
            if existing is not None:
                # same URL is already part of this batch and not delivered:
                # merge the latest kwargs but keep identity, order and result
                stored = serializable_kwargs(kwargs)
                existing.kwargs = {**(kwargs or {})}
                self._inject_original_url_hook(existing.kwargs, url)
                existing.stored_kwargs = stored
                self._safe_execute(
                    binding,
                    "UPDATE delegated_requests SET kwargs = %s::jsonb, updated_at = %s WHERE id = %s",
                    (json.dumps(stored), now, existing.row_id),
                )
                continue

            if url in binding.delivered_urls:
                # delivered in a previous attempt of the same job: skip it
                metadata = self._make_metadata(url, kwargs, durable=True, skip=True)
                self._insert_at_position(queue, metadata, position, i)
                continue

            # persistable kwargs are captured before hooks get wrapped in
            # process-local closures
            stored = serializable_kwargs(kwargs)
            metadata = self._make_metadata(url, kwargs, durable=True, stored_kwargs=stored)
            fresh.append(metadata)

            if position == -1:
                queue.append(metadata)
            else:
                # position inserts are rare (single priority pushes); insert
                # and allocate their sequence immediately so neighbours are
                # always concrete
                queue.insert(position + i, metadata)
                metadata.seq = self._allocate_position_seq(binding, queue, metadata)

        if not fresh:
            return

        try:
            pending_append = [metadata for metadata in fresh if metadata.seq is None]
            if pending_append:
                # append: allocate evenly spaced sequences in one update
                result = db.fetchall(
                    """
                    UPDATE delegated_batches
                       SET next_seq = next_seq + %s, updated_at = %s, expires_at = %s
                     WHERE key = %s
                    RETURNING next_seq
                    """,
                    (SEQ_GAP * len(pending_append), now, now + expiry, binding.key),
                )
                if not result:
                    raise RuntimeError("delegation batch disappeared")
                new_next = float(result[0]["next_seq"])
                _, append_seqs = next_append_seqs(
                    new_next - SEQ_GAP * len(pending_append), len(pending_append)
                )
                for metadata, seq in zip(pending_append, append_seqs):
                    metadata.seq = seq

            seqs = [metadata.seq for metadata in fresh]
            inserted = db.fetchall(
                """
                INSERT INTO delegated_requests
                    (batch_key, seq, url, kwargs, created_at, updated_at)
                SELECT %s, s.seq, s.url, s.kwargs::jsonb, %s, %s
                  FROM unnest(
                         %s::double precision[],
                         %s::text[],
                         %s::text[]
                       ) AS s(seq, url, kwargs)
                RETURNING id, seq, url
                """,
                (
                    binding.key,
                    now,
                    now,
                    [float(seq) for seq in seqs],
                    [metadata.url for metadata in fresh],
                    [json.dumps(metadata.stored_kwargs) for metadata in fresh],
                ),
            )
            ids_by_seq = {float(row["seq"]): row["id"] for row in inserted}
            for metadata, seq in zip(fresh, seqs):
                metadata.seq = float(seq)
                metadata.row_id = ids_by_seq.get(float(seq))
                if metadata.row_id is None:
                    raise RuntimeError(f"did not get an id for delegated request {metadata.url}")
                binding.url_rows[metadata.url] = metadata

        except Exception as e:
            self.log.warning(
                f"Could not persist delegated requests for batch {binding.key}: {e}; "
                f"continuing with this batch in memory"
            )
            binding.durable = False
            for metadata in fresh:
                metadata.durable = False

    @staticmethod
    def _insert_at_position(queue, metadata, position, offset):
        if position == -1:
            queue.append(metadata)
        else:
            queue.insert(position + offset, metadata)

    def _allocate_position_seq(self, binding, queue, metadata):
        """
        Determine a sequence for a position-inserted durable request

        The metadata has already been inserted into the in-memory queue, so
        its neighbours there define the interval. Sequences of delivered
        requests remain in the table, so a midpoint that collides with
        historical data (or an insertion into an emptied queue) is allocated
        beyond the batch's sequence high-water mark instead; delivered rows
        are filtered out on recovery, so this only changes ordering relative
        to history that is never yielded again. The batch is re-sequenced
        into a fresh region if the local interval is used up.
        """
        index = queue.index(metadata)
        lower = queue[index - 1].seq if index > 0 else None
        upper = queue[index + 1].seq if index < len(queue) - 1 else None

        candidate = None
        if lower is not None or upper is not None:
            try:
                candidate = insert_seq(lower, upper)
            except ValueError:
                self._renormalize_seqs(binding, queue)
                index = queue.index(metadata)
                lower = queue[index - 1].seq if index > 0 else None
                upper = queue[index + 1].seq if index < len(queue) - 1 else None
                try:
                    candidate = insert_seq(lower, upper)
                except ValueError:
                    candidate = None

        if candidate is None or self._seq_exists(binding, candidate):
            return self._allocate_tail_seq(binding)
        return candidate

    def _seq_exists(self, binding, seq):
        """
        Check whether a (possibly delivered, possibly active) row uses seq
        """
        db = self._database()
        row = db.fetchone(
            "SELECT 1 FROM delegated_requests WHERE batch_key = %s AND seq = %s",
            (binding.key, seq),
        )
        return row is not None

    def _allocate_tail_seq(self, binding):
        """
        Allocate the next sequence beyond the batch's high-water mark
        """
        db = self._database()
        result = db.fetchall(
            "UPDATE delegated_batches SET next_seq = next_seq + %s WHERE key = %s RETURNING next_seq",
            (SEQ_GAP, binding.key),
        )
        if not result:
            raise RuntimeError("delegation batch disappeared")
        return float(result[0]["next_seq"])

    def _renormalize_seqs(self, binding, queue):
        """
        Re-assign evenly spaced sequences to every request of a batch

        Rows are placed in a fresh region beyond the batch high-water mark so
        sequences kept by delivered (historical) rows cannot collide.
        """
        db = self._database()
        now = time.time()
        persisted = [metadata for metadata in queue if metadata.row_id is not None]
        base = db.fetchone(
            "UPDATE delegated_batches SET next_seq = next_seq + %s, updated_at = %s "
            "WHERE key = %s RETURNING next_seq",
            (SEQ_GAP * len(persisted), now, binding.key),
        )
        if not base:
            raise RuntimeError("delegation batch disappeared")
        high_water = float(base[0]["next_seq"]) - SEQ_GAP * len(persisted)
        ids = [metadata.row_id for metadata in persisted]
        assigned = [high_water + SEQ_GAP * (i + 1) for i in range(len(persisted))]

        db.execute(
            """
            UPDATE delegated_requests AS r
               SET seq = s.seq, updated_at = %s
              FROM unnest(%s::bigint[], %s::double precision[]) AS s(id, seq)
             WHERE r.id = s.id
            """,
            (now, ids, assigned),
        )
        for metadata, seq in zip(persisted, assigned):
            metadata.seq = seq

    @synchronized_method
    def bind_batch(self, queue_name, processor_type, dataset_key, job_id, hook_binder=None):
        """
        Bind a queue to a durable delegation batch, taking it over if needed

        If the batch was owned by a backend whose lease has expired (e.g. after
        a restart), ownership is taken atomically and requests that were left
        in flight without a confirmable result are requeued. Already delivered
        requests are loaded as skip markers, finished-but-undelivered requests
        as recovered results, and queued requests in their original order.

        :raises DelegatedRequestConflict:  When the batch is owned by another
            backend whose lease is still valid.
        """
        if queue_name in self.bindings:
            return self.bindings[queue_name]

        binding = _BatchBinding(
            key=queue_name,
            processor_type=processor_type or "",
            dataset_key=dataset_key or "",
            job_id=job_id,
            durable=True,
            hook_binder=hook_binder,
        )

        db = self._database()
        if db is None:
            binding.durable = False
            self.bindings[queue_name] = binding
            return binding

        now = time.time()
        try:
            db.insert(
                "delegated_batches",
                {
                    "key": binding.key,
                    "processor_type": binding.processor_type,
                    "dataset_key": binding.dataset_key,
                    "job_id": binding.job_id,
                    "status": BATCH_ACTIVE,
                    "owner_id": self.instance_id,
                    "owner_generation": 1,
                    "lease_expires": now + self.proxy_settings.get("proxies.owner-lease", 300),
                    "next_seq": 0,
                    "expires_at": now + self.proxy_settings.get("proxies.batch-expiry", 604800),
                    "created_at": now,
                    "updated_at": now,
                },
                safe=True,
                constraints=("key",),
            )

            row = db.fetchone("SELECT * FROM delegated_batches WHERE key = %s", (binding.key,))
            if row is None:
                raise RuntimeError("delegated batch missing after insert")

            if row["owner_id"] == self.instance_id and row["status"] == BATCH_ACTIVE and \
                    row["lease_expires"] > now and row["owner_generation"] == 1 and \
                    row["created_at"] >= now - 1:
                # we just created it
                binding.generation = 1
            else:
                # atomically take over (or reactivate) the batch; the WHERE
                # clause fences off batches owned by a live other backend
                taken = db.fetchall(
                    """
                    UPDATE delegated_batches
                       SET owner_id = %s,
                           owner_generation = owner_generation + 1,
                           lease_expires = %s,
                           status = %s,
                           processor_type = COALESCE(NULLIF(%s, ''), processor_type),
                           dataset_key = COALESCE(NULLIF(%s, ''), dataset_key),
                           job_id = COALESCE(%s, job_id),
                           updated_at = %s,
                           expires_at = %s
                     WHERE key = %s
                       AND status IN (%s, %s, %s, %s)
                       AND (owner_id IS NULL OR owner_id = %s OR lease_expires <= %s)
                    RETURNING owner_generation
                    """,
                    (
                        self.instance_id,
                        now + self.proxy_settings.get("proxies.owner-lease", 300),
                        BATCH_ACTIVE,
                        binding.processor_type,
                        binding.dataset_key,
                        binding.job_id,
                        now,
                        now + self.proxy_settings.get("proxies.batch-expiry", 604800),
                        binding.key,
                        BATCH_ACTIVE, BATCH_COMPLETED, BATCH_CANCELLED, BATCH_EXPIRED,
                        self.instance_id,
                        now,
                    ),
                )
                if not taken:
                    raise DelegatedRequestConflict(
                        f"Delegated batch {binding.key} is owned by another live backend"
                    )
                binding.generation = taken[0]["owner_generation"]

            self._reconcile_batch(db, binding, now)
            self.bindings[queue_name] = binding

        except DelegatedRequestConflict:
            raise
        except Exception as e:
            self.log.warning(
                f"Could not take over durable delegation batch {binding.key}: {e}; "
                f"using an in-memory queue for this processor"
            )
            binding.durable = False
            self.bindings[queue_name] = binding

        return binding

    def _reconcile_batch(self, db, binding, now):
        """
        Requeue requests orphaned by a dead owner and load the batch state

        This process cannot hold futures for any request of a freshly bound
        batch, so any request still marked as in flight belongs to a dead
        attempt: it is requeued (its attempt recorded as abandoned) and may be
        retried on any proxy.
        """
        orphans = db.fetchall(
            """
            UPDATE delegated_requests
               SET status = %s, claimer = NULL, lease_expires = 0, updated_at = %s
             WHERE batch_key = %s
               AND status IN (%s, %s)
            RETURNING id, attempt_no, created_at
            """,
            (REQUEST_QUEUED, now, binding.key, REQUEST_CLAIMED, REQUEST_RUNNING),
        )
        if orphans:
            self._insert_attempts(
                db,
                [
                    (
                        orphan["id"],
                        orphan["attempt_no"],
                        None,
                        ATTEMPT_ABANDONED,
                        json.dumps({"reason": "owner was gone before the result was confirmed"}),
                        orphan["created_at"] or now,
                        now,
                    )
                    for orphan in orphans
                ],
            )

        binding.delivered_urls = {
            row["url"]
            for row in db.fetchall(
                "SELECT DISTINCT url FROM delegated_requests WHERE batch_key = %s AND delivered = TRUE",
                (binding.key,),
            )
        }

        queue = []
        rows = db.fetchall(
            """
            SELECT id, seq, url, kwargs, status, attempt_no, result_kind, response_summary
              FROM delegated_requests
             WHERE batch_key = %s
               AND delivered = FALSE
               AND status NOT IN (%s, %s)
             ORDER BY seq
            """,
            (binding.key, REQUEST_CANCELLED, REQUEST_EXPIRED),
        )
        for row in rows:
            metadata = self._metadata_from_row(row, binding)
            queue.append(metadata)
            binding.url_rows[metadata.url] = metadata

        self.queue[binding.key] = queue

    def _metadata_from_row(self, row, binding):
        """
        Build a QueuedRequest from a persisted request row
        """
        seq = float(row["seq"])
        row_id = row["id"]
        url = row["url"]
        attempt_no = row["attempt_no"] or 0

        if row["status"] in (REQUEST_SUCCEEDED, REQUEST_FAILED):
            summary = row["response_summary"] or {}
            if row["result_kind"] == RESULT_RESPONSE:
                result = RecoveredProxiedRequest(summary)
            else:
                result = FailedProxiedRequest(rehydrate_error(summary), summary.get("proxy"))

            inflight = InFlightRequest(
                request=None, created=0, proxy=None, url=url, index=self.index, attempt_no=attempt_no
            )
            inflight.result = result
            metadata = QueuedRequest(
                url=url,
                kwargs={},
                status=self.REQUEST_STATUS_WAITING_FOR_YIELD,
                index=self.index,
                durable=True,
                row_id=row_id,
                seq=seq,
            )
            metadata.proxied = inflight
            self.index += 1
            return metadata

        stored_kwargs = row["kwargs"] or {}
        kwargs = self._kwargs_from_stored(stored_kwargs, binding)
        self._inject_original_url_hook(kwargs, url)
        metadata = QueuedRequest(
            url=url,
            kwargs=kwargs,
            status=self.REQUEST_STATUS_QUEUED,
            index=self.index,
            durable=True,
            row_id=row_id,
            seq=seq,
            stored_kwargs=stored_kwargs if isinstance(stored_kwargs, dict) else {},
        )
        self.index += 1
        return metadata

    def _kwargs_from_stored(self, stored_kwargs, binding):
        """
        Reconstruct requests.get() kwargs from persisted kwargs

        Hook references are resolved through the processor's hook binder where
        needed; references that cannot be resolved are dropped.
        """
        stored_kwargs = stored_kwargs if isinstance(stored_kwargs, dict) else {}
        kwargs = {key: value for key, value in stored_kwargs.items() if key != "hooks"}

        hook_refs = (stored_kwargs.get("hooks") or {}).get(HOOK_REFS_MARKER)
        if hook_refs:
            hooks = {}
            for event, refs in hook_refs.items():
                callbacks = []
                for ref in refs:
                    callback = None
                    if ref.get("bind") == "processor" and binding.hook_binder:
                        callback = binding.hook_binder(ref)
                    if callback is None and ref.get("bind") != "processor":
                        callback = resolve_hook_ref(ref)
                    if callback is not None:
                        callbacks.append(callback)
                    else:
                        self.log.debug(
                            f"Could not restore response hook {ref.get('module')}.{ref.get('qualname')} "
                            f"for delegated batch {binding.key}"
                        )
                if callbacks:
                    hooks[event] = callbacks if len(callbacks) > 1 else callbacks[0]
            if hooks:
                kwargs["hooks"] = hooks

        return kwargs

    @synchronized_method
    def get_queue_length(self, queue_name="_"):
        """
        Get the length, of the queue

        :param str queue_name:  Queue name to get the length of
        :return int: Amount of URLs in the queue (regardless of status)
        """
        queue_length = 0
        for queue in list(self.queue.keys()):
            if queue == queue_name or queue_name == "_":
                queue_length += len([q for q in self.queue.get(queue, []) if not q.skip])

        return queue_length

    @synchronized_method
    def _update_proxy_pool(self):
        """
        Update proxy pool when settings change

        Adds new proxies, removes old ones, and preserves proxies with active requests.
        Updates settings for existing proxies that remain in the configuration.
        """
        new_proxy_urls = set(self.proxy_settings["proxies.urls"])
        current_proxy_urls = set(self.proxy_pool.keys())

        # Find proxies to add and remove
        proxies_to_add = new_proxy_urls - current_proxy_urls
        proxies_to_remove = current_proxy_urls - new_proxy_urls

        # Remove proxies that are no longer in config
        for proxy_url in proxies_to_remove:
            if proxy_url in self.proxy_pool:
                proxy_obj = self.proxy_pool[proxy_url].proxy
                # Check if proxy has truly active requests (CLAIMED or RUNNING, not COOLING_OFF)
                if not proxy_obj.has_active_requests():
                    # No active requests - remove immediately
                    del self.proxy_pool[proxy_url]
                    if proxy_url in self.proxy_health:
                        del self.proxy_health[proxy_url]
                    self.proxies_pending_removal.discard(proxy_url)
                    self.log.info(f"Removed proxy {remove_proxy_credentials(proxy_url)} (no longer in config)")
                else:
                    # Has active requests - mark for removal but don't delete yet
                    self.proxies_pending_removal.add(proxy_url)
                    self.log.warning(f"Proxy {remove_proxy_credentials(proxy_url)} marked for removal (has active requests, will not accept new requests)")

        # Update settings for existing proxies
        for proxy_url in (new_proxy_urls & current_proxy_urls):
            if proxy_url in self.proxy_pool:
                proxy_obj = self.proxy_pool[proxy_url].proxy
                proxy_obj.COOLOFF = self.proxy_settings["proxies.cooloff"]
                proxy_obj.MAX_CONCURRENT_OVERALL = self.proxy_settings["proxies.concurrent-overall"]
                proxy_obj.MAX_CONCURRENT_PER_HOST = self.proxy_settings["proxies.concurrent-host"]
                proxy_obj.healthcheck_url = self.proxy_settings.get("proxies.healthcheck-url")
                proxy_obj.healthcheck_timeout = self.proxy_settings.get("proxies.healthcheck-timeout", 5)

        # Add new proxies
        for proxy_url in proxies_to_add:
            self._add_proxy_to_pool(proxy_url)
            self.log.info(f"Added new proxy {remove_proxy_credentials(proxy_url)}")

        if proxies_to_add or proxies_to_remove:
            self.log.info(f"Proxy pool updated: {len(self.proxy_pool)} total proxies "
                         f"({len(proxies_to_add)} added, {len([p for p in proxies_to_remove if p not in self.proxy_pool])} removed)")

    @synchronized_method
    def _add_proxy_to_pool(self, proxy_url):
        """
        Add a proxy to the pool

        Helper method to avoid duplicating proxy creation logic.

        :param str proxy_url: The proxy URL to add (or PROXY_LOCALHOST for direct connection)
        """
        self.proxy_pool[proxy_url] = namedtuple("ProxyEntry", ("proxy", "last_used"))
        self.proxy_pool[proxy_url].proxy = SophisticatedFuturesProxy(
            proxy_url,
            self.log,
            self.proxy_settings.get("proxies.cooloff", 0.1),
            self.proxy_settings.get("proxies.concurrent-overall", 5),
            self.proxy_settings.get("proxies.concurrent-host", 2),
            self.proxy_settings.get("proxies.healthcheck-url"),
            self.proxy_settings.get("proxies.healthcheck-timeout", 5),
        )
        self.proxy_pool[proxy_url].last_used = 0
        self.proxy_health[proxy_url] = True

    @synchronized_method
    def claim_proxy(self, url):
        """
        Find a proxy to do the request with

        Finds a `SophisticatedFuturesProxy` that has an open slot for this URL.

        :param str url:  URL to proxy a request for
        :return SophisticatedFuturesProxy or None:
            - SophisticatedFuturesProxy if a proxy is available
            - None if all proxies are busy (retry later)
        :raises NoProxiesAvailableError: If all proxies unhealthy and fallback disabled
        """
        # Get healthy proxies (excluding those pending removal)
        healthy_proxies = [p for p in self.proxy_pool
                          if self.proxy_health.get(p, True)
                          and p not in self.proxies_pending_removal]

        # If no healthy proxies, check if we should create localhost
        if not healthy_proxies:
            allow_localhost = self.proxy_settings.get("proxies.allow-localhost-fallback", True)

            if allow_localhost and self.PROXY_LOCALHOST not in self.proxy_pool:
                # Create localhost as fallback
                if not self.all_proxies_failed_logged:
                    self.all_proxies_failed_logged = True
                    self.log.error("All configured proxies are unhealthy. Falling back to localhost (direct connection).")
                self._add_proxy_to_pool(self.PROXY_LOCALHOST)
                self.log.info("Created localhost proxy for direct connections")
                healthy_proxies = [self.PROXY_LOCALHOST]
            elif not allow_localhost:
                # Fallback disabled - fail
                if not self.all_proxies_failed_logged:
                    self.all_proxies_failed_logged = True
                    self.log.error("All proxies are unhealthy and localhost fallback is disabled.")
                raise NoProxiesAvailableError("All proxies unhealthy and localhost fallback disabled")
            else:
                # Localhost exists but unhealthy
                raise NoProxiesAvailableError("All proxies including localhost are unhealthy")

        # within the pool, find the least recently used healthy proxy that is available
        sorted_by_cooloff = sorted(
            healthy_proxies, key=lambda p: self.proxy_pool[p].last_used
        )
        for proxy_id in sorted_by_cooloff:
            claimed_proxy = self.proxy_pool[proxy_id].proxy.claim_for(url)
            if claimed_proxy:
                self.proxy_pool[proxy_id].last_used = time.time()
                return claimed_proxy

        # All proxies busy
        return None

    @synchronized_method
    def _initialize_proxy_pool(self):
        """
        Initialize the proxy pool with configured proxies

        Called once during refresh_settings(). If no proxies configured,
        adds localhost. Otherwise adds configured proxies.
        """
        proxies = self.proxy_settings.get("proxies.urls", [])

        # Handle empty/None proxy configuration - add localhost
        if not proxies:
            self.log.warning(
                "No proxies configured (proxies.urls is empty or None). "
                "Using localhost (direct connection) for all requests. "
                "Set Proxy URLs in setting to [\"__localhost__\"] to disable this warning."
            )
            self._add_proxy_to_pool(self.PROXY_LOCALHOST)
            return

        # Initialize configured proxies
        for proxy_url in proxies:
            self._add_proxy_to_pool(proxy_url)

        self.log.debug(f"Proxy pool initialized with {len(self.proxy_pool)} proxies.")

    @synchronized_method
    def manage_requests(self):
        """
        Manage requests asynchronously

        First, make sure proxy status is up to date; then go through the list
        of queued URLs and see if they have been requested, and release the
        proxy accordingly. If the URL is not being requested, and a proxy is
        available, start the request.

        Durable batches additionally persist claims, attempts, results and
        delivery state, fenced by claimer and attempt number so that responses
        arriving after a takeover are ignored. Note that this method does
        *not* return any requested data; `get_results()` does that in the
        original queue order.
        """
        # go through queue and look at the status of each URL
        for queue_name in list(self.queue.keys()):
            if queue_name not in self.queue:
                continue

            binding = self.bindings.get(queue_name)
            if binding is not None and not binding.durable:
                binding = None
            queue = self.queue[queue_name]

            # silently discard placeholders for URLs delivered in an earlier
            # attempt - they must never be yielded or re-requested
            skip_entries = [metadata for metadata in queue if metadata.skip]
            for metadata in skip_entries:
                queue.remove(metadata)

            halted_now = queue_name in self.halted or "_" in self.halted

            # iterate over a snapshot; stale entries may be removed while looping
            for url_metadata in list(queue):
                url = url_metadata.url

                if url_metadata.status == self.REQUEST_STATUS_WAITING_FOR_YIELD:
                    # waiting to be flushed or passed by `get_result()`
                    continue

                if url_metadata.proxied and url_metadata.proxied.request is not None \
                        and url_metadata.proxied.request.done():
                    # collect result and buffer it for yielding
                    # done() here doesn't necessarily mean the request finished
                    # successfully, just that it has returned - a timed out
                    # request will also be done()!
                    self.log.debug(f"Request for {url} finished, collecting result")
                    proxy = url_metadata.proxied.proxy
                    proxy_url = proxy.proxy_url if proxy is not None else None
                    proxy.mark_request_finished(url)

                    # Clean up proxies pending removal if they have no more active requests
                    if proxy is not None and proxy_url in self.proxies_pending_removal \
                            and proxy_url in self.proxy_pool:
                        proxy_obj = self.proxy_pool[proxy_url].proxy
                        # Check if proxy has truly active requests (not just cooling off)
                        if not proxy_obj.has_active_requests():
                            del self.proxy_pool[proxy_url]
                            if proxy_url in self.proxy_health:
                                del self.proxy_health[proxy_url]
                            self.proxies_pending_removal.discard(proxy_url)
                            self.log.info(f"Removed proxy {remove_proxy_credentials(proxy_url)} (completed all active requests)")

                    requeued = False
                    try:
                        response = url_metadata.proxied.request.result()
                        # annotate the response so processors can see which
                        # proxy (if any) handled the request, without its
                        # username and password
                        setattr(
                            response,
                            "_4cat_proxy",
                            remove_proxy_credentials(proxy_url),
                        )
                        url_metadata.proxied.result = response

                        # a zero-row update means we lost ownership: this is a
                        # late response from an attempt that was taken over or
                        # cancelled, and it must be ignored
                        accepted = self._persist_terminal_result(
                            binding, url_metadata, REQUEST_SUCCEEDED, RESULT_RESPONSE,
                            summarize_response(response, proxy_url),
                        )
                        if not accepted:
                            self._discard_stale(binding, queue, url_metadata)
                            continue

                    except requests.exceptions.ProxyError as e:
                        # Proxy connection issue - validate proxy with health probe first
                        probe_succeeded = proxy.probe() if proxy is not None else False
                        if probe_succeeded:
                            self.log.debug(
                                f"Proxy {remove_proxy_credentials(proxy_url)} returned a proxy error for {url}, "
                                f"but health probe to {proxy.healthcheck_url} succeeded; "
                                f"passing original error back to requester"
                            )
                            url_metadata.proxied.result = FailedProxiedRequest(
                                e, proxy_url
                            )
                            accepted = self._persist_terminal_result(
                                binding, url_metadata, REQUEST_FAILED, RESULT_ERROR,
                                summarize_error(e, proxy_url),
                            )
                            if not accepted:
                                self._discard_stale(binding, queue, url_metadata)
                                continue
                        else:
                            if proxy_url is not None:
                                self.proxy_health[proxy_url] = False

                                if proxy_url not in self.proxy_warnings_logged:
                                    self.proxy_warnings_logged.add(proxy_url)
                                    self.log.warning(
                                        f"Proxy {remove_proxy_credentials(proxy_url)} marked as unhealthy due to connection failure and failed health probe "
                                        f"({proxy.healthcheck_url}): {str(e)}"
                                    )

                            # proxy health is independent of request ownership:
                            # only this attempt fails, the request itself is
                            # requeued so another (healthy) proxy can take it
                            if self._persist_proxy_failure(binding, url_metadata, proxy_url, e):
                                url_metadata.status = self.REQUEST_STATUS_QUEUED
                                url_metadata.proxied = None
                                requeued = True
                            else:
                                self._discard_stale(binding, queue, url_metadata)
                                continue

                    except (
                        ConnectionError,
                        asyncioCancelledError,
                        futureCancelledError,
                        requests.exceptions.RequestException,
                        urllib3.exceptions.HTTPError,
                    ) as e:
                        # this is where timeouts, etc, go - the request has a
                        # confirmable (failed) result; other requests in the
                        # batch are untouched
                        url_metadata.proxied.result = FailedProxiedRequest(
                            e, proxy_url
                        )
                        accepted = self._persist_terminal_result(
                            binding, url_metadata, REQUEST_FAILED, RESULT_ERROR,
                            summarize_error(e, proxy_url),
                        )
                        if not accepted:
                            self._discard_stale(binding, queue, url_metadata)
                            continue

                    finally:
                        # success or fail, we can pass it on
                        # Only set to waiting if not requeued after a proxy error
                        if not requeued:
                            url_metadata.status = self.REQUEST_STATUS_WAITING_FOR_YIELD

                if not url_metadata.proxied and not halted_now and url_metadata in queue:
                    # no request running for this URL yet, try to start one
                    try:
                        proxy = self.claim_proxy(url)
                    except NoProxiesAvailableError as e:
                        # All proxies failed and fallback disabled - this
                        # request has a confirmable (failed) result
                        inflight = InFlightRequest(
                            request=None,
                            created=time.time(),
                            proxy=None,
                            url=url,
                            index=url_metadata.index,
                            attempt_no=0,
                        )
                        inflight.result = FailedProxiedRequest(e, None)
                        url_metadata.proxied = inflight
                        self._persist_unclaimed_failure(binding, url_metadata, e)
                        url_metadata.status = self.REQUEST_STATUS_WAITING_FOR_YIELD
                        continue

                    if proxy is None:
                        # No proxy available (all busy)
                        # Try again next loop iteration
                        continue

                    proxy_url = proxy.proxy_url

                    # record the claim before issuing the external request, so
                    # a crash in between is recoverable; a zero-row result
                    # means the request is no longer ours to make
                    attempt_no = 0
                    claimed = self._persist_claim(binding, url_metadata)
                    if binding is not None and binding.durable and claimed is None:
                        self._release_proxy_slot(proxy, url)
                        self._discard_stale(binding, queue, url_metadata)
                        continue
                    if claimed is not None:
                        attempt_no = claimed

                    proxy_definition = (
                        {"http": proxy_url, "https": proxy_url}
                        if proxy_url != self.PROXY_LOCALHOST
                        else None
                    )

                    # start request for URL
                    self.log.debug(f"Request for {url} started")
                    request = self.session.get(
                        **{
                            "url": url,
                            "timeout": 30,
                            "proxies": proxy_definition,
                            **url_metadata.kwargs
                        }
                    )

                    url_metadata.proxied = InFlightRequest(
                        request=request,
                        created=time.time(),
                        proxy=proxy,
                        url=url,
                        index=url_metadata.index,
                        attempt_no=attempt_no,
                    )
                    url_metadata.status = self.REQUEST_STATUS_STARTED

                    proxy.mark_request_started(url)

    def _release_proxy_slot(self, proxy, url):
        """
        Release a claimed proxy slot without issuing the request
        """
        try:
            proxy.mark_request_started(url)
            proxy.mark_request_finished(url)
        except ValueError:
            # the slot was already gone; nothing to do
            pass

    def _discard_stale(self, binding, queue, metadata):
        """
        Drop a late result whose attempt is no longer owned by us
        """
        self.log.debug(
            f"Ignoring late result for {metadata.url} in batch "
            f"{binding.key if binding else '?'}: attempt was taken over or cancelled"
        )
        if metadata in queue:
            queue.remove(metadata)
        if binding is not None:
            binding.url_rows.pop(metadata.url, None)

    def _safe_execute(self, binding, query, replacements):
        """
        Run a persistence statement that is allowed to degrade the batch
        """
        db = self._database()
        if db is None or binding is None or not binding.durable:
            return None
        try:
            return db.execute(query, replacements)
        except Exception as e:
            self._disable_durability(binding, e)
            return None

    def _disable_durability(self, binding, error):
        """
        Give up on persistence for one batch after a database error
        """
        if binding is not None and binding.durable:
            self.log.warning(
                f"Delegation batch {binding.key} continues in memory only after a database error: {error}"
            )
            binding.durable = False

    def _persist_claim(self, binding, metadata):
        """
        Persist that an external attempt is starting

        :return int|None:  the attempt number; None if the claim was lost
            (request cancelled/taken over), or persistence is not in use
        """
        if binding is None or not binding.durable:
            return None
        db = self._database()
        if db is None:
            binding.durable = False
            return None
        now = time.time()
        try:
            claimed = db.fetchall(
                """
                UPDATE delegated_requests
                   SET status = %s, claimer = %s, attempt_no = attempt_no + 1,
                       lease_expires = %s, updated_at = %s
                 WHERE id = %s
                   AND status = %s
                   AND delivered = FALSE
                RETURNING attempt_no
                """,
                (
                    REQUEST_RUNNING,
                    self.instance_id,
                    now + self.proxy_settings.get("proxies.request-lease", 300),
                    now,
                    metadata.row_id,
                    REQUEST_QUEUED,
                ),
            )
            return claimed[0]["attempt_no"] if claimed else None
        except Exception as e:
            self._disable_durability(binding, e)
            return None

    def _persist_terminal_result(self, binding, metadata, request_status, result_kind, summary):
        """
        Persist a confirmable result, fenced by claimer and attempt number

        :return bool:  False when this is a late response for an attempt no
            longer owned by this backend (it must be ignored)
        """
        if binding is None or not binding.durable:
            return True
        db = self._database()
        if db is None:
            binding.durable = False
            return True

        now = time.time()
        inflight = metadata.proxied
        attempt_no = inflight.attempt_no if inflight is not None else 0
        proxy_url = inflight.proxy.proxy_url if inflight is not None and inflight.proxy is not None else None
        outcome = ATTEMPT_SUCCEEDED if result_kind == RESULT_RESPONSE else ATTEMPT_REQUEST_ERROR

        try:
            updated = db.fetchall(
                """
                UPDATE delegated_requests
                   SET status = %s, result_kind = %s, response_summary = %s::jsonb,
                       lease_expires = 0, updated_at = %s
                 WHERE id = %s
                   AND claimer = %s
                   AND attempt_no = %s
                   AND status IN (%s, %s)
                RETURNING id
                """,
                (
                    request_status,
                    result_kind,
                    json.dumps(summary),
                    now,
                    metadata.row_id,
                    self.instance_id,
                    attempt_no,
                    REQUEST_CLAIMED,
                    REQUEST_RUNNING,
                ),
            )
            if not updated:
                return False

            self._insert_attempt(
                db,
                metadata.row_id,
                attempt_no,
                proxy_url,
                outcome,
                summary,
                inflight.created if inflight is not None else now,
                now,
            )
            return True
        except Exception as e:
            self._disable_durability(binding, e)
            return True

    def _persist_unclaimed_failure(self, binding, metadata, exc):
        """
        Persist a failure decided before a proxy was claimed (no proxies)
        """
        if binding is None or not binding.durable:
            return
        db = self._database()
        if db is None:
            binding.durable = False
            return

        now = time.time()
        summary = summarize_error(exc, None)
        try:
            updated = db.fetchall(
                """
                UPDATE delegated_requests
                   SET status = %s, result_kind = %s, response_summary = %s::jsonb,
                       lease_expires = 0, updated_at = %s
                 WHERE id = %s
                   AND status = %s
                   AND delivered = FALSE
                RETURNING id
                """,
                (REQUEST_FAILED, RESULT_ERROR, json.dumps(summary), now,
                 metadata.row_id, REQUEST_QUEUED),
            )
            if updated:
                self._insert_attempt(db, metadata.row_id, 0, None, ATTEMPT_REQUEST_ERROR,
                                     summary, now, now)
        except Exception as e:
            self._disable_durability(binding, e)

    def _persist_proxy_failure(self, binding, metadata, proxy_url, exc):
        """
        Record a failed proxy attempt and requeue the request

        Only this attempt is recorded as failed; the request becomes queued
        again and may be served by another proxy.

        :return bool:  False when the attempt is no longer ours
        """
        if binding is None or not binding.durable:
            return True
        db = self._database()
        if db is None:
            binding.durable = False
            return True

        now = time.time()
        inflight = metadata.proxied
        attempt_no = inflight.attempt_no if inflight is not None else 0
        summary = summarize_error(exc, proxy_url)
        try:
            updated = db.fetchall(
                """
                UPDATE delegated_requests
                   SET status = %s, claimer = NULL, lease_expires = 0, updated_at = %s
                 WHERE id = %s
                   AND claimer = %s
                   AND attempt_no = %s
                   AND status IN (%s, %s)
                RETURNING id
                """,
                (REQUEST_QUEUED, now, metadata.row_id, self.instance_id, attempt_no,
                 REQUEST_CLAIMED, REQUEST_RUNNING),
            )
            if not updated:
                return False

            self._insert_attempt(
                db, metadata.row_id, attempt_no, proxy_url,
                ATTEMPT_PROXY_ERROR, summary, inflight.created if inflight else now, now,
            )
            return True
        except Exception as e:
            self._disable_durability(binding, e)
            return True

    def _insert_attempt(self, db, request_id, attempt_no, proxy_url, outcome, summary,
                        started_at, finished_at):
        """
        Insert one external attempt row
        """
        try:
            db.insert(
                "delegated_attempts",
                {
                    "request_id": request_id,
                    "attempt_no": attempt_no,
                    "proxy": remove_proxy_credentials(proxy_url),
                    "outcome": outcome,
                    "summary": json.dumps(summary or {}),
                    "started_at": started_at or finished_at,
                    "finished_at": finished_at,
                },
                safe=True,
                constraints=("request_id", "attempt_no"),
            )
        except Exception as e:
            # attempt history must never break delivery
            self.log.debug(f"Could not record delegated attempt for request {request_id}: {e}")

    @staticmethod
    def _insert_attempts(db, values):
        """
        Bulk insert attempt rows (used during takeover/cancel/expiry)
        """
        if not values:
            return
        db.execute(
            """
            INSERT INTO delegated_attempts
                (request_id, attempt_no, proxy, outcome, summary, started_at, finished_at)
            SELECT s.request_id, s.attempt_no, s.proxy, s.outcome, s.summary::jsonb,
                   s.started_at, s.finished_at
              FROM unnest(
                     %s::bigint[], %s::integer[], %s::text[], %s::text[],
                     %s::text[], %s::double precision[], %s::double precision[]
                   ) AS s(request_id, attempt_no, proxy, outcome, summary, started_at, finished_at)
            ON CONFLICT (request_id, attempt_no) DO NOTHING
            """,
            (
                [value[0] for value in values],
                [value[1] for value in values],
                [value[2] for value in values],
                [value[3] for value in values],
                [value[4] for value in values],
                [value[5] for value in values],
                [value[6] for value in values],
            ),
        )

    def get_results(self, queue_name="_", preserve_order=True):
        """
        Return available results, without skipping

        Loops through the queue, returning values (and updating the queue) for
        requests that have been finished. If a request is not finished yet, stop
        returning. This ensures that in the end, values are only ever returned
        in the original queue order, at the cost of potential buffering.

        For durable batches, delivery is gated by an atomic UPDATE so that
        duplicate flushers or concurrent backends can never deliver the same
        result twice.

        :param str queue_name:  Queue name to get results from
        :param bool preserve_order:  Return results in the order they were
        added to the queue. This means that other results are buffered and
        potentially remain in the queue, which may in the worst case
        significantly slow down data collection.
        """
        self.manage_requests()

        with self.lock:
            # no results, no return
            if queue_name not in self.queue:
                return

            binding = self.bindings.get(queue_name)
            if binding is not None and not binding.durable:
                binding = None
            queue = self.queue[queue_name]

            # use a snapshot to determine which finished results come next;
            # with ordered delivery, stop at the first unfinished request
            candidates = []
            for url_metadata in list(queue):
                if url_metadata.status == self.REQUEST_STATUS_WAITING_FOR_YIELD and not url_metadata.skip:
                    candidates.append(url_metadata)
                    if preserve_order:
                        break
                elif preserve_order:
                    # ...but as soon as a URL has no finished result, return
                    break

            delivered_ids = set()
            if binding is not None and candidates:
                db = self._database()
                if db is not None:
                    ids = [metadata.row_id for metadata in candidates if metadata.row_id is not None]
                    if ids:
                        try:
                            delivered_rows = db.fetchall(
                                """
                                UPDATE delegated_requests
                                   SET delivered = TRUE, delivered_at = %s
                                 WHERE id = ANY(%s)
                                   AND delivered = FALSE
                                   AND status IN (%s, %s)
                                RETURNING id
                                """,
                                (time.time(), ids, REQUEST_SUCCEEDED, REQUEST_FAILED),
                            )
                            delivered_ids = {row["id"] for row in delivered_rows}
                        except Exception as e:
                            self._disable_durability(binding, e)
                            delivered_ids = set(ids)

            for url_metadata in candidates:
                if url_metadata not in queue:
                    continue

                queue.remove(url_metadata)
                if binding is not None:
                    binding.url_rows.pop(url_metadata.url, None)

                if binding is None or not binding.durable:
                    yield url_metadata.url, url_metadata.proxied.result
                elif url_metadata.row_id in delivered_ids:
                    binding.delivered_urls.add(url_metadata.url)
                    yield url_metadata.url, url_metadata.proxied.result
                # else: already delivered by another consumer; drop silently

    @synchronized_method
    def _halt(self, queue_name="_"):
        """
        Interrupt fetching of results (cancel semantics)

        Can be used when 4CAT is interrupted. Clears queue and tries to cancel
        running requests. Durable batches are marked cancelled: unclaimed
        requests are stopped, running attempts are recorded as cancelled and
        any responses that still arrive are ignored.

        Note that running requests *cannot* always be cancelled via `.cancel()`
        particularly when using `stream=True`. It is therefore recommended to
        use `halt_and_wait()` which is blocking until all running requests have
        properly terminated, instead of calling this method directly.

        :param str queue_name:  Queue name to stop fetching results for. By
        default, halt all queues.
        """
        self.halted.add(queue_name)

        for queue in list(self.queue.keys()):
            if queue_name != "_" and queue != queue_name:
                continue

            binding = self.bindings.get(queue)
            now = time.time()

            # use a list comprehension here to avoid having to modify the
            # list while iterating through it
            for url_metadata in [u for u in self.queue.get(queue, [])]:
                if url_metadata.status != self.REQUEST_STATUS_STARTED:
                    self.queue[queue].remove(url_metadata)
                    if binding is not None:
                        binding.url_rows.pop(url_metadata.url, None)
                else:
                    if url_metadata.proxied is not None and url_metadata.proxied.request is not None:
                        # do not also mark the proxy slot finished: if the
                        # cancelled future still completes, the result
                        # collection path does that exactly once
                        url_metadata.proxied.request.cancel()

            if binding is not None and binding.durable:
                self._cancel_batch_rows(binding, now)

        self.halted.discard(queue_name)

    def _cancel_batch_rows(self, binding, now=None):
        """
        Mark a batch and all of its live requests as cancelled in the database
        """
        now = now or time.time()
        db = self._database()
        if db is None:
            binding.durable = False
            return

        try:
            db.execute(
                "UPDATE delegated_batches SET status = %s, updated_at = %s WHERE key = %s AND status = %s",
                (BATCH_CANCELLED, now, binding.key, BATCH_ACTIVE),
            )

            # unclaimed requests are simply stopped
            db.execute(
                "UPDATE delegated_requests SET status = %s, updated_at = %s WHERE batch_key = %s AND status = %s",
                (REQUEST_CANCELLED, now, binding.key, REQUEST_QUEUED),
            )

            # in-flight attempts owned by us (or whose lease has lapsed) are
            # marked cancelled; a live new owner's attempts are left alone,
            # and late result writes are fenced off by this status
            db.execute(
                """
                INSERT INTO delegated_attempts
                    (request_id, attempt_no, proxy, outcome, summary, started_at, finished_at)
                SELECT id, attempt_no, NULL, %s, %s::jsonb, created_at, %s
                  FROM delegated_requests
                 WHERE batch_key = %s AND status IN (%s, %s) AND attempt_no > 0
                   AND (claimer IS NULL OR claimer = %s OR lease_expires <= %s)
                ON CONFLICT (request_id, attempt_no) DO NOTHING
                """,
                (ATTEMPT_CANCELLED, json.dumps({"reason": "batch cancelled"}), now,
                 binding.key, REQUEST_CLAIMED, REQUEST_RUNNING, self.instance_id, now),
            )
            db.execute(
                """
                UPDATE delegated_requests
                   SET status = %s, claimer = NULL, lease_expires = 0, updated_at = %s
                 WHERE batch_key = %s AND status IN (%s, %s)
                   AND (claimer IS NULL OR claimer = %s OR lease_expires <= %s)
                """,
                (REQUEST_CANCELLED, now, binding.key, REQUEST_CLAIMED, REQUEST_RUNNING,
                 self.instance_id, now),
            )

            # finished-but-undelivered results are explicitly discarded by a
            # flush; mark them disposed so they can never be resurrected
            db.execute(
                """
                UPDATE delegated_requests
                   SET delivered = TRUE, delivered_at = %s
                 WHERE batch_key = %s AND status IN (%s, %s) AND delivered = FALSE
                """,
                (now, binding.key, REQUEST_SUCCEEDED, REQUEST_FAILED),
            )
        except Exception as e:
            self._disable_durability(binding, e)

    def halt_and_wait(self, queue_name="_"):
        """
        Cancel any queued requests and wait until ongoing ones are finished

        Blocking!

        :param str queue_name:  Queue name to stop fetching results for. By
        default, halt all queues.
        """
        self._halt(queue_name)
        while self.get_queue_length(queue_name) > 0:
            # exhaust generator without doing something w/ results
            all(self.get_results(queue_name, preserve_order=False))

        with self.lock:
            if queue_name in self.queue:
                del self.queue[queue_name]
            self.bindings.pop(queue_name, None)

    @synchronized_method
    def detach_batch(self, queue_name="_"):
        """
        Release a durable batch so another backend can take it over

        Used when the owning processor is interrupted but its job will be
        retried. Unclaimed requests stay queued; in-flight attempts are
        recorded as abandoned and requeued, finished-but-undelivered results
        are kept for the takeover to deliver. Futures started by this process
        are cancelled as best effort and any response that still comes back is
        ignored by the claim/attempt fence.
        """
        binding = self.bindings.get(queue_name)
        if binding is None:
            # nothing durable to detach; fall back to plain cancellation
            self._halt(queue_name)
            self.queue.pop(queue_name, None)
            return

        self.halted.add(queue_name)
        now = time.time()
        try:
            queue = self.queue.get(queue_name, [])

            for url_metadata in [u for u in queue]:
                if url_metadata.status == self.REQUEST_STATUS_STARTED and url_metadata.proxied is not None:
                    # proxy slots are left for result collection; the queue
                    # metadata is dropped below so late results are ignored
                    if url_metadata.proxied.request is not None:
                        url_metadata.proxied.request.cancel()

            if binding.durable:
                db = self._database()
                if db is not None:
                    try:
                        db.execute(
                            """
                            UPDATE delegated_batches
                               SET owner_id = NULL, owner_generation = owner_generation + 1,
                                   lease_expires = 0, updated_at = %s
                             WHERE key = %s AND owner_id = %s
                            """,
                            (now, binding.key, self.instance_id),
                        )

                        orphans = db.fetchall(
                            """
                            UPDATE delegated_requests
                               SET status = %s, claimer = NULL, lease_expires = 0, updated_at = %s
                             WHERE batch_key = %s AND status IN (%s, %s)
                               AND (claimer IS NULL OR claimer = %s OR lease_expires <= %s)
                            RETURNING id, attempt_no, created_at
                            """,
                            (REQUEST_QUEUED, now, binding.key, REQUEST_CLAIMED, REQUEST_RUNNING,
                             self.instance_id, now),
                        )
                        self._insert_attempts(
                            db,
                            [
                                (
                                    orphan["id"],
                                    orphan["attempt_no"],
                                    None,
                                    ATTEMPT_ABANDONED,
                                    json.dumps({"reason": "owner detached before the result was confirmed"}),
                                    orphan["created_at"] or now,
                                    now,
                                )
                                for orphan in orphans
                            ],
                        )
                    except Exception as e:
                        self._disable_durability(binding, e)
        finally:
            self.halted.discard(queue_name)
            # the worker is ending; drop all in-memory state for the batch so
            # any late future callbacks cannot deliver results here
            self.queue.pop(queue_name, None)
            self.bindings.pop(queue_name, None)

    @synchronized_method
    def complete_batch(self, queue_name="_"):
        """
        Mark a durable batch as normally completed and release ownership

        Request rows are retained until record retention expires.
        """
        binding = self.bindings.pop(queue_name, None)
        now = time.time()
        self.queue.pop(queue_name, None)

        if binding is None or not binding.durable:
            return

        db = self._database()
        if db is None:
            return
        try:
            db.execute(
                """
                UPDATE delegated_batches
                   SET status = %s, owner_id = NULL, lease_expires = 0, updated_at = %s
                 WHERE key = %s AND owner_id = %s AND status = %s
                """,
                (BATCH_COMPLETED, now, binding.key, self.instance_id, BATCH_ACTIVE),
            )
            # anything still queued at this point is never going to run
            db.execute(
                "UPDATE delegated_requests SET status = %s, updated_at = %s WHERE batch_key = %s AND status = %s",
                (REQUEST_CANCELLED, now, binding.key, REQUEST_QUEUED),
            )
        except Exception as e:
            self.log.debug(f"Could not mark delegated batch {binding.key} completed: {e}")

    @synchronized_method
    def heartbeat(self):
        """
        Renew ownership leases and run throttled expiry/retention maintenance

        Called periodically (every second) by the worker manager from its own
        thread, so leases survive even when a processor thread is blocked on
        work between polls.
        """
        db = self._database()
        if db is None:
            return

        now = time.time()
        owner_lease = self.proxy_settings.get("proxies.owner-lease", 300)
        request_lease = self.proxy_settings.get("proxies.request-lease", 300)
        expiry = self.proxy_settings.get("proxies.batch-expiry", 604800)

        try:
            db.execute(
                """
                UPDATE delegated_batches
                   SET lease_expires = %s, updated_at = %s, expires_at = %s
                 WHERE owner_id = %s AND status = %s
                """,
                (now + owner_lease, now, now + expiry, self.instance_id, BATCH_ACTIVE),
            )
            db.execute(
                """
                UPDATE delegated_requests
                   SET lease_expires = %s
                 WHERE claimer = %s
                   AND status IN (%s, %s)
                   AND lease_expires < %s
                """,
                (now + request_lease, self.instance_id, REQUEST_CLAIMED, REQUEST_RUNNING,
                 now + request_lease / 2),
            )

            if now - self._last_sweep > 60:
                self._sweep(db, now)
                self._last_sweep = now
        except Exception as e:
            self.log.debug(f"Delegation heartbeat failed: {e}")

    def _sweep(self, db, now):
        """
        Expire stale batches and delete records past their retention
        """
        expired = db.fetchall(
            """
            UPDATE delegated_batches
               SET status = %s, owner_id = NULL, lease_expires = 0, updated_at = %s
             WHERE status = %s
               AND expires_at > 0
               AND expires_at < %s
               AND (owner_id IS NULL OR owner_id <> %s)
            RETURNING key
            """,
            (BATCH_EXPIRED, now, BATCH_ACTIVE, now, self.instance_id),
        )
        keys = [row["key"] for row in expired]
        if keys:
            db.execute(
                """
                INSERT INTO delegated_attempts
                    (request_id, attempt_no, proxy, outcome, summary, started_at, finished_at)
                SELECT r.id, r.attempt_no, NULL, %s, %s::jsonb, r.created_at, %s
                  FROM delegated_requests r
                 WHERE r.batch_key = ANY(%s) AND r.status IN (%s, %s) AND r.attempt_no > 0
                ON CONFLICT (request_id, attempt_no) DO NOTHING
                """,
                (ATTEMPT_EXPIRED, json.dumps({"reason": "batch expired before result was confirmed"}),
                 now, keys, REQUEST_CLAIMED, REQUEST_RUNNING),
            )
            db.execute(
                """
                UPDATE delegated_requests
                   SET status = %s, claimer = NULL, lease_expires = 0, updated_at = %s
                 WHERE batch_key = ANY(%s) AND status IN (%s, %s)
                """,
                (REQUEST_EXPIRED, now, keys, REQUEST_CLAIMED, REQUEST_RUNNING),
            )
            db.execute(
                """
                UPDATE delegated_requests
                   SET status = %s, updated_at = %s
                 WHERE batch_key = ANY(%s) AND status = %s
                """,
                (REQUEST_EXPIRED, now, keys, REQUEST_QUEUED),
            )
            self.log.info(f"Expired {len(keys)} stale delegated request batch(es)")

        retention = self.proxy_settings.get("proxies.record-retention", 2592000)
        deleted = db.execute(
            """
            DELETE FROM delegated_batches
             WHERE status IN (%s, %s, %s)
               AND updated_at < %s
            """,
            (BATCH_COMPLETED, BATCH_CANCELLED, BATCH_EXPIRED, now - retention),
        )
        if deleted is not None and getattr(deleted, "rowcount", 0) > 0:
            self.log.debug(f"Deleted {deleted.rowcount} delegated batches past their retention period")
