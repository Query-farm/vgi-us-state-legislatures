"""Shared plumbing: ATTACH settings, polite HTTP, the download cache, pushdown and value helpers.

Every upstream URL can be overridden at ``ATTACH`` time, which is how the test suite
points the worker at recorded fixtures instead of the live services::

    ATTACH 'us_statelegs' (TYPE vgi, LOCATION '...',
        OPENSTATES_API_URL 'http://127.0.0.1:8765/v3',
        PEOPLE_ARCHIVE_URL 'http://127.0.0.1:8765/people.tar.gz',
        CACHE_DIR '/tmp/statelegs-cache');

Nothing is downloaded at import or ATTACH time: a dataset is fetched the first time a
function needs it, verified, and kept in the per-user cache directory.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import platformdirs
from vgi.table_filter_pushdown import AndFilter, ComparisonOp, ConstantFilter, Filter, InFilter

log = logging.getLogger(__name__)

USER_AGENT = "vgi-us-state-legislatures/0.1 (+https://query.farm)"
TIMEOUT_SECONDS = 120.0
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60.0

DEFAULT_OPENSTATES_API_URL = "https://v3.openstates.org"
DEFAULT_PEOPLE_ARCHIVE_URL = "https://codeload.github.com/openstates/people/tar.gz/refs/heads/main"
# Klarner, "State Legislative Election Returns, 1967-2016", doi:10.7910/DVN/3WZFK9 (CC0 1.0).
DEFAULT_KLARNER_URL = "https://dataverse.harvard.edu/api/access/datafile/3231005"
# Shor & McCarty, January 2025 update (CC BY-NC-SA 4.0): individual doi:10.7910/DVN/SGOQ7G,
# aggregate doi:10.7910/DVN/T53FFK.
DEFAULT_SHOR_MCCARTY_URL = "https://dataverse.harvard.edu/api/access/datafile/10853698"
DEFAULT_SHOR_MCCARTY_AGGREGATE_URL = "https://dataverse.harvard.edu/api/access/datafile/10853749"
DEFAULT_MAX_AGE_DAYS = 7


class UpstreamError(RuntimeError):
    """An upstream service returned an error we will not retry, or a download failed verification."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Where the worker reads from, resolved from the ATTACH options."""

    openstates_api_url: str = DEFAULT_OPENSTATES_API_URL
    people_archive_url: str = DEFAULT_PEOPLE_ARCHIVE_URL
    klarner_url: str = DEFAULT_KLARNER_URL
    shor_mccarty_url: str = DEFAULT_SHOR_MCCARTY_URL
    shor_mccarty_aggregate_url: str = DEFAULT_SHOR_MCCARTY_AGGREGATE_URL
    cache_dir: Path = Path(platformdirs.user_cache_dir("vgi-us-state-legislatures", appauthor=False))
    max_age_days: int = DEFAULT_MAX_AGE_DAYS
    offline: bool = False


def settings_from_options(options: dict[str, Any] | None) -> Settings:
    """Build `Settings` from ATTACH options (names lower-cased; absent or NULL means default)."""
    opts = {k.lower(): v for k, v in (options or {}).items() if v is not None}
    d = Settings()

    def url(name: str, default: str) -> str:
        return str(opts.get(name) or default).rstrip("/")

    return Settings(
        openstates_api_url=url("openstates_api_url", d.openstates_api_url),
        people_archive_url=url("people_archive_url", d.people_archive_url),
        klarner_url=url("klarner_url", d.klarner_url),
        shor_mccarty_url=url("shor_mccarty_url", d.shor_mccarty_url),
        shor_mccarty_aggregate_url=url("shor_mccarty_aggregate_url", d.shor_mccarty_aggregate_url),
        cache_dir=Path(str(opts["cache_dir"])).expanduser() if opts.get("cache_dir") else d.cache_dir,
        max_age_days=int(opts.get("max_age_days", d.max_age_days)),
        offline=bool(opts.get("offline", False)),
    )


def settings_of(params: Any) -> Settings:
    """Return the settings for a call, from its ATTACH options when it has any."""
    try:
        options = dict(params.attach_options or {})
    except Exception:  # not invoked through an ATTACHed catalog
        options = {}
    return settings_from_options(options)


# ---------------------------------------------------------------- HTTP


def _retry_delay(err: urllib.error.HTTPError | None, attempt: int) -> float:
    retry_after: str | None = err.headers.get("Retry-After") if err is not None else None
    if retry_after and retry_after.strip().isdigit():
        return float(min(float(retry_after.strip()), MAX_BACKOFF_SECONDS))
    return float(min(1.0 * 2**attempt, MAX_BACKOFF_SECONDS))


def http_get(url: str, headers: dict[str, str] | None = None) -> tuple[bytes, dict[str, str]]:
    """GET ``url``, retrying 429/5xx/network errors with backoff that honours ``Retry-After``.

    Args:
        url: Absolute URL.
        headers: Extra request headers.

    Returns:
        The body and the response headers (lower-cased names).

    Raises:
        urllib.error.HTTPError: A non-retryable status (the caller words the message), or
            retries exhausted on a retryable one.
        UpstreamError: The network failed on every attempt.

    """
    all_headers = {"User-Agent": USER_AGENT, **(headers or {})}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(url, headers=all_headers)
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                body: bytes = response.read()
                return body, {k.lower(): v for k, v in response.headers.items()}
        except urllib.error.HTTPError as err:
            if not (err.code == 429 or err.code >= 500) or attempt == MAX_ATTEMPTS:
                raise
            delay = _retry_delay(err, attempt)
            log.warning("HTTP %s from %s; retrying in %.1fs", err.code, _redact_url(url), delay)
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == MAX_ATTEMPTS:
                reason = getattr(err, "reason", err)
                raise UpstreamError(f"GET {_redact_url(url)} failed: {reason}") from err
            time.sleep(_retry_delay(None, attempt))
    raise UpstreamError(f"GET {_redact_url(url)}: retries exhausted")


def _redact_url(url: str) -> str:
    """Drop any ``apikey`` query parameter from a URL before it is logged."""
    parts = urllib.parse.urlsplit(url)
    query = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query) if k.lower() != "apikey"]
    return urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(query)))


# ---------------------------------------------------------------- download cache


@dataclass(frozen=True, slots=True)
class Dataset:
    """One runtime-downloaded file: where it comes from and how to check it."""

    key: str
    filename: str
    description: str
    licence: str
    source_page: str
    url_of: Callable[[Settings], str]
    verify: Callable[[bytes], None]
    # Static research datasets never change; the people archive is re-checked weekly.
    refreshes: bool = False


@dataclass(frozen=True, slots=True)
class CacheState:
    """What the cache holds for one dataset after an ``ensure`` call."""

    path: Path
    meta: dict[str, Any]
    status: str


_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(key: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(key, threading.Lock())


def meta_path(settings: Settings, ds: Dataset) -> Path:
    """Return the JSON sidecar recording where and when ``ds`` was downloaded."""
    return settings.cache_dir / f"{ds.filename}.meta.json"


def read_meta(settings: Settings, ds: Dataset) -> dict[str, Any]:
    """Read a dataset's cache sidecar; missing or corrupt reads as empty."""
    try:
        loaded = json.loads(meta_path(settings, ds).read_text())
        return loaded if isinstance(loaded, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


@contextlib.contextmanager
def _file_lock(path: Path) -> Iterator[None]:
    """Hold an exclusive advisory lock on ``path`` (POSIX) so concurrent workers download once."""
    try:
        import fcntl
    except ImportError:  # Windows: the per-process lock still applies
        yield
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def ensure_dataset(settings: Settings, ds: Dataset, *, force: bool = False) -> CacheState:
    """Make sure ``ds`` is cached, downloading it (or re-checking a refreshable one) when due.

    Args:
        settings: Endpoints and cache settings.
        ds: The dataset.
        force: Download again now, replacing the cached copy.

    Returns:
        The cached path, its sidecar metadata and what happened: 'cached', 'downloaded',
        'not_modified', 'stale' (a re-check failed, the old copy is served) or 'offline'.

    Raises:
        UpstreamError: No usable cached copy exists and the download failed or did not verify.

    """
    url = ds.url_of(settings)
    path = settings.cache_dir / ds.filename
    if settings.offline or (not force and not ds.refreshes and _fresh(settings, ds, path, url)):
        lock: Any = contextlib.nullcontext()
    else:
        lock = _file_lock(settings.cache_dir / f"{ds.filename}.lock")
    with _lock_for(str(path)), lock:
        meta = read_meta(settings, ds)
        exists = path.is_file() and meta.get("source_url") == url
        if exists and not force:
            if not ds.refreshes:
                return CacheState(path, meta, "cached")
            if time.time() - float(meta.get("checked_epoch", 0)) < settings.max_age_days * 86400:
                return CacheState(path, meta, "cached")
        if settings.offline:
            if exists:
                return CacheState(path, meta, "offline")
            raise UpstreamError(
                f"OFFLINE is set and the {ds.key} dataset is not cached in {settings.cache_dir}; "
                "attach without OFFLINE once to download it"
            )
        headers: dict[str, str] = {}
        if exists and not force and meta.get("etag"):
            headers["If-None-Match"] = str(meta["etag"])
        try:
            body, response_headers = http_get(url, headers)
        except urllib.error.HTTPError as err:
            if err.code == 304 and exists:
                meta = {**meta, "checked_epoch": time.time(), "checked_at": utc_now().isoformat()}
                _write_atomic(meta_path(settings, ds), json.dumps(meta, indent=1).encode())
                return CacheState(path, meta, "not_modified")
            if exists:
                log.warning("re-check of %s failed (HTTP %s); serving the cached copy", ds.key, err.code)
                return CacheState(path, meta, "stale")
            raise UpstreamError(f"downloading the {ds.key} dataset from {url} failed with HTTP {err.code}") from err
        except UpstreamError:
            if exists:
                log.warning("re-check of %s failed; serving the cached copy", ds.key)
                return CacheState(path, meta, "stale")
            raise
        ds.verify(body)
        settings.cache_dir.mkdir(parents=True, exist_ok=True)
        _write_atomic(path, body)
        now = utc_now()
        meta = {
            "dataset": ds.key,
            "source_url": url,
            "downloaded_at": now.isoformat(),
            "checked_at": now.isoformat(),
            "checked_epoch": time.time(),
            "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "etag": response_headers.get("etag", ""),
        }
        _write_atomic(meta_path(settings, ds), json.dumps(meta, indent=1).encode())
        log.info("downloaded %s (%d bytes) into %s", ds.key, len(body), path)
        return CacheState(path, meta, "downloaded")


def _fresh(settings: Settings, ds: Dataset, path: Path, url: str) -> bool:
    """Return True when a cached copy from ``url`` exists (no lock needed to serve it)."""
    return path.is_file() and read_meta(settings, ds).get("source_url") == url


# ---------------------------------------------------------------- pushdown


def flat_filters(filters: Any) -> list[Filter]:
    """Flatten the top-level AND of a pushed-down filter set into its conjuncts."""
    out: list[Filter] = []
    stack = list(filters.filters) if filters is not None else []
    while stack:
        f = stack.pop()
        if isinstance(f, AndFilter):
            stack.extend(f.children)
        else:
            out.append(f)
    return out


def pushed_values(params: Any, column_name: str) -> list[Any] | None:
    """Return the values a pushed ``column = c`` or ``column IN (...)`` predicate allows, if any."""
    try:
        filters = params.current_pushdown_filters
    except Exception:
        return None
    for f in flat_filters(filters):
        if getattr(f, "column_name", None) != column_name:
            continue
        if isinstance(f, ConstantFilter) and f.op == ComparisonOp.EQ:
            return [f.value.as_py()]
        if isinstance(f, InFilter):
            return list(f.values.to_pylist())
    return None


def pushed_range(params: Any, column_name: str) -> tuple[Any, Any]:
    """Return the tightest (low, high) inclusive bounds pushed down on ``column_name``."""
    lo: Any = None
    hi: Any = None
    try:
        filters = params.current_pushdown_filters
    except Exception:
        return None, None
    for f in flat_filters(filters):
        if not isinstance(f, ConstantFilter) or f.column_name != column_name:
            continue
        value = f.value.as_py()
        if value is None:
            continue
        if f.op in (ComparisonOp.GT, ComparisonOp.GE, ComparisonOp.EQ):
            lo = value if lo is None or value > lo else lo
        if f.op in (ComparisonOp.LT, ComparisonOp.LE, ComparisonOp.EQ):
            hi = value if hi is None or value < hi else hi
    return lo, hi


# ---------------------------------------------------------------- value helpers


def clean_str(value: Any) -> str | None:
    """Trim a string; empty and whitespace-only become NULL."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def to_date(value: Any) -> dt.date | None:
    """Parse 'YYYY-MM-DD' (or a leading date of a timestamp) leniently; 'YYYY' and 'YYYY-MM' are NULL."""
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = clean_str(value)
    if text is None or len(text) < 10:
        return None
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError:
        return None


def to_timestamp(value: Any) -> dt.datetime | None:
    """Parse an ISO-8601 timestamp; a naive one is taken as UTC."""
    text = clean_str(value)
    if text is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def utc_now() -> dt.datetime:
    """Return the current time, timezone-aware (UTC)."""
    return dt.datetime.now(dt.UTC)
