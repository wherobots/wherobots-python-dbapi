"""Bounded, best-effort session diagnostics after cursor failure delivery."""

import hashlib
import json
import logging
import random
import threading
import time
from collections import OrderedDict

import requests


_logger = logging.getLogger(__name__)
_MAX_WORKERS = 4
_MAX_ENTRIES = 128
_CACHE_SECONDS = 30.0
_MAX_RESPONSE_BYTES = 65536


class SessionDiagnostics:
    """Share in-flight and recent lookups by URL and authentication context.

    A hung DNS/TLS/HTTP operation retains its slot, so even failures that ignore
    socket timeouts cannot cause unbounded threads. At capacity we skip optional
    diagnostics. There are no retries, and no caller waits for a lookup.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple[str, bytes], float | None] = OrderedDict()
        self._active = 0

    def request(
        self, url: str, headers: dict[str, str] | None, session_id: str
    ) -> None:
        headers = dict(headers or {})
        # Distinct credentials must never share diagnostic responses. Keep only
        # a digest in the short-lived registry, never credentials in log output.
        context = hashlib.sha256(json.dumps(sorted(headers.items())).encode()).digest()
        key = (url, context)
        now = time.monotonic()
        with self._lock:
            for expired, deadline in list(self._entries.items()):
                if deadline is not None and deadline <= now:
                    del self._entries[expired]
            if key in self._entries:
                return
            if self._active >= _MAX_WORKERS:
                _logger.debug("Session diagnostics at capacity; skipping lookup")
                return
            if len(self._entries) >= _MAX_ENTRIES:
                # Never evict a running request: it must remain deduplicated.
                oldest = next(
                    k for k, deadline in self._entries.items() if deadline is not None
                )
                del self._entries[oldest]
            self._entries[key] = None
            self._active += 1

        def lookup() -> None:
            try:
                # Spread correlated failures across processes without delaying
                # execute(), fetch(), or connection shutdown.
                time.sleep(random.uniform(0.0, 0.5))
                details = self._fetch(url, headers)
                if details is not None:
                    status, message = details
                    _logger.warning(
                        "SQL session diagnostic (session=%s): status=%r; firstFailure=%r",
                        session_id,
                        status,
                        message,
                    )
            except Exception as exc:
                # Request exceptions may contain URLs/credentials. Log the type,
                # not arbitrary exception text or the authenticated request.
                _logger.debug(
                    "Session diagnostic lookup failed (%s)", type(exc).__name__
                )
            finally:
                with self._lock:
                    self._active -= 1
                    self._entries[key] = time.monotonic() + _CACHE_SECONDS

        try:
            threading.Thread(
                target=lookup, daemon=True, name="wherobots-session-diagnostic"
            ).start()
        except RuntimeError:
            with self._lock:
                self._active -= 1
                self._entries[key] = time.monotonic() + _CACHE_SECONDS
            _logger.debug("Could not start session diagnostic thread")

    @staticmethod
    def _fetch(
        url: str, headers: dict[str, str]
    ) -> tuple[str | None, str | None] | None:
        with requests.get(
            url, headers=headers, timeout=1.0, allow_redirects=False, stream=True
        ) as response:
            if response.status_code != 200:
                _logger.debug(
                    "Session diagnostic HTTP status: %s", response.status_code
                )
                return None
            data = bytearray()
            for chunk in response.iter_content(chunk_size=8192):
                if len(data) + len(chunk) > _MAX_RESPONSE_BYTES:
                    _logger.debug("Session diagnostic response exceeds byte limit")
                    return None
                data.extend(chunk)
        payload = json.loads(data)
        if not isinstance(payload, dict):
            return None
        status = payload.get("status")
        status = status[:128] if isinstance(status, str) else None
        failure = payload.get("firstFailure")
        message = failure.get("message") if isinstance(failure, dict) else None
        message = message[:4096] if isinstance(message, str) else None
        return (status, message) if status is not None or message is not None else None


session_diagnostics = SessionDiagnostics()
