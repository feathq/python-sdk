"""Polling HTTP client. Uses stdlib only - zero deps."""

import http.client
import json
import socket
import ssl
import threading
import urllib.parse
from dataclasses import dataclass
from typing import Any

from .datafile import Datafile, from_json
from .eval import EvaluationResult, Reason, evaluate
from .types import EvalContext

MIN_POLL_INTERVAL_SECONDS = 5.0
MAX_DATAFILE_BYTES = 10 * 1024 * 1024
# Per-address connect budget. urllib / http.client pick one address from
# getaddrinfo and wait the full timeout before failing - no Happy Eyeballs
# - so worst-case for an N-address host is N times this value when every
# IP is blackholed. Kept tight on the assumption that a healthy CDN
# connect lands in well under a second.
_OPEN_TIMEOUT_SECONDS = 3.0
_READ_TIMEOUT_SECONDS = 10.0
_RETRYABLE_CONNECT_ERRORS = (
    TimeoutError,
    ConnectionRefusedError,
    # OSError covers socket.gaierror, socket.herror, EHOSTUNREACH,
    # ENETUNREACH, ECONNRESET-during-handshake, etc.
    OSError,
)


@dataclass
class ClientConfig:
    api_key: str
    data_plane_url: str
    poll_interval_seconds: float = 30.0
    # If True, start() spawns a background daemon thread that polls
    # on the configured cadence. Tests typically set this False and
    # drive refresh() manually.
    poll_in_background: bool = True


def _validate_https(url: str) -> None:
    """Reject non-https URLs (allow http://localhost / 127.0.0.1 for tests)."""
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError as exc:  # pragma: no cover - urlparse rarely raises
        raise ValueError("dataPlaneUrl is not a valid URL") from exc
    if parsed.scheme == "https":
        return
    if parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1"):
        return
    raise ValueError(
        "data_plane_url must use https:// (http://localhost allowed for tests)"
    )


class _IPHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection that connects to a specific IP while keeping the
    original hostname for SNI and certificate verification."""

    def __init__(
        self,
        host: str,
        port: int | None = None,
        *,
        ipaddr: str | None = None,
        context: ssl.SSLContext | None = None,
        connect_timeout: float = _OPEN_TIMEOUT_SECONDS,
        read_timeout: float = _READ_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(host, port=port, context=context, timeout=connect_timeout)
        self._ipaddr = ipaddr
        self._read_timeout = read_timeout

    def connect(self) -> None:
        target = (self._ipaddr or self.host, self.port)
        self.sock = socket.create_connection(target, timeout=self.timeout)
        if self._tunnel_host:
            self._tunnel()
        self.sock = self._context.wrap_socket(self.sock, server_hostname=self.host)
        # Tighter timeout for connect; broader budget for the response body.
        self.sock.settimeout(self._read_timeout)


class _IPHTTPConnection(http.client.HTTPConnection):
    """Plain-HTTP counterpart of _IPHTTPSConnection (used for loopback)."""

    def __init__(
        self,
        host: str,
        port: int | None = None,
        *,
        ipaddr: str | None = None,
        connect_timeout: float = _OPEN_TIMEOUT_SECONDS,
        read_timeout: float = _READ_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(host, port=port, timeout=connect_timeout)
        self._ipaddr = ipaddr
        self._read_timeout = read_timeout

    def connect(self) -> None:
        target = (self._ipaddr or self.host, self.port)
        self.sock = socket.create_connection(target, timeout=self.timeout)
        self.sock.settimeout(self._read_timeout)


class Client:
    """In-memory datafile cache with background polling.

    Construct, call ready() once (blocks for the first fetch), then
    evaluate(). Thread-safe: the datafile pointer is swapped atomically.
    """

    def __init__(self, config: ClientConfig) -> None:
        _validate_https(config.data_plane_url)
        if config.poll_interval_seconds < MIN_POLL_INTERVAL_SECONDS:
            config.poll_interval_seconds = MIN_POLL_INTERVAL_SECONDS
        self.config = config
        self._datafile: Datafile | None = None
        self._etag: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        # Last IP that successfully completed a connect. Tried first on
        # the next request to skip the per-address retry loop when the
        # resolved set contains an unreachable IP (e.g. CF anycast pop
        # blackholed behind some NATs). Cleared on connect failure so we
        # re-resolve.
        self._sticky_ip: str | None = None

    def ready(self) -> None:
        """Blocking initial fetch + start the background poller (if enabled)."""
        self._fetch_once()
        if self.config.poll_in_background and self._thread is None:
            self._thread = threading.Thread(target=self._poll_loop, daemon=True)
            self._thread.start()

    def close(self) -> None:
        self._stop.set()

    def refresh(self) -> bool:
        """Fetch once. Returns True if the datafile changed."""
        return self._fetch_once()

    def evaluate(
        self,
        flag_key: str,
        default_value: Any,
        context: EvalContext,
    ) -> EvaluationResult:
        df = self._datafile
        if df is None:
            return EvaluationResult(
                value=default_value,
                variation_id=None,
                reason=Reason.ERROR,
                error_message="client not ready: call ready() before evaluate",
            )
        return evaluate(flag_key, default_value, context, df)

    def get_boolean_value(self, flag_key: str, default: bool, context: EvalContext) -> bool:
        r = self.evaluate(flag_key, default, context)
        return r.value if isinstance(r.value, bool) else default

    def get_string_value(self, flag_key: str, default: str, context: EvalContext) -> str:
        r = self.evaluate(flag_key, default, context)
        return r.value if isinstance(r.value, str) else default

    def get_number_value(self, flag_key: str, default: float, context: EvalContext) -> float:
        r = self.evaluate(flag_key, default, context)
        if isinstance(r.value, bool):  # bool isinstance of int - exclude
            return default
        return r.value if isinstance(r.value, (int, float)) else default

    def get_object_value(self, flag_key: str, default: Any, context: EvalContext) -> Any:
        r = self.evaluate(flag_key, default, context)
        return r.value

    def _poll_loop(self) -> None:
        while not self._stop.wait(self.config.poll_interval_seconds):
            try:
                self._fetch_once()
            except Exception:  # noqa: BLE001 - defensive: keep polling on any error
                pass

    def _fetch_once(self) -> bool:
        from . import __version__

        url = self.config.data_plane_url.rstrip("/") + "/sdk/v1/datafile"
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        if host is None:
            raise RuntimeError(f"data_plane_url missing host: {url}")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "User-Agent": f"feat-sdk-python/{__version__}",
        }
        with self._lock:
            if self._etag:
                headers["If-None-Match"] = self._etag

        status, response_headers, body = self._request(host, port, parsed.scheme, path, headers)

        if status in (304, 404):
            return False
        if status != 200:
            raise RuntimeError(f"feat: fetch datafile failed: {status}")

        if len(body) > MAX_DATAFILE_BYTES:
            raise RuntimeError("datafile exceeds maximum allowed size")
        data = json.loads(body.decode("utf-8"))
        new_etag = response_headers.get("ETag") or response_headers.get("etag")
        with self._lock:
            self._datafile = from_json(data)
            if new_etag:
                self._etag = new_etag
        return True

    def _request(
        self,
        host: str,
        port: int,
        scheme: str,
        path: str,
        headers: dict[str, str],
    ) -> tuple[int, dict[str, str], bytes]:
        """Try the sticky IP first, then re-resolve and iterate. Falls
        back to the default resolver if getaddrinfo returns nothing."""
        with self._lock:
            sticky = self._sticky_ip

        if sticky:
            try:
                return self._do_request(host, port, scheme, path, headers, ipaddr=sticky)
            except _RETRYABLE_CONNECT_ERRORS:
                # Sticky IP is now unreachable - drop it and re-resolve.
                with self._lock:
                    if self._sticky_ip == sticky:
                        self._sticky_ip = None

        addresses = self._resolve_addresses(host, port)
        if not addresses:
            return self._do_request(host, port, scheme, path, headers, ipaddr=None)

        last_error: BaseException | None = None
        for ip in addresses:
            if ip == sticky:
                continue  # already tried above
            try:
                result = self._do_request(host, port, scheme, path, headers, ipaddr=ip)
                with self._lock:
                    self._sticky_ip = ip
                return result
            except _RETRYABLE_CONNECT_ERRORS as e:
                last_error = e
        # Re-raise the most recent connect error so callers see the real cause.
        assert last_error is not None  # noqa: S101 - invariant
        raise last_error

    @staticmethod
    def _resolve_addresses(host: str, port: int) -> list[str]:
        try:
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError:
            return []
        seen: set[str] = set()
        out: list[str] = []
        for info in infos:
            ip = info[4][0]
            if ip not in seen:
                seen.add(ip)
                out.append(ip)
        return out

    @staticmethod
    def _do_request(
        host: str,
        port: int,
        scheme: str,
        path: str,
        headers: dict[str, str],
        *,
        ipaddr: str | None,
    ) -> tuple[int, dict[str, str], bytes]:
        if scheme == "https":
            conn: http.client.HTTPConnection = _IPHTTPSConnection(
                host,
                port=port,
                ipaddr=ipaddr,
                context=ssl.create_default_context(),
            )
        else:
            conn = _IPHTTPConnection(host, port=port, ipaddr=ipaddr)
        try:
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            response_headers = {k: v for k, v in resp.getheaders()}
            length_header = response_headers.get("Content-Length") or response_headers.get(
                "content-length"
            )
            if length_header and int(length_header) > MAX_DATAFILE_BYTES:
                raise RuntimeError("datafile exceeds maximum allowed size")
            # +1 so we can detect oversized bodies without a Content-Length header.
            body = resp.read(MAX_DATAFILE_BYTES + 1)
            return status, response_headers, body
        finally:
            conn.close()
