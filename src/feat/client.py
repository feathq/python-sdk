"""Polling + streaming HTTP client. Uses stdlib only - zero deps."""

import http.client
import json
import socket
import ssl
import threading
import urllib.parse
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from .datafile import Datafile, from_json
from .eval import EvaluationResult, Reason, evaluate
from .streaming import SSEStream, SSETransport, StreamWorker
from .types import EvalContext

MIN_POLL_INTERVAL_SECONDS = 5.0
MAX_DATAFILE_BYTES = 10 * 1024 * 1024
_OPEN_TIMEOUT_SECONDS = 3.0
_READ_TIMEOUT_SECONDS = 10.0
# Streaming connections are long-lived: the read timeout only needs to exceed
# the server's heartbeat interval so a wedged socket eventually errors out and
# triggers a reconnect.
_STREAM_READ_TIMEOUT_SECONDS = 90.0
_DEFAULT_STREAM_PATH = "/sdk/v1/datafile/stream"
_RETRYABLE_CONNECT_ERRORS = (
    TimeoutError,
    ConnectionRefusedError,
    OSError,
)


_DEFAULT_URL = "https://data-01.feat.so"


@dataclass
class ClientConfig:
    api_key: str
    # Optional. Defaults to the production endpoint. Override for region
    # pinning, staging, or local development.
    url: str = _DEFAULT_URL
    poll_interval_seconds: float = 30.0
    # If True, start() spawns a background daemon thread that polls
    # on the configured cadence. When streaming is enabled this poll is
    # the safety net behind the live stream. Tests typically set this
    # False and drive refresh() manually.
    poll_in_background: bool = True
    # If True (default), ready() also opens a live SSE stream that pushes
    # datafile updates as they happen; the background poll keeps running as
    # a fallback. Set False to rely on polling alone.
    streaming: bool = True
    # Path of the SSE endpoint, joined onto `url`.
    stream_path: str = _DEFAULT_STREAM_PATH


def _validate_https(url: str) -> None:
    """Reject non-https URLs (allow http://localhost / 127.0.0.1 for tests)."""
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError as exc:  # pragma: no cover - urlparse rarely raises
        raise ValueError("url is not a valid URL") from exc
    if parsed.scheme == "https":
        return
    if parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1"):
        return
    raise ValueError(
        "url must use https:// (http://localhost allowed for tests)"
    )


# Override host->IP for connect while keeping the original hostname for
# SNI and cert verification, which the base classes don't allow.
class _IPHTTPSConnection(http.client.HTTPSConnection):
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
        self.sock.settimeout(self._read_timeout)


class _IPHTTPConnection(http.client.HTTPConnection):
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


_MAX_SSE_LINE_BYTES = MAX_DATAFILE_BYTES + 1


class _HttpClientSSEStream:
    """Wraps an http.client streaming response as an SSEStream.

    Reads the body line by line without buffering the whole (never-ending)
    response. Each readline is capped so a hostile peer cannot exhaust memory.
    """

    def __init__(self, conn: http.client.HTTPConnection, resp: http.client.HTTPResponse) -> None:
        self._conn = conn
        self._resp = resp
        self.status: int = resp.status

    def iter_lines(self) -> Iterator[str]:
        while True:
            raw = self._resp.readline(_MAX_SSE_LINE_BYTES)
            if not raw:
                return
            yield raw.decode("utf-8", "replace")

    def close(self) -> None:
        try:
            self._resp.close()
        finally:
            self._conn.close()


class Client:
    """In-memory datafile cache with background polling.

    Construct, call ready() once (blocks for the first fetch), then
    evaluate(). Thread-safe: the datafile pointer is swapped atomically.
    """

    def __init__(
        self,
        config: ClientConfig,
        *,
        stream_transport: SSETransport | None = None,
    ) -> None:
        _validate_https(config.url)
        if config.poll_interval_seconds < MIN_POLL_INTERVAL_SECONDS:
            config.poll_interval_seconds = MIN_POLL_INTERVAL_SECONDS
        self.config = config
        self._datafile: Datafile | None = None
        self._etag: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._sticky_ip: str | None = None
        # Transport for the SSE stream. Injectable for tests; defaults to the
        # stdlib http.client implementation below.
        self._stream_transport: SSETransport = stream_transport or self._open_sse_stream
        self._stream_worker: StreamWorker | None = None
        self._stream_thread: threading.Thread | None = None

    def ready(self) -> None:
        """Blocking initial fetch, then start the background workers.

        With the default config this opens a live SSE stream and a fallback
        poll loop. The stream applies updates as they happen; the poll is the
        safety net if the stream drops.
        """
        self._fetch_once()
        if self.config.poll_in_background and self._thread is None:
            self._thread = threading.Thread(target=self._poll_loop, daemon=True)
            self._thread.start()
        if self.config.streaming and self._stream_thread is None:
            self._start_streaming()

    def _start_streaming(self) -> None:
        from . import __version__

        url = self.config.url.rstrip("/") + self.config.stream_path
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "User-Agent": f"feat-sdk-python/{__version__}",
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
        }
        self._stream_worker = StreamWorker(
            transport=self._stream_transport,
            url=url,
            headers=headers,
            on_datafile=self._adopt_from_stream,
            stop_event=self._stop,
        )
        self._stream_thread = threading.Thread(
            target=self._stream_worker.run, daemon=True, name="feat-stream"
        )
        self._stream_thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._stream_worker is not None:
            self._stream_worker.stop()

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

        url = self.config.url.rstrip("/") + "/sdk/v1/datafile"
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        if host is None:
            raise RuntimeError(f"url missing host: {url}")
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
        return self._apply_datafile(from_json(data), etag=new_etag)

    def _apply_datafile(self, df: Datafile, etag: str | None = None) -> bool:
        """Adopt `df` only if its version is strictly newer than the current.

        Lock-guarded and version-ordered so concurrent poll + stream updates
        never replace a newer datafile with an older one. `etag` (when given,
        i.e. from a poll response) is always recorded so conditional requests
        stay current even when the datafile itself is not adopted. Returns
        True if the datafile was adopted.
        """
        with self._lock:
            current = self._datafile
            adopt = current is None or df.version > current.version
            if adopt:
                self._datafile = df
            if etag:
                self._etag = etag
            return adopt

    def _adopt_from_stream(self, data: dict[str, Any]) -> None:
        """Apply a datafile pushed over the SSE stream (version-ordered)."""
        try:
            df = from_json(data)
        except (KeyError, TypeError, ValueError):
            # Malformed payload: keep the current datafile.
            return
        self._apply_datafile(df)

    # http.client doesn't iterate getaddrinfo results on connect failure,
    # so a host with one unreachable IP wedges every request until the
    # full timeout. Try sticky first, fall through on connect errors.
    def _request(
        self,
        host: str,
        port: int,
        scheme: str,
        path: str,
        headers: dict[str, str],
    ) -> tuple[int, dict[str, str], bytes]:
        with self._lock:
            sticky = self._sticky_ip

        if sticky:
            try:
                return self._do_request(host, port, scheme, path, headers, ipaddr=sticky)
            except _RETRYABLE_CONNECT_ERRORS:
                with self._lock:
                    if self._sticky_ip == sticky:
                        self._sticky_ip = None

        addresses = self._resolve_addresses(host, port)
        if not addresses:
            return self._do_request(host, port, scheme, path, headers, ipaddr=None)

        last_error: BaseException | None = None
        for ip in addresses:
            if ip == sticky:
                continue
            try:
                result = self._do_request(host, port, scheme, path, headers, ipaddr=ip)
                with self._lock:
                    self._sticky_ip = ip
                return result
            except _RETRYABLE_CONNECT_ERRORS as e:
                last_error = e
        assert last_error is not None  # noqa: S101
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
            body = resp.read(MAX_DATAFILE_BYTES + 1)
            return status, response_headers, body
        finally:
            conn.close()

    # Default SSE transport: open a streaming GET against the stream endpoint.
    # Unlike _do_request this keeps the connection open and hands back a
    # line-yielding wrapper instead of reading the whole body.
    def _open_sse_stream(self, url: str, headers: Mapping[str, str]) -> SSEStream:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        if host is None:
            raise RuntimeError(f"url missing host: {url}")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        request_headers = dict(headers)
        addresses: list[str | None] = list(self._resolve_addresses(host, port))
        if not addresses:
            addresses = [None]
        last_error: BaseException | None = None
        for ipaddr in addresses:
            conn: http.client.HTTPConnection
            if parsed.scheme == "https":
                conn = _IPHTTPSConnection(
                    host,
                    port=port,
                    ipaddr=ipaddr,
                    context=ssl.create_default_context(),
                    read_timeout=_STREAM_READ_TIMEOUT_SECONDS,
                )
            else:
                conn = _IPHTTPConnection(
                    host,
                    port=port,
                    ipaddr=ipaddr,
                    read_timeout=_STREAM_READ_TIMEOUT_SECONDS,
                )
            try:
                conn.request("GET", path, headers=request_headers)
                resp = conn.getresponse()
                return _HttpClientSSEStream(conn, resp)
            except _RETRYABLE_CONNECT_ERRORS as e:
                last_error = e
                conn.close()
        if last_error is not None:
            raise last_error
        raise RuntimeError("feat: could not open datafile stream")
