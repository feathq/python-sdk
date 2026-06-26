"""Live datafile streaming over Server-Sent Events (SSE).

The data plane exposes ``GET {url}/sdk/v1/datafile/stream`` which returns a
``text/event-stream``. On connect, and on every datafile change, the server
pushes a frame::

    event: put
    id: <datafile version number>
    data: <full datafile JSON>

Lines beginning with ``:`` are heartbeat comments and are ignored. The pushed
``data`` is the same datafile JSON served by ``GET /sdk/v1/datafile``.

This module is transport-agnostic: the worker is handed a callable that opens a
stream and returns an :class:`SSEStream`. The default transport (stdlib
``http.client``) lives in ``client.py``; tests inject a stub.
"""

import json
import random
import threading
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Callable, Protocol

# Reconnect backoff bounds (seconds). Each failed/closed connection waits a
# little longer, up to the cap, with jitter to avoid a thundering herd.
_INITIAL_BACKOFF_SECONDS = 1.0
_MAX_BACKOFF_SECONDS = 30.0
_BACKOFF_JITTER = 0.25


@dataclass
class SSEEvent:
    """A single dispatched Server-Sent Event."""

    event: str
    data: str
    id: str | None = None


class SSEStream(Protocol):
    """An open SSE response. Yields decoded text lines until the stream ends."""

    # HTTP status of the response (200 when the stream is established).
    status: int

    def iter_lines(self) -> Iterator[str]:
        """Yield decoded text lines (newline stripped is fine; the parser
        tolerates trailing CR/LF). Returns when the stream closes."""
        ...

    def close(self) -> None:
        """Release the underlying connection. Safe to call more than once."""
        ...


# A transport opens a stream given a URL and request headers.
SSETransport = Callable[[str, Mapping[str, str]], SSEStream]


def parse_sse(lines: Iterable[str]) -> Iterator[SSEEvent]:
    """Parse an SSE byte/line stream into dispatched events.

    Follows the event-stream framing: ``field: value`` lines accumulate into
    the current event; a blank line dispatches it. ``:`` comment lines (used
    for heartbeats) are ignored. Multiple ``data`` lines are joined with "\\n".
    A frame carrying no ``data`` does not dispatch (matching the spec).
    """
    event_type = "message"
    data_parts: list[str] = []
    last_id: str | None = None

    for raw in lines:
        line = raw.rstrip("\n").rstrip("\r")

        if line == "":
            if data_parts:
                yield SSEEvent(
                    event=event_type,
                    data="\n".join(data_parts),
                    id=last_id,
                )
            event_type = "message"
            data_parts = []
            continue

        if line.startswith(":"):
            # Comment / heartbeat.
            continue

        field, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]

        if field == "event":
            event_type = value
        elif field == "data":
            data_parts.append(value)
        elif field == "id":
            last_id = value
        # Other fields (e.g. "retry") are ignored.


class StreamWorker:
    """Holds an SSE connection and applies pushed datafiles.

    Runs a reconnect loop: open the stream, dispatch ``put`` events to the
    ``on_datafile`` callback, and on any close/error wait with exponential
    backoff before reconnecting. ``stop_event`` (shared with the owning client)
    halts the loop; :meth:`stop` also tears down the in-flight connection so a
    blocking read unblocks promptly on shutdown.
    """

    def __init__(
        self,
        *,
        transport: SSETransport,
        url: str,
        headers: Mapping[str, str],
        on_datafile: Callable[[dict[str, Any]], None],
        stop_event: threading.Event,
        initial_backoff_seconds: float = _INITIAL_BACKOFF_SECONDS,
        max_backoff_seconds: float = _MAX_BACKOFF_SECONDS,
        backoff_jitter: float = _BACKOFF_JITTER,
    ) -> None:
        self._transport = transport
        self._url = url
        self._headers = dict(headers)
        self._on_datafile = on_datafile
        self._stop = stop_event
        self._initial_backoff = initial_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._jitter = backoff_jitter
        self._stream_lock = threading.Lock()
        self._stream: SSEStream | None = None
        # Set once a stream reaches status 200; useful as a test signal.
        self.connected = threading.Event()

    def run(self) -> None:
        """Reconnect loop. Returns when ``stop_event`` is set."""
        backoff = self._initial_backoff
        while not self._stop.is_set():
            connected = False
            try:
                connected = self._stream_once()
            except Exception:  # noqa: BLE001 - defensive: any failure -> reconnect
                connected = False
            if self._stop.is_set():
                break
            if connected:
                backoff = self._initial_backoff
            wait = backoff + random.uniform(0.0, backoff * self._jitter)
            if self._stop.wait(wait):
                break
            backoff = min(backoff * 2.0, self._max_backoff)

    def stop(self) -> None:
        """Close the in-flight stream (if any) to unblock a pending read.

        The caller is responsible for setting ``stop_event`` first.
        """
        with self._stream_lock:
            stream = self._stream
        if stream is not None:
            try:
                stream.close()
            except Exception:  # noqa: BLE001 - shutdown is best-effort
                pass

    def _stream_once(self) -> bool:
        """Open one connection and pump events until it closes.

        Returns True if a stream was established (status 200), regardless of
        how many events arrived, so the loop can reset its backoff.
        """
        stream = self._transport(self._url, self._headers)
        with self._stream_lock:
            if self._stop.is_set():
                stream.close()
                return False
            self._stream = stream
        try:
            if stream.status != 200:
                return False
            self.connected.set()
            for event in parse_sse(stream.iter_lines()):
                if self._stop.is_set():
                    break
                if event.event == "put":
                    self._handle_put(event)
            return True
        finally:
            with self._stream_lock:
                self._stream = None
            try:
                stream.close()
            except Exception:  # noqa: BLE001 - close is best-effort
                pass

    def _handle_put(self, event: SSEEvent) -> None:
        try:
            data = json.loads(event.data)
        except (ValueError, TypeError):
            # Malformed frame: ignore and keep the connection.
            return
        if not isinstance(data, dict):
            return
        self._on_datafile(data)
