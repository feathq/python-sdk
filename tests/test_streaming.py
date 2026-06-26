"""Live datafile streaming (SSE) tests.

Drive the streaming worker with a stubbed transport so no network is needed.
Covers SSE parsing, version-ordered adoption, the Authorization header,
reconnect/backoff, fallback wiring, and clean shutdown.
"""

import json
import threading
import time
from typing import Any

from feat import EvalContext
from feat.client import Client, ClientConfig
from feat.streaming import SSEEvent, StreamWorker, parse_sse


# --- fixtures / builders ---------------------------------------------------

TRUE_VAR = {"id": "var-true", "name": "true", "value": True}
FALSE_VAR = {"id": "var-false", "name": "false", "value": False}


def datafile_json(version: int, *, default_variation_id: str = FALSE_VAR["id"]) -> dict[str, Any]:
    """A minimal but complete datafile payload at a given version."""
    return {
        "schemaVersion": 1,
        "envId": "env-1",
        "envKey": "staging",
        "projectId": "proj-1",
        "version": version,
        "etag": f"etag-{version}",
        "generatedAt": "2026-06-26T00:00:00Z",
        "flags": {
            "checkout": {
                "id": "flag-1",
                "key": "checkout",
                "valueType": "boolean",
                "salt": "abcdef0123456789",
                "archived": False,
                "isEnabled": True,
                "offVariationId": FALSE_VAR["id"],
                "defaultVariationId": default_variation_id,
                "defaultRollout": None,
                "defaultBucketingContextKindKey": None,
                "variations": [TRUE_VAR, FALSE_VAR],
                "targets": [],
                "rules": [],
            }
        },
        "segments": {},
        "contextKinds": {
            "user": {
                "key": "user",
                "availableForRules": True,
                "availableForExperiments": True,
            }
        },
    }


def put_frame(version: int, **kwargs: Any) -> list[str]:
    """SSE lines for one `put` carrying the datafile at `version`."""
    payload = json.dumps(datafile_json(version, **kwargs))
    return ["event: put", f"id: {version}", f"data: {payload}", ""]


class StubStream:
    """An in-memory SSEStream yielding pre-baked lines."""

    def __init__(self, lines: list[str], status: int = 200) -> None:
        self._lines = lines
        self.status = status
        self.closed = False

    def iter_lines(self):
        for line in self._lines:
            yield line

    def close(self) -> None:
        self.closed = True


class StubTransport:
    """Callable transport that returns queued streams, one per connection.

    Records the headers it was called with. When the queue is exhausted it
    raises ConnectionError to simulate a connect failure (driving reconnect).
    """

    def __init__(self, streams: list[StubStream]) -> None:
        self._streams = list(streams)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, headers) -> StubStream:
        self.calls.append((url, dict(headers)))
        if not self._streams:
            raise ConnectionError("no more streams")
        return self._streams.pop(0)


def user_ctx(key: str) -> EvalContext:
    return EvalContext(kinds={"user": {"key": key}})


def make_client(transport, **config_overrides: Any) -> Client:
    cfg = ClientConfig(api_key="feat_sdk_test", url="https://localhost", **config_overrides)
    return Client(cfg, stream_transport=transport)


# --- SSE parser ------------------------------------------------------------


def test_parse_sse_basic_put():
    events = list(parse_sse(["event: put", "id: 7", "data: {\"a\":1}", ""]))
    assert events == [SSEEvent(event="put", data='{"a":1}', id="7")]


def test_parse_sse_ignores_heartbeat_comments():
    lines = [":", ": keep-alive", "event: put", "data: x", ""]
    events = list(parse_sse(lines))
    assert len(events) == 1
    assert events[0].event == "put"
    assert events[0].data == "x"


def test_parse_sse_multiple_events_and_multiline_data():
    lines = [
        "event: put",
        "data: line1",
        "data: line2",
        "",
        "event: put",
        "data: second",
        "",
    ]
    events = list(parse_sse(lines))
    assert [e.data for e in events] == ["line1\nline2", "second"]


def test_parse_sse_no_data_does_not_dispatch():
    # A frame with only a comment / no data fields must not dispatch.
    assert list(parse_sse([": ping", ""])) == []


def test_parse_sse_strips_single_leading_space():
    (event,) = list(parse_sse(["data:  two-spaces", ""]))
    # Only the first space after the colon is part of the framing.
    assert event.data == " two-spaces"


# --- adoption semantics ----------------------------------------------------


def test_put_newer_version_adopts_and_evaluation_reflects_it():
    # Seed the client at v1 where the flag is OFF, then push v2 where it is ON.
    transport = StubTransport([StubStream(put_frame(2, default_variation_id=TRUE_VAR["id"]))])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))
    assert client.get_boolean_value("checkout", False, user_ctx("u1")) is False

    worker = _worker(client, transport)
    worker._stream_once()

    assert client._datafile.version == 2
    assert client.get_boolean_value("checkout", False, user_ctx("u1")) is True


def test_put_equal_version_is_ignored():
    transport = StubTransport([StubStream(put_frame(5, default_variation_id=TRUE_VAR["id"]))])
    client = make_client(transport)
    # Current is v5 with the flag OFF; an incoming v5 (ON) must not replace it.
    client._apply_datafile(_from(datafile_json(5, default_variation_id=FALSE_VAR["id"])))

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 5
    assert client.get_boolean_value("checkout", True, user_ctx("u1")) is False


def test_put_older_version_is_ignored():
    transport = StubTransport([StubStream(put_frame(3))])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(9)))

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 9


def test_invalid_json_payload_is_ignored():
    transport = StubTransport([StubStream(["event: put", "data: {not json", ""])])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))

    _worker(client, transport)._stream_once()  # must not raise

    assert client._datafile.version == 1


def test_non_200_status_applies_nothing():
    transport = StubTransport([StubStream(put_frame(2), status=401)])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))

    connected = _worker(client, transport)._stream_once()

    assert connected is False
    assert client._datafile.version == 1


# --- request shape ---------------------------------------------------------


def test_authorization_and_accept_headers_sent():
    transport = StubTransport([StubStream(put_frame(2))])
    client = make_client(transport)
    client.config.streaming = True
    client._fetch_once = lambda: False  # type: ignore[method-assign]
    client.ready()
    _join_when(lambda: transport.calls)
    client.close()

    url, headers = transport.calls[0]
    assert url == "https://localhost/sdk/v1/datafile/stream"
    assert headers["Authorization"] == "Bearer feat_sdk_test"
    assert headers["Accept"] == "text/event-stream"


# --- reconnect / fallback / shutdown ---------------------------------------


def test_reconnect_after_drop_then_applies_update():
    # First connection drops with no events; second delivers v2.
    transport = StubTransport(
        [
            StubStream([]),  # connects (status 200) but yields nothing -> closes
            StubStream(put_frame(2, default_variation_id=TRUE_VAR["id"])),
        ]
    )
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))

    worker = _worker(client, transport, initial_backoff_seconds=0.01, max_backoff_seconds=0.05)
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    try:
        _wait_until(lambda: client._datafile.version == 2)
    finally:
        client._stop.set()
        worker.stop()
        thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert client._datafile.version == 2


def test_failing_transport_keeps_retrying_without_crashing():
    transport = StubTransport([])  # every call raises ConnectionError
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))

    worker = _worker(client, transport, initial_backoff_seconds=0.01, max_backoff_seconds=0.02)
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    try:
        _wait_until(lambda: len(transport.calls) >= 3)
    finally:
        client._stop.set()
        worker.stop()
        thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert client._datafile.version == 1  # unchanged; poll would be the safety net


def test_ready_starts_both_stream_and_poll_threads():
    transport = StubTransport([StubStream([])])  # connect once, then idle/reconnect
    client = make_client(transport, poll_in_background=True, streaming=True)
    client._fetch_once = lambda: False  # type: ignore[method-assign]
    client.ready()
    try:
        assert client._thread is not None and client._thread.is_alive()
        assert client._stream_thread is not None and client._stream_thread.is_alive()
    finally:
        client.close()
        client._stream_thread.join(timeout=2.0)

    assert not client._stream_thread.is_alive()


def test_close_shuts_down_stream_thread_promptly():
    # A stream that blocks until told to stop, mimicking a quiet live socket.
    release = threading.Event()

    class BlockingStream:
        status = 200

        def __init__(self) -> None:
            self.closed = False

        def iter_lines(self):
            release.wait(2.0)
            return iter(())

        def close(self) -> None:
            self.closed = True
            release.set()  # unblock the pending read

    stream = BlockingStream()
    transport = StubTransport([stream])
    client = make_client(transport)
    client._fetch_once = lambda: False  # type: ignore[method-assign]
    client.ready()
    _join_when(lambda: transport.calls)

    start = time.monotonic()
    client.close()
    client._stream_thread.join(timeout=2.0)
    elapsed = time.monotonic() - start

    assert not client._stream_thread.is_alive()
    assert stream.closed is True
    assert elapsed < 1.5  # closed promptly, not after a long read timeout


# --- helpers ---------------------------------------------------------------


def _from(data: dict[str, Any]):
    from feat.datafile import from_json

    return from_json(data)


def _worker(client: Client, transport, **kwargs: Any) -> StreamWorker:
    return StreamWorker(
        transport=transport,
        url="https://localhost/sdk/v1/datafile/stream",
        headers={"Authorization": "Bearer feat_sdk_test"},
        on_datafile=client._adopt_from_stream,
        stop_event=client._stop,
        **kwargs,
    )


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition not met within timeout")


def _join_when(predicate, timeout: float = 2.0) -> None:
    _wait_until(predicate, timeout)
