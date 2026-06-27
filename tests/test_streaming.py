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


class RecordingStop:
    """A stop signal whose wait() records the timeout and never blocks.

    Lets a test drive the reconnect loop synchronously and inspect the backoff
    schedule. After `stop_after` waits it signals stop so run() returns.
    """

    def __init__(self, stop_after: int) -> None:
        self._stop_after = stop_after
        self.waits: list[float] = []
        self._set = False

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout if timeout is not None else 0.0)
        if len(self.waits) >= self._stop_after:
            self._set = True
            return True
        return False


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


# --- streaming disabled ----------------------------------------------------


def test_streaming_disabled_starts_poll_only():
    # streaming=False must not open a stream, but the poll thread still runs.
    transport = StubTransport([StubStream([])])
    client = make_client(transport, streaming=False, poll_in_background=True)
    client._fetch_once = lambda: False  # type: ignore[method-assign]
    client.ready()
    try:
        assert client._thread is not None and client._thread.is_alive()
        assert client._stream_thread is None
        assert client._stream_worker is None
        assert transport.calls == []  # transport never invoked
    finally:
        client.close()
        client._thread.join(timeout=2.0)

    assert not client._thread.is_alive()


# --- backoff ----------------------------------------------------------------


def test_backoff_grows_when_server_accepts_then_closes_immediately():
    # 200 then immediate EOF after seeding is NOT productive: backoff must keep
    # growing (capped) and never reset to the initial value, so a flapping
    # server is not reconnected to every ~initial-backoff seconds.
    transport = StubTransport([StubStream([], status=200) for _ in range(8)])
    stop = RecordingStop(stop_after=6)
    worker = StreamWorker(
        transport=transport,
        url="https://localhost/sdk/v1/datafile/stream",
        headers={},
        on_datafile=lambda _d: None,
        stop_event=stop,  # type: ignore[arg-type]
        initial_backoff_seconds=1.0,
        max_backoff_seconds=8.0,
        backoff_jitter=0.25,
        min_uptime_seconds=30.0,
    )
    worker.run()

    # Bases double each round and then hold at the cap; jitter stays within
    # [base, base * (1 + jitter)].
    expected_bases = [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]
    assert len(worker_waits := stop.waits) == len(expected_bases)
    for wait, base in zip(worker_waits, expected_bases):
        assert base <= wait <= base * 1.25
    assert max(worker_waits) <= 8.0 * 1.25  # cap holds


# --- non-200 ----------------------------------------------------------------


def test_persistent_non_200_backs_off_without_busy_looping():
    # 401/403/429 adopt nothing, never set `connected`, and back off each round
    # instead of tight-looping at a fixed tiny wait.
    transport = StubTransport(
        [StubStream(put_frame(2), status=code) for code in (401, 403, 429, 401)]
    )
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))
    stop = RecordingStop(stop_after=4)
    worker = StreamWorker(
        transport=transport,
        url="https://localhost/sdk/v1/datafile/stream",
        headers={},
        on_datafile=client._adopt_from_stream,
        stop_event=stop,  # type: ignore[arg-type]
        initial_backoff_seconds=1.0,
        max_backoff_seconds=8.0,
        backoff_jitter=0.0,
    )
    worker.run()

    assert client._datafile.version == 1  # nothing adopted
    assert worker.connected.is_set() is False
    assert stop.waits == [1.0, 2.0, 4.0, 8.0]  # grew, did not tight-loop


# --- payload handling -------------------------------------------------------


def test_non_dict_json_payload_is_dropped():
    # `data: 5` parses as valid JSON but is not a datafile dict; drop it.
    transport = StubTransport([StubStream(["event: put", "data: 5", ""])])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))

    _worker(client, transport)._stream_once()  # must not raise

    assert client._datafile.version == 1


# --- heartbeat interleaving -------------------------------------------------


def test_heartbeat_interleaved_with_put_adopts_once():
    payload = json.dumps(datafile_json(2, default_variation_id=TRUE_VAR["id"]))
    lines = [
        ": ping",
        "event: put",
        "id: 2",
        f"data: {payload}",
        "",
        ": ping",
    ]
    transport = StubTransport([StubStream(lines)])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))

    adopted: list[int] = []
    original = client._apply_datafile

    def spy(df, etag=None):
        result = original(df, etag=etag)
        if result:
            adopted.append(df.version)
        return result

    client._apply_datafile = spy  # type: ignore[method-assign]

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 2
    assert adopted == [2]  # exactly one adoption despite the heartbeats


# --- etag propagation -------------------------------------------------------


def test_stream_etag_used_by_subsequent_poll_gets_304():
    # A stream-adopted datafile records its etag so the next conditional poll
    # sends a current If-None-Match and gets a 304 rather than a full 200.
    transport = StubTransport(
        [StubStream(put_frame(2, default_variation_id=TRUE_VAR["id"]))]
    )
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)), etag="etag-1")

    _worker(client, transport)._stream_once()
    assert client._datafile.version == 2
    assert client._etag == "etag-2"  # adopted from the pushed datafile

    captured: dict[str, str] = {}

    def fake_request(host, port, scheme, path, headers):
        captured.update(headers)
        return 304, {}, b""

    client._request = fake_request  # type: ignore[method-assign]

    changed = client.refresh()

    assert changed is False
    assert captured.get("If-None-Match") == "etag-2"


# --- incremental patch ------------------------------------------------------


def flag_obj(key: str, *, flag_id: str, default_variation_id: str) -> dict[str, Any]:
    """A full wire-format flag object, as carried inside a patch `flags` map."""
    return {
        "id": flag_id,
        "key": key,
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


def segment_obj(key: str) -> dict[str, Any]:
    return {"key": key, "rules": []}


def patch_frame(
    from_version: int,
    to_version: int,
    *,
    flags: dict[str, Any] | None = None,
    removed_flags: list[str] | None = None,
    segments: dict[str, Any] | None = None,
    removed_segments: list[str] | None = None,
    etag: str | None = None,
) -> list[str]:
    """SSE lines for one `patch` carrying a delta from `from_version` to `to_version`."""
    payload = json.dumps(
        {
            "from": from_version,
            "to": to_version,
            "etag": etag if etag is not None else f"etag-{to_version}",
            "generatedAt": f"2026-06-27T00:00:0{to_version}Z",
            "flags": flags or {},
            "removedFlags": removed_flags or [],
            "segments": segments or {},
            "removedSegments": removed_segments or [],
        }
    )
    return ["event: patch", f"id: {to_version}", f"data: {payload}", ""]


def test_patch_matching_version_flips_a_later_evaluation():
    # Seed v1 where checkout is OFF, then patch from=1 to=2 flipping it ON.
    transport = StubTransport(
        [
            StubStream(
                patch_frame(
                    1,
                    2,
                    flags={
                        "checkout": flag_obj(
                            "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
                        )
                    },
                )
            )
        ]
    )
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1, default_variation_id=FALSE_VAR["id"])))
    assert client.get_boolean_value("checkout", True, user_ctx("u1")) is False

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 2
    assert client._datafile.generatedAt == "2026-06-27T00:00:02Z"
    assert client.get_boolean_value("checkout", False, user_ctx("u1")) is True


def test_patch_removes_a_flag():
    seed = datafile_json(1)
    seed["flags"]["legacy"] = flag_obj(
        "legacy", flag_id="flag-legacy", default_variation_id=TRUE_VAR["id"]
    )
    transport = StubTransport([StubStream(patch_frame(1, 2, removed_flags=["legacy"]))])
    client = make_client(transport)
    client._apply_datafile(_from(seed))
    assert "legacy" in client._datafile.flags

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 2
    assert "legacy" not in client._datafile.flags
    assert "checkout" in client._datafile.flags  # untouched flag survives


def test_patch_merges_and_removes_segments():
    seed = datafile_json(1)
    seed["segments"] = {"beta-users": segment_obj("beta-users")}
    transport = StubTransport(
        [
            StubStream(
                patch_frame(
                    1,
                    2,
                    segments={"power-users": segment_obj("power-users")},
                    removed_segments=["beta-users"],
                )
            )
        ]
    )
    client = make_client(transport)
    client._apply_datafile(_from(seed))

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 2
    assert "power-users" in client._datafile.segments
    assert "beta-users" not in client._datafile.segments


def test_patch_advances_version_and_etag_so_a_follow_up_poll_304s():
    transport = StubTransport([StubStream(patch_frame(1, 2))])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)), etag="etag-1")

    _worker(client, transport)._stream_once()
    assert client._datafile.version == 2
    assert client._etag == "etag-2"  # advanced by the patch

    captured: dict[str, str] = {}

    def fake_request(host, port, scheme, path, headers):
        captured.update(headers)
        return 304, {}, b""

    client._request = fake_request  # type: ignore[method-assign]

    changed = client.refresh()

    assert changed is False
    assert captured.get("If-None-Match") == "etag-2"


def test_patch_ignored_on_version_mismatch():
    # Current is v5; a patch claiming from=1 does not apply (gap).
    transport = StubTransport(
        [
            StubStream(
                patch_frame(
                    1,
                    2,
                    flags={
                        "checkout": flag_obj(
                            "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
                        )
                    },
                )
            )
        ]
    )
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(5, default_variation_id=FALSE_VAR["id"])), etag="etag-5")

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 5  # unchanged
    assert client._etag == "etag-5"  # etag not regressed/advanced
    assert client.get_boolean_value("checkout", True, user_ctx("u1")) is False


def test_patch_ignored_when_current_is_behind_the_from_version():
    # Current is v1; a patch claiming from=3 is ahead of us (a true gap below
    # the patch). It must not apply, and the version must stay at v1 so the
    # safety-net poll / reconnect can recover the missed versions.
    transport = StubTransport(
        [
            StubStream(
                patch_frame(
                    3,
                    4,
                    flags={
                        "checkout": flag_obj(
                            "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
                        )
                    },
                )
            )
        ]
    )
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1, default_variation_id=FALSE_VAR["id"])), etag="etag-1")

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 1  # unchanged; we were behind the patch
    assert client._etag == "etag-1"  # etag not advanced
    assert client.get_boolean_value("checkout", True, user_ctx("u1")) is False


def test_patch_with_to_not_greater_than_from_is_dropped():
    # A degenerate (to == from) or backward (to < from) patch carries nothing to
    # apply forward and must be dropped, leaving the datafile untouched.
    for to_version in (2, 1):
        transport = StubTransport(
            [
                StubStream(
                    patch_frame(
                        2,
                        to_version,
                        flags={
                            "checkout": flag_obj(
                                "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
                            )
                        },
                    )
                )
            ]
        )
        client = make_client(transport)
        client._apply_datafile(
            _from(datafile_json(2, default_variation_id=FALSE_VAR["id"])), etag="etag-2"
        )

        _worker(client, transport)._stream_once()

        assert client._datafile.version == 2  # unchanged
        assert client._etag == "etag-2"
        assert client.get_boolean_value("checkout", True, user_ctx("u1")) is False


def test_patch_before_any_datafile_is_seeded_is_ignored():
    # No `put`/poll has seeded a datafile yet; a patch has nothing to merge onto.
    transport = StubTransport(
        [
            StubStream(
                patch_frame(
                    1,
                    2,
                    flags={
                        "checkout": flag_obj(
                            "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
                        )
                    },
                )
            )
        ]
    )
    client = make_client(transport)
    assert client._datafile is None

    _worker(client, transport)._stream_once()  # must not raise

    assert client._datafile is None


def test_patch_applied_twice_is_idempotent():
    # The same from->to patch arriving twice (two frames on one connection)
    # applies once; the second sees current.version != from and is a no-op.
    lines = patch_frame(
        1,
        2,
        flags={
            "checkout": flag_obj(
                "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
            )
        },
    ) * 2
    transport = StubTransport([StubStream(lines)])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1, default_variation_id=FALSE_VAR["id"])))

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 2  # advanced once, not twice
    assert client._datafile.generatedAt == "2026-06-27T00:00:02Z"
    assert client.get_boolean_value("checkout", False, user_ctx("u1")) is True


def test_patch_with_bool_versions_is_rejected():
    # JSON true/false deserializes to Python bool (an int subclass); the
    # bool-subclass guard must reject it rather than treat it as version 0/1.
    payload = json.dumps(
        {
            "from": True,
            "to": False,
            "etag": "etag-2",
            "generatedAt": "2026-06-27T00:00:02Z",
            "flags": {},
            "removedFlags": [],
            "segments": {},
            "removedSegments": [],
        }
    )
    transport = StubTransport([StubStream(["event: patch", f"data: {payload}", ""])])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)), etag="etag-1")

    _worker(client, transport)._stream_once()  # must not raise

    assert client._datafile.version == 1
    assert client._etag == "etag-1"


def test_patch_with_non_list_removed_keys_is_dropped():
    # `removedFlags` / `removedSegments` must be lists; a non-list drops the patch.
    for field, value in (("removedFlags", {"checkout": True}), ("removedSegments", "beta")):
        body = {
            "from": 1,
            "to": 2,
            "etag": "etag-2",
            "generatedAt": "2026-06-27T00:00:02Z",
            "flags": {},
            "removedFlags": [],
            "segments": {},
            "removedSegments": [],
        }
        body[field] = value
        payload = json.dumps(body)
        transport = StubTransport([StubStream(["event: patch", f"data: {payload}", ""])])
        client = make_client(transport)
        client._apply_datafile(_from(datafile_json(1)), etag="etag-1")

        _worker(client, transport)._stream_once()  # must not raise

        assert client._datafile.version == 1  # unchanged
        assert client._etag == "etag-1"


def test_patch_omitting_etag_and_generated_at_preserves_current_values():
    # A patch without `etag` / `generatedAt` (or with non-string values) must
    # keep the current values rather than regress them to None.
    seed = datafile_json(1)
    seed["etag"] = "etag-1"
    seed["generatedAt"] = "2026-06-26T00:00:00Z"
    payload = json.dumps(
        {
            "from": 1,
            "to": 2,
            # etag omitted entirely; generatedAt present but non-string.
            "generatedAt": 12345,
            "flags": {},
            "removedFlags": [],
            "segments": {},
            "removedSegments": [],
        }
    )
    transport = StubTransport([StubStream(["event: patch", f"data: {payload}", ""])])
    client = make_client(transport)
    client._apply_datafile(_from(seed), etag="etag-1")

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 2  # patch still applied
    assert client._datafile.etag == "etag-1"  # preserved, not None
    assert client._datafile.generatedAt == "2026-06-26T00:00:00Z"  # preserved
    assert client._etag == "etag-1"  # client etag not regressed


def test_heartbeat_between_two_patches_applies_both_in_order():
    # A `: ping` comment interleaved between two patch frames on one connection
    # is ignored, and both patches still apply in order.
    lines = (
        patch_frame(
            1,
            2,
            flags={
                "checkout": flag_obj(
                    "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
                )
            },
        )
        + [": ping"]
        + patch_frame(
            2,
            3,
            flags={"beta": flag_obj("beta", flag_id="flag-beta", default_variation_id=TRUE_VAR["id"])},
        )
    )
    transport = StubTransport([StubStream(lines)])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1, default_variation_id=FALSE_VAR["id"])))

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 3
    assert client._etag == "etag-3"
    assert "beta" in client._datafile.flags
    assert client.get_boolean_value("checkout", False, user_ctx("u1")) is True


def test_patch_non_dict_payload_is_ignored():
    transport = StubTransport([StubStream(["event: patch", "data: 5", ""])])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))

    _worker(client, transport)._stream_once()  # must not raise

    assert client._datafile.version == 1


def test_patch_malformed_payload_is_ignored():
    # Missing from/to, bad JSON, and a malformed flag object each leave v1 intact.
    bad_json = ["event: patch", "data: {not json", ""]
    missing_keys = ["event: patch", 'data: {"to": 2}', ""]
    bad_flag = json.dumps(
        {
            "from": 1,
            "to": 2,
            "etag": "etag-2",
            "generatedAt": "2026-06-27T00:00:02Z",
            "flags": {"checkout": {"id": "flag-1"}},  # missing required keys
            "removedFlags": [],
            "segments": {},
            "removedSegments": [],
        }
    )
    for lines in (bad_json, missing_keys, ["event: patch", f"data: {bad_flag}", ""]):
        transport = StubTransport([StubStream(lines)])
        client = make_client(transport)
        client._apply_datafile(_from(datafile_json(1, default_variation_id=FALSE_VAR["id"])))

        _worker(client, transport)._stream_once()  # must not raise

        assert client._datafile.version == 1
        assert client.get_boolean_value("checkout", True, user_ctx("u1")) is False


def test_chained_patches_apply_in_order():
    # Two patches in one connection: 1->2 flips checkout ON, 2->3 adds a flag.
    lines = patch_frame(
        1,
        2,
        flags={
            "checkout": flag_obj(
                "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
            )
        },
    ) + patch_frame(
        2,
        3,
        flags={"beta": flag_obj("beta", flag_id="flag-beta", default_variation_id=TRUE_VAR["id"])},
    )
    transport = StubTransport([StubStream(lines)])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1, default_variation_id=FALSE_VAR["id"])))

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 3
    assert client._etag == "etag-3"
    assert "beta" in client._datafile.flags
    assert client.get_boolean_value("checkout", False, user_ctx("u1")) is True


def test_put_then_patch_on_one_connection():
    # A seed `put` at v2 followed by a `patch` 2->3 both land in order.
    lines = put_frame(2, default_variation_id=FALSE_VAR["id"]) + patch_frame(
        2,
        3,
        flags={
            "checkout": flag_obj(
                "checkout", flag_id="flag-1", default_variation_id=TRUE_VAR["id"]
            )
        },
    )
    transport = StubTransport([StubStream(lines)])
    client = make_client(transport)
    client._apply_datafile(_from(datafile_json(1)))

    _worker(client, transport)._stream_once()

    assert client._datafile.version == 3
    assert client.get_boolean_value("checkout", False, user_ctx("u1")) is True


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
        on_patch=client._apply_patch_from_stream,
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
