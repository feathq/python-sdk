"""Polling HTTP client. Uses stdlib only (urllib + threading) - zero deps."""

import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from .datafile import Datafile, from_json
from .eval import EvaluationResult, Reason, evaluate
from .types import EvalContext

MIN_POLL_INTERVAL_SECONDS = 5.0
MAX_DATAFILE_BYTES = 10 * 1024 * 1024


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
        url = self.config.data_plane_url.rstrip("/") + "/sdk/v1/datafile"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {self.config.api_key}")
        with self._lock:
            if self._etag:
                req.add_header("If-None-Match", self._etag)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 - trusted endpoint
                status = resp.status
                if status in (304, 404):
                    return False
                length_header = resp.headers.get("Content-Length")
                if length_header and int(length_header) > MAX_DATAFILE_BYTES:
                    raise RuntimeError("datafile exceeds maximum allowed size")
                body = resp.read(MAX_DATAFILE_BYTES + 1)
                if len(body) > MAX_DATAFILE_BYTES:
                    raise RuntimeError("datafile exceeds maximum allowed size")
                data = json.loads(body.decode("utf-8"))
                new_etag = resp.headers.get("ETag")
        except urllib.error.HTTPError as e:
            if e.code in (304, 404):
                return False
            raise
        with self._lock:
            self._datafile = from_json(data)
            if new_etag:
                self._etag = new_etag
        return True
