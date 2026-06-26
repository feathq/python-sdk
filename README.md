<p align="center">
  <a href="https://feat.so">
    <img src="https://feat.so/logo/wordmark.png" alt="feat.so" width="320" />
  </a>
</p>

---

# feat Python SDK

Server-side Python SDK for [feat](https://feat.so) feature flags. Local flag evaluation against a polled datafile. Zero runtime dependencies (stdlib only).

## Install

```bash
pip install feat-sdk
```

Python 3.10+.

## Usage

```python
from feat import Client, ClientConfig, EvalContext

client = Client(ClientConfig(
    api_key="feat_sdk_...",
    url="https://data-01.feat.so",  # optional; this is the default
))
client.ready()

ctx = EvalContext(
    targeting_key="user-123",
    kinds={"user": {"plan": "pro", "email": "alice@example.com"}},
)

if client.get_boolean_value("checkout-v2", False, ctx):
    # ...
    pass

client.close()
```

Use a **server** API key (`feat_sdk_...`).

## How it works

- Fetches a per-environment datafile and keeps it in memory.
- Streams live updates by default over Server-Sent Events: a background thread
  holds the connection and applies each pushed datafile the moment it changes.
- A background poll runs alongside the stream as a safety net (every 30 seconds
  by default, configurable; ETag-aware via `If-None-Match`). If the stream
  drops, the poll keeps the datafile fresh while the stream reconnects with
  exponential backoff.
- Updates are version-ordered: a datafile is adopted only when its version is
  strictly newer than the one in memory, so the stream and poll never clobber
  each other.
- Evaluation runs in-process: no per-flag network call.
- `close()` stops the stream and poll threads cleanly.

### Disabling streaming

Set `streaming=False` to rely on polling alone:

```python
client = Client(ClientConfig(api_key="feat_sdk_...", streaming=False))
```

## License

MIT
