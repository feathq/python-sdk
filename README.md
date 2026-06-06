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
- Polls every 30 seconds by default (configurable). ETag-aware via `If-None-Match`.
- Evaluation runs in-process: no per-flag network call.
- A background daemon thread handles polling; `close()` stops it cleanly.

## License

MIT
