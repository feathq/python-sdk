# feathq-python-sdk

Server-side Python SDK for [feat](https://feat.so) feature flags. Local flag evaluation against a polled datafile. Zero runtime dependencies (stdlib only).

## Install

```bash
pip install feathq-python-sdk
```

Python 3.10+.

## Usage

```python
from feathq import FeatClient

client = FeatClient(
    api_key="feat_sdk_...",
    data_plane_url="https://data.feat.so",
)
client.ready()

result = client.evaluate(
    flag_key="checkout-v2",
    default_value=False,
    context={
        "targetingKey": "user-123",
        "user": {"plan": "pro", "email": "alice@example.com"},
    },
)

if result.value:
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
