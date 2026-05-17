"""SDK-consumer-facing types.

EvalContext mirrors OpenFeature's pattern: a `targeting_key` shorthand
for `user.key`, and a kinds dict matching the datafile's `contextKinds`
map. Example:

    EvalContext(
        targeting_key="user-123",
        kinds={
            "user": {"key": "user-123", "email": "u@example.com"},
            "organization": {"key": "acme", "plan": "pro"},
        },
    )
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class EvalContext:
    targeting_key: str | None = None
    kinds: dict[str, dict[str, Any]] = field(default_factory=dict)
