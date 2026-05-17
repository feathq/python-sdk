"""Attribute resolution against an EvalContext."""

from typing import Any

from .types import EvalContext


def resolve_attribute(ctx: EvalContext, attribute_path: str) -> Any:
    """Walk an attribute path like "user.email" or "user.address.city".

    Returns None if any segment is missing — operators treat None as a
    non-match rather than throw.
    """
    if not attribute_path:
        return None
    parts = attribute_path.split(".", 1)
    kind_key = parts[0]
    kind_obj = _read_kind(ctx, kind_key)
    if kind_obj is None:
        return None
    if len(parts) == 1:
        return kind_obj.get("key")

    rest = parts[1]
    cur: Any = kind_obj
    for p in rest.split("."):
        if not isinstance(cur, dict):
            return None
        if p not in cur:
            return None
        cur = cur[p]
    return cur


def read_context_key(ctx: EvalContext, kind_key: str) -> str | None:
    """Pull just the `.key` for a context kind. Falls back to
    targeting_key for "user", matching OpenFeature semantics."""
    obj = _read_kind(ctx, kind_key)
    if obj is None:
        return None
    key = obj.get("key")
    return key if isinstance(key, str) else None


def _read_kind(ctx: EvalContext, kind_key: str) -> dict[str, Any] | None:
    if kind_key == "user":
        obj = ctx.kinds.get("user")
        if isinstance(obj, dict):
            return obj
        if ctx.targeting_key:
            return {"key": ctx.targeting_key}
        return None
    obj = ctx.kinds.get(kind_key)
    return obj if isinstance(obj, dict) else None
