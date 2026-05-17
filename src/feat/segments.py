"""Segment matching with recursion for segment_match / segment_not_match."""

from .context import resolve_attribute
from .datafile import ConditionSpec, Datafile
from .operators import match_operator
from .types import EvalContext


def match_segment(segment_key: str, ctx: EvalContext, df: Datafile) -> bool:
    seg = df.segments.get(segment_key)
    if seg is None:
        return False
    return any(_match_segment_rule(rule.conditions, ctx, df) for rule in seg.rules)


def _match_segment_rule(conds: list[ConditionSpec], ctx: EvalContext, df: Datafile) -> bool:
    if not conds:
        return False
    return all(match_condition(c, ctx, df) for c in conds)


def match_condition(cond: ConditionSpec, ctx: EvalContext, df: Datafile) -> bool:
    if cond.operator == "segment_match":
        keys = [v for v in cond.values if isinstance(v, str)]
        return any(match_segment(k, ctx, df) for k in keys)
    if cond.operator == "segment_not_match":
        keys = [v for v in cond.values if isinstance(v, str)]
        return not any(match_segment(k, ctx, df) for k in keys)
    lhs = resolve_attribute(ctx, cond.attributePath)
    return match_operator(cond.operator, lhs, cond.values)
