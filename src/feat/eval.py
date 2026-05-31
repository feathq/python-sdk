"""Evaluation precedence pipeline. Mirrors @feathq/feat-eval bit-for-bit."""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from .bucketing import bucket, pick_by_weight
from .context import read_context_key
from .datafile import Datafile, FlagSpec, RuleSpec
from .segments import match_condition
from .types import EvalContext


class Reason(str, Enum):
    TARGETING_MATCH = "TARGETING_MATCH"
    SPLIT = "SPLIT"
    FALLTHROUGH = "FALLTHROUGH"
    DEFAULT = "DEFAULT"
    DISABLED = "DISABLED"
    ERROR = "ERROR"
    STATIC = "STATIC"


@dataclass
class EvaluationResult:
    value: Any
    variation_id: str | None
    reason: Reason
    error_message: str | None = None


def evaluate(
    flag_key: str,
    default_value: Any,
    ctx: EvalContext,
    df: Datafile,
) -> EvaluationResult:
    """Run the evaluation pipeline:

    1. archived flag        -> off variation        DISABLED
    2. !isEnabled           -> off variation        DISABLED
    3. individual target    -> target variation     TARGETING_MATCH
    4. first matching rule  -> rule variation/rollout TARGETING_MATCH / SPLIT
    5. default              -> default variation/rollout FALLTHROUGH / SPLIT
    6. nothing matched      -> off variation        DEFAULT

    Errors (missing flag, missing variation) return default_value with
    reason ERROR.
    """
    flag = df.flags.get(flag_key)
    if flag is None:
        return EvaluationResult(
            value=default_value,
            variation_id=None,
            reason=Reason.ERROR,
            error_message="flag could not be evaluated",
        )

    if flag.archived or not flag.isEnabled:
        return _resolve_variation(flag, flag.offVariationId, Reason.DISABLED, default_value)

    for target in flag.targets:
        ctx_key = read_context_key(ctx, target.contextKindKey)
        if ctx_key is not None and ctx_key == target.contextKey:
            return _resolve_variation(flag, target.variationId, Reason.TARGETING_MATCH, default_value)

    for rule in flag.rules:
        if not _match_rule(rule, ctx, df):
            continue
        if rule.variationId is not None:
            return _resolve_variation(flag, rule.variationId, Reason.TARGETING_MATCH, default_value)
        if rule.rollout is not None:
            picked = _pick_rollout(flag, rule.rollout, ctx)
            if picked is not None:
                return _resolve_variation(flag, picked, Reason.SPLIT, default_value)

    if flag.defaultVariationId is not None:
        return _resolve_variation(flag, flag.defaultVariationId, Reason.FALLTHROUGH, default_value)
    if flag.defaultRollout is not None:
        picked = _pick_rollout(flag, flag.defaultRollout, ctx)
        if picked is not None:
            return _resolve_variation(flag, picked, Reason.SPLIT, default_value)

    return _resolve_variation(flag, flag.offVariationId, Reason.DEFAULT, default_value)


def _match_rule(rule: RuleSpec, ctx: EvalContext, df: Datafile) -> bool:
    if not rule.groups:
        return False
    return any(
        all(match_condition(cond, ctx, df) for cond in group.conditions)
        and len(group.conditions) > 0
        for group in rule.groups
    )


def _pick_rollout(flag: FlagSpec, rollout, ctx: EvalContext) -> str | None:
    ctx_key = read_context_key(ctx, rollout.bucketingContextKindKey)
    if ctx_key is None:
        return None
    return pick_by_weight(bucket(flag.salt, flag.key, ctx_key), rollout.variations)


def _resolve_variation(
    flag: FlagSpec, variation_id: str, reason: Reason, default_value: Any
) -> EvaluationResult:
    for v in flag.variations:
        if v.id == variation_id:
            return EvaluationResult(value=v.value, variation_id=variation_id, reason=reason)
    return EvaluationResult(
        value=default_value,
        variation_id=None,
        reason=Reason.ERROR,
        error_message="flag could not be evaluated",
    )
