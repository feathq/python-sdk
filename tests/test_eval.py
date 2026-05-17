"""Parity suite — mirrors test/eval.test.ts (JS SDK) and feat/eval_test.go.

New cases should land in all three so we keep eval semantics aligned
across languages. Long-term these should become shared JSON fixtures.
"""

from typing import Any

from feat import EvalContext, evaluate
from feat.datafile import (
    ConditionGroupSpec,
    ConditionSpec,
    ContextKindSpec,
    Datafile,
    FlagSpec,
    Rollout,
    RolloutVariation,
    RuleSpec,
    SegmentRuleSpec,
    SegmentSpec,
    TargetSpec,
    VariationSpec,
)
from feat.eval import Reason

TRUE_VAR = VariationSpec(id="var-true", name="true", value=True)
FALSE_VAR = VariationSpec(id="var-false", name="false", value=False)


def make_df(flags=None, segments=None) -> Datafile:
    return Datafile(
        schemaVersion=1,
        envId="env-1",
        envKey="staging",
        projectId="proj-1",
        version=1,
        etag="etag",
        generatedAt="2026-05-17T00:00:00Z",
        flags=flags or {},
        segments=segments or {},
        contextKinds={
            "user": ContextKindSpec(
                key="user", availableForRules=True, availableForExperiments=True
            ),
        },
    )


def bool_flag(**overrides: Any) -> FlagSpec:
    defaults = dict(
        id="flag-1",
        key="checkout",
        valueType="boolean",
        salt="abcdef0123456789",
        archived=False,
        isEnabled=True,
        offVariationId=FALSE_VAR.id,
        defaultVariationId=FALSE_VAR.id,
        defaultRollout=None,
        defaultBucketingContextKindKey=None,
        variations=[TRUE_VAR, FALSE_VAR],
        targets=[],
        rules=[],
    )
    defaults.update(overrides)
    return FlagSpec(**defaults)


def user_ctx(key: str, **attrs: Any) -> EvalContext:
    obj: dict[str, Any] = {"key": key, **attrs}
    return EvalContext(kinds={"user": obj})


def test_archived_returns_off():
    df = make_df(flags={"checkout": bool_flag(archived=True)})
    r = evaluate("checkout", False, user_ctx("u1"), df)
    assert r.value is False
    assert r.reason == Reason.DISABLED


def test_disabled_returns_off():
    df = make_df(flags={"checkout": bool_flag(isEnabled=False)})
    r = evaluate("checkout", True, user_ctx("u1"), df)
    assert r.value is False
    assert r.reason == Reason.DISABLED


def test_default_when_no_targeting():
    df = make_df(flags={"checkout": bool_flag()})
    r = evaluate("checkout", True, user_ctx("u1"), df)
    assert r.value is False
    assert r.reason == Reason.FALLTHROUGH


def test_individual_target_beats_rules():
    flag = bool_flag(
        targets=[TargetSpec(contextKindKey="user", contextKey="u-vip", variationId=TRUE_VAR.id)],
        rules=[
            RuleSpec(
                id="r1",
                bucketingContextKindKey=None,
                variationId=FALSE_VAR.id,
                rollout=None,
                groups=[
                    ConditionGroupSpec(
                        conditions=[
                            ConditionSpec(
                                attributePath="user.key",
                                operator="is_one_of",
                                values=["u-vip"],
                            )
                        ]
                    )
                ],
            )
        ],
    )
    df = make_df(flags={"checkout": flag})
    r = evaluate("checkout", False, user_ctx("u-vip"), df)
    assert r.value is True
    assert r.reason == Reason.TARGETING_MATCH


def test_rule_ends_with_email():
    flag = bool_flag(
        rules=[
            RuleSpec(
                id="r1",
                bucketingContextKindKey=None,
                variationId=TRUE_VAR.id,
                rollout=None,
                groups=[
                    ConditionGroupSpec(
                        conditions=[
                            ConditionSpec(
                                attributePath="user.email",
                                operator="ends_with",
                                values=["@example.com"],
                            )
                        ]
                    )
                ],
            )
        ]
    )
    df = make_df(flags={"checkout": flag})
    r = evaluate("checkout", False, user_ctx("u1", email="alice@example.com"), df)
    assert r.value is True
    assert r.reason == Reason.TARGETING_MATCH


def test_rule_or_groups():
    flag = bool_flag(
        rules=[
            RuleSpec(
                id="r1",
                bucketingContextKindKey=None,
                variationId=TRUE_VAR.id,
                rollout=None,
                groups=[
                    ConditionGroupSpec(
                        conditions=[
                            ConditionSpec(
                                attributePath="user.email",
                                operator="ends_with",
                                values=["@nope.com"],
                            )
                        ]
                    ),
                    ConditionGroupSpec(
                        conditions=[
                            ConditionSpec(
                                attributePath="user.plan",
                                operator="is_one_of",
                                values=["pro", "enterprise"],
                            )
                        ]
                    ),
                ],
            )
        ]
    )
    df = make_df(flags={"checkout": flag})
    r = evaluate(
        "checkout", False, user_ctx("u1", email="x@elsewhere.com", plan="pro"), df
    )
    assert r.value is True


def test_rollout_deterministic():
    flag = bool_flag(
        defaultVariationId=None,
        defaultRollout=Rollout(
            bucketingContextKindKey="user",
            variations=[
                RolloutVariation(variationId=TRUE_VAR.id, weight=50_000),
                RolloutVariation(variationId=FALSE_VAR.id, weight=50_000),
            ],
        ),
    )
    df = make_df(flags={"checkout": flag})
    r1 = evaluate("checkout", False, user_ctx("stable-key"), df)
    r2 = evaluate("checkout", False, user_ctx("stable-key"), df)
    assert r1.value == r2.value
    assert r1.reason == Reason.SPLIT


def test_rollout_100_percent():
    flag = bool_flag(
        defaultVariationId=None,
        defaultRollout=Rollout(
            bucketingContextKindKey="user",
            variations=[RolloutVariation(variationId=TRUE_VAR.id, weight=100_000)],
        ),
    )
    df = make_df(flags={"checkout": flag})
    for key in ["u1", "u2", "u3", "u4", "u5"]:
        r = evaluate("checkout", False, user_ctx(key), df)
        assert r.value is True


def test_segment_match():
    flag = bool_flag(
        rules=[
            RuleSpec(
                id="r1",
                bucketingContextKindKey=None,
                variationId=TRUE_VAR.id,
                rollout=None,
                groups=[
                    ConditionGroupSpec(
                        conditions=[
                            ConditionSpec(
                                attributePath="",
                                operator="segment_match",
                                values=["internal-users"],
                            )
                        ]
                    )
                ],
            )
        ]
    )
    segs = {
        "internal-users": SegmentSpec(
            key="internal-users",
            rules=[
                SegmentRuleSpec(
                    conditions=[
                        ConditionSpec(
                            attributePath="user.email",
                            operator="ends_with",
                            values=["@feathq.com"],
                        )
                    ]
                )
            ],
        )
    }
    df = make_df(flags={"checkout": flag}, segments=segs)

    hit = evaluate("checkout", False, user_ctx("u1", email="bob@feathq.com"), df)
    assert hit.value is True
    miss = evaluate("checkout", False, user_ctx("u2", email="bob@other.com"), df)
    assert miss.value is False


def test_semver_gte():
    flag = bool_flag(
        rules=[
            RuleSpec(
                id="r1",
                bucketingContextKindKey=None,
                variationId=TRUE_VAR.id,
                rollout=None,
                groups=[
                    ConditionGroupSpec(
                        conditions=[
                            ConditionSpec(
                                attributePath="user.app_version",
                                operator="semver_gte",
                                values=["1.2.0"],
                            )
                        ]
                    )
                ],
            )
        ]
    )
    df = make_df(flags={"checkout": flag})
    newer = evaluate("checkout", False, user_ctx("u1", app_version="1.5.0"), df)
    assert newer.value is True
    older = evaluate("checkout", False, user_ctx("u2", app_version="1.1.5"), df)
    assert older.value is False


def test_missing_flag_returns_error():
    df = make_df()
    r = evaluate("missing", "fallback", user_ctx("u1"), df)
    assert r.reason == Reason.ERROR
    assert r.value == "fallback"
