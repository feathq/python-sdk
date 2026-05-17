"""Wire-format types. JSON field names mirror @feathq/datafile-schema."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class VariationSpec:
    id: str
    name: str
    value: Any


@dataclass
class TargetSpec:
    contextKindKey: str
    contextKey: str
    variationId: str


@dataclass
class ConditionSpec:
    attributePath: str
    operator: str
    values: list[Any]


@dataclass
class ConditionGroupSpec:
    conditions: list[ConditionSpec]


@dataclass
class RolloutVariation:
    variationId: str
    weight: int


@dataclass
class Rollout:
    bucketingContextKindKey: str
    variations: list[RolloutVariation]


@dataclass
class RuleSpec:
    id: str
    bucketingContextKindKey: str | None
    variationId: str | None
    rollout: Rollout | None
    groups: list[ConditionGroupSpec]


@dataclass
class FlagSpec:
    id: str
    key: str
    valueType: str
    salt: str
    archived: bool
    isEnabled: bool
    offVariationId: str
    defaultVariationId: str | None
    defaultRollout: Rollout | None
    defaultBucketingContextKindKey: str | None
    variations: list[VariationSpec]
    targets: list[TargetSpec]
    rules: list[RuleSpec]


@dataclass
class SegmentRuleSpec:
    conditions: list[ConditionSpec]


@dataclass
class SegmentSpec:
    key: str
    rules: list[SegmentRuleSpec]


@dataclass
class ContextKindSpec:
    key: str
    availableForRules: bool
    availableForExperiments: bool


@dataclass
class Datafile:
    schemaVersion: int
    envId: str
    envKey: str
    projectId: str
    version: int
    etag: str
    generatedAt: str
    flags: dict[str, FlagSpec]
    segments: dict[str, SegmentSpec] = field(default_factory=dict)
    contextKinds: dict[str, ContextKindSpec] = field(default_factory=dict)


def from_json(data: dict[str, Any]) -> Datafile:
    """Parse the wire-format dict (typically from response.json()) into a Datafile."""
    return Datafile(
        schemaVersion=data["schemaVersion"],
        envId=data["envId"],
        envKey=data["envKey"],
        projectId=data["projectId"],
        version=data["version"],
        etag=data["etag"],
        generatedAt=data["generatedAt"],
        flags={k: _flag(v) for k, v in data["flags"].items()},
        segments={k: _segment(v) for k, v in data.get("segments", {}).items()},
        contextKinds={
            k: ContextKindSpec(**v) for k, v in data.get("contextKinds", {}).items()
        },
    )


def _flag(d: dict[str, Any]) -> FlagSpec:
    return FlagSpec(
        id=d["id"],
        key=d["key"],
        valueType=d["valueType"],
        salt=d["salt"],
        archived=d["archived"],
        isEnabled=d["isEnabled"],
        offVariationId=d["offVariationId"],
        defaultVariationId=d.get("defaultVariationId"),
        defaultRollout=_rollout(d.get("defaultRollout")),
        defaultBucketingContextKindKey=d.get("defaultBucketingContextKindKey"),
        variations=[VariationSpec(**v) for v in d["variations"]],
        targets=[TargetSpec(**t) for t in d["targets"]],
        rules=[_rule(r) for r in d["rules"]],
    )


def _rule(d: dict[str, Any]) -> RuleSpec:
    return RuleSpec(
        id=d["id"],
        bucketingContextKindKey=d.get("bucketingContextKindKey"),
        variationId=d.get("variationId"),
        rollout=_rollout(d.get("rollout")),
        groups=[
            ConditionGroupSpec(
                conditions=[ConditionSpec(**c) for c in g["conditions"]]
            )
            for g in d["groups"]
        ],
    )


def _rollout(d: dict[str, Any] | None) -> Rollout | None:
    if d is None:
        return None
    return Rollout(
        bucketingContextKindKey=d["bucketingContextKindKey"],
        variations=[RolloutVariation(**v) for v in d["variations"]],
    )


def _segment(d: dict[str, Any]) -> SegmentSpec:
    return SegmentSpec(
        key=d["key"],
        rules=[
            SegmentRuleSpec(conditions=[ConditionSpec(**c) for c in r["conditions"]])
            for r in d["rules"]
        ],
    )
