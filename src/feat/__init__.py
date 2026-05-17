"""feat feature-flag SDK for Python.

Server-side evaluation against a locally-cached datafile fetched from the
feat data plane. Wire format mirrors @feathq/datafile-schema (TypeScript).
"""

from .client import Client, ClientConfig
from .datafile import (
    ConditionSpec,
    ContextKindSpec,
    Datafile,
    FlagSpec,
    Rollout,
    SegmentSpec,
)
from .eval import EvaluationResult, Reason, evaluate
from .types import EvalContext

__all__ = [
    "Client",
    "ClientConfig",
    "ConditionSpec",
    "ContextKindSpec",
    "Datafile",
    "EvalContext",
    "EvaluationResult",
    "FlagSpec",
    "Reason",
    "Rollout",
    "SegmentSpec",
    "evaluate",
]
