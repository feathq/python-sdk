"""feat feature-flag SDK for Python.

Server-side evaluation against a locally-cached datafile.
"""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("feat-sdk")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

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
