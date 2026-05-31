"""Per-operator predicates. Defensive: type-mismatch / parse-failure
returns False rather than raising — matches the JS engine's posture
against malformed contexts at the edge.

segment_match / segment_not_match are dispatched by the rule evaluator
(they recurse into the datafile's segments map), not here.
"""

import re
from datetime import datetime, timezone
from typing import Any, Callable


def match_operator(operator: str, lhs: Any, values: list[Any]) -> bool:
    fn = _OPS.get(operator)
    if fn is None:
        return False
    return fn(lhs, values)


def _is_one_of(lhs: Any, values: list[Any]) -> bool:
    return any(_deep_eq(lhs, v) for v in values)


def _is_not_one_of(lhs: Any, values: list[Any]) -> bool:
    return not _is_one_of(lhs, values)


def _is_empty(lhs: Any, _: list[Any]) -> bool:
    return lhs is None or lhs == ""


def _is_not_empty(lhs: Any, _: list[Any]) -> bool:
    return not _is_empty(lhs, _)


def _contains(lhs: Any, values: list[Any]) -> bool:
    if not isinstance(lhs, str):
        return False
    return any(isinstance(v, str) and v in lhs for v in values)


def _does_not_contain(lhs: Any, values: list[Any]) -> bool:
    if not isinstance(lhs, str):
        return True
    return not any(isinstance(v, str) and v in lhs for v in values)


def _starts_with(lhs: Any, values: list[Any]) -> bool:
    if not isinstance(lhs, str):
        return False
    return any(isinstance(v, str) and lhs.startswith(v) for v in values)


def _ends_with(lhs: Any, values: list[Any]) -> bool:
    if not isinstance(lhs, str):
        return False
    return any(isinstance(v, str) and lhs.endswith(v) for v in values)


# ReDoS guard: cap pattern length and reject the most common catastrophic-
# backtracking shapes (nested unbounded quantifiers, alternation inside a
# starred group). False positives just turn the rule into a non-match,
# which is the safe default.
_REDOS_SHAPES = re.compile(r"\([^)]*[+*][^)]*\)\s*[+*]|\([^)]*\|[^)]*\)\s*[+*]")


def _is_safe_regex(pattern: str) -> bool:
    if len(pattern) > 512:
        return False
    if _REDOS_SHAPES.search(pattern):
        return False
    return True


def _matches_regex(lhs: Any, values: list[Any]) -> bool:
    if not isinstance(lhs, str):
        return False
    for v in values:
        if not isinstance(v, str):
            continue
        if not _is_safe_regex(v):
            continue
        try:
            if re.search(v, lhs) is not None:
                return True
        except re.error:
            continue
    return False


def _deep_eq(a: Any, b: Any) -> bool:
    if a == b:
        return True
    # String/number coercion — matches JS engine.
    if isinstance(a, (int, float)) and isinstance(b, str):
        return str(a) == b or _num_str_eq(a, b)
    if isinstance(a, str) and isinstance(b, (int, float)):
        return a == str(b) or _num_str_eq(b, a)
    return False


def _num_str_eq(num: Any, s: str) -> bool:
    try:
        return float(num) == float(s)
    except (TypeError, ValueError):
        return False


def _to_number(x: Any) -> float | None:
    if isinstance(x, bool):
        return None  # bool isinstance of int — exclude explicitly
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x)
        except ValueError:
            return None
    return None


def _numeric_cmp(cmp: Callable[[float, float], bool]) -> Callable[[Any, list[Any]], bool]:
    def fn(lhs: Any, values: list[Any]) -> bool:
        a = _to_number(lhs)
        if a is None:
            return False
        for v in values:
            b = _to_number(v)
            if b is not None and cmp(a, b):
                return True
        return False
    return fn


def _to_datetime(x: Any) -> datetime | None:
    if isinstance(x, str):
        try:
            # Support ISO-8601 with or without "Z" / offset.
            s = x.replace("Z", "+00:00")
            return datetime.fromisoformat(s)
        except ValueError:
            return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return datetime.fromtimestamp(float(x) / 1000.0, tz=timezone.utc)
    return None


def _date_cmp(cmp: Callable[[datetime, datetime], bool]) -> Callable[[Any, list[Any]], bool]:
    def fn(lhs: Any, values: list[Any]) -> bool:
        a = _to_datetime(lhs)
        if a is None:
            return False
        for v in values:
            b = _to_datetime(v)
            if b is not None and cmp(a, b):
                return True
        return False
    return fn


_SEMVER_RE = re.compile(
    r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$"
)


def _parse_semver(x: Any) -> tuple[int, int, int, str | None] | None:
    if not isinstance(x, str):
        return None
    m = _SEMVER_RE.match(x.strip())
    if m is None:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3)), m.group(4)


def _compare_semver(a: tuple[int, int, int, str | None], b: tuple[int, int, int, str | None]) -> int:
    for i in range(3):
        if a[i] != b[i]:
            return a[i] - b[i]
    ap, bp = a[3], b[3]
    if ap == bp:
        return 0
    if ap is None:
        return 1
    if bp is None:
        return -1
    return (ap > bp) - (ap < bp)


def _semver_cmp(pred: Callable[[int], bool]) -> Callable[[Any, list[Any]], bool]:
    def fn(lhs: Any, values: list[Any]) -> bool:
        a = _parse_semver(lhs)
        if a is None:
            return False
        for v in values:
            b = _parse_semver(v)
            if b is not None and pred(_compare_semver(a, b)):
                return True
        return False
    return fn


_OPS: dict[str, Callable[[Any, list[Any]], bool]] = {
    "is_one_of": _is_one_of,
    "is_not_one_of": _is_not_one_of,
    "is_empty": _is_empty,
    "is_not_empty": _is_not_empty,
    "contains": _contains,
    "does_not_contain": _does_not_contain,
    "starts_with": _starts_with,
    "ends_with": _ends_with,
    "matches_regex": _matches_regex,
    "gt": _numeric_cmp(lambda a, b: a > b),
    "gte": _numeric_cmp(lambda a, b: a >= b),
    "lt": _numeric_cmp(lambda a, b: a < b),
    "lte": _numeric_cmp(lambda a, b: a <= b),
    "before": _date_cmp(lambda a, b: a < b),
    "after": _date_cmp(lambda a, b: a > b),
    "semver_eq": _semver_cmp(lambda c: c == 0),
    "semver_gt": _semver_cmp(lambda c: c > 0),
    "semver_gte": _semver_cmp(lambda c: c >= 0),
    "semver_lt": _semver_cmp(lambda c: c < 0),
    "semver_lte": _semver_cmp(lambda c: c <= 0),
    "segment_match": lambda lhs, values: False,
    "segment_not_match": lambda lhs, values: False,
}
