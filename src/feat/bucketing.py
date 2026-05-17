"""Deterministic bucketing for percentage rollouts.

Algorithm matches @feathq/feat-eval and feat-go-sdk bit-for-bit so the
same context lands in the same variation regardless of which SDK does
the evaluation:

    sha1(salt + "." + flag_key + "." + context_key)
    -> first 8 bytes as big-endian uint64
    -> shift right 4 (drop low bits) for exactly 60 bits
    -> modulo 100_000

Uses SHA-1 for the algorithm (not security); shrugs off Python's hash
randomization since the digest is purely a function of the inputs.
"""

import hashlib

BUCKET_SCALE = 100_000


def bucket(salt: str, flag_key: str, context_key: str) -> int:
    data = f"{salt}.{flag_key}.{context_key}".encode()
    digest = hashlib.sha1(data, usedforsecurity=False).digest()
    first8 = int.from_bytes(digest[:8], "big", signed=False)
    sixty = first8 >> 4
    return sixty % BUCKET_SCALE


def pick_by_weight(bucket_value: int, variations: list) -> str | None:
    """Walk cumulative weights and return the variation whose range
    contains bucket_value. Falls back to the last variation defensively
    if upstream weights underflow the scale.
    """
    cumulative = 0
    for v in variations:
        cumulative += v.weight
        if bucket_value < cumulative:
            return v.variationId
    if variations:
        return variations[-1].variationId
    return None
