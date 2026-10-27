"""Bound exponential retries before evaluating an unbounded failure count."""

from math import frexp, ldexp


def capped_backoff(base: float, exponent: int, maximum: float) -> float:
    """Return min(maximum, base * 2**exponent) for positive finite bounds."""
    # Beyond this exponent even base's smallest possible mantissa exceeds the cap.
    # Compare integers first so arbitrarily large durable counts never overflow.
    if exponent > frexp(maximum)[1] - frexp(base)[1]:
        return maximum
    return min(maximum, ldexp(base, exponent))
