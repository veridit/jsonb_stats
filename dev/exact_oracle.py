#!/usr/bin/env python3
"""Independent oracle for jsonb_stats numeric aggregates.

Computes expected *_agg summaries with exact rational arithmetic (Python
integers and fractions), independent of both the Rust implementation and the
PL/pgSQL reference. Used to generate dev/fixtures/exact_fixture.sql, which the
pgrx tests load via include_str!.

    python3 dev/exact_oracle.py > dev/fixtures/exact_fixture.sql

Semantics (see todo.md "Decisions"):
  state:   count, sum, sum_sq (= sum of x^2), min, max, all exact decimals
  derived: N = n*sum_sq - sum^2
           mean        = sum / n
           sum_sq_diff = N / n
           variance    = N / (n(n-1))            NULL if n <= 1
           stddev      = sqrt(variance)          NULL if n <= 1
           cv_pct      = 100 * stddev / mean     NULL if n <= 1 or sum = 0
  every derived value is rounded once to 2 decimals, half away from zero.
"""

from decimal import Decimal
from fractions import Fraction
from math import isqrt
import json
import sys


# ── exact decimal helpers: a decimal is (mantissa: int, scale: int >= 0) ──

def parse(text):
    # Exact: Decimal(str) keeps every digit; only arithmetic is context-rounded,
    # so take the digits/exponent tuple directly.
    sign, digits, exp = Decimal(text).as_tuple()
    mant = int("".join(map(str, digits)) or "0")
    if sign:
        mant = -mant
    if exp >= 0:
        return (mant * 10 ** exp, 0)
    return (mant, -exp)


def rescale(d, scale):
    m, s = d
    assert scale >= s
    return (m * 10 ** (scale - s), scale)


def add(a, b):
    s = max(a[1], b[1])
    return (rescale(a, s)[0] + rescale(b, s)[0], s)


def mul(a, b):
    return (a[0] * b[0], a[1] + b[1])


def frac(d):
    return Fraction(d[0], 10 ** d[1])


def fmt(d):
    m, s = d
    neg = m < 0
    digits = str(abs(m)).rjust(s + 1, "0")
    out = digits if s == 0 else digits[:-s] + "." + digits[-s:]
    return ("-" if neg else "") + out


def round_half_away(q):
    """Round a Fraction to an integer, half away from zero."""
    n, d = abs(q.numerator), q.denominator
    r = (2 * n + d) // (2 * d)
    return -r if q < 0 else r


def cents(q):
    """Fraction -> 2-decimal string, half away from zero."""
    return fmt((round_half_away(q * 100), 2))


def sqrt_cents(q):
    """Correctly rounded (half away from zero) 2-decimal sqrt of Fraction q >= 0."""
    assert q >= 0
    x = q * 40000  # floor(100*sqrt(q) + 1/2) == (isqrt(floor(40000 q)) + 1) // 2
    return (isqrt(x.numerator // x.denominator) + 1) // 2


def cmp_pick(a, b, want_less):
    fa, fb = frac(a), frac(b)
    if fa != fb:
        return a if (fa < fb) == want_less else b
    return a if a[1] >= b[1] else b  # tie: larger scale wins


# ── per-type value normalisation ──

def normalise(kind, text):
    d = parse(text)
    if kind in ("int", "nat"):
        q = frac(d)
        assert q.denominator == 1, text
        if kind == "nat":
            assert q >= 0
        return (int(q), 0)
    if kind == "dec2":
        q = frac(d) * 100
        assert q.denominator == 1, text
        return (int(q), 2)
    if kind == "float":
        return d
    raise ValueError(kind)


def summarise(kind, texts):
    vals = [normalise(kind, t) for t in texts]
    n = len(vals)
    s = vals[0]
    sq = mul(vals[0], vals[0])
    lo = hi = vals[0]
    for v in vals[1:]:
        s = add(s, v)
        sq = add(sq, mul(v, v))
        lo = cmp_pick(lo, v, True)
        hi = cmp_pick(hi, v, False)
    S1, S2 = frac(s), frac(sq)
    N = n * S2 - S1 * S1
    assert N >= 0
    out = {
        "type": kind + "_agg",
        "count": n,
        "sum": fmt(s),
        "sum_sq": fmt(sq),
        "min": fmt(lo),
        "max": fmt(hi),
        "mean": cents(S1 / n),
        "sum_sq_diff": cents(N / n),
    }
    if n > 1:
        var = N / (n * (n - 1))
        out["variance"] = cents(var)
        out["stddev"] = fmt((sqrt_cents(var), 2))
        if S1 != 0:
            # cv = 100*sqrt(var)/mean ; cv^2 = 10000 * var * n^2 / S1^2
            mag = sqrt_cents(10000 * var * n * n / (S1 * S1))
            out["coefficient_of_variation_pct"] = fmt((mag if S1 > 0 else -mag, 2))
        else:
            out["coefficient_of_variation_pct"] = None
    else:
        out["variance"] = None
        out["stddev"] = None
        out["coefficient_of_variation_pct"] = None
    return out


# ── jsonb text rendering (PostgreSQL key order: length, then bytes) ──

def jsonb_text(obj):
    if obj is None:
        return "null"
    if isinstance(obj, dict):
        keys = sorted(obj, key=lambda k: (len(k.encode()), k.encode()))
        return "{" + ", ".join(json.dumps(k) + ": " + jsonb_text(obj[k]) for k in keys) + "}"
    if isinstance(obj, bool):
        return "true" if obj else "false"
    if isinstance(obj, int):
        return str(obj)
    if isinstance(obj, str):
        if obj.lstrip("-")[:1].isdigit():
            return obj  # pre-formatted number
        return json.dumps(obj)
    raise TypeError(obj)


# ── fixture data ──

def lcg(seed):
    while True:
        seed = (seed * 6364136223846793005 + 1442695040888963407) % 2 ** 64
        yield seed >> 11


def fixture_rows():
    """73 rows, values 0..90,000,000 (a realistic turnover-like distribution)."""
    g = lcg(306)
    rows = []
    for i in range(1, 74):
        r = next(g)
        if i == 1:
            t = 0
        elif i == 73:
            t = 90_000_000
        elif i % 9 == 0:
            t = (r % 900) * 1000  # small firms, round thousands
        else:
            t = r % 90_000_001
        cents_part = next(g) % 100
        dec2 = fmt((t * 100 + cents_part, 2)) if i % 5 else fmt((t * 10 + cents_part % 10, 1))
        flt = repr((t + cents_part / 100) * 1.37)  # shortest round-trip float8 text
        neg = -(t % 1000) - 1
        big = 9_223_372_036_854_775_807 - (r % 1000)
        rows.append({
            "i": i,
            "t_int": str(t),
            "t_nat": str(t),
            "t_dec2": dec2,
            "t_float": flt,
            "neg_int": str(neg),
            "big_int": str(big),
        })
    return rows


KINDS = {
    "t_int": "int",
    "t_nat": "nat",
    "t_dec2": "dec2",
    "t_float": "float",
    "neg_int": "int",
    "big_int": "int",
}

# (name, kind, values as JSON number text). Each value list is aggregated in
# both directions by the tests; the expected summary must not depend on order.
EDGE_CASES = [
    ("tie_half_up", "int", ["1"] + ["0"] * 7),            # mean 0.125 -> 0.13
    ("tie_half_down_negative", "int", ["-1"] + ["0"] * 7),  # mean -0.125 -> -0.13
    ("two_ints", "int", ["1", "2"]),
    ("negative_mean_cv", "int", ["-1", "-3"]),
    ("single_value", "int", ["7"]),
    ("zero_mean", "int", ["-5", "5"]),
    ("repeating_mean", "int", ["1", "2", "2"]),
    ("int_max_twice", "int", ["9223372036854775807", "9223372036854775807"]),
    ("int_min_and_max", "int", ["-9223372036854775808", "9223372036854775807"]),
    ("nat_basic", "nat", ["0", "5", "10"]),
    ("dec2_mixed_scale", "dec2", ["1", "2.5"]),
    ("dec2_zero_sum", "dec2", ["-0.01", "0.01"]),
    ("dec2_small", "dec2", ["0.01", "0.02", "0.02"]),
    ("dec2_huge", "dec2", ["99999999999999999.99", "-0.01"]),
    ("float_point_one_two", "float", ["0.1", "0.2"]),
    ("float_zeros", "float", ["0", "0"]),
    ("float_huge", "float", ["1e308", "-1e308"]),
    ("float_tiny", "float", ["0.000001", "0.000003"]),
    ("float_scale_tie", "float", ["2.5", "2.50"]),
    # i128 boundary (|i128| < 1.7e38): squares, sums and rescales that overflow
    ("i128_square_overflow", "int", ["13043817825332782212", "-13043817825332782212", "1"]),
    ("i128_sum_sq_overflow_by_count", "int", ["9223372036854775807"] * 3),
    ("i128_value_beyond", "int", ["170141183460469231731687303715884105728", "-1"]),
    ("i128_dec2_large", "dec2", ["9999999999999999999.99", "0.01", "-9999999999999999999.99"]),
    ("i128_float_rescale", "float", ["1e20", "0.0000000000000000001", "-1e20"]),
    ("i128_float_38_digits", "float", ["1234567890123456789.0123456789012345678", "1"]),
]


def main():
    rows = fixture_rows()
    w = sys.stdout.write
    w("-- GENERATED by dev/exact_oracle.py. Do not edit by hand.\n")
    w("-- 73 rows with values 0..90,000,000 (turnover-like), plus\n")
    w("-- negative and near-i64::MAX integer columns. exact_fixture_expected holds\n")
    w("-- the exact stats_agg computed with Python rational arithmetic.\n")
    w("CREATE TEMP TABLE exact_fixture(i int PRIMARY KEY, stats jsonb NOT NULL);\n")
    w("INSERT INTO exact_fixture(i, stats) VALUES\n")
    lines = []
    for r in rows:
        stats = {"type": "stats"}
        for key, kind in KINDS.items():
            stats[key] = {"type": kind, "value": r[key]}
        lines.append("  (%d, '%s')" % (r["i"], jsonb_text(stats)))
    w(",\n".join(lines) + ";\n")
    expected = {"type": "stats_agg"}
    for key, kind in KINDS.items():
        expected[key] = summarise(kind, [r[key] for r in rows])
    w("CREATE TEMP TABLE exact_fixture_expected AS SELECT '%s'::jsonb AS agg;\n" % jsonb_text(expected))
    # The parallel test replicates every fixture row 300 times (21,900 rows).
    expected_x300 = {"type": "stats_agg"}
    for key, kind in KINDS.items():
        expected_x300[key] = summarise(kind, [r[key] for r in rows] * 300)
    w("CREATE TEMP TABLE exact_fixture_x300_expected AS SELECT '%s'::jsonb AS agg;\n" % jsonb_text(expected_x300))
    w("CREATE TEMP TABLE exact_edge_cases(name text PRIMARY KEY, kind text NOT NULL, vals text[] NOT NULL, expected jsonb NOT NULL);\n")
    w("INSERT INTO exact_edge_cases(name, kind, vals, expected) VALUES\n")
    lines = []
    for name, kind, vals in EDGE_CASES:
        arr = "ARRAY[" + ", ".join("'%s'" % v for v in vals) + "]"
        lines.append("  ('%s', '%s', %s, '%s')" % (name, kind, arr, jsonb_text(summarise(kind, vals))))
    w(",\n".join(lines) + ";\n")


if __name__ == "__main__":
    main()
