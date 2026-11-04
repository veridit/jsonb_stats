//! Finding what stands out: `jsonb_stats_values` and `jsonb_stats_compare`.
//!
//! `jsonb_stats_values` unpacks variables into rows, so PostgreSQL's own
//! aggregates (percentile_cont, ...) and joins work on any variable.
//! `jsonb_stats_compare` gives per numeric variable unit-free measures of how
//! a subject differs from a reference, from the exact state only.
//!
//! Spec: dev/reference_plpgsql.sql, section "Finding what stands out".

use std::str::FromStr;

use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use pgrx::prelude::*;
use pgrx::{AnyNumeric, JsonB};
use serde_json::{json, Map, Value};

use crate::exact::{ratio_round2, ratio_sqrt_round2, Decimal};
use crate::state::{NumFields, NumKind, StatsState};

const UNKNOWN_AGG_TYPE: &str =
    "Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg";

/// count, sum, sum_sq of a numeric summary (min/max optional, count may be
/// 0 without them: see NumFields::from_summary).
fn num_state(kind: NumKind, key: &str, obj: &Map<String, Value>) -> (i64, Decimal, Decimal) {
    let f = NumFields::from_summary(kind, key, obj);
    (f.count, f.sum, f.sum_sq)
}

fn agg_type_of(obj: &Map<String, Value>) -> &str {
    match obj.get("type") {
        Some(Value::String(s)) => s.as_str(),
        _ => "",
    }
}

fn is_counted(t: &str) -> bool {
    matches!(t, "str_agg" | "bool_agg" | "arr_agg" | "date_agg")
}

/// jsonb key order (length, then bytes), as jsonb_each returns them.
fn jsonb_key_order(a: &str, b: &str) -> std::cmp::Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

fn numeric(d: &Decimal) -> AnyNumeric {
    AnyNumeric::from_str(&d.to_string())
        .unwrap_or_else(|e| pgrx::error!("jsonb_stats: cannot encode number {}: {}", d, e))
}

type ValueRow = (String, String, Option<AnyNumeric>, Option<String>, i64);

/// Rows of one stats_agg. Spec: jsonb_stats_values_plpgsql
fn value_rows(agg: Map<String, Value>) -> Vec<ValueRow> {
    let mut keys: Vec<&String> = agg.keys().filter(|k| *k != "type").collect();
    keys.sort_by(|a, b| jsonb_key_order(a, b));
    let mut rows = Vec::new();
    for key in keys {
        let Value::Object(obj) = &agg[key] else {
            pgrx::error!("jsonb_stats: summary for key '{}' must be an object", key)
        };
        let t = agg_type_of(obj);
        let base = t.strip_suffix("_agg").unwrap_or("").to_string();
        if let Some(kind) = NumKind::from_agg_type(t) {
            let (count, sum, _) = num_state(kind, key, obj);
            rows.push((key.clone(), base, Some(numeric(&sum)), None, count));
        } else if is_counted(t) {
            let Some(Value::Object(counts)) = obj.get("counts") else {
                pgrx::error!(
                    "jsonb_stats: {} for key '{}' has no 'counts' object",
                    t,
                    key
                )
            };
            let mut cats: Vec<&String> = counts.keys().collect();
            cats.sort_by(|a, b| jsonb_key_order(a, b));
            for c in cats {
                let n = match &counts[c] {
                    Value::Number(n) => Decimal::from_str(n.as_str())
                        .ok()
                        .and_then(|d| d.to_i64())
                        .filter(|v| *v >= 0),
                    _ => None,
                }
                .unwrap_or_else(|| {
                    pgrx::error!(
                        "jsonb_stats: {} for key '{}' has an invalid count for '{}'",
                        t,
                        key,
                        c
                    )
                });
                rows.push((key.clone(), base.clone(), None, Some(c.clone()), n));
            }
        } else {
            pgrx::error!(
                "jsonb_stats: unknown aggregate type '{}'. {}",
                t,
                UNKNOWN_AGG_TYPE
            );
        }
    }
    rows
}

/// Variables of a `stats` or `stats_agg` as rows: numeric keys give
/// (value = sum, count); counted keys give one row per category with its
/// count. A `stats` is read as jsonb_stats_to_agg(stats).
// pgrx needs the name!() columns inline in the signature, so no type alias.
#[allow(clippy::type_complexity)]
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_values(
    x: JsonB,
) -> TableIterator<
    'static,
    (
        name!(key, String),
        name!(stat_type, String),
        name!(value, Option<AnyNumeric>),
        name!(text_value, Option<String>),
        name!(count, i64),
    ),
> {
    let Value::Object(m) = x.0 else {
        pgrx::error!("jsonb_stats: jsonb_stats_values input must be an object")
    };
    let agg = match m.get("type") {
        Some(Value::String(t)) if t == "stats" => {
            let mut st = StatsState::default();
            st.accumulate(m);
            match st.to_stats_agg() {
                Value::Object(a) => a,
                _ => unreachable!(),
            }
        }
        Some(Value::String(t)) if t == "stats_delta" => pgrx::error!(
            "jsonb_stats: jsonb_stats_values cannot read a stats_delta; read it with jsonb_stats_delta_to_agg first"
        ),
        None => m,
        Some(Value::String(t)) if t == "stats_agg" => m,
        Some(other) => pgrx::error!("jsonb_stats: expected stats or stats_agg, got type {}", other),
    };
    TableIterator::new(value_rows(agg))
}

// ── compare ──

/// Exact integers of two states at one common scale s: sums S (scaled by
/// 10^s) and N = n sum_sq - sum^2 (scaled by 10^2s).
fn common(
    a: &(i64, Decimal, Decimal),
    b: &(i64, Decimal, Decimal),
) -> (BigInt, BigInt, BigInt, BigInt) {
    let s = [
        a.1.scale(),
        b.1.scale(),
        a.2.scale().div_ceil(2),
        b.2.scale().div_ceil(2),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    let parts = |x: &(i64, Decimal, Decimal)| {
        let s1 = x.1.mantissa_at(s);
        let s2 = x.2.mantissa_at(2 * s);
        let n = BigInt::from(x.0) * s2 - &s1 * &s1;
        (s1, n)
    };
    let (sa, na) = parts(a);
    let (sb, nb) = parts(b);
    (sa, sb, na, nb)
}

fn compare_key(kind: NumKind, a: (i64, Decimal, Decimal), b: (i64, Decimal, Decimal)) -> Value {
    let (s_a, s_b, big_a, big_b) = common(&a, &b);
    let (na, nb) = (BigInt::from(a.0), BigInt::from(b.0));
    let mut m = Map::new();
    m.insert("type".into(), json!(kind.stat_type()));
    m.insert("count_a".into(), json!(a.0));
    m.insert("count_b".into(), json!(b.0));
    m.insert(
        "change_pct".into(),
        if s_a.is_zero() {
            Value::Null
        } else {
            ratio_round2(&((&s_b - &s_a) * 100u8), &s_a.abs()).to_json()
        },
    );
    m.insert(
        "mean_change_pct".into(),
        if a.0 > 0 && b.0 > 0 && !s_a.is_zero() {
            ratio_round2(&((&s_b * &na - &s_a * &nb) * 100u8), &(&nb * s_a.abs())).to_json()
        } else {
            Value::Null
        },
    );
    let mut z = Value::Null;
    if a.0 >= 2 && b.0 >= 2 {
        let d = &s_b * &na - &s_a * &nb;
        let den = &big_a * &nb * &nb * (&nb - 1u8) + &big_b * &na * &na * (&na - 1u8);
        if den.is_positive() {
            let mag = ratio_sqrt_round2(&(&d * &d * (&na - 1u8) * (&nb - 1u8)), &den);
            z = if d.is_negative() { mag.negated() } else { mag }.to_json();
        }
    }
    m.insert("z".into(), z);
    Value::Object(m)
}

/// Compare a subject `b` with a reference `a` (previous period, or peers),
/// both stats_agg, per numeric variable: change_pct, mean_change_pct and a
/// Welch z of the difference in means. Spec: jsonb_stats_compare_plpgsql
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_compare(a: JsonB, b: JsonB) -> JsonB {
    let (Value::Object(a), Value::Object(b)) = (a.0, b.0) else {
        pgrx::error!("jsonb_stats: jsonb_stats_compare inputs must be objects")
    };
    let is_agg = |m: &Map<String, Value>| matches!(m.get("type"), Some(Value::String(t)) if t == "stats_agg");
    if !is_agg(&a) || !is_agg(&b) {
        let show = |m: &Map<String, Value>| {
            m.get("type")
                .map(|v| v.to_string())
                .unwrap_or("NULL".into())
        };
        pgrx::error!(
            "jsonb_stats: jsonb_stats_compare expects two stats_agg, got {} and {}",
            show(&a),
            show(&b)
        );
    }
    let mut keys: Vec<&String> = a.keys().chain(b.keys()).filter(|k| *k != "type").collect();
    keys.sort();
    keys.dedup();
    let mut out = Map::new();
    out.insert("type".into(), json!("stats_compare"));
    for key in keys {
        let side = |m: &Map<String, Value>| -> Option<Map<String, Value>> {
            match m.get(key) {
                None => None,
                Some(Value::Object(o)) => Some(o.clone()),
                Some(_) => pgrx::error!("jsonb_stats: summary for key '{}' must be an object", key),
            }
        };
        let (sa, sb) = (side(&a), side(&b));
        if let (Some(x), Some(y)) = (&sa, &sb) {
            if agg_type_of(x) != agg_type_of(y) {
                pgrx::error!(
                    "jsonb_stats: type mismatch for key '{}': existing {} vs incoming {}",
                    key,
                    agg_type_of(x),
                    agg_type_of(y)
                );
            }
        }
        let t = sa
            .as_ref()
            .or(sb.as_ref())
            .map(agg_type_of)
            .unwrap_or("")
            .to_string();
        if is_counted(&t) {
            continue;
        }
        let Some(kind) = NumKind::from_agg_type(&t) else {
            pgrx::error!(
                "jsonb_stats: unknown aggregate type '{}'. {}",
                t,
                UNKNOWN_AGG_TYPE
            )
        };
        let empty = || (0i64, Decimal::zero(), Decimal::zero());
        let state = |o: &Option<Map<String, Value>>| match o {
            Some(o) => num_state(kind, key, o),
            None => empty(),
        };
        out.insert(key.clone(), compare_key(kind, state(&sa), state(&sb)));
    }
    JsonB(Value::Object(out))
}
