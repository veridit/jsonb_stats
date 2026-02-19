use std::collections::HashMap;

use pgrx::prelude::*;
use pgrx::{Internal, JsonB};
use serde_json::{json, Map, Number, Value};

use crate::helpers::*;
use crate::state::{AggEntry, NumFields, StatsState};

/// Merge two stats_agg JSONB objects (Welford parallel merge for numeric aggs,
/// count-map merging for str_agg/bool_agg/arr_agg/date_agg).
///
/// Spec: dev/reference_plpgsql.sql lines 95-141
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_merge(a: JsonB, b: JsonB) -> JsonB {
    let mut merged: Map<String, Value> = match a.0 {
        Value::Object(m) => m,
        _ => Map::new(),
    };

    let b_map = match b.0 {
        Value::Object(m) => m,
        _ => return JsonB(Value::Object(merged)),
    };

    for (key, summary_b) in b_map {
        if key == "type" {
            continue;
        }

        if let Some(summary_a) = merged.remove(&key) {
            merged.insert(key, merge_summaries(summary_a, summary_b));
        } else {
            // Key only in b — adopt directly
            merged.insert(key, summary_b);
        }
    }

    JsonB(Value::Object(merged))
}

fn merge_summaries(a: Value, b: Value) -> Value {
    let a_obj = match a {
        Value::Object(m) => m,
        _ => return b,
    };
    let b_obj = match b {
        Value::Object(m) => m,
        _ => return Value::Object(a_obj),
    };

    let a_type = get_type(&a_obj);
    let b_type = get_type(&b_obj);
    if a_type != b_type {
        pgrx::error!(
            "jsonb_stats: type mismatch in merge: '{}' vs '{}'",
            a_type, b_type
        );
    }

    match a_type {
        "int_agg" | "float_agg" | "dec2_agg" | "nat_agg" => merge_num_agg(a_obj, &b_obj),
        "str_agg" | "bool_agg" => merge_count_agg(a_obj, &b_obj, false),
        "arr_agg" => merge_count_agg(a_obj, &b_obj, true),
        "date_agg" => merge_date_agg(a_obj, &b_obj),
        other => pgrx::error!(
            "jsonb_stats: unknown aggregate type '{}'. Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg",
            other
        ),
    }
}

/// Welford parallel merge for any numeric agg summaries.
/// Preserves the original type tag from a_obj.
fn merge_num_agg(a: Map<String, Value>, b: &Map<String, Value>) -> Value {
    let count_a = get_f64(&a, "count");
    let count_b = get_f64(b, "count");
    let total_count = count_a + count_b;

    let mean_a = get_f64(&a, "mean");
    let mean_b = get_f64(b, "mean");
    let delta = mean_b - mean_a;

    let new_mean = mean_a + (delta * count_b / total_count);
    let new_ssd = get_f64(&a, "sum_sq_diff")
        + get_f64(b, "sum_sq_diff")
        + (delta * delta * count_a * count_b) / total_count;
    let new_sum = get_f64(&a, "sum") + get_f64(b, "sum");
    let new_min = get_f64(&a, "min").min(get_f64(b, "min"));
    let new_max = get_f64(&a, "max").max(get_f64(b, "max"));

    let type_tag = get_type(&a);

    let mut result = Map::new();
    result.insert("type".to_string(), json!(type_tag));
    result.insert("count".to_string(), num_value(total_count));
    result.insert("sum".to_string(), num_value(new_sum));
    result.insert("min".to_string(), num_value(new_min));
    result.insert("max".to_string(), num_value(new_max));
    result.insert("mean".to_string(), num_value(new_mean));
    result.insert("sum_sq_diff".to_string(), num_value(new_ssd));
    Value::Object(result)
}

/// Merge count maps for str_agg, bool_agg, arr_agg.
/// For arr_agg, also sums the top-level "count" field.
fn merge_count_agg(
    mut a_obj: Map<String, Value>,
    b_obj: &Map<String, Value>,
    is_arr: bool,
) -> Value {
    if is_arr {
        let count_a = get_i64(&a_obj, "count");
        let count_b = get_i64(b_obj, "count");
        a_obj.insert(
            "count".to_string(),
            Value::Number(Number::from(count_a + count_b)),
        );
    }

    // Remove counts from a so we can mutate it independently
    let mut counts_a: Map<String, Value> = a_obj
        .remove("counts")
        .and_then(|v| match v {
            Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default();

    if let Some(Value::Object(counts_b)) = b_obj.get("counts") {
        for (k, v) in counts_b {
            let v_int: i64 = match v {
                Value::Number(n) => n.to_string().parse().unwrap_or(0),
                _ => 0,
            };
            let existing: i64 = counts_a
                .get(k)
                .and_then(|v| match v {
                    Value::Number(n) => n.to_string().parse().ok(),
                    _ => None,
                })
                .unwrap_or(0);
            counts_a.insert(k.clone(), Value::Number(Number::from(existing + v_int)));
        }
    }

    a_obj.insert("counts".to_string(), Value::Object(counts_a));
    Value::Object(a_obj)
}

/// Merge two date_agg objects: merge count maps + min/max dates.
fn merge_date_agg(mut a_obj: Map<String, Value>, b_obj: &Map<String, Value>) -> Value {
    // Merge counts
    let mut counts_a: Map<String, Value> = a_obj
        .remove("counts")
        .and_then(|v| match v {
            Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default();

    if let Some(Value::Object(counts_b)) = b_obj.get("counts") {
        for (k, v) in counts_b {
            let v_int: i64 = match v {
                Value::Number(n) => n.to_string().parse().unwrap_or(0),
                _ => 0,
            };
            let existing: i64 = counts_a
                .get(k)
                .and_then(|v| match v {
                    Value::Number(n) => n.to_string().parse().ok(),
                    _ => None,
                })
                .unwrap_or(0);
            counts_a.insert(k.clone(), Value::Number(Number::from(existing + v_int)));
        }
    }
    a_obj.insert("counts".to_string(), Value::Object(counts_a));

    // Merge min (lexicographic — ISO dates sort correctly)
    if let Some(b_min) = get_str(b_obj, "min") {
        match get_str(&a_obj, "min") {
            Some(a_min) if b_min < a_min => {
                a_obj.insert("min".to_string(), json!(b_min));
            }
            None => {
                a_obj.insert("min".to_string(), json!(b_min));
            }
            _ => {}
        }
    }

    // Merge max
    if let Some(b_max) = get_str(b_obj, "max") {
        match get_str(&a_obj, "max") {
            Some(a_max) if b_max > a_max => {
                a_obj.insert("max".to_string(), json!(b_max));
            }
            None => {
                a_obj.insert("max".to_string(), json!(b_max));
            }
            _ => {}
        }
    }

    Value::Object(a_obj)
}

// ── Internal-state merge sfunc (avoids serde_json round-trip on growing state) ──

/// Merge sfunc using pgrx Internal state. Each input stats_agg JSONB is
/// parsed once into native AggEntry types and merged into the HashMap state.
/// The growing state is never serialized back to JSONB until the finalfunc.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_merge_sfunc(internal: Internal, agg: Option<pgrx::JsonB>) -> Internal {
    let state_ptr: *mut StatsState = match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<StatsState>(),
        None => Box::into_raw(Box::new(StatsState::default())),
    };

    let agg = match agg {
        Some(a) => a,
        None => return Internal::from(Some(pgrx::pg_sys::Datum::from(state_ptr as usize))),
    };

    let state = unsafe { &mut *state_ptr };

    let agg_map = match agg.0 {
        Value::Object(m) => m,
        _ => return Internal::from(Some(pgrx::pg_sys::Datum::from(state_ptr as usize))),
    };

    for (key, summary) in agg_map {
        if key == "type" {
            continue;
        }

        let obj = match summary {
            Value::Object(m) => m,
            _ => continue,
        };

        let incoming = parse_agg_entry(&obj);
        match state.entries.get_mut(&key) {
            Some(existing) => merge_agg_entries(existing, incoming, &key),
            None => {
                state.entries.insert(key, incoming);
            }
        }
    }

    Internal::from(Some(pgrx::pg_sys::Datum::from(state_ptr as usize)))
}

/// Parse a JSONB *_agg object into a native AggEntry.
fn parse_agg_entry(obj: &Map<String, Value>) -> AggEntry {
    match get_type(obj) {
        "int_agg" => AggEntry::IntAgg(parse_num_fields(obj)),
        "float_agg" => AggEntry::FloatAgg(parse_num_fields(obj)),
        "dec2_agg" => AggEntry::Dec2Agg(parse_num_fields(obj)),
        "nat_agg" => AggEntry::NatAgg(parse_num_fields(obj)),
        "str_agg" => AggEntry::StrAgg {
            counts: parse_counts(obj),
        },
        "bool_agg" => AggEntry::BoolAgg {
            counts: parse_counts(obj),
        },
        "arr_agg" => AggEntry::ArrAgg {
            count: get_f64(obj, "count") as i64,
            counts: parse_counts(obj),
        },
        "date_agg" => AggEntry::DateAgg {
            counts: parse_counts(obj),
            min_date: get_str(obj, "min").map(|s| s.to_string()),
            max_date: get_str(obj, "max").map(|s| s.to_string()),
        },
        other => pgrx::error!(
            "jsonb_stats: unknown aggregate type '{}'. Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg",
            other
        ),
    }
}

fn parse_num_fields(obj: &Map<String, Value>) -> NumFields {
    NumFields {
        count: get_f64(obj, "count") as i64,
        sum: get_f64(obj, "sum"),
        min: get_f64(obj, "min"),
        max: get_f64(obj, "max"),
        mean: get_f64(obj, "mean"),
        sum_sq_diff: get_f64(obj, "sum_sq_diff"),
    }
}

/// Parse the "counts" sub-object from a JSONB *_agg into a HashMap.
fn parse_counts(obj: &Map<String, Value>) -> HashMap<String, i64> {
    let mut result = HashMap::new();
    if let Some(Value::Object(counts)) = obj.get("counts") {
        for (k, v) in counts {
            let n: i64 = match v {
                Value::Number(n) => n.to_string().parse().unwrap_or(0),
                _ => 0,
            };
            result.insert(k.clone(), n);
        }
    }
    result
}

/// Welford parallel merge and count-map merge on native AggEntry types.
pub fn merge_agg_entries(existing: &mut AggEntry, incoming: AggEntry, key: &str) {
    // Fail fast on type mismatch
    let e_tag = existing.type_tag();
    let i_tag = incoming.type_tag();
    if e_tag != i_tag {
        pgrx::error!(
            "jsonb_stats: type mismatch for key '{}': existing {} vs incoming {}",
            key, e_tag, i_tag
        );
    }

    match (existing, incoming) {
        // All numeric types: use NumFields::merge
        (AggEntry::IntAgg(a), AggEntry::IntAgg(b))
        | (AggEntry::FloatAgg(a), AggEntry::FloatAgg(b))
        | (AggEntry::Dec2Agg(a), AggEntry::Dec2Agg(b))
        | (AggEntry::NatAgg(a), AggEntry::NatAgg(b)) => {
            a.merge(&b);
        }
        (AggEntry::StrAgg { counts: ca }, AggEntry::StrAgg { counts: cb })
        | (AggEntry::BoolAgg { counts: ca }, AggEntry::BoolAgg { counts: cb }) => {
            for (k, v) in cb {
                *ca.entry(k).or_insert(0) += v;
            }
        }
        (
            AggEntry::ArrAgg {
                count: count_a,
                counts: ca,
            },
            AggEntry::ArrAgg {
                count: count_b,
                counts: cb,
            },
        ) => {
            *count_a += count_b;
            for (k, v) in cb {
                *ca.entry(k).or_insert(0) += v;
            }
        }
        (
            AggEntry::DateAgg {
                counts: ca,
                min_date: min_a,
                max_date: max_a,
            },
            AggEntry::DateAgg {
                counts: cb,
                min_date: min_b,
                max_date: max_b,
            },
        ) => {
            for (k, v) in cb {
                *ca.entry(k).or_insert(0) += v;
            }
            // Merge min
            match (&*min_a, &min_b) {
                (Some(a), Some(b)) if b < a => *min_a = Some(b.clone()),
                (None, Some(_)) => *min_a = min_b,
                _ => {}
            }
            // Merge max
            match (&*max_a, &max_b) {
                (Some(a), Some(b)) if b > a => *max_a = Some(b.clone()),
                (None, Some(_)) => *max_a = max_b,
                _ => {}
            }
        }
        _ => unreachable!(), // type_tag check above guarantees matching variants
    }
}
