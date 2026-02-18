use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::{json, Map, Number, Value};

use crate::helpers::*;

/// Merge two stats_agg JSONB objects (Welford parallel merge for int_agg,
/// count-map merging for str_agg/bool_agg/arr_agg).
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

    match get_type(&a_obj) {
        "int_agg" => merge_int_agg(a_obj, &b_obj),
        "str_agg" | "bool_agg" => merge_count_agg(a_obj, &b_obj, false),
        "arr_agg" => merge_count_agg(a_obj, &b_obj, true),
        _ => Value::Object(a_obj),
    }
}

/// Welford parallel merge for int_agg summaries.
fn merge_int_agg(a: Map<String, Value>, b: &Map<String, Value>) -> Value {
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

    let mut result = Map::new();
    result.insert("type".to_string(), json!("int_agg"));
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
