use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::{json, Map, Number, Value};

use crate::helpers::*;

/// Accumulate a single stats object into the running state (stats -> stats_agg).
///
/// For each key in `stats` (skipping "type"):
/// - INIT path: create a new *_agg summary from the stat value
/// - UPDATE path: update the existing summary with the new value
///
/// Spec: dev/reference_plpgsql.sql lines 8-92
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_accum(state: JsonB, stats: JsonB) -> JsonB {
    let mut new_state: Map<String, Value> = match state.0 {
        Value::Object(m) => m,
        _ => Map::new(),
    };

    let stats_map = match stats.0 {
        Value::Object(m) => m,
        _ => return JsonB(Value::Object(new_state)),
    };

    for (key, stat_obj) in stats_map {
        if key == "type" {
            continue;
        }

        let stat_map = match stat_obj {
            Value::Object(m) => m,
            _ => continue,
        };

        let stat_type = match stat_map.get("type") {
            Some(Value::String(s)) => s.as_str(),
            _ => continue,
        };

        let summary = if let Some(current) = new_state.remove(&key) {
            // UPDATE path
            update_summary(current, &stat_map, stat_type)
        } else {
            // INIT path
            init_summary(&stat_map, stat_type)
        };

        new_state.insert(key, summary);
    }

    JsonB(Value::Object(new_state))
}

/// Initialize a new aggregate summary from a single stat value.
fn init_summary(stat: &Map<String, Value>, stat_type: &str) -> Value {
    match stat_type {
        "int" => init_int_agg(stat),
        "str" | "bool" => init_str_or_bool_agg(stat, stat_type),
        "arr" => init_arr_agg(stat),
        _ => Value::Object(stat.clone()),
    }
}

fn init_int_agg(stat: &Map<String, Value>) -> Value {
    let val = get_f64(stat, "value");
    let mut result = Map::new();
    result.insert("type".to_string(), json!("int_agg"));
    result.insert("count".to_string(), Value::Number(Number::from(1)));
    result.insert("sum".to_string(), num_value(val));
    result.insert("min".to_string(), num_value(val));
    result.insert("max".to_string(), num_value(val));
    result.insert("mean".to_string(), num_value(val));
    result.insert("sum_sq_diff".to_string(), Value::Number(Number::from(0)));
    Value::Object(result)
}

fn init_str_or_bool_agg(stat: &Map<String, Value>, stat_type: &str) -> Value {
    let val_str = match stat.get("value") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => n.to_string(),
        _ => return Value::Null,
    };

    let agg_type = format!("{}_agg", stat_type);
    let mut counts = Map::new();
    counts.insert(val_str, Value::Number(Number::from(1)));

    let mut result = Map::new();
    result.insert("type".to_string(), json!(agg_type));
    result.insert("counts".to_string(), Value::Object(counts));
    Value::Object(result)
}

fn init_arr_agg(stat: &Map<String, Value>) -> Value {
    let mut counts = Map::new();

    // The value can be a JSON array or a PostgreSQL array text representation
    if let Some(Value::Array(arr)) = stat.get("value") {
        for elem in arr {
            let key = match elem {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => continue,
            };
            let existing: i64 = counts
                .get(&key)
                .and_then(|v| match v {
                    Value::Number(n) => n.to_string().parse().ok(),
                    _ => None,
                })
                .unwrap_or(0);
            counts.insert(key, Value::Number(Number::from(existing + 1)));
        }
    } else if let Some(Value::String(s)) = stat.get("value") {
        // PostgreSQL array text format: {a,b,c}
        let trimmed = s.trim_matches(|c| c == '{' || c == '}');
        if !trimmed.is_empty() {
            for elem in trimmed.split(',') {
                let elem = elem.trim();
                let existing: i64 = counts
                    .get(elem)
                    .and_then(|v| match v {
                        Value::Number(n) => n.to_string().parse().ok(),
                        _ => None,
                    })
                    .unwrap_or(0);
                counts.insert(elem.to_string(), Value::Number(Number::from(existing + 1)));
            }
        }
    }

    let mut result = Map::new();
    result.insert("type".to_string(), json!("arr_agg"));
    result.insert("count".to_string(), Value::Number(Number::from(1)));
    result.insert("counts".to_string(), Value::Object(counts));
    Value::Object(result)
}

/// Update an existing aggregate summary with a new stat value.
fn update_summary(current: Value, stat: &Map<String, Value>, stat_type: &str) -> Value {
    let current_obj = match current {
        Value::Object(m) => m,
        _ => return init_summary(stat, stat_type),
    };

    match stat_type {
        "int" => update_int_agg(current_obj, stat),
        "str" | "bool" => update_str_or_bool_agg(current_obj, stat),
        "arr" => update_arr_agg(current_obj, stat),
        _ => Value::Object(current_obj),
    }
}

/// Welford single-value update for int_agg.
fn update_int_agg(mut obj: Map<String, Value>, stat: &Map<String, Value>) -> Value {
    let val = get_f64(stat, "value");
    let count = get_f64(&obj, "count") + 1.0;
    let old_mean = get_f64(&obj, "mean");
    let delta = val - old_mean;
    let new_mean = old_mean + delta / count;
    let new_ssd = get_f64(&obj, "sum_sq_diff") + delta * (val - new_mean);

    obj.insert("type".to_string(), json!("int_agg"));
    obj.insert("count".to_string(), num_value(count));
    obj.insert(
        "sum".to_string(),
        num_value(get_f64(&obj, "sum") + val),
    );
    obj.insert(
        "min".to_string(),
        num_value(get_f64(&obj, "min").min(val)),
    );
    obj.insert(
        "max".to_string(),
        num_value(get_f64(&obj, "max").max(val)),
    );
    obj.insert("mean".to_string(), num_value(new_mean));
    obj.insert("sum_sq_diff".to_string(), num_value(new_ssd));
    Value::Object(obj)
}

/// Increment count for str_agg or bool_agg.
fn update_str_or_bool_agg(mut obj: Map<String, Value>, stat: &Map<String, Value>) -> Value {
    let val_str = match stat.get("value") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => n.to_string(),
        _ => return Value::Object(obj),
    };

    let mut counts: Map<String, Value> = obj
        .remove("counts")
        .and_then(|v| match v {
            Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default();

    let current: i64 = counts
        .get(&val_str)
        .and_then(|v| match v {
            Value::Number(n) => n.to_string().parse().ok(),
            _ => None,
        })
        .unwrap_or(0);
    counts.insert(val_str, Value::Number(Number::from(current + 1)));

    obj.insert("counts".to_string(), Value::Object(counts));
    Value::Object(obj)
}

/// Update arr_agg: increment count and add element counts.
fn update_arr_agg(mut obj: Map<String, Value>, stat: &Map<String, Value>) -> Value {
    let old_count = get_i64(&obj, "count");
    obj.insert(
        "count".to_string(),
        Value::Number(Number::from(old_count + 1)),
    );

    let mut counts: Map<String, Value> = obj
        .remove("counts")
        .and_then(|v| match v {
            Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default();

    if let Some(Value::Array(arr)) = stat.get("value") {
        for elem in arr {
            let key = match elem {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => continue,
            };
            let existing: i64 = counts
                .get(&key)
                .and_then(|v| match v {
                    Value::Number(n) => n.to_string().parse().ok(),
                    _ => None,
                })
                .unwrap_or(0);
            counts.insert(key, Value::Number(Number::from(existing + 1)));
        }
    } else if let Some(Value::String(s)) = stat.get("value") {
        let trimmed = s.trim_matches(|c| c == '{' || c == '}');
        if !trimmed.is_empty() {
            for elem in trimmed.split(',') {
                let elem = elem.trim();
                let existing: i64 = counts
                    .get(elem)
                    .and_then(|v| match v {
                        Value::Number(n) => n.to_string().parse().ok(),
                        _ => None,
                    })
                    .unwrap_or(0);
                counts.insert(elem.to_string(), Value::Number(Number::from(existing + 1)));
            }
        }
    }

    obj.insert("counts".to_string(), Value::Object(counts));
    Value::Object(obj)
}
