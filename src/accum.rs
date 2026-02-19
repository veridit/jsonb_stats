use std::collections::HashMap;

use pgrx::prelude::*;
use pgrx::{Internal, JsonB};
use serde_json::{json, Map, Number, Value};

use crate::helpers::*;
use crate::state::{AggEntry, NumFields, StatsState};

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
        "int" | "float" | "dec2" => init_num_agg(stat, stat_type),
        "nat" => {
            let val = get_f64(stat, "value");
            if val < 0.0 {
                pgrx::error!("jsonb_stats: nat value must be >= 0, got {}", val);
            }
            init_num_agg(stat, "nat")
        }
        "str" | "bool" => init_str_or_bool_agg(stat, stat_type),
        "arr" => init_arr_agg(stat),
        "date" => init_date_agg(stat),
        other => pgrx::error!(
            "jsonb_stats: unknown stat type '{}'. Expected: int, float, dec2, nat, str, bool, arr, date",
            other
        ),
    }
}

fn init_num_agg(stat: &Map<String, Value>, stat_type: &str) -> Value {
    let val = get_f64(stat, "value");
    let agg_type = format!("{}_agg", stat_type);
    let mut result = Map::new();
    result.insert("type".to_string(), json!(agg_type));
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
        _ => pgrx::error!("jsonb_stats: stat of type '{}' has missing or invalid 'value'", stat_type),
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

fn init_date_agg(stat: &Map<String, Value>) -> Value {
    let date_str = match stat.get("value") {
        Some(Value::String(s)) => s.clone(),
        _ => pgrx::error!("jsonb_stats: date stat requires a string 'value'"),
    };

    let mut counts = Map::new();
    counts.insert(date_str.clone(), Value::Number(Number::from(1)));

    let mut result = Map::new();
    result.insert("type".to_string(), json!("date_agg"));
    result.insert("counts".to_string(), Value::Object(counts));
    result.insert("min".to_string(), json!(date_str));
    result.insert("max".to_string(), json!(date_str));
    Value::Object(result)
}

/// Update an existing aggregate summary with a new stat value.
fn update_summary(current: Value, stat: &Map<String, Value>, stat_type: &str) -> Value {
    let current_obj = match current {
        Value::Object(m) => m,
        _ => return init_summary(stat, stat_type),
    };

    match stat_type {
        "int" | "float" | "dec2" => update_num_agg(current_obj, stat),
        "nat" => {
            let val = get_f64(stat, "value");
            if val < 0.0 {
                pgrx::error!("jsonb_stats: nat value must be >= 0, got {}", val);
            }
            update_num_agg(current_obj, stat)
        }
        "str" | "bool" => update_str_or_bool_agg(current_obj, stat),
        "arr" => update_arr_agg(current_obj, stat),
        "date" => update_date_agg(current_obj, stat),
        other => pgrx::error!(
            "jsonb_stats: unknown stat type '{}'. Expected: int, float, dec2, nat, str, bool, arr, date",
            other
        ),
    }
}

/// Welford single-value update for any numeric agg type.
fn update_num_agg(mut obj: Map<String, Value>, stat: &Map<String, Value>) -> Value {
    let val = get_f64(stat, "value");
    let count = get_f64(&obj, "count") + 1.0;
    let old_mean = get_f64(&obj, "mean");
    let delta = val - old_mean;
    let new_mean = old_mean + delta / count;
    let new_ssd = get_f64(&obj, "sum_sq_diff") + delta * (val - new_mean);

    // Preserve the existing type tag
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

/// Update date_agg: increment count for date string, update min/max.
fn update_date_agg(mut obj: Map<String, Value>, stat: &Map<String, Value>) -> Value {
    let date_str = match stat.get("value") {
        Some(Value::String(s)) => s.clone(),
        _ => return Value::Object(obj),
    };

    // Update counts
    let mut counts: Map<String, Value> = obj
        .remove("counts")
        .and_then(|v| match v {
            Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default();

    let current: i64 = counts
        .get(&date_str)
        .and_then(|v| match v {
            Value::Number(n) => n.to_string().parse().ok(),
            _ => None,
        })
        .unwrap_or(0);
    counts.insert(date_str.clone(), Value::Number(Number::from(current + 1)));
    obj.insert("counts".to_string(), Value::Object(counts));

    // Update min/max via string compare (ISO dates sort lexicographically)
    if let Some(Value::String(cur_min)) = obj.get("min") {
        if date_str < *cur_min {
            obj.insert("min".to_string(), json!(date_str));
        }
    }
    if let Some(Value::String(cur_max)) = obj.get("max") {
        if date_str > *cur_max {
            obj.insert("max".to_string(), json!(date_str));
        }
    }

    Value::Object(obj)
}

// ── Internal-state sfunc for the aggregate (avoids serde_json round-trip per row) ──

/// Aggregate sfunc using pgrx Internal state. The state is a native Rust
/// StatsState allocated on the Rust heap (Box), avoiding both JSONB
/// serialization per row and PostgreSQL memory context lifetime issues.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_accum_sfunc(
    internal: Internal,
    stats: pgrx::JsonB,
) -> Internal {
    // Extract existing state or create new one on the Rust heap.
    // Box::into_raw ensures the allocation survives PG memory context resets.
    let state_ptr: *mut StatsState = match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<StatsState>(),
        None => Box::into_raw(Box::new(StatsState::default())),
    };

    let state = unsafe { &mut *state_ptr };

    let stats_map = match stats.0 {
        Value::Object(m) => m,
        _ => {
            return Internal::from(Some(pgrx::pg_sys::Datum::from(state_ptr as usize)));
        }
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
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };

        if let Some(entry) = state.entries.get_mut(&key) {
            update_entry(entry, &stat_map, &stat_type);
        } else {
            state.entries.insert(key, init_entry(&stat_map, &stat_type));
        }
    }

    Internal::from(Some(pgrx::pg_sys::Datum::from(state_ptr as usize)))
}

fn init_entry(stat: &Map<String, Value>, stat_type: &str) -> AggEntry {
    match stat_type {
        "int" => {
            let val = get_f64(stat, "value");
            AggEntry::IntAgg(NumFields::init(val))
        }
        "float" => {
            let val = get_f64(stat, "value");
            AggEntry::FloatAgg(NumFields::init(val))
        }
        "dec2" => {
            let val = get_f64(stat, "value");
            AggEntry::Dec2Agg(NumFields::init(val))
        }
        "nat" => {
            let val = get_f64(stat, "value");
            if val < 0.0 {
                pgrx::error!("jsonb_stats: nat value must be >= 0, got {}", val);
            }
            AggEntry::NatAgg(NumFields::init(val))
        }
        "str" => {
            let val_str = value_to_string(stat)
                .unwrap_or_else(|| pgrx::error!("jsonb_stats: stat of type 'str' has missing or invalid 'value'"));
            let mut counts = HashMap::new();
            counts.insert(val_str, 1);
            AggEntry::StrAgg { counts }
        }
        "bool" => {
            let val_str = value_to_string(stat)
                .unwrap_or_else(|| pgrx::error!("jsonb_stats: stat of type 'bool' has missing or invalid 'value'"));
            let mut counts = HashMap::new();
            counts.insert(val_str, 1);
            AggEntry::BoolAgg { counts }
        }
        "arr" => {
            let mut counts = HashMap::new();
            collect_arr_counts(stat, &mut counts);
            AggEntry::ArrAgg { count: 1, counts }
        }
        "date" => {
            let date_str = match stat.get("value") {
                Some(Value::String(s)) => s.clone(),
                _ => pgrx::error!("jsonb_stats: date stat requires a string 'value'"),
            };
            let mut counts = HashMap::new();
            counts.insert(date_str.clone(), 1);
            AggEntry::DateAgg {
                counts,
                min_date: Some(date_str.clone()),
                max_date: Some(date_str),
            }
        }
        other => pgrx::error!(
            "jsonb_stats: unknown stat type '{}'. Expected: int, float, dec2, nat, str, bool, arr, date",
            other
        ),
    }
}

fn update_entry(entry: &mut AggEntry, stat: &Map<String, Value>, _stat_type: &str) {
    match entry {
        AggEntry::IntAgg(f) | AggEntry::FloatAgg(f) | AggEntry::Dec2Agg(f) => {
            let val = get_f64(stat, "value");
            f.update(val);
        }
        AggEntry::NatAgg(f) => {
            let val = get_f64(stat, "value");
            if val < 0.0 {
                pgrx::error!("jsonb_stats: nat value must be >= 0, got {}", val);
            }
            f.update(val);
        }
        AggEntry::StrAgg { counts } | AggEntry::BoolAgg { counts } => {
            if let Some(val_str) = value_to_string(stat) {
                *counts.entry(val_str).or_insert(0) += 1;
            }
        }
        AggEntry::ArrAgg { count, counts } => {
            *count += 1;
            collect_arr_counts(stat, counts);
        }
        AggEntry::DateAgg {
            counts,
            min_date,
            max_date,
        } => {
            if let Some(Value::String(date_str)) = stat.get("value") {
                *counts.entry(date_str.clone()).or_insert(0) += 1;
                match min_date {
                    Some(cur) if date_str < cur => *min_date = Some(date_str.clone()),
                    None => *min_date = Some(date_str.clone()),
                    _ => {}
                }
                match max_date {
                    Some(cur) if date_str > cur => *max_date = Some(date_str.clone()),
                    None => *max_date = Some(date_str.clone()),
                    _ => {}
                }
            }
        }
    }
}

fn value_to_string(stat: &Map<String, Value>) -> Option<String> {
    match stat.get("value") {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

fn collect_arr_counts(stat: &Map<String, Value>, counts: &mut HashMap<String, i64>) {
    if let Some(Value::Array(arr)) = stat.get("value") {
        for elem in arr {
            let key = match elem {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => continue,
            };
            *counts.entry(key).or_insert(0) += 1;
        }
    } else if let Some(Value::String(s)) = stat.get("value") {
        let trimmed = s.trim_matches(|c| c == '{' || c == '}');
        if !trimmed.is_empty() {
            for elem in trimmed.split(',') {
                *counts.entry(elem.trim().to_string()).or_insert(0) += 1;
            }
        }
    }
}
