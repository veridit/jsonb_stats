use pgrx::prelude::*;
use pgrx::{Internal, JsonB};
use serde_json::{json, Map, Number, Value};

use crate::helpers::*;
use crate::state::{AggEntry, StatsState};

/// Compute derived statistics (variance, stddev, cv_pct) for int_agg summaries,
/// add "type": "stats_agg" to the result, and round numeric fields to 2 decimal places.
///
/// Spec: dev/reference_plpgsql.sql lines 145-176
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_final(state: JsonB) -> JsonB {
    let state_map = match state.0 {
        Value::Object(m) => m,
        _ => return state,
    };

    let mut result = Map::new();
    result.insert("type".to_string(), json!("stats_agg"));

    for (key, summary) in state_map {
        if key == "type" {
            continue;
        }

        let finalized = match summary {
            Value::Object(obj) if get_type(&obj) == "int_agg" => finalize_int_agg(obj),
            other => other,
        };

        result.insert(key, finalized);
    }

    JsonB(Value::Object(result))
}

/// Add derived stats to an int_agg summary and round numeric fields.
fn finalize_int_agg(mut obj: Map<String, Value>) -> Value {
    let count = get_f64(&obj, "count");
    let mean = get_f64(&obj, "mean");
    let ssd = get_f64(&obj, "sum_sq_diff");

    // variance = sum_sq_diff / (count - 1), NULL if count <= 1
    let (variance, stddev, cv_pct) = if count > 1.0 {
        let var = ssd / (count - 1.0);
        let sd = if var >= 0.0 { var.sqrt() } else { f64::NAN };
        let cv = if mean != 0.0 {
            (sd / mean) * 100.0
        } else {
            f64::NAN
        };
        (
            if var.is_finite() {
                round2(var)
            } else {
                Value::Null
            },
            if sd.is_finite() {
                round2(sd)
            } else {
                Value::Null
            },
            if cv.is_finite() {
                round2(cv)
            } else {
                Value::Null
            },
        )
    } else {
        (Value::Null, Value::Null, Value::Null)
    };

    // Round mean and sum_sq_diff
    obj.insert("mean".to_string(), round2(mean));
    obj.insert("sum_sq_diff".to_string(), round2(ssd));
    obj.insert("variance".to_string(), variance);
    obj.insert("stddev".to_string(), stddev);
    obj.insert(
        "coefficient_of_variation_pct".to_string(),
        cv_pct,
    );

    Value::Object(obj)
}

// ── Internal-state finalfunc: converts StatsState → finalized JsonB ──

#[pg_extern(immutable)]
pub unsafe fn jsonb_stats_final_internal(internal: Internal) -> JsonB {
    let state_ptr: *mut StatsState = match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<StatsState>(),
        None => return JsonB(json!({"type": "stats_agg"})),
    };

    // Take ownership so the state is properly freed when this scope ends
    let state = unsafe { Box::from_raw(state_ptr) };

    let mut result = Map::new();
    result.insert("type".to_string(), json!("stats_agg"));

    for (key, entry) in &state.entries {
        let val = match entry {
            AggEntry::IntAgg {
                count,
                sum,
                min,
                max,
                mean,
                sum_sq_diff,
            } => finalize_int_entry(*count, *sum, *min, *max, *mean, *sum_sq_diff),
            AggEntry::StrAgg { counts } => {
                let mut m = Map::new();
                m.insert("type".to_string(), json!("str_agg"));
                let mut c = Map::new();
                for (k, v) in counts {
                    c.insert(k.clone(), Value::Number(Number::from(*v)));
                }
                m.insert("counts".to_string(), Value::Object(c));
                Value::Object(m)
            }
            AggEntry::BoolAgg { counts } => {
                let mut m = Map::new();
                m.insert("type".to_string(), json!("bool_agg"));
                let mut c = Map::new();
                for (k, v) in counts {
                    c.insert(k.clone(), Value::Number(Number::from(*v)));
                }
                m.insert("counts".to_string(), Value::Object(c));
                Value::Object(m)
            }
            AggEntry::ArrAgg { count, counts } => {
                let mut m = Map::new();
                m.insert("type".to_string(), json!("arr_agg"));
                m.insert("count".to_string(), Value::Number(Number::from(*count)));
                let mut c = Map::new();
                for (k, v) in counts {
                    c.insert(k.clone(), Value::Number(Number::from(*v)));
                }
                m.insert("counts".to_string(), Value::Object(c));
                Value::Object(m)
            }
        };
        result.insert(key.clone(), val);
    }

    JsonB(Value::Object(result))
}

fn finalize_int_entry(count: i64, sum: f64, min: f64, max: f64, mean: f64, ssd: f64) -> Value {
    let mut obj = Map::new();
    obj.insert("type".to_string(), json!("int_agg"));
    obj.insert("count".to_string(), Value::Number(Number::from(count)));
    obj.insert("sum".to_string(), num_value(sum));
    obj.insert("min".to_string(), num_value(min));
    obj.insert("max".to_string(), num_value(max));
    obj.insert("mean".to_string(), round2(mean));
    obj.insert("sum_sq_diff".to_string(), round2(ssd));

    if count > 1 {
        let var = ssd / (count as f64 - 1.0);
        let sd = if var >= 0.0 { var.sqrt() } else { f64::NAN };
        let cv = if mean != 0.0 {
            (sd / mean) * 100.0
        } else {
            f64::NAN
        };

        obj.insert(
            "variance".to_string(),
            if var.is_finite() { round2(var) } else { Value::Null },
        );
        obj.insert(
            "stddev".to_string(),
            if sd.is_finite() { round2(sd) } else { Value::Null },
        );
        obj.insert(
            "coefficient_of_variation_pct".to_string(),
            if cv.is_finite() { round2(cv) } else { Value::Null },
        );
    } else {
        obj.insert("variance".to_string(), Value::Null);
        obj.insert("stddev".to_string(), Value::Null);
        obj.insert("coefficient_of_variation_pct".to_string(), Value::Null);
    }

    Value::Object(obj)
}
