use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::{json, Map, Value};

use crate::helpers::*;

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
