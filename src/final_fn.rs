use pgrx::prelude::*;
use pgrx::{Internal, JsonB};
use serde_json::{json, Map, Number, Value};

use crate::helpers::*;
use crate::state::{AggEntry, NumFields, StatsState};

/// Compute derived statistics (variance, stddev, cv_pct) for numeric agg summaries,
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
            Value::Object(obj)
                if matches!(
                    get_type(&obj),
                    "int_agg" | "float_agg" | "dec2_agg" | "nat_agg"
                ) =>
            {
                finalize_num_agg(obj)
            }
            other => other,
        };

        result.insert(key, finalized);
    }

    JsonB(Value::Object(result))
}

/// Add derived stats to a numeric agg summary and round numeric fields.
/// Preserves the original type tag.
fn finalize_num_agg(mut obj: Map<String, Value>) -> Value {
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

#[pg_extern(immutable, parallel_safe)]
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
            AggEntry::IntAgg(f)
            | AggEntry::FloatAgg(f)
            | AggEntry::Dec2Agg(f)
            | AggEntry::NatAgg(f) => finalize_num_entry(entry.type_tag(), f),
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
            AggEntry::DateAgg {
                counts,
                min_date,
                max_date,
            } => {
                let mut m = Map::new();
                m.insert("type".to_string(), json!("date_agg"));
                let mut c = Map::new();
                for (k, v) in counts {
                    c.insert(k.clone(), Value::Number(Number::from(*v)));
                }
                m.insert("counts".to_string(), Value::Object(c));
                if let Some(min) = min_date {
                    m.insert("min".to_string(), json!(min));
                }
                if let Some(max) = max_date {
                    m.insert("max".to_string(), json!(max));
                }
                Value::Object(m)
            }
        };
        result.insert(key.clone(), val);
    }

    JsonB(Value::Object(result))
}

fn finalize_num_entry(type_tag: &str, f: &NumFields) -> Value {
    let mut obj = Map::new();
    obj.insert("type".to_string(), json!(type_tag));
    obj.insert("count".to_string(), Value::Number(Number::from(f.count)));
    obj.insert("sum".to_string(), num_value(f.sum));
    obj.insert("min".to_string(), num_value(f.min));
    obj.insert("max".to_string(), num_value(f.max));
    obj.insert("mean".to_string(), round2(f.mean));
    obj.insert("sum_sq_diff".to_string(), round2(f.sum_sq_diff));

    if f.count > 1 {
        let var = f.sum_sq_diff / (f.count as f64 - 1.0);
        let sd = if var >= 0.0 { var.sqrt() } else { f64::NAN };
        let cv = if f.mean != 0.0 {
            (sd / f.mean) * 100.0
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
