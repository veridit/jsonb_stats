use pgrx::prelude::*;
use pgrx::{Internal, JsonB};
use serde_json::{json, Map, Value};

use crate::state::{AggEntry, NumKind, StatsState};

/// Add derived statistics to a running state and mark it "type": "stats_agg".
///
/// Numeric summaries are rebuilt from their exact state (derived fields on
/// input are ignored and recomputed). Other summaries pass through unchanged.
///
/// Spec: dev/reference_plpgsql.sql jsonb_stats_final_plpgsql
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_final(state: JsonB) -> JsonB {
    let state_map = match state.0 {
        Value::Object(m) => m,
        other => return JsonB(other),
    };

    let mut result = Map::new();
    result.insert("type".to_string(), json!("stats_agg"));
    for (key, summary) in state_map {
        if key == "type" {
            continue;
        }
        let finalized = match &summary {
            Value::Object(obj)
                if obj
                    .get("type")
                    .and_then(Value::as_str)
                    .and_then(NumKind::from_agg_type)
                    .is_some() =>
            {
                AggEntry::from_summary(&key, obj).summary_json()
            }
            _ => summary,
        };
        result.insert(key, finalized);
    }
    JsonB(Value::Object(result))
}

/// Internal-state finalfunc: converts StatsState to a finalized stats_agg.
///
/// # Safety
/// `internal` must be NULL or a `StatsState` pointer created by this
/// extension; PostgreSQL guarantees this for the registered aggregates.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_final_internal(internal: Internal) -> JsonB {
    let state_ptr: *mut StatsState = match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<StatsState>(),
        None => return JsonB(json!({"type": "stats_agg"})),
    };

    // Borrow the state WITHOUT taking ownership. CTE inlining can cause the
    // planner to rescan the aggregate, calling the finalfunc multiple times on
    // the same Internal state. Box::from_raw would free the state on the first
    // call, making subsequent calls use-after-free → SIGSEGV.
    //
    // The state (Rust heap via Box::into_raw in sfunc) is freed by
    // jsonb_stats_combine's Box::from_raw on state2, or leaked until the
    // aggregate memory context is reset at end-of-query.
    let state = unsafe { &*state_ptr };
    JsonB(state.to_stats_agg())
}
