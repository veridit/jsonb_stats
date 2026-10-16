use pgrx::prelude::*;
use pgrx::{Internal, JsonB};
use serde_json::{Map, Value};

use crate::state::StatsState;

/// Accumulate a single stats object into a running state (stats -> state).
///
/// The running state holds, per key, the exact aggregate state: numeric
/// entries carry `count, sum, sum_sq, min, max`; derived fields are added by
/// `jsonb_stats_final`.
///
/// Spec: dev/reference_plpgsql.sql jsonb_stats_accum_plpgsql
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_accum(state: JsonB, stats: JsonB) -> JsonB {
    let mut st = StatsState::default();
    if let Value::Object(m) = state.0 {
        st.merge_summaries(m);
    }
    if let Value::Object(m) = stats.0 {
        st.accumulate(m);
    }
    let out: Map<String, Value> = st
        .entries
        .iter()
        .map(|(k, e)| (k.clone(), e.state_json()))
        .collect();
    JsonB(Value::Object(out))
}

/// Aggregate sfunc using pgrx Internal state. The state is a native Rust
/// StatsState allocated on the Rust heap (Box), avoiding both JSONB
/// serialization per row and PostgreSQL memory context lifetime issues.
///
/// # Safety
/// `internal` must be NULL or a `StatsState` pointer created by this
/// extension; PostgreSQL guarantees this for the registered aggregate.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_accum_sfunc(internal: Internal, stats: Option<pgrx::JsonB>) -> Internal {
    // Box::into_raw ensures the allocation survives PG memory context resets.
    let state_ptr: *mut StatsState = match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<StatsState>(),
        None => Box::into_raw(Box::new(StatsState::default())),
    };
    if let Some(JsonB(Value::Object(m))) = stats {
        let state = unsafe { &mut *state_ptr };
        state.accumulate(m);
    }
    Internal::from(Some(pgrx::pg_sys::Datum::from(state_ptr as usize)))
}
