use pgrx::prelude::*;
use pgrx::{Internal, JsonB};
use serde_json::Value;

use crate::state::StatsState;

/// Merge two stats_agg objects into a finalized stats_agg.
///
/// Numeric summaries are combined from their exact state (count, sum, sum_sq,
/// min, max); derived fields are recomputed, so the result can be stored and
/// merged again without loss.
///
/// Spec: dev/reference_plpgsql.sql jsonb_stats_merge_plpgsql
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_merge(a: JsonB, b: JsonB) -> JsonB {
    let mut st = StatsState::default();
    if let Value::Object(m) = a.0 {
        st.merge_summaries(m);
    }
    if let Value::Object(m) = b.0 {
        st.merge_summaries(m);
    }
    JsonB(st.to_stats_agg())
}

/// Merge sfunc using pgrx Internal state. Each input stats_agg JSONB is
/// parsed once into native AggEntry types and merged into the HashMap state.
/// The growing state is never serialized back to JSONB until the finalfunc.
///
/// # Safety
/// `internal` must be NULL or a `StatsState` pointer created by this
/// extension; PostgreSQL guarantees this for the registered aggregate.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_merge_sfunc(internal: Internal, agg: Option<pgrx::JsonB>) -> Internal {
    let state_ptr: *mut StatsState = match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<StatsState>(),
        None => Box::into_raw(Box::new(StatsState::default())),
    };
    if let Some(JsonB(Value::Object(m))) = agg {
        let state = unsafe { &mut *state_ptr };
        state.merge_summaries(m);
    }
    Internal::from(Some(pgrx::pg_sys::Datum::from(state_ptr as usize)))
}
