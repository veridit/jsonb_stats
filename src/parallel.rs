use pgrx::prelude::*;
use pgrx::Internal;

use crate::state::StatsState;

/// Combine two partial aggregate states (for parallel aggregation).
/// NOT STRICT: must handle NULL inputs from empty worker partitions.
///
/// Memory ownership:
///   state1: borrow (returned as the combined state)
///   state2: take ownership (freed after merging into state1)
///
/// # Safety
/// Both arguments must be NULL or `StatsState` pointers created by this
/// extension's sfuncs/deserialfunc; PostgreSQL guarantees this for the
/// registered aggregates.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_combine(state1: Internal, state2: Internal) -> Internal {
    let ptr1 = state1.unwrap().map(|d| d.cast_mut_ptr::<StatsState>());
    let ptr2 = state2.unwrap().map(|d| d.cast_mut_ptr::<StatsState>());

    match (ptr1, ptr2) {
        (None, None) => {
            // Both empty — return a fresh default state
            let ptr = Box::into_raw(Box::new(StatsState::default()));
            Internal::from(Some(pgrx::pg_sys::Datum::from(ptr as usize)))
        }
        (Some(p), None) => Internal::from(Some(pgrx::pg_sys::Datum::from(p as usize))),
        (None, Some(p)) => Internal::from(Some(pgrx::pg_sys::Datum::from(p as usize))),
        (Some(p1), Some(p2)) => {
            let s1 = unsafe { &mut *p1 };
            // Take ownership of state2 so it's freed when dropped
            let s2 = unsafe { Box::from_raw(p2) };
            for (key, entry) in s2.entries {
                s1.merge_entry(key, entry);
            }
            Internal::from(Some(pgrx::pg_sys::Datum::from(p1 as usize)))
        }
    }
}

/// Serialize aggregate state to bytes for cross-worker IPC.
/// Borrows state (does NOT free) — PG may call this multiple times.
///
/// # Safety
/// `internal` must be NULL or a `StatsState` pointer created by this extension.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_serial(internal: Internal) -> Vec<u8> {
    let ptr: *mut StatsState = match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<StatsState>(),
        None => {
            return serde_json::to_vec(&StatsState::default()).unwrap_or_else(|e| {
                pgrx::error!("jsonb_stats: serialization of empty state failed: {}", e)
            })
        }
    };
    let state = unsafe { &*ptr };
    serde_json::to_vec(state)
        .unwrap_or_else(|e| pgrx::error!("jsonb_stats: serialization failed: {}", e))
}

/// Deserialize aggregate state from bytes received from a worker.
/// The second `Internal` argument is required by PG but unused.
///
/// # Safety
/// Called by PostgreSQL with bytes produced by `jsonb_stats_serial`.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_deserial(bytes: Vec<u8>, _internal: Internal) -> Internal {
    let state: StatsState = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| pgrx::error!("jsonb_stats: deserialization failed: {}", e));
    let ptr = Box::into_raw(Box::new(state));
    Internal::from(Some(pgrx::pg_sys::Datum::from(ptr as usize)))
}
