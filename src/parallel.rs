use pgrx::prelude::*;
use pgrx::Internal;

use crate::merge::merge_agg_entries;
use crate::state::StatsState;

/// Combine two partial aggregate states (for parallel aggregation).
/// NOT STRICT: must handle NULL inputs from empty worker partitions.
///
/// Memory ownership:
///   state1: borrow (returned as the combined state)
///   state2: take ownership (freed after merging into state1)
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_combine(state1: Internal, state2: Internal) -> Internal {
    let ptr1: Option<*mut StatsState> = match state1.unwrap() {
        Some(datum) => Some(datum.cast_mut_ptr::<StatsState>()),
        None => None,
    };
    let ptr2: Option<*mut StatsState> = match state2.unwrap() {
        Some(datum) => Some(datum.cast_mut_ptr::<StatsState>()),
        None => None,
    };

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
                match s1.entries.get_mut(&key) {
                    Some(existing) => merge_agg_entries(existing, entry, &key),
                    None => {
                        s1.entries.insert(key, entry);
                    }
                }
            }
            Internal::from(Some(pgrx::pg_sys::Datum::from(p1 as usize)))
        }
    }
}

/// Serialize aggregate state to bytes for cross-worker IPC.
/// Borrows state (does NOT free) — PG may call this multiple times.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_serial(internal: Internal) -> Vec<u8> {
    let ptr: *mut StatsState = match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<StatsState>(),
        None => return serde_json::to_vec(&StatsState::default()).unwrap(),
    };
    let state = unsafe { &*ptr };
    serde_json::to_vec(state).unwrap_or_else(|e| {
        pgrx::error!("jsonb_stats: serialization failed: {}", e)
    })
}

/// Deserialize aggregate state from bytes received from a worker.
/// The second `Internal` argument is required by PG but unused.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_deserial(bytes: Vec<u8>, _internal: Internal) -> Internal {
    let state: StatsState = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        pgrx::error!("jsonb_stats: deserialization failed: {}", e)
    });
    let ptr = Box::into_raw(Box::new(state));
    Internal::from(Some(pgrx::pg_sys::Datum::from(ptr as usize)))
}
