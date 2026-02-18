use std::collections::HashMap;

/// Native Rust state for the jsonb_stats_agg aggregate.
/// By keeping this as a Rust struct (via pgrx Internal), we avoid
/// serde_json serialization/deserialization on every sfunc call.
#[derive(Default)]
pub struct StatsState {
    pub entries: HashMap<String, AggEntry>,
}

pub enum AggEntry {
    IntAgg {
        count: i64,
        sum: f64,
        min: f64,
        max: f64,
        mean: f64,
        sum_sq_diff: f64,
    },
    StrAgg {
        counts: HashMap<String, i64>,
    },
    BoolAgg {
        counts: HashMap<String, i64>,
    },
    ArrAgg {
        count: i64,
        counts: HashMap<String, i64>,
    },
}
