use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Common fields for all numeric aggregates (int, float, dec2, nat).
/// Welford online algorithm methods live here — written once, used by all.
#[derive(Serialize, Deserialize)]
pub struct NumFields {
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub sum_sq_diff: f64,
}

impl NumFields {
    /// Initialize from a single value.
    pub fn init(val: f64) -> Self {
        NumFields {
            count: 1,
            sum: val,
            min: val,
            max: val,
            mean: val,
            sum_sq_diff: 0.0,
        }
    }

    /// Welford single-value update.
    pub fn update(&mut self, val: f64) {
        self.count += 1;
        let delta = val - self.mean;
        self.mean += delta / (self.count as f64);
        self.sum_sq_diff += delta * (val - self.mean);
        self.sum += val;
        if val < self.min {
            self.min = val;
        }
        if val > self.max {
            self.max = val;
        }
    }

    /// Welford parallel merge.
    pub fn merge(&mut self, other: &NumFields) {
        let ca = self.count as f64;
        let cb = other.count as f64;
        let total = ca + cb;
        let delta = other.mean - self.mean;
        self.mean += delta * cb / total;
        self.sum_sq_diff += other.sum_sq_diff + (delta * delta * ca * cb) / total;
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }
}

/// Native Rust state for the jsonb_stats_agg aggregate.
/// By keeping this as a Rust struct (via pgrx Internal), we avoid
/// serde_json serialization/deserialization on every sfunc call.
#[derive(Default, Serialize, Deserialize)]
pub struct StatsState {
    pub entries: HashMap<String, AggEntry>,
}

#[derive(Serialize, Deserialize)]
pub enum AggEntry {
    IntAgg(NumFields),
    FloatAgg(NumFields),
    Dec2Agg(NumFields),
    NatAgg(NumFields),
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
    DateAgg {
        counts: HashMap<String, i64>,
        min_date: Option<String>,
        max_date: Option<String>,
    },
}

impl AggEntry {
    pub fn type_tag(&self) -> &'static str {
        match self {
            AggEntry::IntAgg(_) => "int_agg",
            AggEntry::FloatAgg(_) => "float_agg",
            AggEntry::Dec2Agg(_) => "dec2_agg",
            AggEntry::NatAgg(_) => "nat_agg",
            AggEntry::StrAgg { .. } => "str_agg",
            AggEntry::BoolAgg { .. } => "bool_agg",
            AggEntry::ArrAgg { .. } => "arr_agg",
            AggEntry::DateAgg { .. } => "date_agg",
        }
    }
}
