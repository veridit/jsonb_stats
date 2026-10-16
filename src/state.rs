use std::collections::HashMap;
use std::str::FromStr;

use num_bigint::BigInt;
use num_traits::Signed;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::exact::{ratio_round2, ratio_sqrt_round2, Decimal};

/// The four numeric stat kinds. They share one exact state and one set of
/// formulas; they differ only in how input values are validated and scaled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NumKind {
    Int,
    Nat,
    Dec2,
    Float,
}

impl NumKind {
    pub fn from_stat_type(t: &str) -> Option<Self> {
        match t {
            "int" => Some(NumKind::Int),
            "nat" => Some(NumKind::Nat),
            "dec2" => Some(NumKind::Dec2),
            "float" => Some(NumKind::Float),
            _ => None,
        }
    }

    pub fn from_agg_type(t: &str) -> Option<Self> {
        Self::from_stat_type(t.strip_suffix("_agg")?)
    }

    pub fn stat_type(self) -> &'static str {
        match self {
            NumKind::Int => "int",
            NumKind::Nat => "nat",
            NumKind::Dec2 => "dec2",
            NumKind::Float => "float",
        }
    }

    pub fn agg_type(self) -> &'static str {
        match self {
            NumKind::Int => "int_agg",
            NumKind::Nat => "nat_agg",
            NumKind::Dec2 => "dec2_agg",
            NumKind::Float => "float_agg",
        }
    }

    /// Validate a stat's "value" and bring it to the kind's canonical scale:
    /// int/nat integral at scale 0, dec2 at scale 2, float as given.
    pub fn parse_value(self, stat: &Map<String, Value>) -> Decimal {
        let text = match stat.get("value") {
            Some(Value::Number(n)) => n.as_str(),
            _ => pgrx::error!(
                "jsonb_stats: stat of type '{}' has missing or non-numeric 'value'",
                self.stat_type()
            ),
        };
        let v = Decimal::from_str(text).unwrap_or_else(|e| pgrx::error!("jsonb_stats: {}", e));
        match self {
            NumKind::Int | NumKind::Nat => {
                if !v.is_integral() {
                    pgrx::error!(
                        "jsonb_stats: {} value must be an integer, got {}",
                        self.stat_type(),
                        text
                    );
                }
                if self == NumKind::Nat && v.is_negative() {
                    pgrx::error!("jsonb_stats: nat value must be >= 0, got {}", text);
                }
                v.round_to(0)
            }
            NumKind::Dec2 => v.with_scale(2).unwrap_or_else(|| {
                pgrx::error!(
                    "jsonb_stats: dec2 value must have at most 2 decimal places, got {}",
                    text
                )
            }),
            NumKind::Float => v,
        }
    }
}

/// Exact state of a numeric aggregate. Combining two states only adds, so the
/// result is independent of order and grouping.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NumFields {
    pub count: i64,
    pub sum: Decimal,
    pub sum_sq: Decimal,
    pub min: Decimal,
    pub max: Decimal,
}

impl NumFields {
    pub fn init(v: Decimal) -> Self {
        NumFields {
            count: 1,
            sum_sq: v.square(),
            sum: v.clone(),
            min: v.clone(),
            max: v,
        }
    }

    pub fn update(&mut self, v: Decimal) {
        self.merge(NumFields::init(v));
    }

    /// The one combine operation, used by accumulate, merge and parallel combine.
    pub fn merge(&mut self, other: NumFields) {
        self.count = self
            .count
            .checked_add(other.count)
            .unwrap_or_else(|| pgrx::error!("jsonb_stats: count overflow"));
        self.sum = self.sum.add(&other.sum);
        self.sum_sq = self.sum_sq.add(&other.sum_sq);
        self.min = std::mem::replace(&mut self.min, Decimal::zero()).pick(other.min, true);
        self.max = std::mem::replace(&mut self.max, Decimal::zero()).pick(other.max, false);
    }

    /// Parse the exact state of a numeric *_agg JSON summary. Derived fields
    /// (mean, variance, ...) are ignored; they are always recomputed.
    pub fn from_summary(kind: NumKind, key: &str, obj: &Map<String, Value>) -> Self {
        let agg = kind.agg_type();
        if !obj.contains_key("sum_sq") {
            pgrx::error!(
                "jsonb_stats: {} for key '{}' lacks 'sum_sq' (created by jsonb_stats < 3.0). Recompute it from the source stats.",
                agg, key
            );
        }
        let count = match obj.get("count") {
            Some(Value::Number(n)) => Decimal::from_str(n.as_str())
                .ok()
                .and_then(|c| c.to_i64())
                .filter(|c| *c >= 1),
            _ => None,
        }
        .unwrap_or_else(|| {
            pgrx::error!(
                "jsonb_stats: {} for key '{}' has invalid 'count' (must be an integer >= 1)",
                agg,
                key
            )
        });
        let field = |name: &str| -> Decimal {
            match obj.get(name) {
                Some(Value::Number(n)) => Decimal::from_str(n.as_str())
                    .unwrap_or_else(|e| pgrx::error!("jsonb_stats: {}", e)),
                _ => pgrx::error!(
                    "jsonb_stats: {} for key '{}' has missing or non-numeric '{}'",
                    agg,
                    key,
                    name
                ),
            }
        };
        let f = NumFields {
            count,
            sum: field("sum"),
            sum_sq: field("sum_sq"),
            min: field("min"),
            max: field("max"),
        };
        if f.n_big().is_negative() {
            pgrx::error!(
                "jsonb_stats: {} for key '{}' is inconsistent (count * sum_sq < sum^2)",
                agg,
                key
            );
        }
        f
    }

    /// Exact integers for the derived formulas: returns (N, S1, s) where the
    /// sum is S1 / 10^s and N = count * sum_sq - sum^2 is scaled by 10^(2s).
    fn integers(&self) -> (BigInt, BigInt, u32) {
        let s = self.sum.scale().max(self.sum_sq.scale().div_ceil(2));
        let s1 = self.sum.mantissa_at(s);
        let s2 = self.sum_sq.mantissa_at(2 * s);
        let n = BigInt::from(self.count) * s2 - &s1 * &s1;
        (n, s1, s)
    }

    fn n_big(&self) -> BigInt {
        self.integers().0
    }

    /// The exact running state as JSON (what jsonb_stats_accum returns).
    pub fn state_json(&self, kind: NumKind) -> Value {
        let mut m = Map::new();
        m.insert("type".into(), json!(kind.agg_type()));
        m.insert("count".into(), json!(self.count));
        m.insert("sum".into(), self.sum.to_json());
        m.insert("sum_sq".into(), self.sum_sq.to_json());
        m.insert("min".into(), self.min.to_json());
        m.insert("max".into(), self.max.to_json());
        Value::Object(m)
    }

    /// Full summary: exact state plus derived fields, each rounded once to
    /// 2 decimals, half away from zero. The single summary builder for every
    /// path that returns a stats_agg.
    pub fn summary_json(&self, kind: NumKind) -> Value {
        let mut v = self.state_json(kind);
        let m = v.as_object_mut().expect("state_json returns an object");

        let (n_big, s1, s) = self.integers();
        let count = BigInt::from(self.count);
        let ten_s = num_traits::pow(BigInt::from(10u8), s as usize);
        let ten_2s = &ten_s * &ten_s;

        // mean = s1 / (count * 10^s); sum_sq_diff = N / (count * 10^2s)
        m.insert(
            "mean".into(),
            ratio_round2(&s1, &(&count * &ten_s)).to_json(),
        );
        m.insert(
            "sum_sq_diff".into(),
            ratio_round2(&n_big, &(&count * &ten_2s)).to_json(),
        );

        if self.count > 1 {
            let var_den = &count * (&count - 1u8) * &ten_2s;
            m.insert("variance".into(), ratio_round2(&n_big, &var_den).to_json());
            m.insert(
                "stddev".into(),
                ratio_sqrt_round2(&n_big, &var_den).to_json(),
            );
            m.insert(
                "coefficient_of_variation_pct".into(),
                if s1 == BigInt::from(0u8) {
                    Value::Null
                } else {
                    // cv^2 = 10000 * N * count / ((count - 1) * s1^2)   (scales cancel)
                    let mag = ratio_sqrt_round2(
                        &(&n_big * &count * 10000u32),
                        &((&count - 1u8) * &s1 * &s1),
                    );
                    if s1.is_negative() { mag.negated() } else { mag }.to_json()
                },
            );
        } else {
            m.insert("variance".into(), Value::Null);
            m.insert("stddev".into(), Value::Null);
            m.insert("coefficient_of_variation_pct".into(), Value::Null);
        }
        v
    }
}

/// Native Rust state for the jsonb_stats_agg aggregate.
/// By keeping this as a Rust struct (via pgrx Internal), we avoid
/// serde_json serialization/deserialization on every sfunc call.
#[derive(Default, Serialize, Deserialize)]
pub struct StatsState {
    pub entries: HashMap<String, AggEntry>,
}

impl StatsState {
    /// Fold one `stats` object (key -> stat) into the state.
    pub fn accumulate(&mut self, stats: Map<String, Value>) {
        for (key, stat) in stats {
            if key == "type" {
                continue;
            }
            let Value::Object(stat) = stat else { continue };
            let Some(Value::String(stat_type)) = stat.get("type") else {
                continue;
            };
            let stat_type = stat_type.clone();
            match self.entries.get_mut(&key) {
                Some(entry) => entry.update(&key, &stat_type, &stat),
                None => {
                    let entry = AggEntry::init(&stat_type, &stat);
                    self.entries.insert(key, entry);
                }
            }
        }
    }

    /// Fold one `stats_agg` object (key -> *_agg summary) into the state.
    /// Non-object summaries are skipped.
    pub fn merge_summaries(&mut self, agg: Map<String, Value>) {
        for (key, summary) in agg {
            if key == "type" {
                continue;
            }
            let Value::Object(obj) = summary else {
                continue;
            };
            let incoming = AggEntry::from_summary(&key, &obj);
            self.merge_entry(key, incoming);
        }
    }

    pub fn merge_entry(&mut self, key: String, incoming: AggEntry) {
        match self.entries.get_mut(&key) {
            Some(existing) => existing.merge(&key, incoming),
            None => {
                self.entries.insert(key, incoming);
            }
        }
    }

    /// Finalized `stats_agg` JSON.
    pub fn to_stats_agg(&self) -> Value {
        let mut result = Map::new();
        result.insert("type".into(), json!("stats_agg"));
        for (key, entry) in &self.entries {
            result.insert(key.clone(), entry.summary_json());
        }
        Value::Object(result)
    }
}

#[derive(Serialize, Deserialize)]
pub enum AggEntry {
    Num(NumKind, NumFields),
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

const UNKNOWN_STAT_TYPE: &str = "Expected: int, float, dec2, nat, str, bool, arr, date";
const UNKNOWN_AGG_TYPE: &str =
    "Expected: int_agg, float_agg, dec2_agg, nat_agg, str_agg, bool_agg, arr_agg, date_agg";

impl AggEntry {
    pub fn type_tag(&self) -> &'static str {
        match self {
            AggEntry::Num(kind, _) => kind.agg_type(),
            AggEntry::StrAgg { .. } => "str_agg",
            AggEntry::BoolAgg { .. } => "bool_agg",
            AggEntry::ArrAgg { .. } => "arr_agg",
            AggEntry::DateAgg { .. } => "date_agg",
        }
    }

    /// New entry from the first stat seen for a key.
    pub fn init(stat_type: &str, stat: &Map<String, Value>) -> AggEntry {
        if let Some(kind) = NumKind::from_stat_type(stat_type) {
            return AggEntry::Num(kind, NumFields::init(kind.parse_value(stat)));
        }
        match stat_type {
            "str" => AggEntry::StrAgg {
                counts: HashMap::from([(category_value(stat_type, stat), 1)]),
            },
            "bool" => AggEntry::BoolAgg {
                counts: HashMap::from([(category_value(stat_type, stat), 1)]),
            },
            "arr" => {
                let mut counts = HashMap::new();
                collect_arr_counts(stat, &mut counts);
                AggEntry::ArrAgg { count: 1, counts }
            }
            "date" => {
                let d = date_value(stat);
                AggEntry::DateAgg {
                    counts: HashMap::from([(d.clone(), 1)]),
                    min_date: Some(d.clone()),
                    max_date: Some(d),
                }
            }
            other => pgrx::error!(
                "jsonb_stats: unknown stat type '{}'. {}",
                other,
                UNKNOWN_STAT_TYPE
            ),
        }
    }

    /// Fold one more stat for the same key into this entry.
    pub fn update(&mut self, key: &str, stat_type: &str, stat: &Map<String, Value>) {
        let tag = self.type_tag();
        if tag.strip_suffix("_agg") != Some(stat_type) {
            pgrx::error!(
                "jsonb_stats: type mismatch for key '{}': existing {} vs incoming {}",
                key,
                tag,
                stat_type
            );
        }
        match self {
            AggEntry::Num(kind, f) => f.update(kind.parse_value(stat)),
            AggEntry::StrAgg { counts } | AggEntry::BoolAgg { counts } => {
                *counts.entry(category_value(stat_type, stat)).or_insert(0) += 1;
            }
            AggEntry::ArrAgg { count, counts } => {
                *count += 1;
                collect_arr_counts(stat, counts);
            }
            AggEntry::DateAgg {
                counts,
                min_date,
                max_date,
            } => {
                let d = date_value(stat);
                *counts.entry(d.clone()).or_insert(0) += 1;
                if min_date.as_ref().is_none_or(|cur| d < *cur) {
                    *min_date = Some(d.clone());
                }
                if max_date.as_ref().is_none_or(|cur| d > *cur) {
                    *max_date = Some(d);
                }
            }
        }
    }

    /// Combine another entry for the same key into this one.
    pub fn merge(&mut self, key: &str, incoming: AggEntry) {
        let (e_tag, i_tag) = (self.type_tag(), incoming.type_tag());
        if e_tag != i_tag {
            pgrx::error!(
                "jsonb_stats: type mismatch for key '{}': existing {} vs incoming {}",
                key,
                e_tag,
                i_tag
            );
        }
        match (self, incoming) {
            (AggEntry::Num(_, a), AggEntry::Num(_, b)) => a.merge(b),
            (AggEntry::StrAgg { counts: ca }, AggEntry::StrAgg { counts: cb })
            | (AggEntry::BoolAgg { counts: ca }, AggEntry::BoolAgg { counts: cb }) => {
                add_counts(ca, cb)
            }
            (
                AggEntry::ArrAgg {
                    count: na,
                    counts: ca,
                },
                AggEntry::ArrAgg {
                    count: nb,
                    counts: cb,
                },
            ) => {
                *na += nb;
                add_counts(ca, cb);
            }
            (
                AggEntry::DateAgg {
                    counts: ca,
                    min_date: min_a,
                    max_date: max_a,
                },
                AggEntry::DateAgg {
                    counts: cb,
                    min_date: min_b,
                    max_date: max_b,
                },
            ) => {
                add_counts(ca, cb);
                if let Some(b) = min_b {
                    if min_a.as_ref().is_none_or(|a| b < *a) {
                        *min_a = Some(b);
                    }
                }
                if let Some(b) = max_b {
                    if max_a.as_ref().is_none_or(|a| b > *a) {
                        *max_a = Some(b);
                    }
                }
            }
            _ => unreachable!("type_tag check guarantees matching variants"),
        }
    }

    /// Parse a JSON `*_agg` summary.
    pub fn from_summary(key: &str, obj: &Map<String, Value>) -> AggEntry {
        let agg_type = match obj.get("type") {
            Some(Value::String(s)) => s.as_str(),
            _ => "",
        };
        if let Some(kind) = NumKind::from_agg_type(agg_type) {
            return AggEntry::Num(kind, NumFields::from_summary(kind, key, obj));
        }
        match agg_type {
            "str_agg" => AggEntry::StrAgg {
                counts: parse_counts(obj),
            },
            "bool_agg" => AggEntry::BoolAgg {
                counts: parse_counts(obj),
            },
            "arr_agg" => AggEntry::ArrAgg {
                count: parse_count(obj),
                counts: parse_counts(obj),
            },
            "date_agg" => AggEntry::DateAgg {
                counts: parse_counts(obj),
                min_date: get_string(obj, "min"),
                max_date: get_string(obj, "max"),
            },
            other => pgrx::error!(
                "jsonb_stats: unknown aggregate type '{}'. {}",
                other,
                UNKNOWN_AGG_TYPE
            ),
        }
    }

    /// Running-state JSON (jsonb_stats_accum): numeric entries carry only
    /// their exact state; derived fields are added by the final step.
    pub fn state_json(&self) -> Value {
        match self {
            AggEntry::Num(kind, f) => f.state_json(*kind),
            other => other.summary_json(),
        }
    }

    /// Finalized JSON for a stats_agg.
    pub fn summary_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("type".into(), json!(self.type_tag()));
        match self {
            AggEntry::Num(kind, f) => return f.summary_json(*kind),
            AggEntry::StrAgg { counts } | AggEntry::BoolAgg { counts } => {
                m.insert("counts".into(), counts_json(counts));
            }
            AggEntry::ArrAgg { count, counts } => {
                m.insert("count".into(), json!(count));
                m.insert("counts".into(), counts_json(counts));
            }
            AggEntry::DateAgg {
                counts,
                min_date,
                max_date,
            } => {
                m.insert("counts".into(), counts_json(counts));
                if let Some(min) = min_date {
                    m.insert("min".into(), json!(min));
                }
                if let Some(max) = max_date {
                    m.insert("max".into(), json!(max));
                }
            }
        }
        Value::Object(m)
    }
}

/// Value of a str/bool stat as its count-map key.
fn category_value(stat_type: &str, stat: &Map<String, Value>) -> String {
    match stat.get("value") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => n.to_string(),
        _ => pgrx::error!(
            "jsonb_stats: stat of type '{}' has missing or invalid 'value'",
            stat_type
        ),
    }
}

fn date_value(stat: &Map<String, Value>) -> String {
    match stat.get("value") {
        Some(Value::String(s)) => s.clone(),
        _ => pgrx::error!("jsonb_stats: date stat requires a string 'value'"),
    }
}

/// Count array elements. The value is a JSON array or PostgreSQL array text `{a,b,c}`.
fn collect_arr_counts(stat: &Map<String, Value>, counts: &mut HashMap<String, i64>) {
    match stat.get("value") {
        Some(Value::Array(arr)) => {
            for elem in arr {
                let key = match elem {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => continue,
                };
                *counts.entry(key).or_insert(0) += 1;
            }
        }
        Some(Value::String(s)) => {
            let trimmed = s.trim_matches(|c| c == '{' || c == '}');
            if !trimmed.is_empty() {
                for elem in trimmed.split(',') {
                    *counts.entry(elem.trim().to_string()).or_insert(0) += 1;
                }
            }
        }
        _ => {}
    }
}

fn add_counts(into: &mut HashMap<String, i64>, from: HashMap<String, i64>) {
    for (k, v) in from {
        *into.entry(k).or_insert(0) += v;
    }
}

fn number_as_i64(v: &Value) -> i64 {
    match v {
        Value::Number(n) => Decimal::from_str(n.as_str())
            .ok()
            .and_then(|d| d.to_i64())
            .unwrap_or(0),
        _ => 0,
    }
}

fn parse_count(obj: &Map<String, Value>) -> i64 {
    obj.get("count").map(number_as_i64).unwrap_or(0)
}

fn parse_counts(obj: &Map<String, Value>) -> HashMap<String, i64> {
    match obj.get("counts") {
        Some(Value::Object(counts)) => counts
            .iter()
            .map(|(k, v)| (k.clone(), number_as_i64(v)))
            .collect(),
        _ => HashMap::new(),
    }
}

fn counts_json(counts: &HashMap<String, i64>) -> Value {
    Value::Object(counts.iter().map(|(k, v)| (k.clone(), json!(v))).collect())
}

fn get_string(obj: &Map<String, Value>, key: &str) -> Option<String> {
    match obj.get(key) {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}
