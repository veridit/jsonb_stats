//! Difference summaries (`stats_delta`): changes to a stock.
//!
//! A unit's summary is added when it enters a stock (`jsonb_stats_to_delta`)
//! and subtracted when it leaves (`jsonb_stats_negate`). Every entry keeps the
//! two flows apart:
//!
//! ```text
//! {"type": "int_delta", "in": {"count", "sum", "sum_sq"}, "out": {...}}
//! {"type": "str_delta", "in": {"counts": {...}}, "out": {...}}   (arr: + "count")
//! ```
//!
//! Each flow is a sum of real summaries, so it is a valid nonnegative state;
//! the stock is `in - out`. Adding deltas only adds flows (exact decimals and
//! integers), so the result does not depend on order, grouping or parallel
//! plan, and the moving-aggregate inverse (`minvfunc`) undoes an addition
//! exactly. min/max are not kept: they cannot be subtracted.
//!
//! Spec: dev/reference_plpgsql.sql, section "Difference summaries".

use std::collections::HashMap;
use std::str::FromStr;

use num_bigint::BigInt;
use num_traits::Signed;
use pgrx::prelude::*;
use pgrx::{Internal, JsonB};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::exact::{ratio_round2, Decimal};
use crate::state::{AggEntry, NumFields, NumKind, StatsState};

const UNKNOWN_SUMMARY_TYPE: &str =
    "Expected a *_agg or *_delta of: int, float, dec2, nat, str, bool, arr, date";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DKind {
    Num(NumKind),
    Str,
    Bool,
    Arr,
    Date,
}

impl DKind {
    fn from_base(base: &str) -> Option<DKind> {
        if let Some(k) = NumKind::from_stat_type(base) {
            return Some(DKind::Num(k));
        }
        match base {
            "str" => Some(DKind::Str),
            "bool" => Some(DKind::Bool),
            "arr" => Some(DKind::Arr),
            "date" => Some(DKind::Date),
            _ => None,
        }
    }

    fn base(self) -> &'static str {
        match self {
            DKind::Num(k) => k.stat_type(),
            DKind::Str => "str",
            DKind::Bool => "bool",
            DKind::Arr => "arr",
            DKind::Date => "date",
        }
    }

    fn delta_type(self) -> String {
        format!("{}_delta", self.base())
    }

    fn agg_type(self) -> String {
        format!("{}_agg", self.base())
    }

    fn zero_flow(self) -> Flow {
        match self {
            DKind::Num(_) => Flow::Num {
                count: 0,
                sum: Decimal::zero(),
                sum_sq: Decimal::zero(),
            },
            _ => Flow::Cat {
                count: 0,
                counts: HashMap::new(),
            },
        }
    }
}

/// One flow (or a stock, which is in - out). Unvalidated while accumulating:
/// the moving-aggregate inverse may pass through partial states.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Flow {
    Num {
        count: i64,
        sum: Decimal,
        sum_sq: Decimal,
    },
    /// `count` is used by arr only.
    Cat {
        count: i64,
        counts: HashMap<String, i64>,
    },
}

fn checked(a: i64, b: i64, negative: bool) -> i64 {
    let r = if negative {
        a.checked_sub(b)
    } else {
        a.checked_add(b)
    };
    r.unwrap_or_else(|| pgrx::error!("jsonb_stats: count overflow"))
}

impl Flow {
    /// `self += sign * other`.
    fn add(&mut self, other: &Flow, negative: bool) {
        match (self, other) {
            (
                Flow::Num { count, sum, sum_sq },
                Flow::Num {
                    count: c,
                    sum: s,
                    sum_sq: q,
                },
            ) => {
                *count = checked(*count, *c, negative);
                sum.add_signed(s, negative);
                sum_sq.add_signed(q, negative);
            }
            (
                Flow::Cat { count, counts },
                Flow::Cat {
                    count: c,
                    counts: cs,
                },
            ) => {
                *count = checked(*count, *c, negative);
                for (k, v) in cs {
                    match counts.get_mut(k) {
                        Some(cur) => {
                            *cur = checked(*cur, *v, negative);
                            if *cur == 0 {
                                counts.remove(k);
                            }
                        }
                        None => {
                            let n = if negative { -*v } else { *v };
                            if n != 0 {
                                counts.insert(k.clone(), n);
                            }
                        }
                    }
                }
            }
            _ => unreachable!("entry type check guarantees matching flows"),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Flow::Num { count, .. } => *count == 0,
            Flow::Cat { count, counts } => *count == 0 && counts.is_empty(),
        }
    }

    /// Validate and bring to canonical form (see the spec), or fail fast.
    /// Spec: jsonb_stats_delta_num_flow_plpgsql / jsonb_stats_delta_cat_flow_plpgsql
    fn canonical(&self, kind: DKind, key: &str, side: &str) -> Flow {
        let fail = |reason: &str| -> ! {
            pgrx::error!(
                "jsonb_stats: {} for key '{}' has an invalid '{}' flow: {}",
                kind.delta_type(),
                key,
                side,
                reason
            )
        };
        match self {
            Flow::Num { count, sum, sum_sq } => {
                if *count < 0 {
                    fail("count must be an integer >= 0");
                }
                let (sum, sum_sq) = match kind {
                    DKind::Num(NumKind::Int | NumKind::Nat) => {
                        if !sum.is_integral() || !sum_sq.is_integral() {
                            fail("sum and sum_sq must be integers");
                        }
                        (sum.round_to(0), sum_sq.round_to(0))
                    }
                    DKind::Num(NumKind::Dec2) => match (sum.with_scale(2), sum_sq.with_scale(4)) {
                        (Some(s), Some(q)) => (s, q),
                        _ => fail("sum must have at most 2 and sum_sq at most 4 decimals"),
                    },
                    _ => (sum.clone().trimmed(), sum_sq.clone().trimmed()),
                };
                if kind == DKind::Num(NumKind::Nat) && sum.is_negative() {
                    fail("sum must be >= 0");
                }
                if !consistent(*count, &sum, &sum_sq) {
                    fail("inconsistent (count * sum_sq < sum^2)");
                }
                Flow::Num {
                    count: *count,
                    sum,
                    sum_sq,
                }
            }
            Flow::Cat { count, counts } => {
                if *count < 0 {
                    fail("count must be an integer >= 0");
                }
                if counts.values().any(|v| *v < 0) {
                    fail("counts must be integers >= 0");
                }
                Flow::Cat {
                    count: *count,
                    counts: counts
                        .iter()
                        .filter(|(_, v)| **v != 0)
                        .map(|(k, v)| (k.clone(), *v))
                        .collect(),
                }
            }
        }
    }

    fn to_json(&self, kind: DKind) -> Value {
        let mut m = Map::new();
        match self {
            Flow::Num { count, sum, sum_sq } => {
                m.insert("count".into(), json!(count));
                m.insert("sum".into(), sum.to_json());
                m.insert("sum_sq".into(), sum_sq.to_json());
            }
            Flow::Cat { count, counts } => {
                if kind == DKind::Arr {
                    m.insert("count".into(), json!(count));
                }
                m.insert(
                    "counts".into(),
                    Value::Object(counts.iter().map(|(k, v)| (k.clone(), json!(v))).collect()),
                );
            }
        }
        Value::Object(m)
    }
}

/// count * sum_sq >= sum^2, and sum_sq = 0 when count = 0.
fn consistent(count: i64, sum: &Decimal, sum_sq: &Decimal) -> bool {
    if count == 0 {
        return sum_sq.is_zero() && sum.is_zero();
    }
    let s = sum.scale().max(sum_sq.scale().div_ceil(2));
    let s1 = sum.mantissa_at(s);
    let s2 = sum_sq.mantissa_at(2 * s);
    !(BigInt::from(count) * s2 - &s1 * &s1).is_negative()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaEntry {
    pub kind: DKind,
    pub inflow: Flow,
    pub outflow: Flow,
}

impl DeltaEntry {
    fn add(&mut self, key: &str, other: &DeltaEntry, negative: bool) {
        if self.kind != other.kind {
            pgrx::error!(
                "jsonb_stats: type mismatch for key '{}': existing {} vs incoming {}",
                key,
                self.kind.delta_type(),
                other.kind.delta_type()
            );
        }
        self.inflow.add(&other.inflow, negative);
        self.outflow.add(&other.outflow, negative);
    }

    fn from_agg_entry(e: AggEntry) -> DeltaEntry {
        let (kind, inflow) = match e {
            AggEntry::Num(k, f) => (
                DKind::Num(k),
                Flow::Num {
                    count: f.count,
                    sum: f.sum,
                    sum_sq: f.sum_sq,
                },
            ),
            AggEntry::StrAgg { counts } => (DKind::Str, Flow::Cat { count: 0, counts }),
            AggEntry::BoolAgg { counts } => (DKind::Bool, Flow::Cat { count: 0, counts }),
            AggEntry::ArrAgg { count, counts } => (DKind::Arr, Flow::Cat { count, counts }),
            AggEntry::DateAgg { counts, .. } => (DKind::Date, Flow::Cat { count: 0, counts }),
        };
        DeltaEntry {
            kind,
            outflow: kind.zero_flow(),
            inflow,
        }
    }

    /// Parse one summary (`*_agg` or `*_delta`), validating every flow.
    /// Spec: jsonb_stats_delta_entry_plpgsql
    fn from_summary(key: &str, summary: &Value) -> DeltaEntry {
        if key == "min_max" {
            pgrx::error!("jsonb_stats: key 'min_max' is reserved in stats_delta");
        }
        let Value::Object(obj) = summary else {
            pgrx::error!("jsonb_stats: summary for key '{}' must be an object", key)
        };
        let t = match obj.get("type") {
            Some(Value::String(s)) => s.as_str(),
            _ => "",
        };
        let (base, is_delta) = if let Some(b) = t.strip_suffix("_delta") {
            (b, true)
        } else if let Some(b) = t.strip_suffix("_agg") {
            (b, false)
        } else {
            ("", false)
        };
        let Some(kind) = DKind::from_base(base) else {
            let shown = if t.is_empty() {
                "<NULL>".to_string()
            } else {
                t.to_string()
            };
            pgrx::error!(
                "jsonb_stats: unknown summary type '{}' for key '{}'. {}",
                shown,
                key,
                UNKNOWN_SUMMARY_TYPE
            )
        };
        if is_delta {
            let flow = |side: &str| -> Flow {
                match obj.get(side) {
                    None => kind.zero_flow(),
                    Some(v) => parse_flow(kind, key, side, v).canonical(kind, key, side),
                }
            };
            return DeltaEntry {
                kind,
                inflow: flow("in"),
                outflow: flow("out"),
            };
        }
        let inflow = match kind {
            DKind::Num(k) => {
                let f = NumFields::from_summary(k, key, obj);
                Flow::Num {
                    count: f.count,
                    sum: f.sum,
                    sum_sq: f.sum_sq,
                }
            }
            _ => {
                let counts = obj.get("counts").cloned().unwrap_or_else(|| json!({}));
                let mut m = Map::new();
                m.insert("counts".into(), counts);
                if kind == DKind::Arr {
                    m.insert(
                        "count".into(),
                        obj.get("count").cloned().unwrap_or_else(|| json!(0)),
                    );
                }
                parse_flow(kind, key, "in", &Value::Object(m))
            }
        }
        .canonical(kind, key, "in");
        DeltaEntry {
            kind,
            outflow: kind.zero_flow(),
            inflow,
        }
    }

    /// Canonical delta JSON, or None when both flows are empty.
    fn to_json(&self, key: &str) -> Option<Value> {
        let i = self.inflow.canonical(self.kind, key, "in");
        let o = self.outflow.canonical(self.kind, key, "out");
        if i.is_empty() && o.is_empty() {
            return None;
        }
        let mut m = Map::new();
        m.insert("type".into(), json!(self.kind.delta_type()));
        if !i.is_empty() {
            m.insert("in".into(), i.to_json(self.kind));
        }
        if !o.is_empty() {
            m.insert("out".into(), o.to_json(self.kind));
        }
        Some(Value::Object(m))
    }
}

/// Parse a flow object strictly (validation of values happens in `canonical`).
fn parse_flow(kind: DKind, key: &str, side: &str, v: &Value) -> Flow {
    let fail = |reason: &str| -> ! {
        pgrx::error!(
            "jsonb_stats: {} for key '{}' has an invalid '{}' flow: {}",
            kind.delta_type(),
            key,
            side,
            reason
        )
    };
    let Value::Object(obj) = v else {
        fail("must be an object")
    };
    let int = |v: Option<&Value>, reason: &str| -> i64 {
        match v {
            Some(Value::Number(n)) => Decimal::from_str(n.as_str())
                .ok()
                .and_then(|d| d.to_i64())
                .filter(|c| *c >= 0)
                .unwrap_or_else(|| fail(reason)),
            _ => fail(reason),
        }
    };
    match kind {
        DKind::Num(_) => {
            let count = int(obj.get("count"), "count must be an integer >= 0");
            let num = |name: &str| -> Decimal {
                match obj.get(name) {
                    Some(Value::Number(n)) => Decimal::from_str(n.as_str())
                        .unwrap_or_else(|e| pgrx::error!("jsonb_stats: {}", e)),
                    _ => fail("sum and sum_sq must be numbers"),
                }
            };
            Flow::Num {
                count,
                sum: num("sum"),
                sum_sq: num("sum_sq"),
            }
        }
        _ => {
            let Some(Value::Object(cs)) = obj.get("counts") else {
                fail("counts must be an object")
            };
            let count = if kind == DKind::Arr {
                int(obj.get("count"), "count must be an integer >= 0")
            } else {
                0
            };
            let counts = cs
                .iter()
                .map(|(k, v)| (k.clone(), int(Some(v), "counts must be integers >= 0")))
                .collect();
            Flow::Cat { count, counts }
        }
    }
}

/// Native state for jsonb_stats_delta_agg and every delta function.
#[derive(Default, Serialize, Deserialize)]
pub struct DeltaState {
    pub entries: HashMap<String, DeltaEntry>,
}

impl DeltaState {
    /// `self += sign * input`. Input: stats_delta, stats_agg or stats.
    /// Spec: jsonb_stats_delta_state_add_plpgsql
    pub fn add_input(&mut self, input: Value, negative: bool) {
        let Value::Object(m) = input else {
            pgrx::error!("jsonb_stats: stats_delta input must be an object")
        };
        match m.get("type") {
            None => {}
            Some(Value::String(t)) if t == "stats" => {
                let mut st = StatsState::default();
                st.accumulate(m);
                for (key, e) in st.entries {
                    if key == "min_max" {
                        pgrx::error!("jsonb_stats: key 'min_max' is reserved in stats_delta");
                    }
                    self.add_entry(key, DeltaEntry::from_agg_entry(e), negative);
                }
                return;
            }
            Some(Value::String(t)) if t == "stats_agg" || t == "stats_delta" => {}
            Some(other) => pgrx::error!(
                "jsonb_stats: expected stats, stats_agg or stats_delta, got type {}",
                other
            ),
        }
        if matches!(m.get("min_max"), Some(Value::Bool(false))) {
            pgrx::error!("jsonb_stats: a stats_agg without min/max (from jsonb_stats_delta_summary) is a result, not an input");
        }
        for (key, summary) in m {
            if key == "type" {
                continue;
            }
            let e = DeltaEntry::from_summary(&key, &summary);
            self.add_entry(key, e, negative);
        }
    }

    fn add_entry(&mut self, key: String, e: DeltaEntry, negative: bool) {
        match self.entries.get_mut(&key) {
            Some(cur) => cur.add(&key, &e, negative),
            None => {
                if negative {
                    pgrx::error!(
                        "jsonb_stats: cannot remove key '{}' that was never added",
                        key
                    );
                }
                self.entries.insert(key, e);
            }
        }
    }

    pub fn merge_state(&mut self, other: DeltaState) {
        for (key, e) in other.entries {
            self.add_entry(key, e, false);
        }
    }

    pub fn swap(&mut self) {
        for e in self.entries.values_mut() {
            std::mem::swap(&mut e.inflow, &mut e.outflow);
        }
    }

    /// Canonical stats_delta. Spec: jsonb_stats_delta_out_plpgsql
    pub fn to_delta(&self) -> Value {
        let mut out = Map::new();
        out.insert("type".into(), json!("stats_delta"));
        for (key, e) in &self.entries {
            if let Some(v) = e.to_json(key) {
                out.insert(key.clone(), v);
            }
        }
        Value::Object(out)
    }
}

fn state_of(x: Value) -> DeltaState {
    let mut st = DeltaState::default();
    st.add_input(x, false);
    st
}

// ── Summary of a stock ──

/// `100 * a / |b|` rounded to 2 decimals (b != 0), from exact decimals.
fn pct(a: &Decimal, b: &Decimal) -> Value {
    if b.is_zero() {
        return Value::Null;
    }
    let s = a.scale().max(b.scale());
    let num = a.mantissa_at(s) * 100u8;
    let den = b.mantissa_at(s).abs();
    ratio_round2(&num, &den).to_json()
}

fn num_summary(kind: NumKind, count: i64, sum: &Decimal, sum_sq: &Decimal) -> Map<String, Value> {
    if count == 0 {
        let mut m = Map::new();
        m.insert("type".into(), json!(kind.agg_type()));
        m.insert("count".into(), json!(0));
        m.insert("sum".into(), sum.to_json());
        m.insert("sum_sq".into(), sum_sq.to_json());
        for f in [
            "mean",
            "sum_sq_diff",
            "variance",
            "stddev",
            "coefficient_of_variation_pct",
        ] {
            m.insert(f.into(), Value::Null);
        }
        return m;
    }
    let f = NumFields {
        count,
        sum: sum.clone(),
        sum_sq: sum_sq.clone(),
        min: Decimal::zero(),
        max: Decimal::zero(),
    };
    let Value::Object(mut m) = f.summary_json(kind) else {
        unreachable!()
    };
    m.remove("min");
    m.remove("max");
    m
}

fn flow_brief(kind: DKind, f: &Flow) -> Value {
    match f {
        Flow::Num { count, sum, .. } => json!({"count": count, "sum": sum.to_json()}),
        other => other.to_json(kind),
    }
}

/// Spec: jsonb_stats_delta_summary_plpgsql
fn delta_summary(stock: Value, period: Option<Value>) -> Value {
    let st = state_of(stock);
    let has_period = period.is_some();
    let pe = period.map(state_of).unwrap_or_default();
    let mut out = Map::new();
    out.insert("type".into(), json!("stats_agg"));
    out.insert("min_max".into(), json!(false));
    let mut keys: Vec<&String> = st.entries.keys().chain(pe.entries.keys()).collect();
    keys.sort();
    keys.dedup();
    for key in keys {
        let se = st.entries.get(key);
        let pe_e = pe.entries.get(key);
        if let (Some(a), Some(b)) = (se, pe_e) {
            if a.kind != b.kind {
                pgrx::error!(
                    "jsonb_stats: type mismatch for key '{}': existing {} vs incoming {}",
                    key,
                    a.kind.delta_type(),
                    b.kind.delta_type()
                );
            }
        }
        let kind = se.or(pe_e).expect("key from one of the maps").kind;
        let mut net = match se {
            Some(e) => e.inflow.clone(),
            None => kind.zero_flow(),
        };
        if let Some(e) = se {
            net.add(&e.outflow, true);
        }
        let net = net.canonical(kind, key, "stock");
        let empty_period = pe_e.is_none_or(|e| e.inflow.is_empty() && e.outflow.is_empty());
        let mut flows = None;
        if has_period {
            let (pin, pout) = match pe_e {
                Some(e) => (e.inflow.clone(), e.outflow.clone()),
                None => (kind.zero_flow(), kind.zero_flow()),
            };
            let mut prev = net.clone();
            prev.add(&pin, true);
            prev.add(&pout, false);
            let prev = prev.canonical(kind, key, "previous stock");
            let pin = pin.canonical(kind, key, "in");
            let pout = pout.canonical(kind, key, "out");
            flows = Some((prev, pin, pout));
        }
        if net.is_empty() && empty_period {
            continue;
        }
        let entry = match (&net, kind) {
            (Flow::Num { count, sum, sum_sq }, DKind::Num(k)) => {
                let mut m = num_summary(k, *count, sum, sum_sq);
                if let Some((prev, pin, pout)) = &flows {
                    let (
                        Flow::Num { sum: s0, .. },
                        Flow::Num { sum: si, .. },
                        Flow::Num { sum: so, .. },
                    ) = (prev, pin, pout)
                    else {
                        unreachable!()
                    };
                    let mut change = sum.clone();
                    change.add_signed(s0, true);
                    let mut churn = si.clone().abs();
                    churn.add_assign(&so.clone().abs());
                    m.insert("in".into(), flow_brief(kind, pin));
                    m.insert("out".into(), flow_brief(kind, pout));
                    m.insert("change_pct".into(), pct(&change, s0));
                    m.insert("churn_pct".into(), pct(&churn, s0));
                }
                m
            }
            _ => {
                let Value::Object(mut m) = net.to_json(kind) else {
                    unreachable!()
                };
                m.insert("type".into(), json!(kind.agg_type()));
                if let Some((_, pin, pout)) = &flows {
                    m.insert("in".into(), pin.to_json(kind));
                    m.insert("out".into(), pout.to_json(kind));
                }
                m
            }
        };
        out.insert(key.clone(), Value::Object(entry));
    }
    Value::Object(out)
}

// ── SQL functions ──

/// Entry event: a stats_agg (or stats, or stats_delta) as a stats_delta.
/// Spec: jsonb_stats_to_delta_plpgsql
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_to_delta(x: JsonB) -> JsonB {
    JsonB(state_of(x.0).to_delta())
}

/// Exit event: the negation of a stats_agg, stats or stats_delta (in and out swapped).
/// Spec: jsonb_stats_negate_plpgsql
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_negate(x: JsonB) -> JsonB {
    let mut st = state_of(x.0);
    st.swap();
    JsonB(st.to_delta())
}

/// Sum of two deltas. Spec: jsonb_stats_delta_merge_plpgsql
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_delta_merge(a: JsonB, b: JsonB) -> JsonB {
    let mut st = state_of(a.0);
    st.add_input(b.0, false);
    JsonB(st.to_delta())
}

/// A stock (sum of all deltas up to a date) as a stats_agg without min/max,
/// with flows and change/churn rates for an optional period (the sum of the
/// deltas inside the last period). Spec: jsonb_stats_delta_summary_plpgsql
#[pg_extern(immutable, parallel_safe)]
pub fn jsonb_stats_delta_summary(
    stock: Option<JsonB>,
    period: default!(Option<JsonB>, "NULL"),
) -> Option<JsonB> {
    let stock = stock?;
    Some(JsonB(delta_summary(stock.0, period.map(|p| p.0))))
}

unsafe fn state_ptr(internal: Internal) -> *mut DeltaState {
    match internal.unwrap() {
        Some(datum) => datum.cast_mut_ptr::<DeltaState>(),
        None => Box::into_raw(Box::new(DeltaState::default())),
    }
}

fn to_internal(p: *mut DeltaState) -> Internal {
    Internal::from(Some(pgrx::pg_sys::Datum::from(p as usize)))
}

/// Transition: add one delta (also used as the moving-aggregate msfunc).
///
/// # Safety
/// `internal` must be NULL or a `DeltaState` pointer created by this extension.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_delta_sfunc(internal: Internal, d: Option<JsonB>) -> Internal {
    let p = unsafe { state_ptr(internal) };
    if let Some(JsonB(v)) = d {
        unsafe { &mut *p }.add_input(v, false);
    }
    to_internal(p)
}

/// Moving-aggregate inverse: remove a delta added before. Exact.
///
/// # Safety
/// `internal` must be NULL or a `DeltaState` pointer created by this extension.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_delta_minvfunc(internal: Internal, d: Option<JsonB>) -> Internal {
    let p = unsafe { state_ptr(internal) };
    if let Some(JsonB(v)) = d {
        unsafe { &mut *p }.add_input(v, true);
    }
    to_internal(p)
}

/// # Safety
/// Both arguments must be NULL or `DeltaState` pointers created by this extension.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_delta_combine(state1: Internal, state2: Internal) -> Internal {
    let p1 = state1.unwrap().map(|d| d.cast_mut_ptr::<DeltaState>());
    let p2 = state2.unwrap().map(|d| d.cast_mut_ptr::<DeltaState>());
    match (p1, p2) {
        (None, None) => to_internal(Box::into_raw(Box::new(DeltaState::default()))),
        (Some(p), None) | (None, Some(p)) => to_internal(p),
        (Some(a), Some(b)) => {
            let s2 = unsafe { Box::from_raw(b) };
            unsafe { &mut *a }.merge_state(*s2);
            to_internal(a)
        }
    }
}

/// # Safety
/// `internal` must be NULL or a `DeltaState` pointer created by this extension.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_delta_serial(internal: Internal) -> Vec<u8> {
    let r = match internal.unwrap() {
        Some(d) => serde_json::to_vec(unsafe { &*d.cast_mut_ptr::<DeltaState>() }),
        None => serde_json::to_vec(&DeltaState::default()),
    };
    r.unwrap_or_else(|e| pgrx::error!("jsonb_stats: serialization failed: {}", e))
}

/// # Safety
/// Called by PostgreSQL with bytes produced by `jsonb_stats_delta_serial`.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_delta_deserial(bytes: Vec<u8>, _internal: Internal) -> Internal {
    let st: DeltaState = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| pgrx::error!("jsonb_stats: deserialization failed: {}", e));
    to_internal(Box::into_raw(Box::new(st)))
}

/// Final function (plain and moving): canonical stats_delta. Borrows the
/// state, which window aggregation keeps using.
///
/// # Safety
/// `internal` must be NULL or a `DeltaState` pointer created by this extension.
#[pg_extern(immutable, parallel_safe)]
pub unsafe fn jsonb_stats_delta_final(internal: Internal) -> JsonB {
    match internal.unwrap() {
        Some(d) => JsonB(unsafe { &*d.cast_mut_ptr::<DeltaState>() }.to_delta()),
        None => JsonB(json!({"type": "stats_delta"})),
    }
}
