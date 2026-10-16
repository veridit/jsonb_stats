use pgrx::prelude::*;
use pgrx::{AnyElement, JsonB};
use serde_json::{json, Map, Value};

use crate::exact::Decimal;

/// Create a typed `stat` JSONB from any scalar value.
/// Returns: {"type": "<type_name>", "value": <value>}
///
/// Type mapping:
///   smallint, integer, bigint -> "int"
///   real, double precision    -> "float" (shortest round-trip decimal text)
///   numeric                   -> "dec2"  (rounded to 2 decimals, half away from zero)
///   boolean -> "bool", text/varchar -> "str", date -> "date", arrays -> "arr"
///   anything else             -> "str" (the type's text output)
#[pg_extern(immutable, parallel_safe, strict)]
pub fn stat(value: AnyElement) -> JsonB {
    let oid = value.oid();
    let datum = value.datum();

    let (type_name, json_value) = unsafe {
        match oid {
            pg_sys::INT2OID => (
                "int",
                json!(i16::from_datum(datum, false).unwrap_or_default()),
            ),
            pg_sys::INT4OID => (
                "int",
                json!(i32::from_datum(datum, false).unwrap_or_default()),
            ),
            pg_sys::INT8OID => (
                "int",
                json!(i64::from_datum(datum, false).unwrap_or_default()),
            ),
            pg_sys::FLOAT4OID => {
                let v = f32::from_datum(datum, false).unwrap_or_default();
                ("float", finite_number(v.is_finite(), &v.to_string(), &v))
            }
            pg_sys::FLOAT8OID => {
                let v = f64::from_datum(datum, false).unwrap_or_default();
                ("float", finite_number(v.is_finite(), &v.to_string(), &v))
            }
            pg_sys::NUMERICOID => {
                // NaN / Infinity are the only numeric outputs that do not parse.
                let text = output_text(oid, datum);
                let dec = text.parse::<Decimal>().unwrap_or_else(|_| {
                    pgrx::error!(
                        "jsonb_stats: stat() cannot represent non-finite value {}",
                        text
                    )
                });
                ("dec2", dec.round_to(2).to_json())
            }
            pg_sys::BOOLOID => (
                "bool",
                json!(bool::from_datum(datum, false).unwrap_or_default()),
            ),
            pg_sys::TEXTOID | pg_sys::VARCHAROID => (
                "str",
                json!(String::from_datum(datum, false).unwrap_or_default()),
            ),
            pg_sys::DATEOID => match pgrx::datum::Date::from_datum(datum, false) {
                Some(d) => ("date", json!(d.to_string())),
                None => ("date", Value::Null),
            },
            _ if pg_sys::get_element_type(oid) != pg_sys::InvalidOid => {
                ("arr", json!(output_text(oid, datum)))
            }
            _ => ("str", json!(output_text(oid, datum))),
        }
    };

    let mut obj = Map::new();
    obj.insert("type".to_string(), json!(type_name));
    obj.insert("value".to_string(), json_value);
    JsonB(Value::Object(obj))
}

/// A JSON number from Rust's shortest round-trip float text, or an error for
/// NaN / Infinity (which JSON and the aggregates cannot represent).
fn finite_number(is_finite: bool, text: &str, shown: &dyn std::fmt::Display) -> Value {
    if !is_finite {
        pgrx::error!(
            "jsonb_stats: stat() cannot represent non-finite value {}",
            shown
        );
    }
    let dec: Decimal = text
        .parse()
        .unwrap_or_else(|e| pgrx::error!("jsonb_stats: stat() cannot encode {}: {}", text, e));
    dec.to_json()
}

/// The value's text form via the type's output function (works for any type).
unsafe fn output_text(oid: pg_sys::Oid, datum: pg_sys::Datum) -> String {
    let mut out_fn = pg_sys::InvalidOid;
    let mut is_varlena = false;
    unsafe {
        pg_sys::getTypeOutputInfo(oid, &mut out_fn, &mut is_varlena);
        let cstr = pg_sys::OidOutputFunctionCall(out_fn, datum);
        let s = std::ffi::CStr::from_ptr(cstr)
            .to_string_lossy()
            .into_owned();
        pg_sys::pfree(cstr.cast());
        s
    }
}

/// Add "type": "stats" to a JSONB object containing stat entries.
#[pg_extern(name = "stats", immutable, parallel_safe, strict)]
pub fn stats_from_jsonb(input: JsonB) -> JsonB {
    let mut obj = match input.0 {
        Value::Object(m) => m,
        _ => return input,
    };
    obj.insert("type".to_string(), json!("stats"));
    JsonB(Value::Object(obj))
}

/// State transition function for jsonb_stats_agg(text, jsonb).
/// Inserts code->stat into the state object, adding "type":"stats" on first call.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn jsonb_stats_sfunc(state: JsonB, code: &str, stat_val: JsonB) -> JsonB {
    let mut obj = match state.0 {
        Value::Object(m) => m,
        _ => Map::new(),
    };

    obj.insert(code.to_string(), stat_val.0);

    if !obj.contains_key("type") {
        obj.insert("type".to_string(), json!("stats"));
    }

    JsonB(Value::Object(obj))
}
