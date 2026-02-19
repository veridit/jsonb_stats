use pgrx::prelude::*;
use pgrx::{AnyElement, JsonB};
use serde_json::{json, Map, Value};

/// Create a typed `stat` JSONB from any scalar value.
/// Returns: {"type": "<type_name>", "value": <value>}
///
/// Type mapping:
///   int4 -> "int", float8 -> "float", bool -> "bool",
///   text -> "str", date -> "date", numeric -> "dec2"
#[pg_extern(immutable, parallel_safe, strict)]
pub fn stat(value: AnyElement) -> JsonB {
    let oid = value.oid();
    let datum = value.datum();

    let (type_name, json_value) = unsafe {
        if oid == pg_sys::INT4OID {
            let v = i32::from_datum(datum, false).unwrap_or(0);
            ("int", json!(v))
        } else if oid == pg_sys::FLOAT8OID {
            let v = f64::from_datum(datum, false).unwrap_or(0.0);
            ("float", serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null))
        } else if oid == pg_sys::BOOLOID {
            let v = bool::from_datum(datum, false).unwrap_or(false);
            ("bool", json!(v))
        } else if oid == pg_sys::TEXTOID || oid == pg_sys::VARCHAROID {
            let v = String::from_datum(datum, false).unwrap_or_default();
            ("str", json!(v))
        } else if oid == pg_sys::DATEOID {
            let v = pgrx::datum::Date::from_datum(datum, false);
            match v {
                Some(d) => ("date", json!(d.to_string())),
                None => ("date", Value::Null),
            }
        } else if oid == pg_sys::NUMERICOID {
            let v = pgrx::AnyNumeric::from_datum(datum, false);
            match v {
                Some(n) => {
                    let s = n.to_string();
                    let num_val = serde_json::from_str::<Value>(&s)
                        .unwrap_or_else(|_| json!(s));
                    ("dec2", num_val)
                }
                None => ("dec2", Value::Null),
            }
        } else {
            // Fallback: convert to string representation
            let v = String::from_datum(datum, false).unwrap_or_default();
            ("str", json!(v))
        }
    };

    let mut obj = Map::new();
    obj.insert("type".to_string(), json!(type_name));
    obj.insert("value".to_string(), json_value);
    JsonB(Value::Object(obj))
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
