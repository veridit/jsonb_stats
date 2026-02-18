use serde_json::{Number, Value};

type Map = serde_json::Map<String, Value>;

/// Extract an f64 from a JSON object by key.
/// With `arbitrary_precision`, Number::as_f64() returns None,
/// so we parse from the string representation.
pub fn get_f64(obj: &Map, key: &str) -> f64 {
    match obj.get(key) {
        Some(Value::Number(n)) => n.to_string().parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Extract an i64 from a JSON object by key.
pub fn get_i64(obj: &Map, key: &str) -> i64 {
    match obj.get(key) {
        Some(Value::Number(n)) => n.to_string().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

/// Get the "type" string from a JSON object.
pub fn get_type(obj: &Map) -> &str {
    match obj.get("type") {
        Some(Value::String(s)) => s.as_str(),
        _ => "",
    }
}

/// Create a JSON number from f64, using integer representation when the value is exact.
/// This matches PostgreSQL's numeric behavior where 100.0 is stored as 100.
pub fn num_value(v: f64) -> Value {
    if v.fract() == 0.0 && v.abs() < (i64::MAX as f64) {
        Value::Number(Number::from(v as i64))
    } else {
        Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

/// Round f64 to 2 decimal places, preserving exact representation via arbitrary_precision.
/// E.g. round2(100.0) produces the JSON number 100.00 (not 100 or 100.0).
pub fn round2(v: f64) -> Value {
    // format!("{:.2}", v) always produces exactly 2 decimal places
    serde_json::from_str(&format!("{:.2}", v)).unwrap()
}
