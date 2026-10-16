//! Exact decimal arithmetic for numeric aggregates.
//!
//! A [`Decimal`] is `mantissa / 10^scale` with an arbitrary-precision
//! mantissa. Addition and multiplication are exact, so accumulating and
//! merging summaries is associative and commutative: the result does not
//! depend on row order, grouping, parallel plan or CPU.
//!
//! Derived statistics are rationals (or square roots of rationals) of the
//! exact state; they are rounded exactly once, to 2 decimals, half away from
//! zero (PostgreSQL `round(x, 2)`). See dev/reference_plpgsql.sql.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use num_bigint::{BigInt, Sign};
use num_integer::Integer;
use num_traits::{Signed, Zero};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decimal {
    mantissa: BigInt,
    scale: u32,
}

/// Largest decimal exponent accepted when parsing ("1e400" style input).
/// PostgreSQL numeric itself allows about 131072 digits before the point.
const MAX_EXPONENT: i64 = 131_072;

fn pow10(n: u32) -> BigInt {
    num_traits::pow(BigInt::from(10u8), n as usize)
}

impl Decimal {
    pub fn zero() -> Self {
        Decimal {
            mantissa: BigInt::zero(),
            scale: 0,
        }
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn is_negative(&self) -> bool {
        self.mantissa.is_negative()
    }

    /// True if the value is a whole number (regardless of scale).
    pub fn is_integral(&self) -> bool {
        (&self.mantissa % pow10(self.scale)).is_zero()
    }

    /// Same value at a larger scale (exact).
    fn rescaled(&self, scale: u32) -> BigInt {
        debug_assert!(scale >= self.scale);
        if scale == self.scale {
            self.mantissa.clone()
        } else {
            &self.mantissa * pow10(scale - self.scale)
        }
    }

    /// Change the scale. Fails (returns None) if digits would be lost.
    pub fn with_scale(&self, scale: u32) -> Option<Self> {
        if scale >= self.scale {
            return Some(Decimal {
                mantissa: self.rescaled(scale),
                scale,
            });
        }
        let (q, r) = self.mantissa.div_rem(&pow10(self.scale - scale));
        r.is_zero().then_some(Decimal { mantissa: q, scale })
    }

    /// Round to `scale` decimals, half away from zero.
    pub fn round_to(&self, scale: u32) -> Self {
        if scale >= self.scale {
            return Decimal {
                mantissa: self.rescaled(scale),
                scale,
            };
        }
        let den = pow10(self.scale - scale);
        Decimal {
            mantissa: round_half_away(&self.mantissa, &den),
            scale,
        }
    }

    pub fn add(&self, other: &Decimal) -> Decimal {
        let scale = self.scale.max(other.scale);
        Decimal {
            mantissa: self.rescaled(scale) + other.rescaled(scale),
            scale,
        }
    }

    pub fn mul(&self, other: &Decimal) -> Decimal {
        Decimal {
            mantissa: &self.mantissa * &other.mantissa,
            scale: self.scale + other.scale,
        }
    }

    pub fn square(&self) -> Decimal {
        self.mul(self)
    }

    /// Numeric comparison (scale-insensitive).
    pub fn cmp_value(&self, other: &Decimal) -> Ordering {
        let scale = self.scale.max(other.scale);
        self.rescaled(scale).cmp(&other.rescaled(scale))
    }

    /// Min/max helper: the lesser (`want_less`) or greater value. Equal values
    /// keep the one with the larger scale, so the printed result is the same
    /// regardless of argument order.
    pub fn pick(self, other: Decimal, want_less: bool) -> Decimal {
        match self.cmp_value(&other) {
            Ordering::Equal => {
                if self.scale >= other.scale {
                    self
                } else {
                    other
                }
            }
            Ordering::Less => {
                if want_less {
                    self
                } else {
                    other
                }
            }
            Ordering::Greater => {
                if want_less {
                    other
                } else {
                    self
                }
            }
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        let n = serde_json::Number::from_str(&self.to_string())
            .unwrap_or_else(|e| pgrx::error!("jsonb_stats: cannot encode number {}: {}", self, e));
        serde_json::Value::Number(n)
    }

    pub fn negated(self) -> Decimal {
        Decimal {
            mantissa: -self.mantissa,
            scale: self.scale,
        }
    }

    /// The value as an i64, if it is integral and in range.
    pub fn to_i64(&self) -> Option<i64> {
        if !self.is_integral() {
            return None;
        }
        let whole = &self.mantissa / pow10(self.scale);
        i64::try_from(whole).ok()
    }
}

/// Round the rational `num / den` (den > 0) to an integer, half away from zero.
fn round_half_away(num: &BigInt, den: &BigInt) -> BigInt {
    debug_assert!(den.is_positive());
    let mag = (num.abs() * 2u8 + den) / (den * 2u8);
    if num.is_negative() {
        -mag
    } else {
        mag
    }
}

/// The rational `num / den` (den != 0) rounded to 2 decimals, half away from zero.
pub fn ratio_round2(num: &BigInt, den: &BigInt) -> Decimal {
    let (num, den) = if den.is_negative() {
        (-num, -den)
    } else {
        (num.clone(), den.clone())
    };
    Decimal {
        mantissa: round_half_away(&(num * 100u8), &den),
        scale: 2,
    }
}

/// `sqrt(num / den)` (num >= 0, den > 0) rounded to 2 decimals, half away from zero.
/// Uses floor(100*sqrt(q) + 1/2) = (isqrt(floor(40000 q)) + 1) div 2, which is
/// exact because sqrt(q) * 200 = sqrt(40000 q) and isqrt(floor(x)) = floor(sqrt(x)).
pub fn ratio_sqrt_round2(num: &BigInt, den: &BigInt) -> Decimal {
    debug_assert!(!num.is_negative() && den.is_positive());
    let floor = (num * 40000u32) / den;
    let mantissa = (floor.sqrt() + 1u8) / 2u8;
    Decimal { mantissa, scale: 2 }
}

impl Decimal {
    /// Mantissa of `self` at `scale` (scale >= self.scale).
    pub fn mantissa_at(&self, scale: u32) -> BigInt {
        self.rescaled(scale)
    }
}

impl FromStr for Decimal {
    type Err = String;

    /// Parse JSON number text: `-?digits(.digits)?([eE][+-]?digits)?`.
    fn from_str(s: &str) -> Result<Self, String> {
        let err = || format!("invalid number '{s}'");
        let (body, exp) = match s.find(['e', 'E']) {
            Some(i) => {
                let exp: i64 = s[i + 1..].parse().map_err(|_| err())?;
                if exp.abs() > MAX_EXPONENT {
                    return Err(format!("number '{s}' is out of range"));
                }
                (&s[..i], exp)
            }
            None => (s, 0),
        };
        let (neg, body) = match body.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, body),
        };
        let (int_part, frac_part) = match body.split_once('.') {
            Some((i, f)) => (i, f),
            None => (body, ""),
        };
        if int_part.is_empty()
            || !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
            || (body.contains('.') && frac_part.is_empty())
        {
            return Err(err());
        }
        let digits = format!("{int_part}{frac_part}");
        let mut mantissa = BigInt::from_str(&digits).map_err(|_| err())?;
        if neg {
            mantissa = -mantissa;
        }
        // value = digits * 10^(exp - frac_len); PostgreSQL keeps scale
        // max(0, frac_len - exp) for this input, which we mirror.
        let scale = frac_part.len() as i64 - exp;
        if scale >= 0 {
            Ok(Decimal {
                mantissa,
                scale: scale as u32,
            })
        } else {
            Ok(Decimal {
                mantissa: mantissa * pow10((-scale) as u32),
                scale: 0,
            })
        }
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = self.mantissa.magnitude().to_str_radix(10);
        let scale = self.scale as usize;
        let sign = if self.mantissa.sign() == Sign::Minus {
            "-"
        } else {
            ""
        };
        if scale == 0 {
            return write!(f, "{sign}{digits}");
        }
        let padded = format!("{digits:0>width$}", width = scale + 1);
        let (i, frac) = padded.split_at(padded.len() - scale);
        write!(f, "{sign}{i}.{frac}")
    }
}

// Serialized as a JSON string for the parallel-worker state (bytea), so no
// precision is ever lost in transit.
impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Decimal::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod unit {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn parse_and_print_preserve_scale() {
        for s in [
            "0",
            "-1",
            "2.50",
            "0.000001",
            "-0.01",
            "9223372036854775807",
            "123.456",
        ] {
            assert_eq!(d(s).to_string(), s);
        }
        assert_eq!(d("1e3").to_string(), "1000");
        assert_eq!(d("1.5e1").to_string(), "15");
        assert_eq!(d("1.25e-1").to_string(), "0.125");
        assert_eq!(d("-0").to_string(), "0");
        for bad in ["", "-", "1.", ".5", "1e", "abc", "1.2.3", "+1"] {
            assert!(bad.parse::<Decimal>().is_err(), "{bad}");
        }
    }

    #[test]
    fn exact_add_mul_and_scale() {
        assert_eq!(d("0.1").add(&d("0.2")).to_string(), "0.3");
        assert_eq!(d("2.5").square().to_string(), "6.25");
        assert_eq!(d("1").with_scale(2).unwrap().to_string(), "1.00");
        assert_eq!(d("1.50").with_scale(1).unwrap().to_string(), "1.5");
        assert!(d("1.234").with_scale(2).is_none());
        assert_eq!(d("1.235").round_to(2).to_string(), "1.24");
        assert_eq!(d("-1.235").round_to(2).to_string(), "-1.24");
        assert_eq!(d("1.234").round_to(2).to_string(), "1.23");
        assert!(d("3.00").is_integral());
        assert!(!d("3.01").is_integral());
        assert_eq!(d("2.0").to_i64(), Some(2));
        assert_eq!(d("2.5").to_i64(), None);
        assert_eq!(d("99999999999999999999").to_i64(), None);
        assert_eq!(d("-2.50").negated().to_string(), "2.50");
    }

    #[test]
    fn pick_is_order_independent_on_ties() {
        assert_eq!(d("2.5").pick(d("2.50"), true).to_string(), "2.50");
        assert_eq!(d("2.50").pick(d("2.5"), true).to_string(), "2.50");
        assert_eq!(d("1").pick(d("2"), true).to_string(), "1");
        assert_eq!(d("1").pick(d("2"), false).to_string(), "2");
    }

    #[test]
    fn rounding_half_away_from_zero() {
        let b = |v: i64| BigInt::from(v);
        assert_eq!(ratio_round2(&b(1), &b(8)).to_string(), "0.13");
        assert_eq!(ratio_round2(&b(-1), &b(8)).to_string(), "-0.13");
        assert_eq!(ratio_round2(&b(1), &b(-8)).to_string(), "-0.13");
        assert_eq!(ratio_round2(&b(5000), &b(1)).to_string(), "5000.00");
        assert_eq!(ratio_round2(&b(0), &b(3)).to_string(), "0.00");
        assert_eq!(ratio_sqrt_round2(&b(5000), &b(1)).to_string(), "70.71");
        assert_eq!(ratio_sqrt_round2(&b(1), &b(2)).to_string(), "0.71");
        assert_eq!(ratio_sqrt_round2(&b(0), &b(2)).to_string(), "0.00");
        assert_eq!(ratio_sqrt_round2(&b(25), &b(1)).to_string(), "5.00");
        // sqrt(0.00015625) = 0.0125 -> 1.25 hundredths -> 0.01
        assert_eq!(
            ratio_sqrt_round2(&b(15625), &b(100_000_000)).to_string(),
            "0.01"
        );
        // sqrt(0.000225) = 0.015 -> exact tie -> 0.02 (half away from zero)
        assert_eq!(
            ratio_sqrt_round2(&b(225), &b(1_000_000)).to_string(),
            "0.02"
        );
    }
}
