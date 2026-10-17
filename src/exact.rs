//! Exact decimal arithmetic for numeric aggregates.
//!
//! A [`Decimal`] is `mantissa / 10^scale`. Addition and multiplication are
//! exact, so accumulating and merging summaries is associative and
//! commutative: the result does not depend on row order, grouping, parallel
//! plan or CPU.
//!
//! **Fast path.** The mantissa is an `i128` while it fits (about 38 digits),
//! and an arbitrary-precision `BigInt` otherwise. Every operation is checked:
//! an `i128` overflow redoes that operation in `BigInt`, and a `BigInt` result
//! that fits in `i128` is stored as `i128` again. Both forms hold the same
//! exact integer, and the form is chosen only by magnitude, so it never
//! affects a result and never appears in the JSON.
//!
//! Derived statistics are rationals (or square roots of rationals) of the
//! exact state; they are rounded exactly once, to 2 decimals, half away from
//! zero (PostgreSQL `round(x, 2)`). See dev/reference_plpgsql.sql.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// An exact integer: `i128` while it fits, `BigInt` beyond.
/// Invariant: `Big` never holds a value that fits in `i128`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Mant {
    Small(i128),
    Big(BigInt),
}

impl Mant {
    fn from_big(b: BigInt) -> Mant {
        match b.to_i128() {
            Some(v) => Mant::Small(v),
            None => Mant::Big(b),
        }
    }

    fn to_big(&self) -> BigInt {
        match self {
            Mant::Small(v) => BigInt::from(*v),
            Mant::Big(b) => b.clone(),
        }
    }

    fn is_negative(&self) -> bool {
        match self {
            Mant::Small(v) => *v < 0,
            Mant::Big(b) => b.is_negative(),
        }
    }

    /// In place, without cloning a BigInt accumulator (the hot path once a
    /// running sum has outgrown i128).
    fn add_assign(&mut self, o: &Mant) {
        match (&mut *self, o) {
            (Mant::Small(a), Mant::Small(b)) => match a.checked_add(*b) {
                Some(v) => *a = v,
                None => *self = Mant::Big(BigInt::from(*a) + *b),
            },
            (Mant::Big(a), Mant::Small(b)) => {
                *a += *b;
                self.normalize();
            }
            (Mant::Big(a), Mant::Big(b)) => {
                *a += b;
                self.normalize();
            }
            (Mant::Small(a), Mant::Big(b)) => *self = Mant::from_big(b + *a),
        }
    }

    /// Restore the invariant: a Big that fits in i128 becomes Small.
    fn normalize(&mut self) {
        if let Mant::Big(b) = self {
            if let Some(v) = b.to_i128() {
                *self = Mant::Small(v);
            }
        }
    }

    fn mul(&self, o: &Mant) -> Mant {
        if let (Mant::Small(a), Mant::Small(b)) = (self, o) {
            if let Some(v) = a.checked_mul(*b) {
                return Mant::Small(v);
            }
        }
        Mant::from_big(self.to_big() * o.to_big())
    }

    /// `self * 10^d`.
    fn scale_up(&self, d: u32) -> Mant {
        if d == 0 {
            return self.clone();
        }
        if let Mant::Small(a) = self {
            if let Some(v) = 10i128.checked_pow(d).and_then(|p| a.checked_mul(p)) {
                return Mant::Small(v);
            }
        }
        Mant::from_big(self.to_big() * pow10(d))
    }

    fn neg(self) -> Mant {
        match self {
            Mant::Small(v) => match v.checked_neg() {
                Some(n) => Mant::Small(n),
                None => Mant::Big(-BigInt::from(v)),
            },
            Mant::Big(b) => Mant::from_big(-b),
        }
    }

    fn cmp(&self, o: &Mant) -> Ordering {
        match (self, o) {
            (Mant::Small(a), Mant::Small(b)) => a.cmp(b),
            // By the invariant, a Big lies outside the i128 range.
            (Mant::Small(_), Mant::Big(b)) => {
                if b.is_negative() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Mant::Big(a), Mant::Small(_)) => {
                if a.is_negative() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (Mant::Big(a), Mant::Big(b)) => a.cmp(b),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decimal {
    mantissa: Mant,
    scale: u32,
}

/// Largest decimal exponent accepted when parsing ("1e400" style input).
/// PostgreSQL numeric itself allows about 131072 digits before the point.
const MAX_EXPONENT: i64 = 131_072;

fn pow10(n: u32) -> BigInt {
    num_traits::pow(BigInt::from(10u8), n as usize)
}

impl Decimal {
    fn new(mantissa: BigInt, scale: u32) -> Decimal {
        Decimal {
            mantissa: Mant::from_big(mantissa),
            scale,
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
        if self.scale == 0 {
            return true;
        }
        match (&self.mantissa, 10i128.checked_pow(self.scale)) {
            (Mant::Small(v), Some(p)) => v % p == 0,
            _ => (self.mantissa.to_big() % pow10(self.scale)).is_zero(),
        }
    }

    /// Mantissa at a larger scale (exact).
    fn rescaled(&self, scale: u32) -> Mant {
        debug_assert!(scale >= self.scale);
        self.mantissa.scale_up(scale - self.scale)
    }

    /// Change the scale. Fails (returns None) if digits would be lost.
    pub fn with_scale(&self, scale: u32) -> Option<Self> {
        if scale >= self.scale {
            return Some(Decimal {
                mantissa: self.rescaled(scale),
                scale,
            });
        }
        let (q, r) = self.mantissa.to_big().div_rem(&pow10(self.scale - scale));
        r.is_zero().then(|| Decimal::new(q, scale))
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
        Decimal::new(round_half_away(&self.mantissa.to_big(), &den), scale)
    }

    #[cfg(test)]
    pub fn add(&self, other: &Decimal) -> Decimal {
        let mut out = self.clone();
        out.add_assign(other);
        out
    }

    /// In-place `self += other`.
    pub fn add_assign(&mut self, other: &Decimal) {
        match self.scale.cmp(&other.scale) {
            Ordering::Equal => self.mantissa.add_assign(&other.mantissa),
            Ordering::Greater => {
                let o = other.rescaled(self.scale);
                self.mantissa.add_assign(&o);
            }
            Ordering::Less => {
                self.mantissa = self.rescaled(other.scale);
                self.mantissa.add_assign(&other.mantissa);
                self.scale = other.scale;
            }
        }
    }

    pub fn mul(&self, other: &Decimal) -> Decimal {
        Decimal {
            mantissa: self.mantissa.mul(&other.mantissa),
            scale: self.scale + other.scale,
        }
    }

    pub fn square(&self) -> Decimal {
        self.mul(self)
    }

    /// Numeric comparison (scale-insensitive).
    pub fn cmp_value(&self, other: &Decimal) -> Ordering {
        match self.scale.cmp(&other.scale) {
            Ordering::Equal => self.mantissa.cmp(&other.mantissa),
            Ordering::Less => self.rescaled(other.scale).cmp(&other.mantissa),
            Ordering::Greater => self.mantissa.cmp(&other.rescaled(self.scale)),
        }
    }

    /// Whether `candidate` should replace `self` as the running minimum
    /// (`want_less`) or maximum. Equal values prefer the larger scale, so the
    /// printed result does not depend on the order values arrive in.
    pub fn replaced_by(&self, candidate: &Decimal, want_less: bool) -> bool {
        match candidate.cmp_value(self) {
            Ordering::Equal => candidate.scale > self.scale,
            Ordering::Less => want_less,
            Ordering::Greater => !want_less,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        let n = serde_json::Number::from_str(&self.to_string())
            .unwrap_or_else(|e| pgrx::error!("jsonb_stats: cannot encode number {}: {}", self, e));
        serde_json::Value::Number(n)
    }

    pub fn negated(self) -> Decimal {
        Decimal {
            mantissa: self.mantissa.neg(),
            scale: self.scale,
        }
    }

    /// The value as an i64, if it is integral and in range.
    pub fn to_i64(&self) -> Option<i64> {
        if !self.is_integral() {
            return None;
        }
        let whole = self.mantissa.to_big() / pow10(self.scale);
        i64::try_from(whole).ok()
    }

    /// Mantissa of `self` at `scale` (scale >= self.scale), as a BigInt.
    /// Used by the once-per-summary derived statistics.
    pub fn mantissa_at(&self, scale: u32) -> BigInt {
        self.rescaled(scale).to_big()
    }

    #[cfg(test)]
    fn is_small(&self) -> bool {
        matches!(self.mantissa, Mant::Small(_))
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
    Decimal::ratio_round(num, den, 2)
}

impl Decimal {
    /// The rational `num / den` (den != 0) rounded to `scale` decimals, half away from zero.
    pub fn ratio_round(num: &BigInt, den: &BigInt, scale: u32) -> Decimal {
        let (num, den) = if den.is_negative() {
            (-num, -den)
        } else {
            (num.clone(), den.clone())
        };
        Decimal::new(round_half_away(&(num * pow10(scale)), &den), scale)
    }

    /// The rational `num / den` (den > 0) rounded up (toward +inf) to `scale` decimals.
    pub fn ratio_ceil(num: &BigInt, den: &BigInt, scale: u32) -> Decimal {
        debug_assert!(den.is_positive());
        Decimal::new((num * pow10(scale)).div_ceil(den), scale)
    }
}

/// `sqrt(num / den)` (num >= 0, den > 0) rounded to 2 decimals, half away from zero.
/// Uses floor(100*sqrt(q) + 1/2) = (isqrt(floor(40000 q)) + 1) div 2, which is
/// exact because sqrt(q) * 200 = sqrt(40000 q) and isqrt(floor(x)) = floor(sqrt(x)).
pub fn ratio_sqrt_round2(num: &BigInt, den: &BigInt) -> Decimal {
    debug_assert!(!num.is_negative() && den.is_positive());
    let floor = (num * 40000u32) / den;
    Decimal::new((floor.sqrt() + 1u8) / 2u8, 2)
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
        // Up to 38 digits always fit in an i128: no allocation.
        let mut mantissa = if int_part.len() + frac_part.len() <= 38 {
            Mant::Small(
                int_part
                    .bytes()
                    .chain(frac_part.bytes())
                    .fold(0i128, |acc, b| acc * 10 + i128::from(b - b'0')),
            )
        } else {
            Mant::from_big(BigInt::from_str(&format!("{int_part}{frac_part}")).map_err(|_| err())?)
        };
        if neg {
            mantissa = mantissa.neg();
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
                mantissa: mantissa.scale_up((-scale) as u32),
                scale: 0,
            })
        }
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (neg, digits) = match &self.mantissa {
            Mant::Small(v) => (*v < 0, v.unsigned_abs().to_string()),
            Mant::Big(b) => (b.is_negative(), b.magnitude().to_str_radix(10)),
        };
        let sign = if neg { "-" } else { "" };
        let scale = self.scale as usize;
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
    fn min_max_is_order_independent_on_ties() {
        let r = |a: &str, b: &str, less| d(a).replaced_by(&d(b), less);
        assert!(r("2.5", "2.50", true), "larger scale wins a tie (min)");
        assert!(!r("2.50", "2.5", true), "smaller scale never wins a tie");
        assert!(r("2.5", "2.50", false), "larger scale wins a tie (max)");
        assert!(r("2", "1", true) && !r("1", "2", true));
        assert!(r("1", "2", false) && !r("2", "1", false));
        assert!(r("-1", "-1.5", true) && !r("-1.5", "-1", true));
        assert_eq!(
            d("123456789012345678.9").to_string(),
            "123456789012345678.9"
        );
        assert_eq!(
            d("-12345678901234567.8").to_string(),
            "-12345678901234567.8"
        );
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

    #[test]
    fn fast_path_promotes_and_demotes_exactly() {
        let max = i128::MAX.to_string(); // 170141183460469231731687303715884105727
                                         // parse: 38 digits small, 39 digits big unless it fits
        assert!(d("99999999999999999999999999999999999999").is_small());
        assert!(d(&max).is_small());
        assert!(!d("170141183460469231731687303715884105728").is_small());
        // add: overflow promotes, and the sum is exact
        let s = d(&max).add(&d("1"));
        assert!(!s.is_small());
        assert_eq!(s.to_string(), "170141183460469231731687303715884105728");
        // demotion: back in range becomes small again (canonical form)
        let back = s.add(&d("-1"));
        assert!(back.is_small());
        assert_eq!(back, d(&max));
        // mul: overflow promotes (13043817825332782213² > i128::MAX; ...212² fits)
        assert!(d("13043817825332782212").square().is_small());
        let sq = d("13043817825332782213").square();
        assert!(!sq.is_small());
        assert_eq!(sq.to_string(), "170141183460469231748655437451289177369");
        // rescale overflow (scale gap) promotes
        let r = d("1e20").add(&d("0.0000000000000000001"));
        assert_eq!(r.to_string(), "100000000000000000000.0000000000000000001");
        // i128::MIN negation and comparison across forms
        let min = d(&i128::MIN.to_string());
        assert!(min.is_small());
        assert_eq!(
            min.clone().negated().to_string(),
            "170141183460469231731687303715884105728"
        );
        assert_eq!(d(&max).cmp_value(&s), Ordering::Less);
        assert_eq!(s.cmp_value(&d(&max)), Ordering::Greater);
        assert!(d("1e40").is_integral() && !d("1e40").is_small());
        assert_eq!(
            d("-170141183460469231731687303715884105729").to_string(),
            "-170141183460469231731687303715884105729"
        );
    }

    #[test]
    fn fast_path_matches_bigint_model() {
        use num_bigint::BigInt as B;
        // Pure-BigInt model of a decimal: (mantissa, scale).
        fn model(s: &str) -> (B, u32) {
            let (neg, body) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
            let (i, f) = body.split_once('.').unwrap_or((body, ""));
            let m: B = format!("{i}{f}").parse().unwrap();
            (if neg { -m } else { m }, f.len() as u32)
        }
        fn at(m: &(B, u32), k: u32) -> B {
            &m.0 * pow10(k - m.1)
        }
        fn show(m: &B, k: u32) -> String {
            let neg = m.is_negative();
            let digits = m.magnitude().to_str_radix(10);
            let body = if k == 0 {
                digits
            } else {
                let p = format!("{digits:0>w$}", w = k as usize + 1);
                let (a, b) = p.split_at(p.len() - k as usize);
                format!("{a}.{b}")
            };
            if neg {
                format!("-{body}")
            } else {
                body
            }
        }
        // Deterministic xorshift PRNG.
        let mut st: u64 = 0x9E3779B97F4A7C15;
        let mut rnd = move |n: u64| {
            st ^= st << 13;
            st ^= st >> 7;
            st ^= st << 17;
            st % n
        };
        let boundary = [
            i128::MAX.to_string(),
            (i128::MAX - 1).to_string(),
            i128::MIN.to_string(),
            "170141183460469231731687303715884105728".to_string(),
            "13043817825332782212".to_string(),
            "13043817825332782213".to_string(),
            "99999999999999999999999999999999999999".to_string(),
        ];
        let mut gen = |rnd: &mut dyn FnMut(u64) -> u64| -> String {
            if rnd(10) == 0 {
                let b = &boundary[rnd(boundary.len() as u64) as usize];
                let sc = rnd(3) as usize;
                let s = if sc > 0 && b.len() > sc + 1 && !b.starts_with('-') {
                    format!("{}.{}", &b[..b.len() - sc], &b[b.len() - sc..])
                } else {
                    b.clone()
                };
                return if rnd(2) == 0 && !s.starts_with('-') {
                    format!("-{s}")
                } else {
                    s
                };
            }
            let ilen = 1 + rnd(30) as usize;
            let flen = rnd(31) as usize;
            let digit = |rnd: &mut dyn FnMut(u64) -> u64| char::from(b'0' + rnd(10) as u8);
            let mut i: String = (0..ilen).map(|_| digit(rnd)).collect();
            i = i.trim_start_matches('0').to_string();
            if i.is_empty() {
                i = "0".into();
            }
            let f: String = (0..flen).map(|_| digit(rnd)).collect();
            let s = if flen > 0 { format!("{i}.{f}") } else { i };
            if rnd(2) == 0 {
                format!("-{s}")
            } else {
                s
            }
        };
        for case in 0..50_000 {
            let (sa, sb) = (gen(&mut rnd), gen(&mut rnd));
            let (a, b) = (d(&sa), d(&sb));
            let (ma, mb) = (model(&sa), model(&sb));
            // parse / display round-trip
            assert_eq!(a.to_string(), show(&ma.0, ma.1), "parse {sa}");
            // add
            let k = ma.1.max(mb.1);
            let sum = a.add(&b);
            assert_eq!(
                sum.to_string(),
                show(&(at(&ma, k) + at(&mb, k)), k),
                "#{case} {sa} + {sb}"
            );
            // in-place accumulation of several values (exercises Big += Small etc.)
            let mut acc = a.clone();
            acc.add_assign(&b);
            acc.add_assign(&a);
            assert_eq!(
                acc.to_string(),
                show(&(at(&ma, k) * 2u8 + at(&mb, k)), k),
                "#{case} acc {sa} {sb}"
            );
            // mul / square
            assert_eq!(
                a.mul(&b).to_string(),
                show(&(&ma.0 * &mb.0), ma.1 + mb.1),
                "#{case} {sa} * {sb}"
            );
            assert_eq!(
                a.square().to_string(),
                show(&(&ma.0 * &ma.0), 2 * ma.1),
                "#{case} {sa}^2"
            );
            // compare (scale-insensitive) and min/max tie rule
            assert_eq!(
                a.cmp_value(&b),
                at(&ma, k).cmp(&at(&mb, k)),
                "#{case} cmp {sa} {sb}"
            );
            // rounding to 2 decimals
            let want_r = if ma.1 <= 2 {
                show(&(&ma.0 * pow10(2 - ma.1)), 2)
            } else {
                show(&round_half_away(&ma.0, &pow10(ma.1 - 2)), 2)
            };
            assert_eq!(a.round_to(2).to_string(), want_r, "#{case} round {sa}");
            // integrality and negation
            assert_eq!(
                a.is_integral(),
                (&ma.0 % pow10(ma.1)).is_zero(),
                "#{case} integral {sa}"
            );
            assert_eq!(
                a.clone().negated().to_string(),
                show(&-ma.0.clone(), ma.1),
                "#{case} neg {sa}"
            );
            // canonical form: equal values at equal scale compare equal structurally
            assert_eq!(sum.clone().add(&d("0")), sum, "#{case} canonical");
        }
    }
}
