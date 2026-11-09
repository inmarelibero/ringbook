//! Fixed-point prices and quantities, parsed from ASCII without floats.
//!
//! Prices and quantities are stored as integers in units of `10^-decimals`.
//! The scale belongs to the instrument and is not stored in each value.

use core::fmt;

/// The number of decimal places an instrument's integer values represent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scale {
    decimals: u8,
    factor: i64,
}

impl Scale {
    /// The largest scale whose factor still fits in an `i64`.
    pub const MAX_DECIMALS: u8 = 18;

    /// Creates a scale, or returns `None` above [`Scale::MAX_DECIMALS`].
    #[must_use]
    pub const fn new(decimals: u8) -> Option<Self> {
        if decimals > Self::MAX_DECIMALS {
            return None;
        }
        Some(Self {
            decimals,
            factor: 10_i64.pow(decimals as u32),
        })
    }

    /// Returns the number of decimal places.
    #[must_use]
    pub const fn decimals(self) -> u8 {
        self.decimals
    }

    /// Returns the integer value that represents exactly one whole unit.
    #[must_use]
    pub const fn factor(self) -> i64 {
        self.factor
    }

    /// Parses an unsigned decimal such as `106217.5` into scaled units.
    ///
    /// Digits beyond the scale are accepted only if they are zero, so a value
    /// is never silently rounded.
    ///
    /// # Errors
    ///
    /// Returns a [`DecimalError`] if the input is empty, is not a plain
    /// unsigned decimal, needs more precision than the scale has, or
    /// overflows an `i64`.
    pub fn parse(self, input: &[u8]) -> Result<i64, DecimalError> {
        if input.is_empty() {
            return Err(DecimalError::Empty);
        }

        let (whole, fraction) = match input.iter().position(|&byte| byte == b'.') {
            Some(dot) => (&input[..dot], Some(&input[dot + 1..])),
            None => (input, None),
        };
        if whole.is_empty() || fraction.is_some_and(<[u8]>::is_empty) {
            return Err(DecimalError::Malformed);
        }
        let fraction = fraction.unwrap_or(&[]);
        let decimals = usize::from(self.decimals);

        let mut value = 0_i64;
        for &byte in whole {
            value = append_digit(value, digit(byte)?)?;
        }
        for (position, &byte) in fraction.iter().enumerate() {
            let next = digit(byte)?;
            if position < decimals {
                value = append_digit(value, next)?;
            } else if next != 0 {
                return Err(DecimalError::ExcessPrecision);
            }
        }
        for _ in fraction.len().min(decimals)..decimals {
            value = append_digit(value, 0)?;
        }
        Ok(value)
    }

    /// Returns a value that formats `raw` as a decimal, without trailing zeros.
    #[must_use]
    pub const fn display(self, raw: i64) -> ScaledDisplay {
        ScaledDisplay { raw, scale: self }
    }
}

fn digit(byte: u8) -> Result<i64, DecimalError> {
    if byte.is_ascii_digit() {
        Ok(i64::from(byte - b'0'))
    } else {
        Err(DecimalError::Malformed)
    }
}

fn append_digit(value: i64, digit: i64) -> Result<i64, DecimalError> {
    value
        .checked_mul(10)
        .and_then(|shifted| shifted.checked_add(digit))
        .ok_or(DecimalError::Overflow)
}

/// Formats a scaled integer as a decimal. Created by [`Scale::display`].
#[derive(Clone, Copy, Debug)]
pub struct ScaledDisplay {
    raw: i64,
    scale: Scale,
}

impl fmt::Display for ScaledDisplay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.raw < 0 {
            formatter.write_str("-")?;
        }
        let magnitude = self.raw.unsigned_abs();
        let factor = self.scale.factor.unsigned_abs();
        write!(formatter, "{}", magnitude / factor)?;

        let mut fraction = magnitude % factor;
        if fraction == 0 {
            return Ok(());
        }
        let mut width = usize::from(self.scale.decimals);
        while fraction.is_multiple_of(10) {
            fraction /= 10;
            width -= 1;
        }
        write!(formatter, ".{fraction:0width$}")
    }
}

/// Why a decimal could not be parsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecimalError {
    /// The input was empty.
    Empty,
    /// The input was not a plain unsigned decimal.
    Malformed,
    /// The input has non-zero digits beyond the scale.
    ExcessPrecision,
    /// The value does not fit in an `i64`.
    Overflow,
}

impl fmt::Display for DecimalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "decimal is empty",
            Self::Malformed => "decimal is malformed",
            Self::ExcessPrecision => "decimal has more precision than the scale allows",
            Self::Overflow => "decimal overflows a 64-bit integer",
        })
    }
}

impl core::error::Error for DecimalError {}

/// A price in an instrument's scaled units.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(pub i64);

impl Price {
    /// Parses a price using the instrument's scale.
    ///
    /// # Errors
    ///
    /// Returns a [`DecimalError`] under the conditions of [`Scale::parse`].
    pub fn parse(scale: Scale, input: &[u8]) -> Result<Self, DecimalError> {
        scale.parse(input).map(Self)
    }
}

/// A quantity in an instrument's scaled units.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(pub i64);

impl Qty {
    /// A quantity of zero, which removes a level from a book.
    pub const ZERO: Self = Self(0);

    /// Parses a quantity using the instrument's scale.
    ///
    /// # Errors
    ///
    /// Returns a [`DecimalError`] under the conditions of [`Scale::parse`].
    pub fn parse(scale: Scale, input: &[u8]) -> Result<Self, DecimalError> {
        scale.parse(input).map(Self)
    }

    /// Returns whether this quantity is zero.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn scale(decimals: u8) -> Scale {
        Scale::new(decimals).unwrap()
    }

    #[test]
    fn rejects_scales_whose_factor_would_overflow() {
        assert!(Scale::new(18).is_some());
        assert!(Scale::new(19).is_none());
    }

    #[test]
    fn parses_whole_and_fractional_values() {
        assert_eq!(scale(2).parse(b"106217.5"), Ok(10_621_750));
        assert_eq!(scale(3).parse(b"0.001"), Ok(1));
        assert_eq!(scale(0).parse(b"42"), Ok(42));
        assert_eq!(scale(4).parse(b"7"), Ok(70_000));
    }

    #[test]
    fn accepts_zero_digits_beyond_the_scale() {
        assert_eq!(scale(2).parse(b"1.5000"), Ok(150));
        assert_eq!(scale(0).parse(b"3.00"), Ok(3));
    }

    #[test]
    fn never_rounds_non_zero_digits_beyond_the_scale() {
        assert_eq!(scale(2).parse(b"1.001"), Err(DecimalError::ExcessPrecision));
        assert_eq!(scale(0).parse(b"1.5"), Err(DecimalError::ExcessPrecision));
    }

    #[test]
    fn rejects_anything_but_plain_unsigned_decimals() {
        assert_eq!(scale(2).parse(b""), Err(DecimalError::Empty));
        for input in [
            &b"."[..],
            b".5",
            b"5.",
            b"-1",
            b"+1",
            b"1.2.3",
            b"1e5",
            b"1 ",
            b"abc",
        ] {
            assert_eq!(scale(2).parse(input), Err(DecimalError::Malformed));
        }
    }

    #[test]
    fn reports_overflow_at_the_i64_limit() {
        assert_eq!(scale(0).parse(b"9223372036854775807"), Ok(i64::MAX));
        assert_eq!(
            scale(0).parse(b"9223372036854775808"),
            Err(DecimalError::Overflow)
        );
        assert_eq!(scale(18).parse(b"10"), Err(DecimalError::Overflow));
    }

    #[test]
    fn displays_without_trailing_zeros() {
        assert_eq!(scale(2).display(10_621_750).to_string(), "106217.5");
        assert_eq!(scale(3).display(1).to_string(), "0.001");
        assert_eq!(scale(2).display(300).to_string(), "3");
        assert_eq!(scale(2).display(-150).to_string(), "-1.5");
        assert_eq!(scale(0).display(i64::MIN).to_string(), i64::MIN.to_string());
    }

    #[test]
    fn typed_values_use_the_scale() {
        let price = Price::parse(scale(1), b"100.5").unwrap();
        assert_eq!(price, Price(1005));
        assert!(Price(2) > Price(1));
        assert!(Qty::parse(scale(3), b"0").unwrap().is_zero());
    }

    proptest! {
        #[test]
        fn display_and_parse_are_inverses(raw in 0_i64.., decimals in 0_u8..=18) {
            let scale = scale(decimals);
            prop_assert_eq!(scale.parse(scale.display(raw).to_string().as_bytes()), Ok(raw));
        }

        #[test]
        fn arbitrary_bytes_never_panic(input in proptest::collection::vec(any::<u8>(), 0..32)) {
            let _ = scale(8).parse(&input);
        }
    }
}
