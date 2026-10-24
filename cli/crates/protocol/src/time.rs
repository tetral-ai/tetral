//! RFC 3339 times in UTC, the only time form on the wire.

use crate::field::{Text, TextRule};

/// `YYYY-MM-DDTHH:MM:SS[.fraction]Z` naming a time that exists: month 1 to
/// 12, a day of that month, hour at most 23, minute at most 59 and second at
/// most 60, for a leap second.
pub type Rfc3339Utc = Text<Utc>;

pub enum Utc {}

impl TextRule for Utc {
    const NAME: &'static str = "RFC 3339 UTC time";
    fn accepts(text: &str) -> bool {
        let bytes = text.as_bytes();
        let digits = |range: std::ops::Range<usize>| -> Option<u32> {
            let part = bytes.get(range)?;
            part.iter().all(u8::is_ascii_digit).then(|| {
                part.iter()
                    .fold(0, |value, digit| value * 10 + u32::from(digit - b'0'))
            })
        };
        let separators = [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')];
        if !separators
            .iter()
            .all(|&(at, byte)| bytes.get(at) == Some(&byte))
        {
            return false;
        }
        let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
            digits(0..4),
            digits(5..7),
            digits(8..10),
            digits(11..13),
            digits(14..16),
            digits(17..19),
        ) else {
            return false;
        };
        let rest = &bytes[19..];
        let fraction_ok = match rest {
            [b'Z'] => true,
            [b'.', fraction @ .., b'Z'] => {
                !fraction.is_empty() && fraction.iter().all(u8::is_ascii_digit)
            }
            _ => false,
        };
        fraction_ok
            && (1..=12).contains(&month)
            && (1..=days_in_month(year, month)).contains(&day)
            && hour <= 23
            && minute <= 59
            && second <= 60
    }
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_times_that_exist_are_accepted() {
        for accepted in [
            "2026-02-28T23:59:60Z",
            "2028-02-29T00:00:00Z",
            "2000-02-29T00:00:00.5Z",
            "2026-12-31T23:59:59.123456Z",
        ] {
            assert!(Utc::accepts(accepted), "{accepted}");
        }
        for refused in [
            "2026-02-29T00:00:00Z",
            "1900-02-29T00:00:00Z",
            "2026-02-30T00:00:00Z",
            "2026-04-31T00:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-00-01T00:00:00Z",
            "2026-01-00T00:00:00Z",
            "2026-01-01T24:00:00Z",
            "2026-01-01T00:60:00Z",
            "2026-01-01T00:00:61Z",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00.Z",
            "2026-01-01 00:00:00Z",
        ] {
            assert!(!Utc::accepts(refused), "{refused}");
        }
    }
}
