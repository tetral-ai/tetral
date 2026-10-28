//! RFC 3339 times in UTC, the only time form on the wire.

use std::time::{Duration, SystemTime};

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

/// The system time that `time` names; a leap second reads as the first
/// second of the next minute, and digits of the fraction past nanoseconds
/// are dropped.
pub fn system_time(time: &Rfc3339Utc) -> SystemTime {
    let text = time.as_str();
    let number = |range: std::ops::Range<usize>| -> i64 {
        text[range]
            .parse()
            .expect("an accepted time has its digits there")
    };
    let days = days_from_civil(number(0..4), number(5..7), number(8..10));
    let seconds = days * 86_400 + number(11..13) * 3_600 + number(14..16) * 60 + number(17..19);
    let fraction = text[19..text.len() - 1].trim_start_matches('.');
    let nanos = fraction
        .bytes()
        .chain(std::iter::repeat(b'0'))
        .take(9)
        .fold(0, |value, digit| value * 10 + u32::from(digit - b'0'));
    let since_epoch = Duration::new(seconds.unsigned_abs(), 0);
    let whole = if seconds >= 0 {
        SystemTime::UNIX_EPOCH + since_epoch
    } else {
        SystemTime::UNIX_EPOCH - since_epoch
    };
    whole + Duration::from_nanos(u64::from(nanos))
}

/// The days from 1970-01-01 to a date of the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
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

    #[test]
    fn a_time_reads_as_its_system_time() {
        let at = |text: &str| system_time(&Rfc3339Utc::new(text).unwrap());
        let since_epoch =
            |seconds: u64, nanos: u32| SystemTime::UNIX_EPOCH + Duration::new(seconds, nanos);
        assert_eq!(at("1970-01-01T00:00:00Z"), SystemTime::UNIX_EPOCH);
        assert_eq!(at("2000-03-01T00:00:00Z"), since_epoch(951_868_800, 0));
        assert_eq!(
            at("2028-02-29T12:30:15.25Z"),
            since_epoch(1_835_440_215, 250_000_000)
        );
        assert_eq!(at("2026-12-31T23:59:60Z"), at("2027-01-01T00:00:00Z"));
        assert_eq!(
            at("2026-01-01T00:00:00.1234567891Z"),
            since_epoch(1_767_225_600, 123_456_789)
        );
        assert_eq!(
            at("1969-12-31T23:59:59.5Z"),
            SystemTime::UNIX_EPOCH - Duration::from_millis(500)
        );
    }
}
