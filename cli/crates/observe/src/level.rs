//! The lowest level written, from `TETRAL_LOG_LEVEL`.

use std::ffi::OsStr;

/// The environment variable that sets the lowest level written.
pub const LEVEL_VARIABLE: &str = "TETRAL_LOG_LEVEL";

/// The lowest level that reaches both sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

/// `TETRAL_LOG_LEVEL` holds a value other than `debug`, `info`, `warn` and
/// `error`, the empty value included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("TETRAL_LOG_LEVEL must be debug, info, warn or error")]
pub struct InvalidLevel;

impl Level {
    /// Reads `TETRAL_LOG_LEVEL`; unset gives `Info`.
    pub fn from_env() -> Result<Level, InvalidLevel> {
        Level::parse(std::env::var_os(LEVEL_VARIABLE).as_deref())
    }

    /// Parses the variable's value, `None` when it is unset.
    pub fn parse(value: Option<&OsStr>) -> Result<Level, InvalidLevel> {
        let Some(value) = value else {
            return Ok(Level::Info);
        };
        match value.to_str() {
            Some("debug") => Ok(Level::Debug),
            Some("info") => Ok(Level::Info),
            Some("warn") => Ok(Level::Warn),
            Some("error") => Ok(Level::Error),
            _ => Err(InvalidLevel),
        }
    }

    pub(crate) fn allows(self, level: &tracing::Level) -> bool {
        let rank = match *level {
            tracing::Level::TRACE | tracing::Level::DEBUG => Level::Debug,
            tracing::Level::INFO => Level::Info,
            tracing::Level::WARN => Level::Warn,
            tracing::Level::ERROR => Level::Error,
        };
        rank >= self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_is_info_and_only_the_four_names_are_accepted() {
        assert_eq!(Level::parse(None), Ok(Level::Info));
        for (value, level) in [
            ("debug", Level::Debug),
            ("info", Level::Info),
            ("warn", Level::Warn),
            ("error", Level::Error),
        ] {
            assert_eq!(Level::parse(Some(OsStr::new(value))), Ok(level));
        }
        for refused in ["verbose", "", "INFO", " info"] {
            assert_eq!(
                Level::parse(Some(OsStr::new(refused))),
                Err(InvalidLevel),
                "{refused:?}"
            );
        }
    }
}
