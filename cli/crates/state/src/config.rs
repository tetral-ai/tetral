//! `config.toml`: the app the root belongs to, Engine's URL, the instance, and
//! the local options.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};

use crate::files::{self, IoFailure};
use crate::paths::{self, StateRoot};

/// The largest `config.toml` read; a larger one does not follow the format.
pub const CONFIG_MAX_BYTES: usize = 65_536;

/// The configuration. A member missing or of another type and a `driver` that
/// is not an absolute path break the format. An unknown member is ignored, so
/// that a root which a newer `tetral` wrote stays readable and replaceable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub app: String,
    pub server_url: String,
    pub instance_id: String,
    pub drives_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub computer_use: Option<ComputerUse>,
}

/// The `[computer_use]` table, present once it was first written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputerUse {
    pub enabled: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "absolute"
    )]
    pub driver: Option<PathBuf>,
}

fn absolute<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<PathBuf>, D::Error> {
    let path = PathBuf::from(String::deserialize(deserializer)?);
    if !path.is_absolute() {
        return Err(serde::de::Error::custom("driver is not an absolute path"));
    }
    Ok(Some(path))
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Io(#[from] IoFailure),
    #[error("{} does not follow the configuration format", path.display())]
    Format { path: PathBuf },
}

/// Reads the root's `config.toml`; `None` when it does not exist.
pub fn read(root: &StateRoot) -> Result<Option<Config>, ConfigError> {
    read_with_limit(root, CONFIG_MAX_BYTES)
}

/// `read` with another size limit, for tests.
pub fn read_with_limit(root: &StateRoot, max_bytes: usize) -> Result<Option<Config>, ConfigError> {
    let path = root.join(paths::CONFIG);
    let Some(bytes) = files::read_bounded(&path, max_bytes)? else {
        return Ok(None);
    };
    parse(&path, &bytes, max_bytes).map(Some)
}

fn parse(path: &Path, bytes: &[u8], max_bytes: usize) -> Result<Config, ConfigError> {
    let format = || ConfigError::Format {
        path: path.to_path_buf(),
    };
    if bytes.len() > max_bytes {
        return Err(format());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| format())?;
    toml::from_str(text).map_err(|_| format())
}

/// A complete configuration encoded and checked before any credential is replaced.
pub struct EncodedConfig(String);

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error(
        "the bootstrap and existing local settings produce a configuration larger than 65,536 bytes"
    )]
    TooLarge,
    #[error("the configuration cannot be encoded as TOML")]
    Format,
}

impl Config {
    pub fn encode(&self) -> Result<EncodedConfig, EncodeError> {
        let text = toml::to_string(self).map_err(|_| EncodeError::Format)?;
        if text.len() > CONFIG_MAX_BYTES {
            return Err(EncodeError::TooLarge);
        }
        Ok(EncodedConfig(text))
    }
}

impl EncodedConfig {
    /// Writes exactly the bytes checked by `Config::encode`.
    pub fn replace(&self, root: &StateRoot) -> Result<(), IoFailure> {
        files::replace(&root.join(paths::CONFIG), self.0.as_bytes())
    }
}

/// Whether the app field alone can fit in a readable configuration. TOML
/// escaping counts toward the byte limit; no separate character limit applies.
pub fn app_fits(app: &str) -> bool {
    toml::to_string(&std::collections::BTreeMap::from([("app", app)]))
        .is_ok_and(|text| text.len() <= CONFIG_MAX_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_configuration_and_app_limits_count_toml_escaping() {
        assert!(app_fits("app"));
        assert!(!app_fits(&"\u{7f}".repeat(12_000)));
        let mut config = Config {
            app: "app".into(),
            server_url: "https://example.com".into(),
            instance_id: "i".into(),
            drives_enabled: true,
            computer_use: None,
        };
        let encoded = config.encode().unwrap();
        assert_eq!(
            parse(
                Path::new("config.toml"),
                encoded.0.as_bytes(),
                CONFIG_MAX_BYTES
            )
            .unwrap(),
            config
        );
        let room = CONFIG_MAX_BYTES - encoded.0.len();
        config.instance_id.push_str(&"a".repeat(room));
        let encoded = config.encode().unwrap();
        assert_eq!(encoded.0.len(), 65_536);
        assert!(
            parse(
                Path::new("config.toml"),
                encoded.0.as_bytes(),
                CONFIG_MAX_BYTES
            )
            .is_ok()
        );
        config.instance_id.push('a');
        assert!(matches!(config.encode(), Err(EncodeError::TooLarge)));
    }
}
