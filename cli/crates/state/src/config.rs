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

pub fn replace(root: &StateRoot, config: &Config) -> Result<(), IoFailure> {
    let text = toml::to_string(config).expect("a configuration always serializes");
    files::replace(&root.join(paths::CONFIG), text.as_bytes())
}
