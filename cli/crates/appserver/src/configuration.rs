//! The configuration an App Server starts from, read under the state-root
//! lock: `config.toml` and `credentials.json` both exist and parse, every
//! value follows the bootstrap's rules, and both files name the same
//! `server_url` and `instance_id`. A pair written half by one `configure` and
//! half by another fails when the two name different ones; with the same two,
//! the halves agree, since `config.toml` holds no other bootstrap value.

use std::path::PathBuf;

use protocol::bootstrap::{Bootstrap, BootstrapError, ServerUrl};
use protocol::id::Id;
use state::atomic::IoFailure;
use state::config::{self, Config, ConfigError};
use state::credentials;
use state::paths::{self, StateRoot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Configuration {
    pub config: Config,
    pub credentials: Bootstrap,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error(transparent)]
    Io(#[from] IoFailure),
    #[error("{} is missing", path.display())]
    Missing { path: PathBuf },
    #[error(transparent)]
    Config(ConfigError),
    #[error("{}: {error}", path.display())]
    Credentials {
        path: PathBuf,
        error: BootstrapError,
    },
    #[error("{}: {member} breaks the bootstrap's rule", path.display())]
    Rule { path: PathBuf, member: &'static str },
    #[error("config.toml and credentials.json name different values of {member}")]
    Mismatch { member: &'static str },
}

impl From<ConfigError> for ConfigurationError {
    fn from(error: ConfigError) -> ConfigurationError {
        match error {
            ConfigError::Io(failure) => ConfigurationError::Io(failure),
            format => ConfigurationError::Config(format),
        }
    }
}

pub fn read(root: &StateRoot) -> Result<Configuration, ConfigurationError> {
    let config_path = root.join(paths::CONFIG);
    let credentials_path = root.join(paths::CREDENTIALS);
    let config = config::read(root)?.ok_or(ConfigurationError::Missing {
        path: config_path.clone(),
    })?;
    let bytes = credentials::read(root)?.ok_or(ConfigurationError::Missing {
        path: credentials_path.clone(),
    })?;
    let credentials =
        Bootstrap::decode(&bytes).map_err(|error| ConfigurationError::Credentials {
            path: credentials_path,
            error,
        })?;
    let rule = |member| ConfigurationError::Rule {
        path: config_path.clone(),
        member,
    };
    ServerUrl::new(config.server_url.as_str()).map_err(|_| rule("server_url"))?;
    Id::new(config.instance_id.as_str()).map_err(|_| rule("instance_id"))?;
    if config.server_url != credentials.server_url.as_str() {
        return Err(ConfigurationError::Mismatch {
            member: "server_url",
        });
    }
    if config.instance_id != credentials.instance_id.as_str() {
        return Err(ConfigurationError::Mismatch {
            member: "instance_id",
        });
    }
    Ok(Configuration {
        config,
        credentials,
    })
}
