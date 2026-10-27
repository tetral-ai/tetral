//! `tetral configure --from-stdin`: reads one bootstrap from stdin, creates a
//! missing root and `run/`, and then, holding the state-root lock, removes
//! the leftovers of interrupted replacements and replaces `credentials.json`
//! and then `config.toml`.
//!
//! Each file is replaced atomically, `credentials.json` first, so a command
//! killed or failing at any point leaves the old pair, the new pair, or the
//! new credential beside the old configuration, which
//! `appserver::configuration` refuses when the two name different
//! `server_url` or `instance_id`. A failure under the lock also leaves the
//! root, `run/` and the lock file, which is never unlinked, and does not
//! restore the leftovers it removed.

use std::io::Read;

use appserver::lock::{self, LockError};
use protocol::bootstrap::{BOOTSTRAP_MAX_BYTES, Bootstrap};
use serde_json::{Value, json};
use state::config::{self, Config};
use state::credentials;
use state::files;
use state::paths::{self, StateRoot};

use crate::commands::{app_mismatch, config_failed, io_failed};
use crate::output::{Code, CommandError};

pub fn run(root: &StateRoot, app: &str, stdin: impl Read) -> Result<Value, CommandError> {
    let mut input = Vec::new();
    stdin
        .take(BOOTSTRAP_MAX_BYTES as u64 + 1)
        .read_to_end(&mut input)
        .map_err(|_| CommandError::new(Code::InvalidBootstrap, "stdin could not be read"))?;
    let bootstrap = Bootstrap::decode(&input)
        .map_err(|error| CommandError::new(Code::InvalidBootstrap, error.to_string()))?;

    files::create_private_dirs(&root.join(paths::RUN)).map_err(io_failed)?;
    let _lock = lock::acquire(root).map_err(|error| match error {
        LockError::Held => CommandError::new(
            Code::ServerRunning,
            "an App Server or another command owns the state root",
        ),
        LockError::Io(failure) => io_failed(failure),
    })?;
    files::remove_interrupted_replacements(root.path()).map_err(io_failed)?;
    commit(root, app, &bootstrap, &mut |barrier| {
        state::seams::barrier_blocking(barrier);
        true
    })?;
    Ok(json!({}))
}

/// The steps under the lock. `reached` is called at each of the barriers
/// `configure.locked`, `configure.credentials_replaced` and
/// `configure.config_replaced`; a `false` answer stops the commit there, as
/// if the process were killed.
fn commit(
    root: &StateRoot,
    app: &str,
    bootstrap: &Bootstrap,
    reached: &mut dyn FnMut(&'static str) -> bool,
) -> Result<(), CommandError> {
    if !reached("configure.locked") {
        return Ok(());
    }
    let existing = config::read(root).map_err(config_failed)?;
    if existing
        .as_ref()
        .is_some_and(|existing| existing.app != app)
    {
        return Err(app_mismatch());
    }
    let config = Config {
        app: app.to_owned(),
        server_url: bootstrap.server_url.as_str().to_owned(),
        instance_id: bootstrap.instance_id.as_str().to_owned(),
        drives_enabled: existing
            .as_ref()
            .is_some_and(|existing| existing.drives_enabled),
        computer_use: existing.and_then(|existing| existing.computer_use),
    };
    let encoded = config
        .encode()
        .map_err(|error| CommandError::new(Code::InvalidBootstrap, error.to_string()))?;
    credentials::replace(root, &bootstrap.encode()).map_err(io_failed)?;
    if !reached("configure.credentials_replaced") {
        return Ok(());
    }
    encoded.replace(root).map_err(io_failed)?;
    reached("configure.config_replaced");
    Ok(())
}

#[cfg(test)]
mod tests {
    use appserver::configuration::{self, ConfigurationError};
    use serde_json::json;

    use super::*;

    fn bootstrap(
        server_url: &str,
        instance: &str,
        credential: &str,
        key: &str,
        expires: &str,
    ) -> Bootstrap {
        let object = json!({
            "server_url": server_url,
            "instance_id": instance,
            "credential_id": credential,
            "connection_key": key,
            "expires_at": expires,
        });
        Bootstrap::decode(&serde_json::to_vec(&object).unwrap()).unwrap()
    }

    #[derive(Debug, PartialEq)]
    enum Read {
        Old,
        New,
        Mismatch,
    }

    #[test]
    fn configuration_capacity_is_checked_before_replacing_credentials() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot::new(directory.path().join("root").as_os_str()).unwrap();
        files::create_private_dirs(&root.join(paths::RUN)).unwrap();
        let _held = lock::acquire(&root).unwrap();
        let old = bootstrap(
            "https://example.com",
            "old",
            "old",
            "tetral_ck_old",
            "2027-01-01T00:00:00Z",
        );
        commit(&root, "app", &old, &mut |_| true).unwrap();
        let before_config = std::fs::read(root.join(paths::CONFIG)).unwrap();
        let before_credentials = credentials::read(&root).unwrap();
        let oversized = bootstrap(
            "https://example.com",
            &"\u{7f}".repeat(12_000),
            "new",
            "tetral_ck_new",
            "2027-01-01T00:00:00Z",
        );
        let error = commit(&root, "app", &oversized, &mut |_| true).unwrap_err();
        assert_eq!(error.code, Code::InvalidBootstrap);
        assert!(!error.message.contains("tetral_ck_"));
        assert_eq!(
            std::fs::read(root.join(paths::CONFIG)).unwrap(),
            before_config
        );
        assert_eq!(credentials::read(&root).unwrap(), before_credentials);
        assert_eq!(configuration::read(&root).unwrap().credentials, old);
    }

    #[test]
    fn configuration_commit_rechecks_the_app_from_the_locked_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot::new(directory.path().join("root").as_os_str()).unwrap();
        files::create_private_dirs(&root.join(paths::RUN)).unwrap();
        let _held = lock::acquire(&root).unwrap();
        let old = bootstrap(
            "https://example.com",
            "old",
            "old",
            "tetral_ck_old",
            "2027-01-01T00:00:00Z",
        );
        commit(&root, "app", &old, &mut |_| true).unwrap();
        let credentials_before = credentials::read(&root).unwrap();
        let mut config_before = Vec::new();
        let error = commit(&root, "app", &old, &mut |stage| {
            assert_eq!(stage, "configure.locked");
            let mut current = config::read(&root).unwrap().unwrap();
            current.app = "other-app".into();
            current.encode().unwrap().replace(&root).unwrap();
            config_before = std::fs::read(root.join(paths::CONFIG)).unwrap();
            true
        })
        .unwrap_err();
        assert_eq!(error.code, Code::AppMismatch);
        assert_eq!(
            std::fs::read(root.join(paths::CONFIG)).unwrap(),
            config_before
        );
        assert_eq!(credentials::read(&root).unwrap(), credentials_before);
    }

    #[test]
    fn a_commit_stopped_at_each_barrier_leaves_the_old_pair_the_new_pair_or_a_mismatch() {
        let old = bootstrap(
            "https://old.example.com",
            "inst_old",
            "ccred_old",
            "tetral_ck_old",
            "2026-11-01T00:00:00Z",
        );
        let new = bootstrap(
            "http://127.0.0.1:8080",
            "inst_new",
            "ccred_new",
            "tetral_ck_new",
            "2027-01-01T00:00:00Z",
        );
        for (stop_at, expected) in [
            ("configure.locked", Read::Old),
            ("configure.credentials_replaced", Read::Mismatch),
            ("configure.config_replaced", Read::New),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let root = StateRoot::new(directory.path().join("root").as_os_str()).unwrap();
            files::create_private_dirs(&root.join(paths::RUN)).unwrap();
            let held = lock::acquire(&root).unwrap();
            commit(&root, "app", &old, &mut |_| true).unwrap();
            commit(&root, "app", &new, &mut |barrier| barrier != stop_at).unwrap();
            drop(held);

            let read = match configuration::read(&root) {
                Ok(read) if read.credentials == old && read.config.instance_id == "inst_old" => {
                    Read::Old
                }
                Ok(read)
                    if read.credentials == new
                        && read.config.instance_id == "inst_new"
                        && read.config.server_url == "http://127.0.0.1:8080" =>
                {
                    Read::New
                }
                Err(ConfigurationError::Mismatch { .. })
                    if credentials::read(&root).unwrap() == Some(new.encode()) =>
                {
                    Read::Mismatch
                }
                other => panic!("stopped at {stop_at}: {other:?}"),
            };
            assert_eq!(read, expected, "stopped at {stop_at}");
        }
    }

    #[test]
    fn a_pair_that_differs_in_server_url_or_instance_id_alone_is_a_mismatch() {
        let old = bootstrap(
            "https://old.example.com",
            "inst_old",
            "ccred_old",
            "tetral_ck_old",
            "2026-11-01T00:00:00Z",
        );
        for (member, new) in [
            (
                "server_url",
                bootstrap(
                    "https://new.example.com",
                    "inst_old",
                    "ccred_old",
                    "tetral_ck_old",
                    "2026-11-01T00:00:00Z",
                ),
            ),
            (
                "instance_id",
                bootstrap(
                    "https://old.example.com",
                    "inst_new",
                    "ccred_old",
                    "tetral_ck_old",
                    "2026-11-01T00:00:00Z",
                ),
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let root = StateRoot::new(directory.path().join("root").as_os_str()).unwrap();
            files::create_private_dirs(&root.join(paths::RUN)).unwrap();
            let held = lock::acquire(&root).unwrap();
            commit(&root, "app", &old, &mut |_| true).unwrap();
            commit(&root, "app", &new, &mut |barrier| {
                barrier != "configure.credentials_replaced"
            })
            .unwrap();
            drop(held);
            match configuration::read(&root) {
                Err(ConfigurationError::Mismatch { member: found }) => {
                    assert_eq!(found, member)
                }
                other => panic!("{member} differs: {other:?}"),
            }
        }
    }
}
