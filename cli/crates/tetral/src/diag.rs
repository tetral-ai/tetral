//! `tetral diag --output <path>`: a `tar.gz` of the root's log files, its
//! `config.toml`, the system's facts and the App Server's status and metrics,
//! written beside an App Server or with none, without the root's lock.
//! `credentials.json` is read only for the connection key that every entry
//! is searched for, with `tetral_ck_`, before it is added; a match fails the
//! command and leaves no file. The archive is written to a temporary file
//! with mode `0600` in the output's directory and renamed to `<path>`.

use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use state::paths::{self, StateRoot};

use crate::output::{Code, CommandError};

/// What every connection key starts with.
const KEY_PREFIX: &[u8] = b"tetral_ck_";

/// The log files rotation keeps, in the bundle's `logs/`.
const LOG_FILES: [&str; 3] = ["tetral.log", "tetral.log.1", "tetral.log.2"];

pub fn run(root: &StateRoot, output: &Path) -> Result<Value, CommandError> {
    let key = connection_key(root)?;
    let mut entries = Vec::new();
    for name in LOG_FILES {
        let path = root.join(paths::LOGS).join(name);
        if let Some(bytes) = read_if_present(&path)? {
            entries.push((format!("logs/{name}"), bytes));
        }
    }
    if let Some(bytes) = read_if_present(&root.join(paths::CONFIG))? {
        entries.push((paths::CONFIG.to_owned(), bytes));
    }
    entries.push(("system.json".to_owned(), json_bytes(&system())));
    let (status, metrics) = server_answers(root);
    entries.push(("status.json".to_owned(), json_bytes(&status)));
    if let Some(metrics) = metrics {
        entries.push(("metrics.txt".to_owned(), metrics.into_bytes()));
    }
    for (name, bytes) in &entries {
        let holds = |needle: &[u8]| bytes.windows(needle.len()).any(|window| window == needle);
        if holds(KEY_PREFIX) || key.as_deref().is_some_and(holds) {
            return Err(CommandError::new(
                Code::IoFailed,
                format!("the bundle entry {name} holds a connection key"),
            ));
        }
    }
    write_archive(output, &entries)?;
    Ok(json!({}))
}

/// The connection key of `credentials.json`, when it reads as a bootstrap.
fn connection_key(root: &StateRoot) -> Result<Option<Vec<u8>>, CommandError> {
    let bytes = state::credentials::read(root).map_err(crate::commands::io_failed)?;
    Ok(bytes
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| value["connection_key"].as_str().map(str::to_owned))
        .filter(|key| !key.is_empty())
        .map(String::into_bytes))
}

fn read_if_present(path: &Path) -> Result<Option<Vec<u8>>, CommandError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(failed(path, &error)),
    }
}

fn system() -> Value {
    json!({
        "cli_version": env!("CARGO_PKG_VERSION"),
        "os": std::env::consts::OS,
        "os_version": os_version(),
        "arch": std::env::consts::ARCH,
    })
}

#[cfg(target_os = "linux")]
fn os_version() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|release| release.trim().to_owned())
        .unwrap_or_default()
}

#[cfg(target_os = "macos")]
fn os_version() -> String {
    std::process::Command::new("/usr/bin/sw_vers")
        .arg("-productVersion")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_owned())
        .unwrap_or_default()
}

/// The App Server's status and metrics when it answers both within five
/// seconds; otherwise a status of `not_running` and no metrics.
fn server_answers(root: &StateRoot) -> (Value, Option<String>) {
    let answered = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()
        .and_then(|runtime| {
            let client = sdk::Client::new(root).ok()?;
            runtime.block_on(client.status_and_metrics()).ok()
        });
    match answered {
        Some((status, metrics)) => (
            serde_json::to_value(status).expect("a status always serializes"),
            Some(metrics),
        ),
        None => (json!({ "server_state": "not_running" }), None),
    }
}

fn json_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec_pretty(value).expect("a JSON value always serializes")
}

/// Writes `entries` as a `tar.gz` to a temporary file with mode `0600` in
/// `output`'s directory, then renames it to `output`; a failure leaves no
/// file.
fn write_archive(output: &Path, entries: &[(String, Vec<u8>)]) -> Result<(), CommandError> {
    let directory = output.parent().unwrap_or(Path::new("/"));
    let name = output
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let temporary = directory.join(format!(".{name}.{}.tmp", std::process::id()));
    let written = write_to(&temporary, entries)
        .and_then(|()| std::fs::rename(&temporary, output).map_err(|error| failed(output, &error)));
    if written.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    written
}

fn write_to(path: &Path, entries: &[(String, Vec<u8>)]) -> Result<(), CommandError> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| failed(path, &error))?;
    let mut archive = tar::Builder::new(GzEncoder::new(file, Compression::default()));
    let mut logs = tar::Header::new_gnu();
    logs.set_entry_type(tar::EntryType::Directory);
    logs.set_mode(0o700);
    logs.set_size(0);
    logs.set_cksum();
    let appended = archive
        .append_data(&mut logs, "logs/", io::empty())
        .and_then(|()| {
            for (name, bytes) in entries {
                let mut header = tar::Header::new_gnu();
                header.set_mode(0o600);
                header.set_size(bytes.len() as u64);
                header.set_cksum();
                archive.append_data(&mut header, name, bytes.as_slice())?;
            }
            archive.into_inner()?.finish()?.sync_all()
        });
    appended.map_err(|error| failed(path, &error))
}

fn failed(path: &Path, error: &io::Error) -> CommandError {
    CommandError::new(Code::IoFailed, format!("{}: {error}", path.display()))
}
