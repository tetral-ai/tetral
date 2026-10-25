//! `outbox/`, where the Report outbox freezes each report before its first
//! send: `outbox/<n>/<message_id>.json`, `<n>` the App Server's number for
//! the report's connection.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::files::{self, IoFailure};

/// Deletes `outbox` and creates it again empty, as an App Server does at its
/// start, since no report outlives its connection.
pub fn reset(outbox: &Path) -> Result<(), IoFailure> {
    remove_tree(outbox)?;
    files::create_private_dirs(outbox)
}

/// Freezes report `message_id` of connection `connection`: written through a
/// temporary file, so a reader sees the whole report or none.
pub fn write(
    outbox: &Path,
    connection: u64,
    message_id: &str,
    bytes: &[u8],
) -> Result<(), IoFailure> {
    let path = report_path(outbox, connection, message_id)?;
    files::create_private_dirs(&connection_dir(outbox, connection))?;
    files::replace(&path, bytes)
}

/// The frozen report, at most `limit + 1` bytes of it.
pub fn read(
    outbox: &Path,
    connection: u64,
    message_id: &str,
    limit: usize,
) -> Result<Option<Vec<u8>>, IoFailure> {
    files::read_bounded(&report_path(outbox, connection, message_id)?, limit)
}

/// Deletes one report; a missing one is no failure.
pub fn remove(outbox: &Path, connection: u64, message_id: &str) -> Result<(), IoFailure> {
    let path = report_path(outbox, connection, message_id)?;
    match fs::remove_file(&path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(IoFailure::new(&path, error)),
        _ => Ok(()),
    }
}

/// Deletes the directory of connection `connection` with every report in it.
pub fn remove_connection(outbox: &Path, connection: u64) -> Result<(), IoFailure> {
    remove_tree(&connection_dir(outbox, connection))
}

fn connection_dir(outbox: &Path, connection: u64) -> PathBuf {
    outbox.join(connection.to_string())
}

/// A report's file, for a `message_id` that is one plain file name.
fn report_path(outbox: &Path, connection: u64, message_id: &str) -> Result<PathBuf, IoFailure> {
    let plain = !message_id.is_empty()
        && message_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    let path = connection_dir(outbox, connection).join(format!("{message_id}.json"));
    if plain {
        Ok(path)
    } else {
        Err(IoFailure::new(
            &path,
            io::Error::from(io::ErrorKind::InvalidInput),
        ))
    }
}

/// `std::fs::remove_dir_all` follows no symbolic link inside the tree.
fn remove_tree(path: &Path) -> Result<(), IoFailure> {
    match fs::remove_dir_all(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(IoFailure::new(path, error)),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn mode(path: &Path) -> u32 {
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn reports_are_frozen_per_connection_and_the_outbox_starts_empty() {
        let directory = tempfile::tempdir().unwrap();
        let outbox = directory.path().join("outbox");
        write(&outbox, 3, "r1", b"{\"a\":1}").unwrap();
        assert_eq!(mode(&outbox.join("3")), 0o700);
        assert_eq!(mode(&outbox.join("3/r1.json")), 0o600);
        assert_eq!(
            read(&outbox, 3, "r1", 1024).unwrap(),
            Some(b"{\"a\":1}".to_vec())
        );
        remove(&outbox, 3, "r1").unwrap();
        remove(&outbox, 3, "r1").unwrap();
        assert_eq!(read(&outbox, 3, "r1", 1024).unwrap(), None);

        write(&outbox, 3, "r2", b"{}").unwrap();
        remove_connection(&outbox, 3).unwrap();
        assert!(!outbox.join("3").exists());
        assert!(write(&outbox, 3, "../r3", b"{}").is_err());

        write(&outbox, 4, "r1", b"{}").unwrap();
        reset(&outbox).unwrap();
        assert_eq!(mode(&outbox), 0o700);
        assert_eq!(fs::read_dir(&outbox).unwrap().count(), 0);
    }
}
