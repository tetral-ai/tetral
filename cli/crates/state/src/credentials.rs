//! `credentials/credentials.json`: the bootstrap object that `tetral configure` installed,
//! connection key included. Its format and rules are those of
//! `protocol::bootstrap`; this crate, which depends on no other workspace
//! crate, reads and writes its bytes.

use crate::files::{self, IoFailure};
use crate::paths::{self, StateRoot};
use crate::private::{self, CheckError};
use rustix::fs::{Mode, OFlags};
use std::io;

/// The largest `credentials/credentials.json` read, the bootstrap's own limit; the
/// reader gets one byte more of a larger file, so that the bootstrap's
/// decoder refuses it.
pub const CREDENTIALS_MAX_BYTES: usize = 65_536;

/// Reads the root's `credentials/credentials.json`; `None` when it does not exist.
pub fn read(root: &StateRoot) -> Result<Option<Vec<u8>>, IoFailure> {
    read_with_limit(root, CREDENTIALS_MAX_BYTES)
}

/// `read` with another size limit, for tests.
pub fn read_with_limit(root: &StateRoot, max_bytes: usize) -> Result<Option<Vec<u8>>, IoFailure> {
    files::read_bounded(&root.join(paths::CREDENTIALS), max_bytes)
}

/// Replaces the root's `credentials/credentials.json` atomically, as `configure` and a
/// credential renewal do.
pub fn replace(root: &StateRoot, bytes: &[u8]) -> Result<(), IoFailure> {
    create_directory(root).map_err(|error| match error {
        CheckError::Io(failure) => failure,
        other => IoFailure::new(
            &root.join(paths::CREDENTIAL_DIRECTORY),
            io::Error::new(io::ErrorKind::PermissionDenied, other),
        ),
    })?;
    files::replace_observed(&root.join(paths::CREDENTIALS), bytes, || {
        crate::seams::barrier_blocking("credentials.replacement_written")
    })
}

fn create_directory(root: &StateRoot) -> Result<(), CheckError> {
    private::check_credentials(root)?;
    files::create_private_dirs(&root.join(paths::CREDENTIAL_DIRECTORY))?;
    private::check_credentials(root)
}

/// Prepares credential storage while the caller holds the exclusive root
/// lock, before any connection or restricted child is admitted. Read-only
/// commands never call this. A rename preserves legacy bytes and identity;
/// conflicting sources require operator resolution, never an automatic choice.
pub fn prepare_under_lock(root: &StateRoot) -> Result<(), CheckError> {
    private::check_credentials(root)?;
    let old = root.join(paths::LEGACY_CREDENTIALS);
    let new = root.join(paths::CREDENTIALS);
    if old
        .try_exists()
        .map_err(|error| IoFailure::new(&old, error))?
        && new
            .try_exists()
            .map_err(|error| IoFailure::new(&new, error))?
    {
        return Err(IoFailure::new(&old, io::Error::new(io::ErrorKind::AlreadyExists,
            format!("both legacy and current credentials exist: {} and {}; resolve the conflicting credential files before starting", old.display(), new.display()))).into());
    }
    create_directory(root)?;
    files::remove_interrupted_replacements(root.path())?;
    let directory = root.join(paths::CREDENTIAL_DIRECTORY);
    files::remove_interrupted_replacements(&directory)?;
    if old
        .try_exists()
        .map_err(|error| IoFailure::new(&old, error))?
    {
        std::fs::rename(&old, &new).map_err(|error| IoFailure::new(&old, error))?;
        crate::seams::barrier_blocking("credentials.migrated");
        for path in [&directory, root.path()] {
            rustix::fs::open(
                path,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .and_then(|file| rustix::fs::fsync(&file))
            .map_err(|error| IoFailure::new(path, error))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    fn legacy(root: &StateRoot, bytes: &[u8]) {
        files::replace(&root.join(paths::LEGACY_CREDENTIALS), bytes).unwrap();
    }

    #[test]
    fn migration_preserves_bytes_identity_and_removes_interrupted_secret_replacements() {
        let temporary = tempfile::tempdir().unwrap();
        let root = StateRoot::new(temporary.path().as_os_str()).unwrap();
        legacy(&root, b"original credential bytes");
        let inode = std::fs::metadata(root.join(paths::LEGACY_CREDENTIALS))
            .unwrap()
            .ino();
        std::fs::write(root.join(".replace-old"), b"interrupted secret").unwrap();
        assert_eq!(
            read(&root).unwrap(),
            None,
            "readers never migrate or use legacy credentials"
        );
        assert!(root.join(paths::LEGACY_CREDENTIALS).exists());
        prepare_under_lock(&root).unwrap();
        assert!(!root.join(paths::LEGACY_CREDENTIALS).exists());
        assert!(!root.join(".replace-old").exists());
        assert_eq!(read(&root).unwrap().unwrap(), b"original credential bytes");
        assert_eq!(
            std::fs::metadata(root.join(paths::CREDENTIALS))
                .unwrap()
                .ino(),
            inode
        );
        let directory = root.join(paths::CREDENTIAL_DIRECTORY);
        let before = std::fs::metadata(&directory).unwrap();
        assert_eq!(before.permissions().mode() & 0o777, 0o700);
        std::fs::write(directory.join(".replace-stopped"), b"stopped renewal").unwrap();
        prepare_under_lock(&root).unwrap();
        assert!(!directory.join(".replace-stopped").exists());
        replace(&root, b"renewed bytes").unwrap();
        assert_eq!(
            std::fs::metadata(&directory).unwrap().ino(),
            before.ino(),
            "renewal keeps the confidential directory stable"
        );
        assert_eq!(read(&root).unwrap().unwrap(), b"renewed bytes");
    }

    #[test]
    fn conflicting_or_unsafe_sources_are_preserved_for_explicit_recovery() {
        let temporary = tempfile::tempdir().unwrap();
        let root = StateRoot::new(temporary.path().as_os_str()).unwrap();
        replace(&root, b"current").unwrap();
        legacy(&root, b"legacy");
        let error = prepare_under_lock(&root).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("both legacy and current credentials exist")
        );
        for path in [paths::LEGACY_CREDENTIALS, paths::CREDENTIALS] {
            assert!(
                error
                    .to_string()
                    .contains(root.join(path).to_str().unwrap()),
                "{error}"
            );
        }
        assert_eq!(read(&root).unwrap().unwrap(), b"current");
        assert_eq!(
            std::fs::read(root.join(paths::LEGACY_CREDENTIALS)).unwrap(),
            b"legacy"
        );
        std::fs::remove_file(root.join(paths::CREDENTIALS)).unwrap();
        std::fs::remove_dir(root.join(paths::CREDENTIAL_DIRECTORY)).unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        symlink(elsewhere.path(), root.join(paths::CREDENTIAL_DIRECTORY)).unwrap();
        assert!(matches!(
            prepare_under_lock(&root),
            Err(CheckError::Unsafe {
                reason: private::Unsafe::SymbolicLink,
                ..
            })
        ));
        assert!(root.join(paths::LEGACY_CREDENTIALS).is_file());
        assert!(!elsewhere.path().join("credentials.json").exists());
    }

    #[test]
    fn failed_remnant_cleanup_leaves_the_legacy_credential_recoverable() {
        let temporary = tempfile::tempdir().unwrap();
        let root = StateRoot::new(temporary.path().as_os_str()).unwrap();
        legacy(&root, b"legacy");
        create_directory(&root).unwrap();
        std::fs::write(root.join(".replace-stopped"), b"secret").unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let result = prepare_under_lock(&root);
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        if rustix::process::geteuid().is_root() {
            return;
        }
        assert!(result.is_err());
        assert!(root.join(paths::LEGACY_CREDENTIALS).exists());
        assert!(root.join(".replace-stopped").exists());
        assert!(!root.join(paths::CREDENTIALS).exists());
        prepare_under_lock(&root).unwrap();
        assert_eq!(read(&root).unwrap().unwrap(), b"legacy");
    }
}
