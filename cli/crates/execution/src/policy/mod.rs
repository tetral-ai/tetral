//! A call's permissions: its `sandbox_policy` validated (`validate`) and
//! translated into a Codex `PermissionProfile` (`translate`), before any side
//! effect and without reading a user file or starting a process.

mod translate;
mod validate;

use std::path::{Path, PathBuf};

use codex_protocol::models::{ManagedFileSystemPermissions, PermissionProfile};
use codex_protocol::permissions::FileSystemPath;
use protocol::tool::{ProtocolError, RejectionCode};
use serde_json::Value;

pub use translate::{DriveMount, PreparationInputs, SessionDrives, TranslationInputs, translate};
pub use validate::{Network, ValidPolicy, validate};

/// A call refused for its policy: `invalid_input` or `unsupported_policy`,
/// with fixed text naming the failing field.
pub type PolicyRejection = ProtocolError<RejectionCode>;

/// The permissions of one task or internal repository preparation. Tool
/// permissions are built at admission, translated
/// again with the binding's drives as the task starts, and lose the drives
/// its drive check found unmounted; a running process never changes its
/// permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPermissions {
    profile: PermissionProfile,
    drives_directory: Option<PathBuf>,
    unix_sockets: UnixSockets,
    drive_server_port: Option<u16>,
}

/// Where a task's commands may connect to a Unix socket, which the macOS
/// launch decides apart from the profile's file rules: never below the
/// state root, the socket directory or the task directories; under
/// `direct`, below a tool task's own temporary directory, which lies inside
/// the task directories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnixSockets {
    pub denied: [PathBuf; 3],
    /// The task's private temporary directory; preparation has none.
    pub own: Option<PathBuf>,
}

impl TaskPermissions {
    /// Internal repository preparation has no caller-supplied tool policy.
    pub fn preparation(inputs: &PreparationInputs) -> Result<Self, PolicyRejection> {
        Ok(Self {
            profile: translate::preparation(inputs)?,
            drives_directory: inputs
                .drives
                .as_ref()
                .map(|drives| drives.directory.clone()),
            unix_sockets: UnixSockets {
                denied: [
                    inputs.state_root.clone(),
                    inputs.socket_directory.clone(),
                    inputs.task_directories.clone(),
                ],
                own: None,
            },
            drive_server_port: inputs.drive_server_port,
        })
    }

    /// Validates and translates the payload's `sandbox_policy`, absent or not.
    pub fn admit(
        policy: Option<&Value>,
        inputs: &TranslationInputs,
    ) -> Result<TaskPermissions, PolicyRejection> {
        let policy = validate(policy)?;
        Ok(TaskPermissions {
            profile: translate(&policy, inputs)?,
            drives_directory: inputs
                .drives
                .as_ref()
                .map(|drives| drives.directory.clone()),
            unix_sockets: UnixSockets {
                denied: [
                    inputs.state_root.clone(),
                    inputs.socket_directory.clone(),
                    inputs.task_directories.clone(),
                ],
                own: Some(inputs.task_temporary_directory.clone()),
            },
            drive_server_port: inputs.drive_server_port,
        })
    }

    pub fn profile(&self) -> &PermissionProfile {
        &self.profile
    }

    /// The configured drive path, including when no drive could be mounted.
    /// This environment value does not confer filesystem permissions.
    pub fn drives_directory(&self) -> Option<&Path> {
        self.drives_directory.as_deref()
    }

    pub fn unix_sockets(&self) -> &UnixSockets {
        &self.unix_sockets
    }

    /// The port of the App Server's NFS server at admission, which the
    /// macOS launch keeps every command from connecting to.
    pub fn drive_server_port(&self) -> Option<u16> {
        self.drive_server_port
    }

    /// These permissions without any entry at or below one of `paths`.
    pub fn without(&self, paths: &[PathBuf]) -> TaskPermissions {
        let mut profile = self.profile.clone();
        if let PermissionProfile::Managed {
            file_system: ManagedFileSystemPermissions::Restricted { entries, .. },
            ..
        } = &mut profile
        {
            entries.retain(|entry| match &entry.path {
                FileSystemPath::Path { path } => path.to_abs_path().is_ok_and(|path| {
                    !paths
                        .iter()
                        .any(|left_out| path.as_path().starts_with(left_out))
                }),
                _ => true,
            });
        }
        TaskPermissions {
            profile,
            drives_directory: self.drives_directory.clone(),
            unix_sockets: self.unix_sockets.clone(),
            drive_server_port: self.drive_server_port,
        }
    }
}
