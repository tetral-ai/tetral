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
        }
    }
}
