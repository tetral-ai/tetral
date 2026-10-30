//! A call's permissions: its `sandbox_policy` validated (`validate`) and
//! translated into a Codex `PermissionProfile` (`translate`), before any side
//! effect and without reading a user file or starting a process.

mod translate;
mod validate;

use std::path::PathBuf;

use codex_protocol::models::{ManagedFileSystemPermissions, PermissionProfile};
use codex_protocol::permissions::FileSystemPath;
use protocol::tool::{ProtocolError, RejectionCode};
use serde_json::Value;

pub use translate::{DriveMount, SessionDrives, TranslationInputs, translate};
pub use validate::{Network, ValidPolicy, validate};

/// A call refused for its policy: `invalid_input` or `unsupported_policy`,
/// with fixed text naming the failing field.
pub type PolicyRejection = ProtocolError<RejectionCode>;

/// The permissions of one task. They are built at admission, translated
/// again with the binding's drives as the task starts, and lose the drives
/// its drive check found unmounted; a running process never changes its
/// permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPermissions {
    profile: PermissionProfile,
}

impl TaskPermissions {
    /// Validates and translates the payload's `sandbox_policy`, absent or not.
    pub fn admit(
        policy: Option<&Value>,
        inputs: &TranslationInputs,
    ) -> Result<TaskPermissions, PolicyRejection> {
        let policy = validate(policy)?;
        Ok(TaskPermissions {
            profile: translate(&policy, inputs)?,
        })
    }

    pub fn profile(&self) -> &PermissionProfile {
        &self.profile
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
        TaskPermissions { profile }
    }
}
