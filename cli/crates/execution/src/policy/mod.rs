//! A call's permissions: its `sandbox_policy` validated (`validate`) and
//! translated into a Codex `PermissionProfile` (`translate`), before any side
//! effect and without reading a user file or starting a process.

mod translate;
mod validate;

use codex_protocol::models::PermissionProfile;
use protocol::tool::{ProtocolError, RejectionCode};
use serde_json::Value;

pub use translate::{DriveMount, SessionDrives, TranslationInputs, translate};
pub use validate::{Network, ValidPolicy, validate};

/// A call refused for its policy: `invalid_input` or `unsupported_policy`,
/// with fixed text naming the failing field.
pub type PolicyRejection = ProtocolError<RejectionCode>;

/// The permissions of one task. They are built once, at admission, and kept
/// with the task; nothing rebuilds them, so a running process never changes
/// its permissions.
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
}
