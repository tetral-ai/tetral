//! A call's permissions: its `sandbox_policy` validated (`validate`) before
//! any side effect and without reading a user file or starting a process.

mod validate;

use protocol::tool::{ProtocolError, RejectionCode};

pub use validate::{Network, ValidPolicy, validate};

/// A call refused for its policy: `invalid_input` or `unsupported_policy`,
/// with fixed text naming the failing field.
pub type PolicyRejection = ProtocolError<RejectionCode>;
