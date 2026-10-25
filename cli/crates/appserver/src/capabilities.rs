//! The capabilities that every ready of this App Server advertises,
//! computed once at start from the providers this build has: Session
//! preparation always, and drives without mounting until a drive mount
//! provider exists. A build with the feature `test-seams` advertises as
//! `tool_execution` the fragment that `TETRAL_TEST_TOOL_EXECUTION` holds, so
//! that cross-process tests can negotiate tool execution with the providers
//! this build has.

use protocol::connection::{Capabilities, DriveCapability};
use protocol::field::True;

use crate::startup::StartError;

/// The variable whose value a `test-seams` build advertises as its ready's
/// `tool_execution`.
#[cfg(feature = "test-seams")]
pub const TEST_TOOL_EXECUTION_VARIABLE: &str = "TETRAL_TEST_TOOL_EXECUTION";

/// The capabilities, or a start error when a `test-seams` build is given a
/// value that is not a `tool_execution` fragment.
pub fn assemble() -> Result<Capabilities, StartError> {
    Ok(Capabilities {
        session_preparation: True,
        drives: DriveCapability { mount: false },
        tool_execution: test_tool_execution()?,
        local_mcp: None,
    })
}

#[cfg(feature = "test-seams")]
fn test_tool_execution() -> Result<Option<protocol::tool::ToolExecutionCapability>, StartError> {
    let Some(value) = std::env::var_os(TEST_TOOL_EXECUTION_VARIABLE) else {
        return Ok(None);
    };
    let value = value.as_encoded_bytes();
    let invalid = |reason: String| {
        StartError::TestToolExecution(format!(
            "{TEST_TOOL_EXECUTION_VARIABLE} is not a tool_execution fragment: {reason}"
        ))
    };
    // One JSON value, so that the wrapping below cannot add members.
    serde_json::from_slice::<serde::de::IgnoredAny>(value)
        .map_err(|error| invalid(error.to_string()))?;
    let extension = [b"{\"tool_execution\":", value, b"}"].concat();
    protocol::decode::<protocol::tool::ReadyExtension>(&extension)
        .map(|extension| Some(extension.tool_execution))
        .map_err(|error| invalid(error.to_string()))
}

#[cfg(not(feature = "test-seams"))]
fn test_tool_execution() -> Result<Option<protocol::tool::ToolExecutionCapability>, StartError> {
    Ok(None)
}
