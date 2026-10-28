//! The capabilities that every ready of this App Server advertises,
//! computed once at start: Session preparation always, drives without
//! mounting until a drive mount provider exists, and `tool_execution` with
//! the tools whose startup checks pass (`execution::capability`). A build
//! with the feature `test-seams` advertises as `tool_execution` the fragment
//! that `TETRAL_TEST_TOOL_EXECUTION` holds, when it is set, so that
//! cross-process tests can choose the fragment.

use std::path::Path;

use execution::capability::StartupChecks;
use protocol::connection::{Capabilities, DriveCapability};
use protocol::field::True;

use crate::startup::StartError;

/// The variable whose value a `test-seams` build advertises as its ready's
/// `tool_execution`.
#[cfg(feature = "test-seams")]
pub const TEST_TOOL_EXECUTION_VARIABLE: &str = "TETRAL_TEST_TOOL_EXECUTION";

/// The capabilities, or a start error when a `test-seams` build is given a
/// value that is not a `tool_execution` fragment.
pub fn assemble(tetral: &Path) -> Result<Capabilities, StartError> {
    let tool_execution = match test_tool_execution()? {
        Some(fragment) => Some(fragment),
        None => execution::capability::fragment(startup_checks(tetral)),
    };
    Ok(Capabilities {
        session_preparation: True,
        drives: DriveCapability { mount: false },
        tool_execution,
        local_mcp: None,
    })
}

/// The startup checks of tool execution: the running `tetral`, which every
/// tool's restricted child runs, and a shell for `exec_command`.
fn startup_checks(tetral: &Path) -> StartupChecks {
    StartupChecks {
        restricted_child: executable(tetral),
        shell: ["/bin/bash", "/bin/sh"]
            .into_iter()
            .any(|shell| executable(Path::new(shell))),
    }
}

/// Whether `path` resolves to a regular file that this process may execute.
fn executable(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.is_file())
        && rustix::fs::access(path, rustix::fs::Access::EXEC_OK).is_ok()
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
