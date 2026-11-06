//! The capabilities that every ready of this App Server advertises,
//! computed once at start: Session preparation always, drives mounting
//! when the mount probe at start succeeded (the FUSE mount on Linux; on
//! macOS a recorded `drives_enabled` and a listening NFS server),
//! `tool_execution` with the tools
//! whose startup checks pass (`execution::capability`) and `local_mcp`
//! always, whether or not a local server runs, since the tool-list reports
//! alone decide which local tools this computer offers. A build with the
//! feature `test-seams` advertises as `tool_execution` the fragment that
//! `TETRAL_TEST_TOOL_EXECUTION` holds, when it is set, so that
//! cross-process tests can choose the fragment.

use std::path::Path;

use drive::mount::Mounting;
use execution::capability::StartupChecks;
use protocol::connection::{Capabilities, DriveCapability};
use protocol::field::{ConstInt, True};
use protocol::tool::LocalMcpCapability;

use crate::startup::StartError;

/// The variable whose value a `test-seams` build advertises as its ready's
/// `tool_execution`.
#[cfg(feature = "test-seams")]
pub const TEST_TOOL_EXECUTION_VARIABLE: &str = "TETRAL_TEST_TOOL_EXECUTION";

/// The capabilities, or a start error when a `test-seams` build is given a
/// value that is not a `tool_execution` fragment.
pub fn assemble(tetral: &Path, mounting: &Mounting) -> Result<Capabilities, StartError> {
    let tool_execution = match test_tool_execution()? {
        Some(fragment) => fragment,
        None => execution::capability::fragment(startup_checks(tetral)),
    };
    Ok(Capabilities {
        session_preparation: True,
        drives: DriveCapability {
            mount: mounting.can_mount(),
        },
        tool_execution: Some(tool_execution),
        local_mcp: Some(LOCAL_MCP),
    })
}

const LOCAL_MCP: LocalMcpCapability = LocalMcpCapability {
    schema_version: ConstInt,
};

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
    // One JSON value, so that the wrapping below cannot add members. The
    // fragment is checked beside `local_mcp`, as every ready carries it.
    serde_json::from_slice::<serde::de::IgnoredAny>(value)
        .map_err(|error| invalid(error.to_string()))?;
    let extension = [
        b"{\"local_mcp\":{\"schema_version\":1},\"tool_execution\":",
        value,
        b"}",
    ]
    .concat();
    protocol::decode::<protocol::tool::ReadyExtension>(&extension)
        .map(|extension| Some(extension.tool_execution))
        .map_err(|error| invalid(error.to_string()))
}

#[cfg(not(feature = "test-seams"))]
fn test_tool_execution() -> Result<Option<protocol::tool::ToolExecutionCapability>, StartError> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_a_runnable_tetral_the_ready_offers_only_cancel_beside_local_mcp() {
        let capabilities = assemble(
            Path::new("/nonexistent/tetral"),
            &Mounting::Unavailable("no FUSE".into()),
        )
        .unwrap();
        let encoded = serde_json::to_value(&capabilities).unwrap();
        assert_eq!(encoded["tool_execution"]["tools"], serde_json::json!([]));
        assert_eq!(
            encoded["tool_execution"]["controls"],
            serde_json::json!(["cancel"])
        );
        assert_eq!(
            encoded["local_mcp"],
            serde_json::json!({"schema_version": 1})
        );
        // A ready carrying it passes the protocol's checks.
        let fixtures: Vec<serde_json::Value> = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../protocol/connection-v1.fixtures.json"
        )))
        .unwrap();
        let mut ready = fixtures
            .into_iter()
            .find(|fixture| fixture["name"] == "ready_local_mcp_without_tools")
            .unwrap()["value"]
            .take();
        ready["capabilities"] = encoded;
        protocol::decode::<protocol::connection::Ready>(&serde_json::to_vec(&ready).unwrap())
            .unwrap();
    }
}
