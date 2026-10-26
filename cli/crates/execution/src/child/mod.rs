//! The entry points that `tetral` runs as inside a task's sandbox, selected
//! by `argv[1]` [`ENTRY`] before any other argument is parsed. Each writes
//! [`STARTED`] on the launch-status descriptor that the App Server passes it:
//! `exec` right before it executes its command, and a failure to start as
//! one JSON object `{kind, message, path}`, before it or, when the exec
//! itself fails, after it; a file tool's entry (`edit`, `view-image`) at its
//! own start, before it reads one request `{cwd, input}` from stdin and
//! writes one result envelope to stdout. The end of that descriptor tells
//! nothing: the sandbox's own stages hold it open until the sandbox exits.

pub mod edit;
pub mod exec;
pub mod view_image;

use std::ffi::OsString;
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use protocol::tool::{HelperEnvelope, HelperStatus, HelperTool, Object, ToolError, ToolResult};
use rustix::io::Errno;
use serde::{Deserialize, Serialize};

use crate::exec::tool_error;
use crate::isolation::PASSED_DESCRIPTOR;

/// `argv[1]` of a task's launcher entry.
pub const ENTRY: &str = "__tetral-task";

/// What an entry writes on the launch-status descriptor when its command
/// starts.
pub const STARTED: &[u8] = b"\n";

/// The test seam barrier at which a file tool's entry waits after it has
/// written [`STARTED`].
const FILE_TOOL_STARTED: &str = "file_tool.started";

/// The most bytes of a file tool's request: an `input` within its budget,
/// with the metadata budget for the working directory and the framing.
const REQUEST_MAX_BYTES: usize =
    protocol::tool::limits::TOOL_INPUT_JSON_BYTES + protocol::tool::limits::METADATA_JSON_BYTES;

/// Runs the entry that `arguments`, the arguments after [`ENTRY`], name.
/// `exec` returns only when its command could not start.
pub fn run(arguments: &[OsString]) -> ExitCode {
    match arguments.split_first() {
        Some((kind, rest)) if kind == exec::KIND => exec::run(rest),
        Some((kind, [])) if kind == edit::KIND => run_file_tool(HelperTool::Edit, edit::run),
        Some((kind, [])) if kind == view_image::KIND => {
            run_file_tool(HelperTool::ViewImage, view_image::run)
        }
        _ => ExitCode::FAILURE,
    }
}

/// What a file tool's entry reads from stdin: the binding's canonical
/// working directory and the call's `input`.
#[derive(Debug, Serialize, Deserialize)]
pub struct FileRequest {
    pub cwd: PathBuf,
    pub input: serde_json::Value,
}

/// The arguments of `tetral`, after its own path, that run the entry of
/// `kind`.
pub fn file_tool_arguments(kind: &str) -> Vec<OsString> {
    vec![ENTRY.into(), kind.into()]
}

/// Reports the start, reads the request, runs `tool` and writes its result
/// envelope. A request that does not decode as the tool's input fails the
/// entry, which its supervisor reports as `helper_failure`.
fn run_file_tool(
    kind: HelperTool,
    tool: fn(&Path, serde_json::Value) -> Option<Result<ToolResult, ToolError>>,
) -> ExitCode {
    // SAFETY: the launch places the launch-status pipe at this descriptor
    // for this entry alone, and nothing else in the process holds it.
    let status = unsafe { OwnedFd::from_raw_fd(PASSED_DESCRIPTOR) };
    if rustix::io::write(&status, STARTED).is_err() {
        return ExitCode::FAILURE;
    }
    drop(status);
    state::seams::barrier_blocking(FILE_TOOL_STARTED);
    let mut request = Vec::new();
    let read = std::io::stdin()
        .take(REQUEST_MAX_BYTES as u64 + 1)
        .read_to_end(&mut request);
    if read.is_err() || request.len() > REQUEST_MAX_BYTES {
        return ExitCode::FAILURE;
    }
    let Ok(request) = serde_json::from_slice::<FileRequest>(&request) else {
        return ExitCode::FAILURE;
    };
    let Some(result) = tool(&request.cwd, request.input) else {
        return ExitCode::FAILURE;
    };
    let envelope = match result {
        Ok(result) => HelperEnvelope {
            tool: kind,
            status: HelperStatus::Success,
            truncated: false,
            error: None,
            result,
            metrics: None,
            drive_sync: None,
        },
        Err(error) => failed(kind, error),
    };
    let mut stdout = std::io::stdout().lock();
    match stdout
        .write_all(&protocol::encode(&envelope))
        .and_then(|()| stdout.flush())
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

/// The result envelope of a file tool's failure: `status: error`, `result:
/// {}`.
pub fn failed(tool: HelperTool, error: ToolError) -> HelperEnvelope {
    HelperEnvelope {
        tool,
        status: HelperStatus::Error,
        truncated: false,
        error: Some(error),
        result: ToolResult::Object(Object::new()),
        metrics: None,
        drive_sync: None,
    }
}

/// Why a working directory cannot be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectoryError {
    NotFound,
    NotDirectory,
    Inaccessible,
    /// Any other error of the operating system.
    Other,
}

impl DirectoryError {
    fn of(errno: Errno) -> DirectoryError {
        match errno {
            Errno::NOENT | Errno::NOTDIR => DirectoryError::NotFound,
            Errno::ACCESS | Errno::PERM => DirectoryError::Inaccessible,
            _ => DirectoryError::Other,
        }
    }

    /// The tool error that names `directory`, with the Go Helper's
    /// messages; `Other` is `cwd_inaccessible` for a file tool, while
    /// `exec_command` reports it as `exec_spawn_failed` (`exec`).
    fn tool_error(self, directory: &Path) -> ToolError {
        let (kind, message) = match self {
            DirectoryError::NotFound => ("cwd_not_found", "cwd does not exist"),
            DirectoryError::NotDirectory => ("cwd_not_directory", "cwd is not a directory"),
            DirectoryError::Inaccessible | DirectoryError::Other => {
                ("cwd_inaccessible", "cwd is not accessible")
            }
        };
        tool_error(kind, message, Some(directory.display().to_string()))
    }
}

/// Checks that `directory` exists and is a directory.
fn check_directory(directory: &Path) -> Result<(), DirectoryError> {
    let stat = rustix::fs::stat(directory).map_err(DirectoryError::of)?;
    if rustix::fs::FileType::from_raw_mode(stat.st_mode).is_dir() {
        Ok(())
    } else {
        Err(DirectoryError::NotDirectory)
    }
}

fn enter_directory(directory: &Path) -> Result<(), DirectoryError> {
    rustix::process::chdir(directory).map_err(DirectoryError::of)
}

/// The absolute path that a file tool's `path` names: an absolute one as
/// given; a relative one joined to `cwd` once `cwd` has passed the checks
/// that a command's directory passes, so that a missing working directory
/// is the call's error and never a fallback.
fn resolve(cwd: &Path, path: &str) -> Result<PathBuf, ToolError> {
    let path = Path::new(path);
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    check_directory(cwd)
        .and_then(|()| enter_directory(cwd))
        .map_err(|error| error.tool_error(cwd))?;
    Ok(cwd.join(path))
}
