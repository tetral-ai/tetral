//! The supervisor side of the file tools, which run only as a restricted
//! child, `tetral __tetral-task <kind>` (`child`): the request it writes to
//! the child's stdin, the result it reads from the child's stdout, and the
//! result envelopes of a child that gave no valid result and of a cancelled
//! call. A result is valid when it is one result envelope of the call's
//! tool, finished, untruncated and without `metrics`, which the report adds,
//! and, for `view_image`, holds one image that image acceptance takes, its
//! Base64 canonical and its `size_bytes` and `mime` those of the decoded
//! bytes, so that no report of an image Engine or a model would refuse is
//! made; anything else, more than [`RESULT_MAX_BYTES`] included, is
//! `helper_failure`.

use std::path::Path;

use base64::Engine;
use protocol::image::{Acceptance, Format, HEADER_MAX_BYTES};
use protocol::tool::{
    EditInput, HelperEnvelope, HelperStatus, HelperTool, MediaResult, ToolInput, ToolResult,
    ViewImageInput, limits,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};

use crate::child::{self, FileRequest};
use crate::exec::tool_error;

/// The most bytes of a child's result: a complete media tool-status.
pub const RESULT_MAX_BYTES: usize = limits::MEDIA_TOOL_STATUS_JSON_BYTES;

/// The most bytes of one read of a child's stdout or stderr.
const READ_CHUNK_BYTES: usize = 65_536;

/// The message of a child that gave no valid result.
pub const NO_VALID_RESULT: &str = "the tool's process gave no valid result";

/// A file tool's call, as its child runs it.
#[derive(Debug, Clone)]
pub enum FileCall {
    Edit(EditInput),
    ViewImage(ViewImageInput),
}

impl FileCall {
    /// The call of `tool`, when it is a file tool.
    pub fn of(tool: &ToolInput) -> Option<FileCall> {
        match tool {
            ToolInput::Edit(input) => Some(FileCall::Edit(input.clone())),
            ToolInput::ViewImage(input) => Some(FileCall::ViewImage(input.clone())),
            ToolInput::ExecCommand(_) => None,
        }
    }

    pub fn tool(&self) -> HelperTool {
        match self {
            FileCall::Edit(_) => HelperTool::Edit,
            FileCall::ViewImage(_) => HelperTool::ViewImage,
        }
    }

    /// The arguments of `tetral`, after its own path, that run its child.
    pub fn arguments(&self) -> Vec<String> {
        let kind = match self {
            FileCall::Edit(_) => child::edit::KIND,
            FileCall::ViewImage(_) => child::view_image::KIND,
        };
        child::file_tool_arguments(kind)
            .into_iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect()
    }

    /// The request that the child reads: `cwd`, the binding's canonical
    /// working directory, and the call's input.
    pub fn request(&self, cwd: &Path) -> Vec<u8> {
        let input = match self {
            FileCall::Edit(input) => serde_json::to_value(input),
            FileCall::ViewImage(input) => serde_json::to_value(input),
        };
        protocol::encode(&FileRequest {
            cwd: cwd.to_path_buf(),
            input: input.expect("a tool input serializes"),
        })
    }

    /// The result envelope of `bytes`, the child's whole stdout, when it is
    /// a valid result of this call.
    pub fn result(&self, bytes: &[u8]) -> Option<HelperEnvelope> {
        let envelope: HelperEnvelope = serde_json::from_slice(bytes).ok()?;
        let finished = matches!(envelope.status, HelperStatus::Success | HelperStatus::Error);
        let valid = envelope.tool == self.tool()
            && finished
            && !envelope.truncated
            && envelope.metrics.is_none()
            && envelope.drive_sync.is_none()
            && match &envelope.result {
                ToolResult::Media(media) => accepted(media),
                _ => true,
            };
        valid.then_some(envelope)
    }

    /// The result envelope `helper_failure` with `message`: a child that
    /// gave no valid result, or a start that failed.
    pub fn failure(&self, message: &str) -> HelperEnvelope {
        child::failed(self.tool(), tool_error("helper_failure", message, None))
    }

    /// The result envelope of a call that a cancel ended, before or after its
    /// child `started`.
    pub fn cancelled(&self, started: bool) -> HelperEnvelope {
        let message = if started {
            "cancelled after the call started"
        } else {
            "cancelled before the call started"
        };
        child::failed(self.tool(), tool_error("cancelled", message, None))
    }
}

/// The child's pipes: the request still to write, and the result read so
/// far.
pub struct Pipes {
    stdin: Option<ChildStdin>,
    request: Vec<u8>,
    written: usize,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    result: Vec<u8>,
    overflowed: bool,
    out_buffer: Vec<u8>,
    err_buffer: Vec<u8>,
}

impl Pipes {
    pub fn new(
        stdin: Option<ChildStdin>,
        request: Vec<u8>,
        stdout: Option<ChildStdout>,
        stderr: Option<ChildStderr>,
    ) -> Pipes {
        Pipes {
            stdin,
            request,
            written: 0,
            stdout,
            stderr,
            result: Vec::new(),
            overflowed: false,
            out_buffer: vec![0; READ_CHUNK_BYTES],
            err_buffer: vec![0; READ_CHUNK_BYTES],
        }
    }

    /// Whether the child's stdout has ended, so that its result is all
    /// there.
    pub fn received(&self) -> bool {
        self.stdout.is_none()
    }

    /// The child's whole stdout, unless it wrote more than
    /// [`RESULT_MAX_BYTES`].
    pub fn result(&self) -> Option<&[u8]> {
        (!self.overflowed).then_some(&self.result[..])
    }

    /// One step: a write of the request, a read of stdout, or a read of
    /// stderr, which is discarded. Cancel-safe: each step's effect is applied
    /// once its operation has completed.
    pub async fn step(&mut self) {
        let writing = self.stdin.is_some();
        tokio::select! {
            written = write_some(self.stdin.as_mut(), &self.request[self.written..]), if writing => {
                match written {
                    Ok(count) if count > 0 => self.written += count,
                    _ => self.stdin = None,
                }
                // The request's end is the child's end of file.
                if self.written == self.request.len() {
                    self.stdin = None;
                }
            }
            read = read_some(self.stdout.as_mut(), &mut self.out_buffer), if self.stdout.is_some() => {
                match read {
                    Ok(count) if count > 0 => {
                        if count > RESULT_MAX_BYTES - self.result.len() {
                            self.overflowed = true;
                            self.stdout = None;
                        } else {
                            self.result.extend_from_slice(&self.out_buffer[..count]);
                        }
                    }
                    _ => self.stdout = None,
                }
            }
            read = read_some(self.stderr.as_mut(), &mut self.err_buffer), if self.stderr.is_some() => {
                if !matches!(read, Ok(count) if count > 0) {
                    self.stderr = None;
                }
            }
            else => std::future::pending::<()>().await,
        }
    }

    /// Lets go of every pipe, the end of the request included.
    pub fn close(&mut self) {
        self.stdin = None;
        self.stdout = None;
        self.stderr = None;
    }
}

/// Whether `media` holds one image that image acceptance takes, in
/// canonical Base64, with the `size_bytes` and `mime` of its decoded bytes.
fn accepted(media: &MediaResult) -> bool {
    let Ok(image) = base64::engine::general_purpose::STANDARD.decode(&media.data_base64) else {
        return false;
    };
    let Some(format) = Format::of(&image) else {
        return false;
    };
    let header = &image[..image.len().min(HEADER_MAX_BYTES)];
    format.mime() == media.mime
        && u64::try_from(media.size_bytes.get()) == Ok(image.len() as u64)
        && format.dimensions(header).is_some_and(|dimensions| {
            Acceptance::default()
                .check(image.len() as u64, dimensions)
                .is_ok()
        })
}

async fn write_some(stdin: Option<&mut ChildStdin>, bytes: &[u8]) -> std::io::Result<usize> {
    match stdin {
        Some(stdin) => stdin.write(bytes).await,
        None => std::future::pending().await,
    }
}

async fn read_some<R: AsyncRead + Unpin>(
    pipe: Option<&mut R>,
    buffer: &mut [u8],
) -> std::io::Result<usize> {
    match pipe {
        Some(pipe) => pipe.read(buffer).await,
        None => std::future::pending().await,
    }
}
