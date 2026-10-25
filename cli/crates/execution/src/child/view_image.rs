//! `view_image`'s launcher entry, `__tetral-task view-image`: reads one
//! PNG, JPEG, GIF or WebP image, recognized by its content, that image
//! acceptance takes, whole and in canonical Base64. The first failing step
//! decides the error: the working directory of a relative path, the file's
//! `stat`, an empty file, the open and the first bytes, the format, the
//! dimensions in the header, acceptance by the `stat` size and the
//! dimensions, before the rest is read, then a read whose length is not
//! the `stat` size.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

use base64::Engine;
use protocol::field::Count;
use protocol::image::{Acceptance, Format, HEADER_MAX_BYTES, SIGNATURE_BYTES};
use protocol::tool::{MediaResult, ToolError, ToolResult, ViewImageInput};

use crate::exec::tool_error;

/// The entry's kind, after [`super::ENTRY`].
pub const KIND: &str = "view-image";

pub(super) fn run(cwd: &Path, input: serde_json::Value) -> Option<Result<ToolResult, ToolError>> {
    let input: ViewImageInput = serde_json::from_value(input).ok()?;
    Some(view_image(cwd, &input, &Acceptance::default()).map(ToolResult::Media))
}

fn view_image(
    cwd: &Path,
    input: &ViewImageInput,
    acceptance: &Acceptance,
) -> Result<MediaResult, ToolError> {
    let path = super::resolve(cwd, input.path.as_str())?;
    let failed =
        |kind: &str, message: &str| tool_error(kind, message, Some(path.display().to_string()));
    let unreadable = || failed("not_found", "path could not be read");
    let metadata = fs::metadata(&path).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => failed("not_found", "path does not exist"),
        _ => unreadable(),
    })?;
    if metadata.is_dir() {
        return Err(failed("is_directory", "path is a directory"));
    }
    // A FIFO or a device has a `stat` size of 0, so it is never opened.
    let size = metadata.len();
    if size == 0 {
        return Err(failed("unsupported_format", "empty file"));
    }
    let mut file = File::open(&path).map_err(|_| unreadable())?;
    let mut content = Vec::new();
    (&mut file)
        .take(SIGNATURE_BYTES as u64)
        .read_to_end(&mut content)
        .map_err(|_| unreadable())?;
    let Some(format) = Format::of(&content) else {
        return Err(failed(
            "unsupported_format",
            "unsupported image format; supported formats are PNG, JPEG, GIF, and WebP",
        ));
    };
    let header = (HEADER_MAX_BYTES as u64).min(size + 1);
    (&mut file)
        .take(header.saturating_sub(content.len() as u64))
        .read_to_end(&mut content)
        .map_err(|_| unreadable())?;
    let Some(dimensions) = format.dimensions(&content) else {
        return Err(failed(
            "unsupported_format",
            "image dimensions could not be read",
        ));
    };
    acceptance
        .check(size, dimensions)
        .map_err(|message| failed("too_large", &message))?;
    file.take((size + 1).saturating_sub(content.len() as u64))
        .read_to_end(&mut content)
        .map_err(|_| unreadable())?;
    if content.len() as u64 != size {
        return Err(failed("not_found", "path changed while reading"));
    }
    Ok(MediaResult {
        mime: format.mime(),
        size_bytes: Count::saturating(size),
        data_base64: base64::engine::general_purpose::STANDARD.encode(&content),
    })
}
