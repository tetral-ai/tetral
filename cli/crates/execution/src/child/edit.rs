//! `edit`'s launcher entry, `__tetral-task edit`: replaces an exact
//! substring of one existing UTF-8 file and writes the new content in its
//! place atomically. It never creates the file. The first failing step
//! decides the error: the input, the working directory of a relative path,
//! the file's `stat`, its size, its bounded read, its encoding, the match
//! and the new content's size, then the write.

use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Read, Write};
use std::ops::Range;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use protocol::field::Count;
use protocol::tool::{EditInput, EditResult, ToolError, ToolResult};
use serde_json::json;

use crate::exec::tool_error;

/// The entry's kind, after [`super::ENTRY`].
pub const KIND: &str = "edit";

/// The most bytes of a file that `edit` reads, and of the content it
/// writes.
pub const MAX_BYTES: u64 = 33_554_432;

/// How many names a temporary file of the write tries before it fails.
const TEMPORARY_NAME_ATTEMPTS: usize = 16;

pub(super) fn run(cwd: &Path, input: serde_json::Value) -> Option<Result<ToolResult, ToolError>> {
    let input: EditInput = serde_json::from_value(input).ok()?;
    Some(edit(cwd, &input, MAX_BYTES).map(ToolResult::Edit))
}

fn edit(cwd: &Path, input: &EditInput, max_bytes: u64) -> Result<EditResult, ToolError> {
    let old = input.old_string.as_str();
    let new = input.new_string.value().map_or("", String::as_str);
    let replace_all = input.replace_all.value().copied().unwrap_or(false);
    if old == new {
        return Err(tool_error(
            "invalid_input",
            "old_string and new_string must differ",
            None,
        ));
    }
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
    if !metadata.is_file() {
        return Err(unreadable());
    }
    let too_large = || failed("too_large", "file exceeds 32 MiB edit cap");
    if metadata.len() > max_bytes {
        return Err(too_large());
    }
    let content = read_bounded(&path, max_bytes).map_err(|_| unreadable())?;
    if content.len() as u64 > max_bytes {
        return Err(too_large());
    }
    let Ok(content) = String::from_utf8(content) else {
        return Err(tool_error(
            "invalid_input",
            "file content must be UTF-8",
            None,
        ));
    };
    let spans = spans(&content, old);
    if spans.is_empty() {
        return Err(failed("match_not_found", "old_string was not found"));
    }
    if spans.len() > 1 && !replace_all {
        let mut error = failed("match_ambiguous", "old_string matched more than once");
        error.detail = json!({"matches": spans.len()}).as_object().cloned();
        return Err(error);
    }
    let removed: usize = spans.iter().map(|span| span.len()).sum();
    let next_len = (content.len() - removed) as u64 + spans.len() as u64 * new.len() as u64;
    if next_len > max_bytes {
        return Err(failed("too_large", "replacement exceeds 32 MiB edit cap"));
    }
    let mut next = String::with_capacity(next_len as usize);
    let mut kept = 0;
    for span in &spans {
        next.push_str(&content[kept..span.start]);
        next.push_str(new);
        kept = span.end;
    }
    next.push_str(&content[kept..]);
    write_atomically(
        &path,
        next.as_bytes(),
        metadata.permissions().mode() & 0o7777,
    )
    .map_err(|_| failed("write_failed", "write failed"))?;
    Ok(EditResult {
        replacements: Count::saturating(spans.len() as u64),
        bytes_written: Count::saturating(next.len() as u64),
    })
}

/// The file's content, read up to `max_bytes` and one byte more, so that a
/// file that grew past the cap is seen without reading the rest.
fn read_bounded(path: &Path, max_bytes: u64) -> io::Result<Vec<u8>> {
    let mut content = Vec::new();
    File::open(path)?
        .take(max_bytes + 1)
        .read_to_end(&mut content)?;
    Ok(content)
}

/// The byte ranges of `old` in `content`, left to right and not
/// overlapping. When the exact scan finds none, one more scan maps the
/// curly quotes to their ASCII forms in both; each maps one Unicode scalar
/// value to one, so its matches are the same scalar values of the original
/// content, which the replacement keeps byte for byte outside the spans.
fn spans(content: &str, old: &str) -> Vec<Range<usize>> {
    let exact: Vec<Range<usize>> = content
        .match_indices(old)
        .map(|(start, matched)| start..start + matched.len())
        .collect();
    if !exact.is_empty() {
        return exact;
    }
    let (mapped, quotes) = straight_quotes(content);
    let (needle, _) = straight_quotes(old);
    // Each mapped quote is one byte, its original three.
    let original = |at: usize| at + 2 * quotes.partition_point(|&quote| quote < at);
    mapped
        .match_indices(&needle)
        .map(|(start, matched)| original(start)..original(start + matched.len()))
        .collect()
}

/// `text` with U+2018, U+2019, U+201A and U+201B as `'` and U+201C,
/// U+201D, U+201E and U+201F as `"`, and the byte offsets in it of the
/// quotes it mapped.
fn straight_quotes(text: &str) -> (String, Vec<usize>) {
    let mut mapped = String::with_capacity(text.len());
    let mut quotes = Vec::new();
    for character in text.chars() {
        let straight = match character {
            '\u{2018}'..='\u{201b}' => '\'',
            '\u{201c}'..='\u{201f}' => '"',
            other => other,
        };
        if straight != character {
            quotes.push(mapped.len());
        }
        mapped.push(straight);
    }
    (mapped, quotes)
}

/// Writes `content` in place of the regular file at `path` through a new
/// file beside the file `path` names, through any symbolic link: created
/// exclusively, written, synced, given `mode` and renamed over it, then its
/// directory synced when possible. Any failure removes the new file.
fn write_atomically(path: &Path, content: &[u8], mode: u32) -> io::Result<()> {
    let target = fs::canonicalize(path)?;
    let (directory, name) = target
        .parent()
        .zip(target.file_name())
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    let (mut file, temporary) =
        create_temporary(directory, &name.to_string_lossy(), TEMPORARY_NAME_ATTEMPTS)?;
    let written = file
        .write_all(content)
        .and_then(|()| file.sync_all())
        .and_then(|()| {
            drop(file);
            fs::set_permissions(&temporary, Permissions::from_mode(mode))
        })
        .and_then(|()| fs::rename(&temporary, &target));
    if written.is_err() {
        let _ = fs::remove_file(&temporary);
        return written;
    }
    if let Ok(directory) = File::open(directory) {
        let _ = directory.sync_all();
    }
    Ok(())
}

/// A new file `.<name>.tetral-tmp-<16 hexadecimal characters>` in
/// `directory`, created exclusively with mode 0600, with at most `attempts`
/// names tried.
fn create_temporary(directory: &Path, name: &str, attempts: usize) -> io::Result<(File, PathBuf)> {
    for _ in 0..attempts {
        let suffix: String = rand::random::<[u8; 8]>()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let temporary = directory.join(format!(".{name}.tetral-tmp-{suffix}"));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
        {
            Ok(file) => return Ok((file, temporary)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::from(io::ErrorKind::AlreadyExists))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_read_of_a_file_stops_one_byte_past_the_cap() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("large.txt");
        fs::write(&path, vec![b'x'; 4_096 + 4 * 1_024]).unwrap();
        assert_eq!(read_bounded(&path, 4_096).unwrap().len(), 4_097);
    }
}
