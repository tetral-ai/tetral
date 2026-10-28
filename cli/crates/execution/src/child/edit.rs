//! `edit`'s launcher entry, `__tetral-task edit`: replaces an exact
//! substring of one existing UTF-8 file and writes the new content in its
//! place directly. It never creates the file. The first failing step
//! decides the error: the input, the working directory of a relative path,
//! the file's `stat`, its size, its bounded read, its encoding, the match
//! and the new content's size, then the write.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::Range;
use std::path::Path;

use protocol::field::Count;
use protocol::tool::{EditInput, EditResult, ToolError, ToolResult};
use serde_json::json;

use crate::exec::tool_error;

/// The entry's kind, after [`super::ENTRY`].
pub const KIND: &str = "edit";

/// The most bytes of a file that `edit` reads, and of the content it
/// writes.
pub const MAX_BYTES: u64 = 33_554_432;

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
    write_in_place(&path, next.as_bytes()).map_err(|_| failed("write_failed", "write failed"))?;
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

/// Writes the validated replacement into the existing file. Opening does
/// not require creating a sibling file. A failed or interrupted write can
/// leave partial content, as with an ordinary file write.
fn write_in_place(path: &Path, content: &[u8]) -> io::Result<()> {
    OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)?
        .write_all(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn editing_an_existing_writable_file_needs_no_writable_parent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("document.txt");
        fs::write(&path, "old text").unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o500)).unwrap();
        let input = serde_json::from_value(
            json!({"path":"document.txt","old_string":"old","new_string":"new"}),
        )
        .unwrap();
        let result = edit(directory.path(), &input, MAX_BYTES);
        let sibling = fs::write(directory.path().join("sibling"), "not allowed");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert!(
            sibling.is_err(),
            "this permission fixture requires an unprivileged user"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "new text");
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn failed_validation_and_a_read_only_file_keep_the_original_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("document.txt");
        fs::write(&path, "old old").unwrap();
        for (old, new, all, cap, expected) in [
            ("missing", "new", false, MAX_BYTES, "match_not_found"),
            ("old", "new", false, MAX_BYTES, "match_ambiguous"),
            ("old", "too much text", true, 7, "too_large"),
        ] {
            let input = serde_json::from_value(
                json!({"path":"document.txt","old_string":old,"new_string":new,"replace_all":all}),
            )
            .unwrap();
            assert_eq!(
                edit(directory.path(), &input, cap)
                    .unwrap_err()
                    .kind
                    .as_str(),
                expected
            );
            assert_eq!(fs::read(&path).unwrap(), b"old old");
        }
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
        let input = serde_json::from_value(
            json!({"path":"document.txt","old_string":"old","new_string":"new","replace_all":true}),
        )
        .unwrap();
        let result = edit(directory.path(), &input, MAX_BYTES);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            result.unwrap_err().kind.as_str(),
            "write_failed",
            "this permission fixture requires an unprivileged user"
        );
        assert_eq!(fs::read(&path).unwrap(), b"old old");
    }

    #[test]
    fn the_read_of_a_file_stops_one_byte_past_the_cap() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("large.txt");
        fs::write(&path, vec![b'x'; 4_096 + 4 * 1_024]).unwrap();
        assert_eq!(read_bounded(&path, 4_096).unwrap().len(), 4_097);
    }
}
