//! Drive paths: paths relative to a drive's root, as every drive message
//! names a file.
//!
//! A drive path is NFC text of at most 1,024 UTF-8 bytes whose `/`-separated
//! segments are nonempty, at most 255 bytes each, never `.` or `..`, and hold
//! no control or format character. Two paths of one listing never differ only
//! in case under Unicode full case folding, so every machine can hold them.

use unicode_properties::{GeneralCategory, UnicodeGeneralCategory};

use crate::field::{Text, TextRule};

pub const PATH_MAX_UTF8_BYTES: usize = 1024;
pub const SEGMENT_MAX_UTF8_BYTES: usize = 255;

/// A drive path naming a file.
pub type DrivePath = Text<FilePath>;

/// A drive path naming a file, or a directory when it ends with `/`, as
/// exclusion paths do.
pub type ExcludePath = Text<FileOrDirectoryPath>;

pub enum FilePath {}

impl TextRule for FilePath {
    const NAME: &'static str = "drive path";
    fn accepts(text: &str) -> bool {
        path_rule(text).is_ok()
    }
}

pub enum FileOrDirectoryPath {}

impl TextRule for FileOrDirectoryPath {
    const NAME: &'static str = "drive path or directory path";
    fn accepts(text: &str) -> bool {
        let file = text.strip_suffix('/').unwrap_or(text);
        path_rule(file).is_ok() && text.len() <= PATH_MAX_UTF8_BYTES
    }
}

/// The first drive path rule that `path` breaks.
pub fn path_rule(path: &str) -> Result<(), &'static str> {
    if path.len() > PATH_MAX_UTF8_BYTES {
        return Err("drive path over 1,024 bytes");
    }
    for segment in path.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err("drive path segment empty, '.' or '..'");
        }
        if segment.len() > SEGMENT_MAX_UTF8_BYTES {
            return Err("drive path segment over 255 bytes");
        }
    }
    if path.chars().any(|character| {
        matches!(
            character.general_category(),
            GeneralCategory::Control | GeneralCategory::Format
        )
    }) {
        return Err("drive path with a control or format character");
    }
    if !unicode_normalization::is_nfc(path) {
        return Err("drive path not in NFC");
    }
    Ok(())
}

/// The key under which two paths that differ only in case are equal.
pub fn case_folded(path: &str) -> String {
    caseless::default_case_fold_str(path)
}
