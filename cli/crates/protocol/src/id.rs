//! Protocol identities.
//!
//! An identity is opaque: the CLI compares it and sends it back, and never
//! reads meaning into it. Only the `state` crate turns a [`SafeId`] into a
//! path component; every other identity reaches the filesystem only as an
//! internal name that its owner assigns.

use crate::field::{NulFree, Text, TextRule};

/// A nonempty string without U+0000.
pub type Id = Text<NulFree>;

/// An identity that may name a directory: an [`Id`] that holds no `/` and is
/// neither `.` nor `..`. `session_id` and `drive_id` are safe identities.
pub type SafeId = Text<Safe>;

pub enum Safe {}

impl TextRule for Safe {
    const NAME: &'static str = "identity without '/' that is not '.' or '..'";
    fn accepts(text: &str) -> bool {
        NulFree::accepts(text) && !text.contains('/') && text != "." && text != ".."
    }
}

impl SafeId {
    pub fn to_id(&self) -> Id {
        Id::new(self.as_str()).expect("a safe identity is an identity")
    }
}
