//! `credentials.json`: the bootstrap object that `tetral configure` installed,
//! connection key included. Its format and rules are those of
//! `protocol::bootstrap`; this crate, which depends on no other workspace
//! crate, reads and writes its bytes.

use crate::files::{self, IoFailure};
use crate::paths::{self, StateRoot};

/// The largest `credentials.json` read, the bootstrap's own limit; the
/// reader gets one byte more of a larger file, so that the bootstrap's
/// decoder refuses it.
pub const CREDENTIALS_MAX_BYTES: usize = 65_536;

/// Reads the root's `credentials.json`; `None` when it does not exist.
pub fn read(root: &StateRoot) -> Result<Option<Vec<u8>>, IoFailure> {
    read_with_limit(root, CREDENTIALS_MAX_BYTES)
}

/// `read` with another size limit, for tests.
pub fn read_with_limit(root: &StateRoot, max_bytes: usize) -> Result<Option<Vec<u8>>, IoFailure> {
    files::read_bounded(&root.join(paths::CREDENTIALS), max_bytes)
}

/// Replaces the root's `credentials.json` with `bytes`.
pub fn replace(root: &StateRoot, bytes: &[u8]) -> Result<(), IoFailure> {
    files::replace(&root.join(paths::CREDENTIALS), bytes)
}
