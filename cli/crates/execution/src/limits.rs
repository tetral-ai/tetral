//! How much of each stream one report or reply returns. An instruction's
//! `limits` may lower the defaults, never raise them; a `tool.execute`'s
//! apply to that task's `running` and `terminal` reports, a control's only to
//! its own reply, and neither changes what the capture keeps.

use protocol::field::Maybe;
use protocol::tool::VisibleLimits;

/// The bytes of each stream that one reply returns at most.
pub const VISIBLE_BYTES: usize = 51_200;

/// The lines of each stream that one reply returns at most.
pub const VISIBLE_LINES: usize = 2_000;

/// The byte and line budget of each stream in one reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Visible {
    pub bytes: usize,
    pub lines: usize,
}

impl Visible {
    /// The budget that `requested` asks for: each value that is positive and
    /// below its default, otherwise the default.
    pub fn requested(requested: Option<&VisibleLimits>) -> Visible {
        let lowered = |value: Option<&Maybe<i64>>, default: usize| {
            value
                .and_then(Maybe::value)
                .and_then(|value| usize::try_from(*value).ok())
                .filter(|value| (1..default).contains(value))
                .unwrap_or(default)
        };
        Visible {
            bytes: lowered(requested.map(|limits| &limits.visible_bytes), VISIBLE_BYTES),
            lines: lowered(requested.map(|limits| &limits.visible_lines), VISIBLE_LINES),
        }
    }
}
