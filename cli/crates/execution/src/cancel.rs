//! `tool.cancel` as a task's supervisor handles it: the supervisor records
//! the intent at receipt and replies at once; the reply confirms receipt,
//! not the exit, and the task's `terminal` report gives the outcome.

/// How a cancel finds its task, which decides its reply's `cancelled` and
/// `was_running` and what the supervisor does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Receipt {
    /// Staged: the task never starts, and its `terminal` report says
    /// `cancelled` with null exit facts and empty streams.
    Staged,
    /// Running: further writes stop and the termination procedure begins.
    Running,
    /// Already being ended: further writes stop and the cancel joins that
    /// termination.
    Terminating,
    /// Ended before the cancel was handled: its outcome stands.
    Ended,
}

impl Receipt {
    pub fn cancelled(self) -> bool {
        self != Receipt::Ended
    }

    pub fn was_running(self) -> bool {
        matches!(self, Receipt::Running | Receipt::Terminating)
    }
}
