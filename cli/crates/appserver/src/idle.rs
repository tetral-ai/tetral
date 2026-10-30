//! The idle exit of `server run --idle-exit <duration>`, which the
//! lifecycle task runs. The idle start is the App Server's start, moved to
//! the arrival of every tool and local MCP instruction the Connection owner
//! reads and to the exit of every `exec_command` command; the idle time
//! counts only while no such command runs. When it reaches the duration,
//! the exit begins: every Session's drive sync uploads and commits its
//! pending changes as a final upload in keep mode, ending 30 seconds later,
//! and then the shutdown starts. A tool instruction read before that upload
//! ended ends the exit: the upload is released, so its uploads and commits
//! go on as ordinary ones, and the idle start moves to the instruction. The
//! lifecycle task handles these messages in arrival order, which separates
//! the two cases.

use std::collections::BTreeSet;
use std::time::Duration;

use connection::Connection;
use drive::final_upload::{FinalKeeper, FinalMode, FinalUploadRequest};
use protocol::id::Id;
use protocol::ports::ActivitySink;
use tokio::sync::mpsc;

use crate::lifecycle::Lifecycle;

/// How long the upload before an idle exit may run.
pub const UPLOAD_WAIT: Duration = Duration::from_secs(30);

/// The test seam barrier between the start of the upload before an idle
/// exit and the wait for its end.
const UPLOADING: &str = "idle.uploading";

/// The test seam counter of each reading of the clock by the timer and each
/// message it handled.
const CHECKED: &str = "idle.checked";

/// What the Connection owner and the Task supervisors report.
#[derive(Debug)]
pub(crate) enum Activity {
    Arrived(Duration),
    Started,
    Exited(Duration),
}

impl ActivitySink for Lifecycle {
    fn tool_instruction_arrived(&self, at: Duration) {
        self.activity(Activity::Arrived(at));
    }

    fn exec_started(&self) {
        self.activity(Activity::Started);
    }

    fn exec_exited(&self, at: Duration) {
        self.activity(Activity::Exited(at));
    }
}

/// The idle start and the running `exec_command` commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Timer {
    limit: Duration,
    start: Duration,
    running: u64,
}

impl Timer {
    fn arrived(&mut self, at: Duration) {
        self.start = self.start.max(at);
    }

    fn started(&mut self) {
        self.running += 1;
    }

    fn exited(&mut self, at: Duration) {
        self.running = self.running.saturating_sub(1);
        self.start = self.start.max(at);
    }

    /// When the exit begins, unless a command runs.
    fn due(&self) -> Option<Duration> {
        (self.running == 0).then(|| self.start + self.limit)
    }
}

/// Waits until `due`, reading the clock as the test seam counter
/// `idle.checked`; for ever without one.
pub(crate) async fn sleep_until(due: Option<Duration>) {
    match due {
        Some(due) => state::seams::sleep_until_counted(due, CHECKED).await,
        None => std::future::pending().await,
    }
}

/// The idle exit as the lifecycle task holds it.
pub(crate) struct IdleExit {
    timer: Timer,
    connection: Connection,
    round: u64,
    /// The keepers of the upload before the exit, while it runs.
    uploading: Option<Vec<FinalKeeper>>,
    uploaded: mpsc::UnboundedSender<u64>,
    ended: mpsc::UnboundedReceiver<u64>,
}

impl IdleExit {
    /// An exit after `limit`, counted from `start`.
    pub fn new(limit: Duration, start: Duration, connection: Connection) -> IdleExit {
        let (uploaded, ended) = mpsc::unbounded_channel();
        IdleExit {
            timer: Timer {
                limit,
                start,
                running: 0,
            },
            connection,
            round: 0,
            uploading: None,
            uploaded,
            ended,
        }
    }

    /// When the exit begins: never while its upload or a command runs.
    pub fn due(&self) -> Option<Duration> {
        self.timer.due().filter(|_| self.uploading.is_none())
    }

    /// Waits for the end of an upload before the exit: true for the upload
    /// still running, after which the shutdown starts.
    pub async fn uploaded(&mut self) -> bool {
        let round = self.ended.recv().await;
        let current = round == Some(self.round) && self.uploading.is_some();
        state::seams::count(CHECKED);
        current
    }

    pub fn activity(&mut self, activity: Activity) {
        match activity {
            Activity::Arrived(at) => {
                self.timer.arrived(at);
                for keeper in self.uploading.take().into_iter().flatten() {
                    keeper.release();
                }
            }
            Activity::Started => self.timer.started(),
            Activity::Exited(at) => self.timer.exited(at),
        }
        state::seams::count(CHECKED);
    }

    /// Begins the exit: the final upload in keep mode of every drive with a
    /// pending change, in every Session, and a task that tells the round's
    /// end once each has ended.
    pub async fn begin(&mut self) {
        self.round += 1;
        let mut uploads = Vec::new();
        for sync in self.connection.drive_syncs().await {
            let drives: BTreeSet<String> = sync
                .tables()
                .await
                .pending_changes
                .into_keys()
                .map(|(drive, _)| drive)
                .collect();
            let drives: Vec<Id> = drives
                .into_iter()
                .filter_map(|drive| Id::new(drive).ok())
                .collect();
            if drives.is_empty() {
                continue;
            }
            uploads.push(sync.final_upload(FinalUploadRequest {
                drives,
                end: sync.now() + UPLOAD_WAIT,
                mode: FinalMode::Keep,
            }));
        }
        self.uploading = Some(uploads.iter().map(|upload| upload.keeper()).collect());
        let (round, uploaded) = (self.round, self.uploaded.clone());
        tokio::spawn(async move {
            state::seams::barrier(UPLOADING).await;
            for mut upload in uploads {
                upload.ended().await;
            }
            let _ = uploaded.send(round);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: Duration = Duration::from_secs(60);

    fn timer() -> Timer {
        Timer {
            limit: MINUTE,
            start: Duration::ZERO,
            running: 0,
        }
    }

    fn secs(value: u64) -> Duration {
        Duration::from_secs(value)
    }

    #[test]
    fn the_exit_is_due_a_duration_after_the_last_activity_and_never_while_a_command_runs() {
        // With no activity, not at 59 seconds but at 60.
        assert_eq!(timer().due(), Some(MINUTE));

        // A tool.stdin at 50 seconds moves it to 110.
        let mut stdin = timer();
        stdin.arrived(secs(50));
        assert_eq!(stdin.due(), Some(secs(110)));

        // A background command running past 60 seconds holds it until 60
        // seconds after its exit.
        let mut background = timer();
        background.started();
        assert_eq!(background.due(), None);
        background.exited(secs(90));
        assert_eq!(background.due(), Some(secs(150)));

        // An arrival older than the idle start moves nothing.
        background.arrived(secs(10));
        assert_eq!(background.due(), Some(secs(150)));
    }
}
