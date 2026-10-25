//! The termination procedure of a running task: SIGTERM to the task, after
//! [`GRACE`] SIGKILL to the launched process group and to the first process
//! of the task's PID namespace, then a wait of at most [`KILL_WAIT`] for the
//! exit, after which the supervisor stops waiting and reports no exit facts.
//! At a connection's end the procedure starts with SIGKILL
//! ([`Termination::kill`]). A sent signal is never taken as the exit.
//!
//! On Linux SIGTERM goes to the PID namespace's first process, Codex's inner
//! stage, which passes it on to the command's process group; one sent to the
//! launched process group would end the whole sandbox at once, through
//! `bwrap`'s `--die-with-parent`, and leave the command no grace. SIGKILL to
//! the namespace's first process makes the kernel end every other process of
//! the namespace. On macOS both go to the process group.

use std::time::Duration;

use rustix::process::{Pid, Signal};

/// How long the task has between SIGTERM and SIGKILL.
pub const GRACE: Duration = Duration::from_secs(2);

/// How long the supervisor waits for the exit after SIGKILL.
pub const KILL_WAIT: Duration = Duration::from_secs(2);

/// The waits of the procedure, each a constant but for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Steps {
    pub grace: Duration,
    pub kill_wait: Duration,
}

impl Default for Steps {
    fn default() -> Steps {
        Steps {
            grace: GRACE,
            kill_wait: KILL_WAIT,
        }
    }
}

/// A termination in progress of the task whose launched process is `pid`,
/// with the time its next step is due on the seam clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Termination {
    pid: u32,
    steps: Steps,
    killed: bool,
    due: Duration,
}

impl Termination {
    /// Sends SIGTERM to the task whose launched process is `pid`.
    pub fn begin(pid: u32, steps: Steps) -> Termination {
        signal_task(pid, Signal::TERM);
        Termination {
            pid,
            steps,
            killed: false,
            due: state::seams::now() + steps.grace,
        }
    }

    /// Sends SIGKILL at once to the task whose launched process is `pid`,
    /// as at a connection's end.
    pub fn kill(pid: u32, steps: Steps) -> Termination {
        kill_task(pid);
        Termination {
            pid,
            steps,
            killed: true,
            due: state::seams::now() + steps.kill_wait,
        }
    }

    /// When the next step is due.
    pub fn due(&self) -> Duration {
        self.due
    }

    /// Whether SIGKILL has been sent.
    pub fn killed(&self) -> bool {
        self.killed
    }

    /// Takes the step that is due: SIGKILL after the grace, giving the step
    /// after it; `None` once the wait after SIGKILL has passed.
    pub fn step(self) -> Option<Termination> {
        if self.killed {
            return None;
        }
        kill_task(self.pid);
        Some(Termination {
            killed: true,
            due: state::seams::now() + self.steps.kill_wait,
            ..self
        })
    }
}

/// Sends SIGINT to the task whose launched process is `pid`, as [`Termination::begin`]
/// sends SIGTERM.
pub fn interrupt(pid: u32) {
    signal_task(pid, Signal::INT);
}

fn group(pid: u32) -> Option<Pid> {
    process_id(pid)
}

fn process_id(pid: u32) -> Option<Pid> {
    i32::try_from(pid).ok().and_then(Pid::from_raw)
}

fn leader(pid: u32) -> Option<Pid> {
    crate::process::namespace_leader(pid).and_then(process_id)
}

#[cfg(target_os = "linux")]
fn signal_task(pid: u32, signal: Signal) {
    match leader(pid) {
        Some(leader) => {
            let _ = rustix::process::kill_process(leader, signal);
        }
        None => {
            if let Some(group) = group(pid) {
                let _ = rustix::process::kill_process_group(group, signal);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn signal_task(pid: u32, signal: Signal) {
    if let Some(group) = group(pid) {
        let _ = rustix::process::kill_process_group(group, signal);
    }
}

fn kill_task(pid: u32) {
    #[cfg(target_os = "linux")]
    let leader = leader(pid);
    if let Some(group) = group(pid) {
        let _ = rustix::process::kill_process_group(group, Signal::KILL);
    }
    #[cfg(target_os = "linux")]
    if let Some(leader) = leader {
        let _ = rustix::process::kill_process(leader, Signal::KILL);
    }
}
