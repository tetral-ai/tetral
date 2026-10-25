//! The termination procedure of a running task: SIGTERM to the task, after
//! [`GRACE`] SIGKILL to the launched process group and to the first process
//! of the task's PID namespace, then a wait of at most [`KILL_WAIT`] for the
//! exit, after which the supervisor stops waiting and reports no exit facts.
//!
//! On Linux SIGTERM goes to the PID namespace's first process, Codex's inner
//! stage, which passes it on to the command's process group; one sent to the
//! launched process group would end the whole sandbox at once, through
//! `bwrap`'s `--die-with-parent`, and leave the command no grace. On macOS it
//! goes to the process group.

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

    /// When the next step is due.
    pub fn due(&self) -> Duration {
        self.due
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

fn group(pid: u32) -> Option<Pid> {
    i32::try_from(pid).ok().and_then(Pid::from_raw)
}

#[cfg(target_os = "linux")]
fn signal_task(pid: u32, signal: Signal) {
    match linux::namespace_leader(pid) {
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
    let leader = linux::namespace_leader(pid);
    if let Some(group) = group(pid) {
        let _ = rustix::process::kill_process_group(group, Signal::KILL);
    }
    #[cfg(target_os = "linux")]
    if let Some(leader) = leader {
        let _ = rustix::process::kill_process(leader, Signal::KILL);
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::VecDeque;

    use rustix::process::Pid;

    /// The first process of the PID namespace below the launched process
    /// `pid`: its first descendant, breadth first, whose PID namespace lies
    /// deeper than this process's, which is the process that the namespace
    /// was created with. `None` while the sandbox has no namespace yet, or
    /// no longer.
    pub(super) fn namespace_leader(pid: u32) -> Option<Pid> {
        let own = namespace_depth("self")?;
        let mut queue = VecDeque::from(children(pid));
        while let Some(process) = queue.pop_front() {
            if namespace_depth(&process.to_string()).is_some_and(|depth| depth > own) {
                return i32::try_from(process).ok().and_then(Pid::from_raw);
            }
            queue.extend(children(process));
        }
        None
    }

    /// How many PID namespaces the process `process` of `/proc` is seen
    /// from: the number of IDs on its `NSpid` line.
    fn namespace_depth(process: &str) -> Option<usize> {
        let status = std::fs::read_to_string(format!("/proc/{process}/status")).ok()?;
        status
            .lines()
            .find_map(|line| line.strip_prefix("NSpid:"))
            .map(|ids| ids.split_whitespace().count())
    }

    /// The children of every thread of `pid`.
    fn children(pid: u32) -> Vec<u32> {
        let Ok(threads) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
            return Vec::new();
        };
        threads
            .flatten()
            .filter_map(|thread| std::fs::read_to_string(thread.path().join("children")).ok())
            .flat_map(|children| {
                children
                    .split_whitespace()
                    .filter_map(|child| child.parse().ok())
                    .collect::<Vec<u32>>()
            })
            .collect()
    }
}
