//! A task's processes as the operating system shows them. On Linux they
//! are the processes of the task's PID namespace, whose first process the
//! task's record names by its ID and start time, so that no later process
//! with a reused ID is taken for it, and whose namespace is held open until
//! its processes are confirmed gone, so that no later namespace with a
//! reused inode is taken for it (`hold_namespace`). On macOS they are the
//! process group that the launched process leads, and the group's ID stands
//! where Linux has the namespace's inode (`macos.rs`). Where no isolation
//! runs a task, there are none. A process that has exited and waits to be
//! reaped is gone: no check here counts it, whoever its parent is.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
#[cfg(target_os = "linux")]
pub use linux::{
    boot_id, hold_namespace, leader, namespace, namespace_empty, namespace_leader, start_time,
};
#[cfg(target_os = "macos")]
pub use macos::{
    boot_id, hold_namespace, leader, namespace, namespace_empty, namespace_leader, start_time,
};

/// The first of a task's processes inside its sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Leader {
    pub pid: u32,
    /// Its start time: clock ticks since the boot on Linux, microseconds
    /// since the epoch on macOS.
    pub start_time: u64,
    /// The inode of its PID namespace on Linux; its process group's ID on
    /// macOS.
    pub namespace: u64,
}

/// Ends with SIGKILL the processes of the task whose record names `leader`
/// and `namespace`, of which `leader` is alive or which still has
/// processes. On Linux SIGKILL to the leader makes the kernel end its whole
/// namespace; on macOS the group's members outlive the leader, so the
/// signal goes to the group.
pub fn kill_recorded(leader: u32, namespace: u64) {
    #[cfg(target_os = "macos")]
    {
        let _ = leader;
        macos::kill_group(namespace);
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = namespace;
        if let Some(pid) = i32::try_from(leader)
            .ok()
            .and_then(rustix::process::Pid::from_raw)
        {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
        }
    }
}

/// The `error.os_code` and cause of a boot ID that could not be read.
pub fn boot_id_unread(error: &std::io::Error) -> (i32, String) {
    (
        error.raw_os_error().unwrap_or(0),
        format!("the boot ID could not be read: {error}"),
    )
}

/// Whether the processes that a record of this boot names cannot be the
/// task's, so that its record is confirmed without a signal: on macOS a live
/// process with the recorded leader's ID and another start time than
/// `recorded_start`, or a process of another user in the recorded group
/// (`macos.rs`); never on Linux.
pub fn not_the_tasks(leader: u32, recorded_start: u64, namespace: u64) -> bool {
    #[cfg(target_os = "macos")]
    {
        macos::start_time(leader).is_some_and(|start| start != recorded_start)
            || macos::group_has_another_user(namespace)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (leader, recorded_start, namespace);
        false
    }
}

/// Whether a recorded task whose leader has ended but whose processes
/// remain must still be signalled: on macOS, where they outlive it.
pub const MEMBERS_OUTLIVE_THE_LEADER: bool = cfg!(target_os = "macos");

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod none {
    use super::Leader;

    pub fn namespace_leader(_launched: u32) -> Option<u32> {
        None
    }

    pub fn leader(_launched: u32) -> Option<Leader> {
        None
    }

    pub fn namespace_empty(_namespace: u64) -> bool {
        true
    }

    pub fn namespace(_process: u32) -> Option<u64> {
        None
    }

    pub fn hold_namespace(_process: u32, _namespace: u64) -> Option<std::fs::File> {
        None
    }

    pub fn start_time(_process: u32) -> Option<u64> {
        None
    }

    pub fn boot_id() -> std::io::Result<String> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub use none::{
    boot_id, hold_namespace, leader, namespace, namespace_empty, namespace_leader, start_time,
};
