//! A task's processes as the operating system shows them. On Linux they
//! are the processes of the task's PID namespace, whose first process the
//! task's record names by its ID and start time, so that no later process
//! with a reused ID is taken for it, and whose namespace is held open while
//! the task's supervisor runs, so that no later namespace with a reused
//! inode is taken for it (`hold_namespace`). Where no isolation runs a task,
//! there are none.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{
    boot_id, hold_namespace, leader, namespace, namespace_empty, namespace_leader, start_time,
};

/// The first of a task's processes inside its sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Leader {
    pub pid: u32,
    /// Its start time in clock ticks since the boot.
    pub start_time: u64,
    /// The inode of its PID namespace.
    pub namespace: u64,
}

#[cfg(not(target_os = "linux"))]
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

    pub fn boot_id() -> Option<String> {
        None
    }
}
#[cfg(not(target_os = "linux"))]
pub use none::{
    boot_id, hold_namespace, leader, namespace, namespace_empty, namespace_leader, start_time,
};
