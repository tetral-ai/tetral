//! A task's processes on Linux, through `/proc`: the first process of its
//! PID namespace, which the namespace was created with, the namespace's
//! inode, whether any process this user can inspect is still in it, and
//! the boot ID of the running system.

use std::collections::VecDeque;
use std::fs::File;
use std::os::unix::fs::MetadataExt;

use super::Leader;

const BOOT_ID: &str = "/proc/sys/kernel/random/boot_id";

/// The first process of the PID namespace below the launched process
/// `launched`: its first descendant, breadth first, whose PID namespace
/// lies deeper than this process's. `None` while the sandbox has no
/// namespace yet, or no longer.
pub fn namespace_leader(launched: u32) -> Option<u32> {
    let own = namespace_depth("self")?;
    let mut queue = VecDeque::from(children(launched));
    while let Some(process) = queue.pop_front() {
        if namespace_depth(&process.to_string()).is_some_and(|depth| depth > own) {
            return Some(process);
        }
        queue.extend(children(process));
    }
    None
}

/// [`namespace_leader`] with its start time and its namespace.
pub fn leader(launched: u32) -> Option<Leader> {
    let pid = namespace_leader(launched)?;
    Some(Leader {
        pid,
        start_time: start_time(pid)?,
        namespace: namespace(pid)?,
    })
}

/// Whether no process that this user can inspect is in the PID namespace
/// whose inode is `namespace`.
pub fn namespace_empty(namespace: u64) -> bool {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return false;
    };
    !entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .any(|process| self::namespace(process) == Some(namespace))
}

/// The PID namespace of `process` held open, when its inode is
/// `namespace`. Once a namespace is freed, the kernel may give its inode to
/// a namespace created later; an open namespace is not freed, so while the
/// file is open the inode names no other namespace.
pub fn hold_namespace(process: u32, namespace: u64) -> Option<File> {
    let file = File::open(format!("/proc/{process}/ns/pid")).ok()?;
    (file.metadata().ok()?.ino() == namespace).then_some(file)
}

/// The inode of the PID namespace of `process`, from `/proc/<pid>/ns/pid`,
/// a link to `pid:[<inode>]`.
pub fn namespace(process: u32) -> Option<u64> {
    let link = std::fs::read_link(format!("/proc/{process}/ns/pid")).ok()?;
    link.to_str()?
        .strip_prefix("pid:[")?
        .strip_suffix(']')?
        .parse()
        .ok()
}

/// The start time of `process` in clock ticks since the boot: the 22nd
/// field of `/proc/<pid>/stat`, the 20th after the command's name, which
/// ends at the line's last `)`.
pub fn start_time(process: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{process}/stat")).ok()?;
    let (_, fields) = stat.rsplit_once(')')?;
    fields.split_whitespace().nth(19)?.parse().ok()
}

/// The boot ID of the running system.
pub fn boot_id() -> Option<String> {
    let id = std::fs::read_to_string(BOOT_ID).ok()?;
    Some(id.trim().to_owned())
}

/// How many PID namespaces `process` of `/proc` is seen from: the number of
/// IDs on its `NSpid` line.
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
