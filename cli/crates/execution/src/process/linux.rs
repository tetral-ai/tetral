//! A task's processes on Linux, through `/proc`: the first process of its
//! PID namespace, which the namespace was created with, the namespace's
//! inode, whether any process this user can inspect is still in it, and
//! the boot ID of the running system. A process that has exited keeps its
//! entry and namespace link until its parent, or the subreaper or init that
//! inherited it, reaps it.

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

/// Whether no process that this user can inspect and that has not exited
/// is in the PID namespace whose inode is `namespace`.
pub fn namespace_empty(namespace: u64) -> bool {
    processes().is_some_and(|mut processes| {
        !processes.any(|process| {
            self::namespace(process) == Some(namespace)
                && stat(process).is_some_and(|stat| !stat.exited)
        })
    })
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

/// The start time of `process` in clock ticks since the boot, while it has
/// not exited.
pub fn start_time(process: u32) -> Option<u64> {
    stat(process)
        .filter(|stat| !stat.exited)
        .map(|stat| stat.start_time)
}

/// The boot ID of the running system.
pub fn boot_id() -> std::io::Result<String> {
    Ok(std::fs::read_to_string(BOOT_ID)?.trim().to_owned())
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

/// What `/proc/<pid>/stat` gives of a process.
struct Stat {
    /// Every thread has exited and the process waits to be reaped.
    exited: bool,
    /// In clock ticks since the boot.
    start_time: u64,
}

/// The 3rd (state), 20th (threads) and 22nd (start time) fields of the
/// line as proc(5) numbers them, read after the command's name, which ends
/// at the line's last `)`. A thread-group leader that exited while another
/// of its threads runs also reads `Z`, with more than one thread.
fn stat(process: u32) -> Option<Stat> {
    let stat = std::fs::read_to_string(format!("/proc/{process}/stat")).ok()?;
    let (_, fields) = stat.rsplit_once(')')?;
    let fields: Vec<&str> = fields.split_whitespace().collect();
    Some(Stat {
        exited: *fields.first()? == "Z" && *fields.get(17)? == "1",
        start_time: fields.get(19)?.parse().ok()?,
    })
}

/// The IDs of the processes `/proc` lists, if it can be read.
fn processes() -> Option<impl Iterator<Item = u32>> {
    let entries = std::fs::read_dir("/proc").ok()?;
    Some(
        entries
            .flatten()
            .filter_map(|entry| entry.file_name().to_str()?.parse().ok()),
    )
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
