//! A task's processes on macOS: the process group that the launched
//! `sandbox-exec` leads, whose ID stands where Linux has the inode of the
//! task's PID namespace. `sandbox-exec` executes the command in its own
//! process, so the launched process is the task's first process. Within one
//! boot a process group ID is not reused while the group has members, so a
//! group with members and the recorded ID is the task's. A descendant that
//! leaves the group is not one of the task's processes.

use rustix::io::Errno;
use rustix::process::Pid;

use super::Leader;

/// The task's first process: the launched one itself.
pub fn namespace_leader(launched: u32) -> Option<u32> {
    Some(launched)
}

/// The launched process with its start time; its group is its own, as it
/// leads the session the launch made.
pub fn leader(launched: u32) -> Option<Leader> {
    Some(Leader {
        pid: launched,
        start_time: start_time(launched)?,
        namespace: u64::from(launched),
    })
}

/// Whether no process is in the process group `group`. A member that has
/// exited but is not yet reaped still counts.
pub fn namespace_empty(group: u64) -> bool {
    let Some(group) = pid(group) else {
        return true;
    };
    rustix::process::test_kill_process_group(group) == Err(Errno::SRCH)
}

/// The process group of `process`.
pub fn namespace(process: u32) -> Option<u64> {
    let group = rustix::process::getpgid(Some(pid(u64::from(process))?)).ok()?;
    u64::try_from(group.as_raw_nonzero().get()).ok()
}

/// A process group has no handle to hold.
pub fn hold_namespace(_process: u32, _group: u64) -> Option<std::fs::File> {
    None
}

/// The start time of `process` in microseconds since the epoch, from
/// `proc_pidinfo`.
pub fn start_time(process: u32) -> Option<u64> {
    let pid = libc::c_int::try_from(process).ok()?;
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).ok()?;
    // SAFETY: the buffer is a zeroed `proc_bsdinfo` of `size` bytes, which
    // the call fills for `PROC_PIDTBSDINFO`.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        return None;
    }
    // SAFETY: the call wrote the whole structure.
    let info = unsafe { info.assume_init() };
    info.pbi_start_tvsec
        .checked_mul(1_000_000)?
        .checked_add(info.pbi_start_tvusec)
}

/// The boot session UUID of the running system, `kern.bootsessionuuid`.
pub fn boot_id() -> Option<String> {
    let mut buffer = [0u8; 64];
    let mut length = buffer.len();
    // SAFETY: the name is NUL-terminated and the buffer holds `length`
    // bytes, which the call does not exceed.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            buffer.as_mut_ptr().cast(),
            &raw mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return None;
    }
    let text = buffer.get(..length)?;
    let text = text.strip_suffix(b"\0").unwrap_or(text);
    let id = std::str::from_utf8(text).ok()?.trim();
    (!id.is_empty()).then(|| id.to_owned())
}

/// Ends every process of the group `group` with SIGKILL.
pub fn kill_group(group: u64) {
    if let Some(group) = pid(group) {
        let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
    }
}

fn pid(id: u64) -> Option<Pid> {
    i32::try_from(id).ok().and_then(Pid::from_raw)
}
