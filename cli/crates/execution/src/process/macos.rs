//! A task's processes on macOS: the process group that the launched
//! `sandbox-exec` leads, whose ID stands where Linux has the inode of the
//! task's PID namespace. `sandbox-exec` executes the command in its own
//! process, so the launched process is the task's first process. Within one
//! boot a process group ID is not reused while the group has members, a new
//! group takes its leader's process ID, and every process of a task runs as
//! the App Server's user, so only a reused group whose own leader has
//! already exited can still be taken for the task's: a live process with the
//! recorded leader's ID but another start time, or a process of another user
//! in the group, shows that the group is not the task's. A descendant that
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

/// Whether no process that has not exited is in the process group `group`.
/// `kill` signals no member that has exited: it answers `ESRCH` for a group
/// without members and `EPERM` for one where it may signal none, which holds
/// only such members or another user's processes. `proc_pidinfo` gives
/// nothing of a member that has exited.
pub fn namespace_empty(group: u64) -> bool {
    let Some(group) = pid(group) else {
        return true;
    };
    match rustix::process::test_kill_process_group(group) {
        Err(Errno::SRCH) => true,
        Err(Errno::PERM) => members(group)
            .into_iter()
            .all(|member| owner(member).is_none()),
        _ => false,
    }
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
/// `proc_pidinfo`, while it has not exited.
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
pub fn boot_id() -> super::BootId {
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
        return Err(super::unread_boot_id(&std::io::Error::last_os_error()));
    }
    let text = buffer.get(..length).unwrap_or_default();
    let text = text.strip_suffix(b"\0").unwrap_or(text);
    match std::str::from_utf8(text).map(str::trim) {
        Ok(id) if !id.is_empty() => Ok(id.to_owned()),
        _ => Err((0, "the boot session UUID is not text".to_owned())),
    }
}

/// Whether a process in the group `group` belongs to a user other than
/// this process's effective user.
pub fn group_has_another_user(group: u64) -> bool {
    let Some(group) = pid(group) else {
        return false;
    };
    let user = rustix::process::geteuid().as_raw();
    members(group)
        .into_iter()
        .filter_map(owner)
        .any(|owner| owner != user)
}

/// The processes `proc_listpgrppids` lists in the group `group`, including
/// those that have exited and wait to be reaped.
fn members(group: Pid) -> Vec<libc::pid_t> {
    let group = group.as_raw_nonzero().get();
    // SAFETY: without a buffer the call writes nothing and gives how many
    // process IDs a list may hold.
    let capacity = unsafe { libc::proc_listpgrppids(group, std::ptr::null_mut(), 0) };
    let Ok(capacity) = usize::try_from(capacity) else {
        return Vec::new();
    };
    let mut members: Vec<libc::pid_t> = vec![0; capacity];
    let Ok(size) = libc::c_int::try_from(std::mem::size_of_val(members.as_slice())) else {
        return Vec::new();
    };
    // SAFETY: the buffer holds `size` bytes of process IDs, which the call
    // does not exceed.
    let listed = unsafe { libc::proc_listpgrppids(group, members.as_mut_ptr().cast(), size) };
    members.truncate(usize::try_from(listed).unwrap_or(0));
    members
}

/// The effective user of `process`, from `proc_pidinfo`; none once it has
/// exited.
fn owner(process: libc::pid_t) -> Option<libc::uid_t> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdshortinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdshortinfo>()).ok()?;
    // SAFETY: the buffer is a zeroed `proc_bsdshortinfo` of `size` bytes,
    // which the call fills for `PROC_PIDT_SHORTBSDINFO`.
    let written = unsafe {
        libc::proc_pidinfo(
            process,
            libc::PROC_PIDT_SHORTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    // SAFETY: the call wrote the whole structure.
    (written == size).then(|| unsafe { info.assume_init() }.pbsi_uid)
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
