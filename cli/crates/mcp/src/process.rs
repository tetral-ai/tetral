//! A local server's process, `<driver> mcp`: spawned without a sandbox in a
//! new process group, in the working directory it serves, with the App
//! Server's environment less `TETRAL_HOME`. Its stdin and stdout are the MCP
//! stdio transport; its stderr is read as it arrives and each chunk is
//! recorded as its size, never its content.

use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use rustix::process::{Pid, Signal};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// A spawned process with its transport pipes.
pub(crate) struct Process {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    group: Pid,
}

/// Spawns `<driver> mcp` in `cwd`, recording its stderr under `server`.
pub(crate) fn spawn(driver: &Path, cwd: &Path, server: &'static str) -> std::io::Result<Process> {
    let mut child = Command::new(driver)
        .arg("mcp")
        .current_dir(cwd)
        .env_remove(state::paths::ROOT_VARIABLE)
        .process_group(0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = child.id().expect("a child just spawned has its process ID");
    let group = Pid::from_raw(pid as i32).expect("a process ID is positive");
    let stdin = child.stdin.take().expect("stdin is piped");
    let stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");
    tokio::spawn(async move {
        let mut chunk = vec![0; 8_192];
        while let Ok(read) = stderr.read(&mut chunk).await {
            if read == 0 {
                break;
            }
            observe::info!(
                "mcp.server_stderr",
                mcp.server = observe::Id(server),
                mcp.stderr_bytes = read
            );
        }
    });
    Ok(Process {
        child,
        stdin,
        stdout,
        group,
    })
}

impl Process {
    pub fn group(&self) -> Pid {
        self.group
    }
}

/// Stops the process whose group is `group` and whose leader is `child`:
/// SIGTERM to the group and, when any member remains `grace` later on the
/// seam clock, SIGKILL to the group. Returns once the leader is reaped and
/// no member remains unsignalled by SIGKILL.
pub(crate) async fn stop(child: &mut Child, group: Pid, grace: Duration) {
    let deadline = state::seams::now() + grace;
    let _ = rustix::process::kill_process_group(group, Signal::TERM);
    let leader_exited = tokio::select! {
        _ = child.wait() => true,
        () = state::seams::sleep_until_counted(deadline, "mcp.stop_checked") => false,
    };
    if leader_exited {
        if !group_remains(group) {
            return;
        }
        state::seams::sleep_until_counted(deadline, "mcp.stop_checked").await;
        if !group_remains(group) {
            return;
        }
    }
    let _ = rustix::process::kill_process_group(group, Signal::KILL);
    let _ = child.wait().await;
}

/// Whether any process of `group` remains that has not exited. On macOS
/// `kill` signals no member that has exited.
#[cfg(not(target_os = "linux"))]
pub(crate) fn group_remains(group: Pid) -> bool {
    rustix::process::test_kill_process_group(group).is_ok()
}

/// Whether any process of `group` remains that has not exited. `kill` still
/// signals a member that has exited and waits to be reaped, so `/proc` is
/// read: such a member's state is `Z` with one thread, the 3rd and 20th
/// fields of its stat line as proc(5) numbers them, the 5th its group, read
/// after the command's name, which ends at the line's last `)`. When `/proc`
/// cannot be read, the group remains.
#[cfg(target_os = "linux")]
pub(crate) fn group_remains(group: Pid) -> bool {
    let group = group.as_raw_nonzero().get().to_string();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return true;
    };
    entries
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.parse::<u32>().is_ok())
        })
        .any(|entry| {
            let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
                return false;
            };
            let Some((_, fields)) = stat.rsplit_once(')') else {
                return false;
            };
            let fields: Vec<&str> = fields.split_whitespace().collect();
            let exited = fields.first() == Some(&"Z") && fields.get(17) == Some(&"1");
            fields.get(2) == Some(&group.as_str()) && !exited
        })
}

/// How a process ended: `exit code <n>` or `signal <n>`.
pub(crate) fn describe(status: ExitStatus) -> String {
    match (status.code(), status.signal()) {
        (Some(code), _) => format!("exit code {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => status.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::CommandExt;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn a_group_whose_members_have_exited_does_not_remain() {
        let mut child = std::process::Command::new("sleep")
            .arg("600")
            .process_group(0)
            .spawn()
            .unwrap();
        let group = Pid::from_child(&child);
        let running = group_remains(group);
        // SIGKILL; the member is reaped only after the checks.
        child.kill().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while group_remains(group) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let exited = group_remains(group);
        child.wait().unwrap();
        assert!(running, "the running member counts");
        assert!(
            !exited,
            "the killed, unreaped member stops counting within 10 s"
        );
    }

    #[cfg(target_os = "linux")]
    mod threads {
        use std::io::Write;
        use std::os::fd::AsRawFd;

        use super::*;

        const STACK: usize = 64 * 1024;

        /// Waits up to ten seconds for `done`, failing with `what`.
        fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !done() {
                assert!(Instant::now() < deadline, "{what} within 10 s");
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        /// What the member needs: the pipe end it waits on and the stack of
        /// the thread it starts.
        struct Plan {
            reader: libc::c_int,
            stack: *mut libc::c_void,
        }

        /// A member that leads its own group, starts a second thread, waits
        /// for a byte, then ends its own thread only, so that it reads `Z`
        /// while the second runs. Only async-signal-safe calls follow the
        /// clone of a threaded process.
        extern "C" fn member(plan: *mut libc::c_void) -> libc::c_int {
            // SAFETY: `plan` points at the parent's `Plan`, copied with its
            // memory into this process; the thread's stack lies in that copy.
            unsafe {
                let plan = &*plan.cast::<Plan>();
                let thread = libc::CLONE_VM
                    | libc::CLONE_FS
                    | libc::CLONE_FILES
                    | libc::CLONE_SIGHAND
                    | libc::CLONE_THREAD
                    | libc::CLONE_SYSVSEM;
                if libc::setpgid(0, 0) == -1
                    || libc::clone(second, plan.stack, thread, std::ptr::null_mut()) == -1
                {
                    libc::_exit(1);
                }
                let mut byte = 0u8;
                libc::read(plan.reader, (&raw mut byte).cast(), 1);
                libc::syscall(libc::SYS_exit, 0);
            }
            0
        }

        extern "C" fn second(_: *mut libc::c_void) -> libc::c_int {
            loop {
                // SAFETY: no argument; it returns only after a signal handler.
                unsafe { libc::pause() };
            }
        }

        /// The highest 16-byte aligned address of `stack`, where a downward
        /// stack starts.
        fn top(stack: &mut [u8]) -> *mut libc::c_void {
            let end = stack.as_mut_ptr_range().end as usize;
            (end & !15) as *mut libc::c_void
        }

        /// The member, killed and reaped when the case ends.
        struct Member(libc::pid_t);

        impl Drop for Member {
            fn drop(&mut self) {
                // SAFETY: the process is this one's child, not yet reaped.
                unsafe {
                    libc::kill(self.0, libc::SIGKILL);
                    libc::waitpid(self.0, std::ptr::null_mut(), 0);
                }
            }
        }

        fn state(process: libc::pid_t) -> String {
            let stat = std::fs::read_to_string(format!("/proc/{process}/stat")).unwrap();
            let (_, fields) = stat.rsplit_once(')').unwrap();
            fields.split_whitespace().next().unwrap().to_owned()
        }

        #[test]
        fn a_member_whose_first_thread_exited_while_another_runs_remains() {
            let (reader, mut writer) = std::io::pipe().unwrap();
            let mut stacks = vec![0u8; 2 * STACK];
            let (own, thread) = stacks.split_at_mut(STACK);
            let plan = Plan {
                reader: reader.as_raw_fd(),
                stack: top(thread),
            };
            // SAFETY: `member` runs on its own stack in a copy of this memory.
            let pid = unsafe {
                libc::clone(
                    member,
                    top(own),
                    libc::SIGCHLD,
                    (&raw const plan).cast_mut().cast(),
                )
            };
            assert!(pid > 0, "clone: {}", std::io::Error::last_os_error());
            let member = Member(pid);
            let group = Pid::from_raw(member.0).unwrap();
            wait_until("the member leading its group with two threads", || {
                rustix::process::getpgid(Some(group)) == Ok(group)
                    && std::fs::read_dir(format!("/proc/{pid}/task"))
                        .unwrap()
                        .count()
                        == 2
            });
            writer.write_all(b"x").unwrap();
            wait_until("the first thread exited", || state(pid) == "Z");
            assert!(group_remains(group));
        }
    }
}
