//! Each case keeps a process that has exited unreaped as its own child, as a
//! parent or subreaper that does not reap it would.

use std::process::{Child, Command};
use std::time::{Duration, Instant};

use super::*;

/// Waits up to ten seconds for `done`, failing with `what`.
fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "{what} within 10 s");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A child killed and reaped when the case ends, a failed assertion
/// included.
struct Owned(Child);

impl Drop for Owned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn kill(child: &Owned) {
    let pid = rustix::process::Pid::from_child(&child.0);
    rustix::process::kill_process(pid, rustix::process::Signal::KILL).unwrap();
}

#[test]
fn a_process_that_has_exited_has_no_start_time() {
    let child = Owned(Command::new("sleep").arg("600").spawn().unwrap());
    let pid = child.0.id();
    assert!(start_time(pid).is_some());
    kill(&child);
    wait_until("the killed, unreaped child without a start time", || {
        start_time(pid).is_none()
    });
}

#[cfg(target_os = "macos")]
#[test]
fn a_process_group_whose_members_have_exited_is_empty() {
    use std::os::unix::process::CommandExt;

    let child = Owned(
        Command::new("sleep")
            .arg("600")
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    let group = u64::from(child.0.id());
    assert!(!namespace_empty(group), "the running member counts");
    kill(&child);
    wait_until("the group of a killed, unreaped member empty", || {
        namespace_empty(group)
    });
    assert!(
        !namespace_empty(root_group_leader()),
        "a group of another user's running processes, which kill may not signal, counts"
    );
}

/// A root-owned process that leads its own process group, other than the
/// system's first processes.
#[cfg(target_os = "macos")]
fn root_group_leader() -> u64 {
    let listing = Command::new("/bin/ps")
        .args(["-axo", "pid=,pgid=,uid="])
        .output()
        .unwrap();
    String::from_utf8_lossy(&listing.stdout)
        .lines()
        .find_map(|line| {
            let fields: Vec<u64> = line
                .split_whitespace()
                .filter_map(|field| field.parse().ok())
                .collect();
            (fields.len() == 3 && fields[0] == fields[1] && fields[2] == 0 && fields[0] > 100)
                .then_some(fields[0])
        })
        .expect("a root-owned process group leader")
}

#[cfg(target_os = "linux")]
mod namespace {
    use std::io::Write;
    use std::os::fd::AsRawFd;

    use super::*;

    const STACK: usize = 64 * 1024;

    /// What the namespace's first process needs: the pipe end it waits on
    /// and the stack of the thread it starts.
    struct Plan {
        reader: libc::c_int,
        stack: *mut libc::c_void,
    }

    /// The first process of a new PID namespace: starts a second thread,
    /// waits for a byte, then ends its own thread only, so that it reads
    /// `Z` while the second runs. Only async-signal-safe calls follow the
    /// clone of a threaded process.
    extern "C" fn first(plan: *mut libc::c_void) -> libc::c_int {
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
            if libc::clone(second, plan.stack, thread, std::ptr::null_mut()) == -1 {
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

    /// The namespace's first process, killed and reaped when the case ends.
    struct First(libc::pid_t);

    impl Drop for First {
        fn drop(&mut self) {
            // SAFETY: the process is this one's child, not yet reaped.
            unsafe {
                libc::kill(self.0, libc::SIGKILL);
                libc::waitpid(self.0, std::ptr::null_mut(), 0);
            }
        }
    }

    fn state(process: u32) -> String {
        let stat = std::fs::read_to_string(format!("/proc/{process}/stat")).unwrap();
        let (_, fields) = stat.rsplit_once(')').unwrap();
        fields.split_whitespace().next().unwrap().to_owned()
    }

    #[test]
    fn a_pid_namespace_is_empty_once_every_thread_of_its_processes_has_exited() {
        let (reader, mut writer) = std::io::pipe().unwrap();
        let mut stacks = vec![0u8; 2 * STACK];
        let (own, thread) = stacks.split_at_mut(STACK);
        let plan = Plan {
            reader: reader.as_raw_fd(),
            stack: top(thread),
        };
        let flags = libc::CLONE_NEWUSER | libc::CLONE_NEWPID | libc::SIGCHLD;
        // SAFETY: `first` runs on its own stack in a copy of this memory.
        let pid =
            unsafe { libc::clone(first, top(own), flags, (&raw const plan).cast_mut().cast()) };
        assert!(pid > 0, "clone: {}", std::io::Error::last_os_error());
        let first = First(pid);
        let pid = u32::try_from(first.0).unwrap();
        let namespace = namespace(pid).unwrap();
        wait_until("the second thread started", || {
            std::fs::read_dir(format!("/proc/{pid}/task"))
                .unwrap()
                .count()
                == 2
        });
        assert!(!namespace_empty(namespace), "a running process counts");

        writer.write_all(b"x").unwrap();
        wait_until("the first thread exited", || state(pid) == "Z");
        assert!(
            !namespace_empty(namespace),
            "a process whose first thread exited while another runs counts"
        );

        // SAFETY: the process is this one's child, not yet reaped.
        assert_eq!(unsafe { libc::kill(first.0, libc::SIGKILL) }, 0);
        wait_until("the namespace of a killed, unreaped process empty", || {
            namespace_empty(namespace)
        });
        assert_eq!(super::namespace(pid), Some(namespace), "still unreaped");
    }
}
