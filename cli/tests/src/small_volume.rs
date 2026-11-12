//! The volumes a test makes for its machine's directory.
//!
//! A volume of 7 GiB for a test whose App Server must find its state root's
//! volume short of free space. On Linux it is a tmpfs, which only root
//! mounts: the test runs again as the root of a user and mount namespace of
//! its own, and the volume goes with the namespace. On a Mac it is a sparse
//! disk image the test attaches and detaches, which needs no root.
//!
//! A volume in the host's memory ([`MemoryVolume`]): `/dev/shm` on Linux, a
//! RAM disk the test attaches and detaches on a Mac.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A directory in the host's memory. Dropping it detaches a Mac's RAM disk.
pub struct MemoryVolume {
    path: PathBuf,
    #[cfg(target_os = "macos")]
    device: String,
}

impl MemoryVolume {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `/dev/shm`, or `None` on a host without it.
    #[cfg(target_os = "linux")]
    pub fn attach() -> Option<MemoryVolume> {
        let path = PathBuf::from("/dev/shm");
        path.is_dir().then_some(MemoryVolume { path })
    }

    /// A new APFS volume of 2 GiB, which takes memory only as it fills.
    #[cfg(target_os = "macos")]
    pub fn attach() -> Option<MemoryVolume> {
        static VOLUMES: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let run = |command: &mut Command| {
            let run = command.output().expect("the command runs");
            assert!(
                run.status.success(),
                "{command:?}: {}",
                String::from_utf8_lossy(&run.stderr)
            );
            String::from_utf8_lossy(&run.stdout).trim().to_owned()
        };
        // In sectors of 512 bytes.
        let device = run(Command::new("hdiutil").args(["attach", "-nomount", "ram://4194304"]));
        let volume = MemoryVolume {
            path: PathBuf::from("/Volumes").join(format!(
                "tetral-tests-{}-{}",
                std::process::id(),
                VOLUMES.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            )),
            device,
        };
        let name = volume.path.file_name().expect("the volume's name");
        run(Command::new("diskutil")
            .args(["erasevolume", "APFS"])
            .arg(name)
            .arg(&volume.device));
        Some(volume)
    }
}

#[cfg(target_os = "macos")]
impl Drop for MemoryVolume {
    fn drop(&mut self) {
        let _ = Command::new("hdiutil")
            .args(["detach", "-quiet", "-force", &self.device])
            .status();
    }
}

/// The free bytes of the volume `directory` is on, as the App Server reads
/// them.
pub fn free_space(directory: &Path) -> u64 {
    let stat = rustix::fs::statvfs(directory).expect("statvfs of the volume");
    stat.f_bavail.saturating_mul(stat.f_frsize)
}

/// Runs `body` with a directory on an empty volume of 7 GiB. `test` is the
/// name of the calling test, which on Linux runs a second time, alone, in
/// the namespace that holds the volume; only that run reaches `body`.
#[cfg(target_os = "linux")]
pub fn on_small_volume(test: &str, body: impl FnOnce(&Path)) {
    const INSIDE: &str = "TETRAL_TESTS_SMALL_VOLUME";
    const DONE: &str = "TETRAL_TESTS_SMALL_VOLUME_DONE";
    if let Some(volume) = std::env::var_os(INSIDE) {
        body(Path::new(&volume));
        let done = std::env::var_os(DONE).expect("the path of the second run's mark");
        return std::fs::write(done, "").expect("the second run's mark");
    }
    let point = tempfile::tempdir().expect("a temporary directory");
    // A second run that finds no test named `test` runs nothing and exits 0.
    let outside = tempfile::tempdir().expect("a temporary directory");
    let done = outside.path().join("done");
    let status = Command::new("unshare")
        .args(["--user", "--map-root-user", "--mount", "sh", "-c"])
        .arg(r#"mount -t tmpfs -o size=7g tmpfs "$1" && shift && exec "$@""#)
        .arg("sh")
        .arg(point.path())
        .arg(std::env::current_exe().expect("the test binary"))
        .args(["--exact", test, "--nocapture"])
        .env(INSIDE, point.path())
        .env(DONE, &done)
        .status()
        .expect("unshare runs");
    assert!(
        status.success(),
        "{test} on the small volume ended {status}"
    );
    assert!(done.exists(), "the second run ran no test named {test}");
}

#[cfg(target_os = "macos")]
pub fn on_small_volume(_test: &str, body: impl FnOnce(&Path)) {
    /// Detaches the image, with what is still mounted on its volume first.
    struct Detach<'a>(&'a Path);
    impl Drop for Detach<'_> {
        fn drop(&mut self) {
            drop(crate::drives::Unmount(self.0.to_path_buf()));
            let _ = Command::new("hdiutil")
                .args(["detach", "-quiet", "-force"])
                .arg(self.0)
                .status();
        }
    }
    let directory = tempfile::tempdir().expect("a temporary directory");
    let image = directory.path().join("small.sparseimage");
    let volume = directory.path().join("volume");
    let hdiutil = |command: &mut Command| {
        let run = command.output().expect("hdiutil runs");
        assert!(
            run.status.success(),
            "{command:?}: {}",
            String::from_utf8_lossy(&run.stderr)
        );
    };
    hdiutil(
        Command::new("hdiutil")
            .args(["create", "-quiet", "-size", "7g", "-type", "SPARSE"])
            .args(["-fs", "APFS", "-volname", "tetral-tests"])
            .arg(&image),
    );
    hdiutil(
        Command::new("hdiutil")
            .args(["attach", "-quiet", "-nobrowse", "-mountpoint"])
            .arg(&volume)
            .arg(&image),
    );
    let volume = volume.canonicalize().expect("the attached volume");
    let _detach = Detach(&volume);
    body(&volume);
}
