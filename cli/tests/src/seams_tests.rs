//! The seam clock's part of a failure's records never blocks. A module of
//! the library, not of `seams`, which the drive's NFS test includes as a file.

use std::thread;
use std::time::Duration;

use crate::seams::Seams;

#[test]
fn a_failure_reads_the_clock_without_waiting_for_a_held_state() {
    let directory = tempfile::tempdir().unwrap();
    let seams = Seams::serve(directory.path(), Duration::from_secs(1000));
    let (taken, held) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let shared = seams.shared.clone();
    let holder = thread::spawn(move || {
        let _state = shared.lock();
        taken.send(()).unwrap();
        let _ = released.recv();
    });
    held.recv().unwrap();
    let while_held = seams.shared.standing();
    release.send(()).unwrap();
    holder.join().unwrap();
    assert!(
        while_held.contains("its standing is not known"),
        "{while_held}"
    );
    let after = seams.shared.standing();
    assert!(after.starts_with("stands at 1000.000 s"), "{after}");
}
