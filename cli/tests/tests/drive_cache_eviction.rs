//! Layer 2. The real App Server's content cache on a state root whose
//! volume runs short of free space, against the reference control server
//! and through a real mount: which committed content it deletes, when, and
//! what it keeps.

use std::io::{Read as _, Write as _};
use std::path::Path;
use std::time::{Duration, SystemTime};

use serde_json::Value;
use tetral_tests::app_server::{PAUSE, wait_until};
use tetral_tests::drives::{self, DRIVE, DriveMachine, READ_WRITE, commit, sha256, write};
use tetral_tests::small_volume::{free_space, on_small_volume};
use tetral_tests::tasks::SESSION;

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

/// The free space the App Server keeps on its state root's volume.
const FLOOR: u64 = 5 * GIB;

/// The size of each committed file.
const FILE: u64 = 64 * MIB;

/// Appends zeros to `filler` until `free` bytes of its volume are free.
fn fill_to(filler: &Path, free: u64) {
    let volume = filler.parent().unwrap();
    let mut left = free_space(volume)
        .checked_sub(free)
        .expect("the volume has more free space than the fill leaves");
    let mut file = std::fs::File::options()
        .create(true)
        .append(true)
        .open(filler)
        .unwrap();
    let block = vec![0_u8; usize::try_from(MIB).unwrap()];
    while left > 0 {
        let count = usize::try_from(left.min(MIB)).unwrap();
        file.write_all(&block[..count]).unwrap();
        left -= count as u64;
    }
    file.sync_all().unwrap();
}

/// Sets the last read time of `blob` to `ago` before now.
fn read_ago(blob: &Path, ago: Duration) {
    std::fs::File::options()
        .write(true)
        .open(blob)
        .unwrap()
        .set_modified(SystemTime::now() - ago)
        .unwrap();
}

#[test]
fn a_volume_short_of_free_space_loses_the_content_read_longest_ago_a_minute_after_its_read() {
    on_small_volume(
        "a_volume_short_of_free_space_loses_the_content_read_longest_ago_a_minute_after_its_read",
        evicts,
    );
}

fn evicts(volume: &Path) {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a state root on a volume of 7 GiB with three committed files of 64 MiB read in turn and a change no command commits, filled until 96 MiB less than 5 GiB is free",
    );
    let free = free_space(volume);
    assert!(
        (6 * GIB..=8 * GIB).contains(&free),
        "the volume has {free} bytes free; the test needs a volume of 6 to 8 GiB"
    );
    case.observe(format!("the volume has {free} bytes free"));

    let names = ["a.bin", "b.bin", "c.bin"];
    let sums: Vec<String> = (b'a'..=b'c')
        .map(|byte| sha256(&vec![byte; usize::try_from(FILE).unwrap()]))
        .collect();
    let machine =
        DriveMachine::start_seeded_under(&mut case, volume, &[(DRIVE, READ_WRITE)], |engine| {
            for (name, byte) in names.iter().zip(b'a'..=b'c') {
                let content = String::from_utf8(vec![byte; usize::try_from(FILE).unwrap()]);
                commit(engine, DRIVE, &[write(name, &content.unwrap())]);
            }
        });
    let mount = machine.mount(DRIVE);
    let blob = |index: usize| machine.root.join("cache/blobs").join(&sums[index]);
    let downloads = |index: usize| -> usize {
        drives::requests(&machine.engine, "download_content", DRIVE)
            .iter()
            .filter(|request| request["content_sha256"] == sums[index].as_str())
            .count()
    };
    // Reads from outside the CLI: a command would commit the pending file.
    let read = |index: usize| {
        let content = std::fs::read(mount.join(names[index])).unwrap();
        assert_eq!(sha256(&content), sums[index], "{}", names[index]);
    };
    let pending = vec![b'p'; 16 << 20];
    // A Mac's NFS client may answer the read from its own memory: the
    // Session's data holds the file's content too.
    let stored = machine
        .root
        .join("sessions")
        .join(SESSION)
        .join("content")
        .join(DRIVE);
    let pending_stays = || {
        assert!(
            std::fs::read(mount.join("pending.bin")).unwrap() == pending,
            "pending.bin changed"
        );
        let kept: Vec<_> = std::fs::read_dir(&stored)
            .unwrap()
            .map(|entry| std::fs::read(entry.unwrap().path()).unwrap())
            .collect();
        assert!(
            kept.len() == 1 && kept[0] == pending,
            "the Session's data holds {} content files",
            kept.len()
        );
        let sent = |endpoint: &str| drives::requests(&machine.engine, endpoint, DRIVE);
        assert_eq!(
            (sent("upload_content"), sent("commit")),
            (Vec::<Value>::new(), Vec::new())
        );
    };
    std::fs::write(mount.join("pending.bin"), &pending).unwrap();

    // Filled before the reads, so that they are seconds old when the space
    // runs short: after the second read 32 MiB are missing, after the third
    // 96 MiB, between one file's size and two. The App Server reads the free
    // space after each download.
    let filler = volume.join("filler");
    fill_to(&filler, FLOOR + 3 * FILE - 96 * MIB);
    // A Mac's NFS client answers the read of bytes it has read before from
    // its own memory, with no request to the App Server. Of each file it
    // reads only the first bytes here, so that the file's next read must ask
    // for the rest.
    let read_head = |index: usize| {
        let mut head = [0; 4096];
        std::fs::File::open(mount.join(names[index]))
            .unwrap()
            .read_exact(&mut head)
            .unwrap();
    };
    for index in 0..3 {
        if cfg!(target_os = "macos") {
            read_head(index);
        } else {
            read(index);
        }
        assert_eq!(downloads(index), 1);
    }
    std::thread::sleep(PAUSE);
    let short = free_space(volume);
    assert!(
        (0..3).all(|index| blob(index).exists())
            && (FLOOR - 2 * FILE + 1..FLOOR - FILE).contains(&short),
        "{short} bytes are free; the test needs all three files cached and more than one file's size and less than two missing to 5 GiB"
    );
    case.observe(format!(
        "with {short} bytes free and each file read within the last minute, the cache held all three"
    ));

    // The listing that the last download made due runs a second after the
    // listing before it, while every file was read within the last minute.
    machine.step(Duration::from_secs(1));
    std::thread::sleep(PAUSE);
    assert!((0..3).all(|index| blob(index).exists()));
    for (index, seconds) in [180, 170, 160].into_iter().enumerate() {
        read_ago(&blob(index), Duration::from_secs(seconds));
    }
    machine.step(Duration::from_secs(8));
    std::thread::sleep(PAUSE);
    assert!(
        (0..3).all(|index| blob(index).exists()),
        "content deleted before the 10th second since the App Server started"
    );
    machine.step(Duration::from_secs(1));
    wait_until("the two files read first deleted", || {
        !blob(0).exists() && !blob(1).exists()
    });
    std::thread::sleep(PAUSE);
    let freed = free_space(volume);
    assert!(blob(2).exists() && freed >= FLOOR, "{freed} bytes free");
    pending_stays();
    case.observe(format!(
        "with every read more than a minute ago, the 10th second of the App Server's clock deleted a.bin and b.bin, read first, and kept c.bin: {freed} bytes are free; pending.bin is as written and nothing of it was sent"
    ));
    case.pass();

    let mut case = record.case(
        "the kept file read again, and then one of the deleted files, whose download leaves 32 MiB missing",
    );
    read(2);
    assert_eq!(downloads(2), 1, "c.bin, kept, was downloaded again");
    read(0);
    assert_eq!(downloads(0), 2);
    let short = free_space(volume);
    assert!(short < FLOOR, "{short} bytes free");
    machine.step(Duration::from_secs(10));
    std::thread::sleep(PAUSE);
    assert!(blob(0).exists() && blob(2).exists());
    case.observe(format!(
        "c.bin was read with no download and a.bin with a second one, which left {short} bytes free; 10 seconds on the cache held both, c.bin's read being its last read now"
    ));
    case.pass();

    let mut case = record.case(
        "the volume filled until 256 MiB less than 5 GiB is free, more than all cached content, and every read more than a minute ago",
    );
    fill_to(&filler, FLOOR - 256 * MIB);
    for index in 0..3 {
        if blob(index).exists() {
            read_ago(&blob(index), Duration::from_secs(70));
        }
    }
    machine.step(Duration::from_secs(10));
    wait_until("every committed content deleted", || {
        (0..3).all(|index| !blob(index).exists())
    });
    let left = free_space(volume);
    assert!(left < FLOOR, "{left} bytes free");
    let before: Vec<usize> = (0..3).map(downloads).collect();
    for index in 0..3 {
        read(index);
        // On a Mac the client has read all of a.bin and c.bin before.
        if cfg!(target_os = "linux") || index == 1 {
            assert_eq!(downloads(index), before[index] + 1, "{}", names[index]);
        }
    }
    pending_stays();
    case.observe(format!(
        "the cache held no committed content, with {left} bytes free; each file read again was downloaded again, and pending.bin is as written with nothing of it sent"
    ));
    case.pass();
}
