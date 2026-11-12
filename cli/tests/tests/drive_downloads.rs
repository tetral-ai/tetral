//! Layer 2. Downloads of the real App Server against the reference control server,
//! through real mounts: what a command's read gets when the content's
//! download stalls, is answered `503` or is refused, and what its running
//! result says of a download it waits for; how often content that two
//! Sessions read is stored; and, on a Mac, a search of 20,000 files by
//! several readers, a read that ends with its download, and a read that
//! waits minutes for its download and through a pause of the App Server.

use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, READ_ONLY, commit, in_drive, outcome, sha256, write,
};
use tetral_tests::machine::answer_fault;
use tetral_tests::tasks::WAIT_CHECKED;

/// The statuses of the downloads of `content`, in arrival order.
fn downloads(machine: &DriveMachine, content: &str) -> Vec<Value> {
    drives::requests(&machine.engine, "download_content", DRIVE)
        .into_iter()
        .filter(|request| request["content_sha256"] == sha256(content.as_bytes()))
        .map(|request| request["status"].clone())
        .collect()
}

#[test]
fn a_download_that_stalls_is_listed_in_the_running_result_and_tried_again_after_ten_seconds() {
    let record = tetral_tests::record!();
    let mut case = record
        .case("a command reading a file whose first download stops after its first 4,096 bytes");
    let content = "0123456789abcdef".repeat(16_384);
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("big.txt", &content)]);
    });
    // The server writes a throttled download at the pace of its own clock:
    // 4,096 bytes for the one second it moves, and no more while it stands.
    answer_fault(
        &machine.engine,
        json!({"endpoint": "download_content", "action": "throttle", "bytes_per_second": 4096}),
    );
    let seams = &machine.machine().seams;
    let checked = seams.count(WAIT_CHECKED);
    let expected = machine.project.join("expected");
    std::fs::write(&expected, &content).unwrap();
    let reading = machine.start_background(
        &format!(
            "cmp {} '{}' && echo same",
            in_drive(DRIVE, "big.txt"),
            expected.display()
        ),
        2_000,
    );
    machine.wait_for_start(checked);
    wait_until("the download's request", || {
        downloads(&machine, &content).len() == 1
    });
    machine.engine.advance(Duration::from_secs(1));
    // The App Server's stall time counts from the bytes it has, so it must
    // hold them before its clock moves.
    let partial = machine.root.join("cache/tmp");
    wait_until("the App Server holds the first 4,096 bytes", || {
        std::fs::read_dir(&partial).is_ok_and(|entries| {
            entries
                .flatten()
                .any(|entry| entry.metadata().is_ok_and(|file| file.len() == 4_096))
        })
    });

    seams.advance(Duration::from_secs(2));
    let running = machine.report(&reading, "running");
    let waited = &running["result"]["drive_sync"]["downloading"];
    assert_eq!(waited.as_array().map(Vec::len), Some(1), "{running}");
    assert!(
        waited[0]["path"]
            .as_str()
            .is_some_and(|path| path.ends_with(&format!("/drives/{DRIVE}/big.txt"))),
        "{running}"
    );
    assert_eq!(
        (&waited[0]["downloaded_bytes"], &waited[0]["size_bytes"]),
        (&json!(4_096), &json!(content.len())),
        "{running}"
    );
    case.observe(format!(
        "after the command's 2-second wait its running result listed {}",
        waited[0]
    ));

    // Ten seconds without a byte end the attempt, and the next one starts
    // after the client's first retry delay of at most 1.2 seconds. Up to
    // nine seconds the clock moves a second at a time, each followed by a
    // pause: an attempt given up sooner would be followed by a second one
    // here.
    for second in 3..=9 {
        seams.advance(Duration::from_secs(1));
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            downloads(&machine, &content).len(),
            1,
            "a second download {second} seconds after the last byte"
        );
    }
    machine.advance_until(
        "the second download",
        Duration::from_millis(1_200),
        Duration::from_secs(12),
        || downloads(&machine, &content).len() == 2,
    );
    let (exit, stdout, _) = outcome(&machine.terminal(&reading));
    assert_eq!((exit, stdout.as_str()), (0, "same\n"));
    // The second download asks for the bytes the first did not bring.
    assert_eq!(downloads(&machine, &content), [json!(200), json!(206)]);
    case.observe(format!(
        "ten seconds of the App Server's clock after the last byte it asked for the rest in a second download, and the read returned all {} bytes",
        content.len()
    ));
    case.pass();
}

#[test]
fn a_download_answered_503_is_tried_again_until_it_passes() {
    let record = tetral_tests::record!();
    let mut case =
        record.case("a command reading a file whose first two downloads are answered 503");
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("waits.txt", "waited")]);
    });
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "download_content", "action": "error",
            "code": "unavailable", "status": 503, "count": 2,
        }),
    );
    // The read's wait covers the clock's steps below.
    let reading =
        machine.start_command_waiting(&format!("cat {}", in_drive(DRIVE, "waits.txt")), 50_000);
    // The client's retry delays are at most 1.2 and 2.4 seconds.
    machine.advance_until(
        "the third download",
        Duration::from_millis(2_400),
        Duration::from_secs(24),
        || downloads(&machine, "waited").len() == 3,
    );
    let (exit, stdout, _) = outcome(&machine.terminal(&reading));
    assert_eq!((exit, stdout.as_str()), (0, "waited"));
    assert_eq!(
        downloads(&machine, "waited"),
        [json!(503), json!(503), json!(200)]
    );
    case.observe("two downloads were answered 503, the third 200, and the read returned the file");
    case.pass();
}

#[test]
fn a_download_refused_as_missing_or_denied_fails_its_read_at_once() {
    let record = tetral_tests::record!();
    let machine = {
        let mut case = record.case("a drive with two files, mounted read-only");
        let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
            commit(
                engine,
                DRIVE,
                &[
                    write("missing.txt", "missing"),
                    write("refused.txt", "refused"),
                ],
            );
        });
        case.pass();
        machine
    };
    for (file, content, code, status) in [
        ("missing.txt", "missing", "content_not_found", 404),
        ("refused.txt", "refused", "drive_access_denied", 403),
    ] {
        let mut case = record.case(&format!(
            "a read of a file whose downloads are answered {code}"
        ));
        answer_fault(
            &machine.engine,
            json!({"endpoint": "download_content", "action": "error", "code": code, "count": 100}),
        );
        // No clock moves here: a download tried again after a delay would
        // wait for one, and the result would not come.
        let terminal = machine.command(&format!(
            "cat {} 2>&1; echo \"exit=$?\"",
            in_drive(DRIVE, file)
        ));
        answer_fault(&machine.engine, json!({"action": "clear"}));
        let (_, stdout, _) = outcome(&terminal);
        assert!(
            stdout.contains("Input/output error") && stdout.ends_with("exit=1\n"),
            "{terminal}"
        );
        let answered = downloads(&machine, content);
        assert!(
            !answered.is_empty() && answered.iter().all(|answer| answer == status),
            "{answered:?}"
        );
        case.observe(format!(
            "its {} downloads were answered {status} {code} and the read wrote {stdout:?} with the App Server's clock standing",
            answered.len()
        ));
        case.pass();
    }
}

#[test]
fn two_sessions_that_read_one_file_store_its_content_once() {
    const SIZE: usize = 64 << 20;
    const OTHER_SESSION: &str = "sess_2";
    let record = tetral_tests::record!();
    let mut case = record.case(
        "two Sessions on one machine that bind the same drive and read its file of 64 MiB in turn",
    );
    let content = "0123456789abcdef".repeat(SIZE / 16);
    let sum = sha256(content.as_bytes());
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("shared.bin", &content)]);
    });
    let issued = machine.prepare_with(
        OTHER_SESSION,
        "bind_other",
        &machine.project,
        &[(DRIVE, READ_ONLY)],
        &[],
    );
    let report = machine.session_report(&issued);
    assert_eq!(report["status"], "prepared", "{report}");
    machine.answers_settled();
    // The bytes the state root uses on its own file system, its mounts
    // left out.
    let usage = || -> u64 {
        let du = std::process::Command::new("du")
            .args(["-skx"])
            .arg(&machine.root)
            .output()
            .unwrap();
        let kib: u64 = String::from_utf8_lossy(&du.stdout)
            .split_whitespace()
            .next()
            .and_then(|kib| kib.parse().ok())
            .unwrap_or_else(|| panic!("du: {}", String::from_utf8_lossy(&du.stderr)));
        kib << 10
    };
    let read = |session: &str| {
        let file = machine
            .root
            .join("sessions")
            .join(session)
            .join("drives")
            .join(DRIVE)
            .join("shared.bin");
        assert_eq!(sha256(&std::fs::read(file).unwrap()), sum);
    };
    let before = usage();
    read(tetral_tests::tasks::SESSION);
    let after_first = usage();
    read(OTHER_SESSION);
    let after_second = usage();
    assert!(
        after_first - before >= SIZE as u64 && after_second.saturating_sub(after_first) < 4 << 20,
        "the state root used {before} bytes, {after_first} after the first read and {after_second} after the second"
    );
    assert_eq!(downloads(&machine, &content), [json!(200)]);
    case.observe(format!(
        "the state root used {before} bytes, {after_first} after the first Session's read and {after_second} after the second's; the content was downloaded once"
    ));
    case.pass();
}

// A Mac mount's files are read over NFS, by as many readers as search.
#[cfg(target_os = "macos")]
#[test]
fn a_search_by_several_readers_finds_in_20_000_files_of_a_mount_what_it_finds_in_a_local_copy() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a drive of 20,000 files in 100 folders, one in 50 holding the word searched, and a local copy of them; eight greps at a time search each",
    );
    let files: Vec<(String, String)> = (0..20_000)
        .map(|index| {
            let mut content = format!("file {index}\nplain text\n");
            if index % 50 == 0 {
                content += &format!("needle {index}\n");
            }
            (format!("corpus/{:03}/{index:05}.txt", index / 200), content)
        })
        .collect();
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        // A commit holds at most 10,000 changes.
        for half in files.chunks(10_000) {
            let changes: Vec<Value> = half
                .iter()
                .map(|(path, content)| write(path, content))
                .collect();
            commit(engine, DRIVE, &changes);
        }
    });
    for (path, content) in &files {
        let local = machine.project.join("local").join(path);
        std::fs::create_dir_all(local.parent().unwrap()).unwrap();
        std::fs::write(local, content).unwrap();
    }
    // Each grep writes a file of its own: lines of several writers to one
    // pipe would mix.
    let search = |from: &str, to: &str| {
        format!(
            "out=\"$PWD\"; mkdir \"$out/{to}.parts\" && cd {from} && find . -type f -print0 | xargs -0 -P 8 -n 200 sh -c 'grep -n needle /dev/null \"$@\" >> \"$0/$$\"; true' \"$out/{to}.parts\" && cat \"$out/{to}.parts\"/* | LC_ALL=C sort > \"$out/{to}.txt\""
        )
    };
    // 20,000 downloads, eight at a time, each stored before it is read:
    // some 10 ms each on Linux, with room for a Mac's NFS client.
    let limit = Duration::from_secs(20 * 60);
    let started = std::time::Instant::now();
    let mount = machine.command_within(limit, &search(&in_drive(DRIVE, "corpus"), "mount"));
    let took = started.elapsed();
    assert_eq!(outcome(&mount).0, 0, "{mount}");
    let local = machine.command_within(limit, &search("local/corpus", "local"));
    assert_eq!(outcome(&local).0, 0, "{local}");
    let compared = machine.command("cmp mount.txt local.txt && wc -l < mount.txt");
    assert_eq!(outcome(&compared).1.trim(), "400", "{compared}");
    case.observe(format!(
        "both searches found the same 400 lines; the search of the mount took {} s",
        took.as_secs()
    ));
    case.pass();
}

/// Moves the reference control server's clock and the App Server's by a
/// second for each second that passes, until `done` holds or `limit` has
/// passed: for what a Mac's NFS client times by the wall clock.
#[cfg(target_os = "macos")]
fn in_real_time(
    machine: &DriveMachine,
    limit: Duration,
    what: &str,
    mut done: impl FnMut() -> bool,
) {
    let started = std::time::Instant::now();
    let mut moved = 0;
    while !done() {
        assert!(started.elapsed() < limit, "{what} did not happen");
        if started.elapsed().as_secs() > moved {
            machine.step_together(Duration::from_secs(1));
            moved += 1;
        } else {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// The terminal report of `task`, if it has come.
#[cfg(target_os = "macos")]
fn ended(machine: &DriveMachine, task: &str) -> Option<Value> {
    machine
        .reports(task)
        .into_iter()
        .find(|report| report["phase"] == "terminal")
}

// A Mac's NFS client asks again for a READ answered `NFS3ERR_JUKEBOX` only
// after 4, 8, 16 and then every 30 seconds. A download of 24 seconds ends
// while the client's second request waits, 19 to 34 seconds after the
// first; with a wait too short to reach it, the read ends later than the
// 30 seconds the test allows.
#[cfg(target_os = "macos")]
#[test]
fn a_read_ends_with_its_download_of_24_seconds() {
    /// At 64,000 bytes a second the download takes 24 seconds.
    const BYTES: usize = 1_536_000;
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that reads a file of 1,536,000 bytes whose download arrives at 64,000 bytes a second, each second of both clocks a second of the wall clock",
    );
    let content = "a download of 24".repeat(BYTES / 16);
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("slow.bin", &content)]);
    });
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "download_content", "content_sha256": sha256(content.as_bytes()),
            "action": "throttle", "bytes_per_second": 64_000, "count": 10,
        }),
    );
    let started = std::time::Instant::now();
    let read = machine.start_background(
        &format!("cat {} > copy.bin", in_drive(DRIVE, "slow.bin")),
        50_000,
    );
    let mut terminal = None;
    in_real_time(
        &machine,
        Duration::from_secs(120),
        "the end of the read of slow.bin",
        || {
            terminal = ended(&machine, &read);
            terminal.is_some()
        },
    );
    let took = started.elapsed();
    let terminal = terminal.unwrap();
    assert_eq!(terminal["result"]["result"]["exit_code"], 0, "{terminal}");
    let copy = std::fs::read(machine.project.join("copy.bin")).unwrap();
    assert_eq!(sha256(&copy), sha256(content.as_bytes()));
    assert_eq!(downloads(&machine, &content), [json!(200)]);
    // The download took its 24 seconds, and the read ended with it.
    assert!(
        (Duration::from_secs(23)..Duration::from_secs(30)).contains(&took),
        "the read ended after {took:?}"
    );
    case.observe(format!(
        "the read ended after {:.1} s with the file's content, its one download having taken 24 s",
        took.as_secs_f64()
    ));
    case.pass();
}

// The mount is hard and its READs are answered `NFS3ERR_JUKEBOX` while a
// download goes on: what a Mac's NFS client makes of minutes of that, and
// of a server that answers nothing for 20 seconds, is its own to show.
#[cfg(target_os = "macos")]
#[test]
fn a_read_waits_minutes_for_its_download_and_through_a_pause_of_the_app_server_without_an_error() {
    /// At 64,000 bytes a second the download takes 190 seconds.
    const SLOW_BYTES: usize = 12_160_000;
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that reads a file of 12,160,000 bytes whose download arrives at 64,000 bytes a second, each second of both clocks a second of the wall clock",
    );
    let slow = "a slow download.".repeat(SLOW_BYTES / 16);
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(
            engine,
            DRIVE,
            &[
                write("slow.bin", &slow),
                write("first.txt", "first"),
                write("second.txt", "second"),
            ],
        );
    });
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "download_content", "content_sha256": sha256(slow.as_bytes()),
            "action": "throttle", "bytes_per_second": 64_000, "count": 10,
        }),
    );
    let started = std::time::Instant::now();
    let read = machine.start_background(
        &format!("cat {} > copy.bin", in_drive(DRIVE, "slow.bin")),
        50_000,
    );
    let mut terminal = None;
    in_real_time(
        &machine,
        Duration::from_secs(400),
        "the end of the read of slow.bin",
        || {
            terminal = ended(&machine, &read);
            terminal.is_some()
        },
    );
    let took = started.elapsed();
    let terminal = terminal.unwrap();
    let result = &terminal["result"]["result"];
    assert!(
        result["exit_code"] == 0
            && result["stderr"]["text"] == ""
            && took >= Duration::from_secs(180),
        "after {took:?}: {terminal}"
    );
    let copy = std::fs::read(machine.project.join("copy.bin")).unwrap();
    assert_eq!(sha256(&copy), sha256(slow.as_bytes()));
    let requests = downloads(&machine, &slow).len();
    case.observe(format!(
        "the read ended after {} s with exit 0, nothing on stderr and the file's content; {requests} download requests",
        took.as_secs()
    ));
    case.pass();

    let mut case = record.case(
        "two commands that wait for held downloads while the App Server is stopped for 20 seconds, one of them cancelled meanwhile",
    );
    for content in ["first", "second"] {
        answer_fault(
            &machine.engine,
            json!({
                "endpoint": "download_content", "content_sha256": sha256(content.as_bytes()),
                "action": "hold_request", "count": 1_000,
            }),
        );
    }
    let first = machine.start_background(
        &format!("exec cat {}", in_drive(DRIVE, "first.txt")),
        50_000,
    );
    let second = machine.start_background(
        &format!(
            "echo $$ > second.pid; exec cat {}",
            in_drive(DRIVE, "second.txt")
        ),
        50_000,
    );
    in_real_time(
        &machine,
        Duration::from_secs(60),
        "both downloads held",
        || drives::held(&machine.engine) >= 2,
    );
    let signal = |name: &str| {
        let sent = std::process::Command::new("kill")
            .args([name, &machine.machine().app_server.pid().to_string()])
            .status()
            .unwrap();
        assert!(sent.success(), "kill {name}");
    };
    signal("-STOP");
    let cancel = machine.cancel(&second, json!({}));
    std::thread::sleep(Duration::from_secs(20));
    signal("-CONT");
    answer_fault(&machine.engine, json!({"action": "clear"}));
    answer_fault(&machine.engine, json!({"action": "release"}));

    let mut reply = None;
    in_real_time(
        &machine,
        Duration::from_secs(60),
        "the reply to the cancel",
        || {
            reply = machine.accepted_reply(&cancel);
            reply.is_some()
        },
    );
    let reply = reply.unwrap();
    assert_eq!(reply["result"]["result"]["cancelled"], true, "{reply}");
    let pid = std::fs::read_to_string(machine.project.join("second.pid")).unwrap();
    in_real_time(
        &machine,
        Duration::from_secs(60),
        "the end of the cancelled read's process",
        || {
            !std::process::Command::new("kill")
                .args(["-0", pid.trim()])
                .stderr(std::process::Stdio::null())
                .status()
                .unwrap()
                .success()
        },
    );
    let mut terminal = None;
    in_real_time(
        &machine,
        Duration::from_secs(300),
        "the end of the other read",
        || {
            terminal = ended(&machine, &first);
            terminal.is_some()
        },
    );
    let terminal = terminal.unwrap();
    let result = &terminal["result"]["result"];
    assert!(
        result["exit_code"] == 0
            && result["stdout"]["text"] == "first"
            && result["stderr"]["text"] == "",
        "{terminal}"
    );
    assert_eq!(
        machine.output(&format!("cat {}", in_drive(DRIVE, "second.txt"))),
        "second"
    );
    case.observe("once the App Server ran again and the downloads passed, the cancel was answered cancelled and its read's process ended, the other read printed first with nothing on stderr, and a new read printed second");
    case.pass();
}
