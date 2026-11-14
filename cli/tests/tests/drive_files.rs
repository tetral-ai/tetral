//! Layer 2. Files of a drive on the real App Server against the reference control
//! server, through real mounts: the names a drive refuses, the executable
//! bit, the paths that stay on the machine, and the commits a command's
//! changes become.

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until_within;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, READ_WRITE, commit, delete, in_drive, outcome, sha256, write,
    write_executable,
};
use tetral_tests::tasks::WAIT_CHECKED;

fn paths(machine: &DriveMachine) -> Vec<String> {
    drives::files(&machine.engine, DRIVE)
        .into_iter()
        .map(|(path, _)| path)
        .collect()
}

#[test]
fn a_name_that_differs_only_in_case_is_rejected_and_removed_and_a_case_rename_commits() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that writes Report.md beside report.md, then one that renames report.md to Report.md",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(
            engine,
            DRIVE,
            &[write("report.md", "report"), write("Straße.md", "strasse")],
        );
    });

    let terminal = machine.command(&format!("printf agent > {}", in_drive(DRIVE, "Report.md")));
    let (_, _, sync) = outcome(&terminal);
    let rejected = sync["rejected"].as_array().expect("rejections");
    assert_eq!(rejected.len(), 1, "{terminal}");
    assert!(
        rejected[0]["path"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/drives/{DRIVE}/Report.md")),
        "{terminal}"
    );
    assert_eq!(rejected[0]["change"], "write", "{terminal}");
    assert_eq!(rejected[0]["reason"], "path_rule", "{terminal}");
    assert_eq!(
        rejected[0]["rule"], "the path differs only in case from another file's",
        "{terminal}"
    );
    let mut listed: Vec<String> = machine
        .output(&format!("ls {}", in_drive(DRIVE, "")))
        .lines()
        .map(str::to_owned)
        .collect();
    listed.sort();
    assert_eq!(listed, ["Straße.md", "report.md"]);
    case.observe("the write of Report.md is in the result as a path_rule rejection with the commit's rule, and the file is gone from the mount");

    let terminal = machine.command(&format!(
        "cd {} && mv report.md Report.md",
        in_drive(DRIVE, "")
    ));
    let (exit, _, sync) = outcome(&terminal);
    assert_eq!(exit, 0, "{terminal}");
    assert_eq!(sync["rejected"], json!([]), "{terminal}");
    assert_eq!(paths(&machine), ["Report.md", "Straße.md"]);
    case.observe("the rename of report.md to Report.md was committed; the drive holds Report.md and Straße.md");
    case.pass();
}

#[cfg(target_os = "linux")]
#[test]
fn names_a_drive_cannot_hold_fail_with_einval_and_an_nfd_name_commits_as_nfc() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that creates a name that is not UTF-8, one with a tab and one with U+200B, renames a file to a name with a tab, and writes an NFD name",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    let script = format!(
        r#"cd {}
for name in "$(printf 'bad\377.txt')" "$(printf 'tab\tname.txt')" "$(printf 'zero\342\200\213width.txt')"; do touch "$name" 2>&1; echo "exit=$?"; done
printf ok > ok.txt
mv ok.txt "$(printf 'renamed\t.txt')" 2>&1; echo "exit=$?"
printf nfd > "$(printf 'cafe\314\201.txt')"; echo "exit=$?""#,
        in_drive(DRIVE, "")
    );
    let output = machine.output(&script);
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 9, "{output}");
    for failed in 0..4 {
        // GNU mv words EINVAL from rename(2) as a move into a subdirectory
        // of itself.
        let einval = lines[failed * 2].ends_with("Invalid argument")
            || failed == 3 && lines[failed * 2].contains("to a subdirectory of itself");
        assert!(einval, "{output}");
        assert_eq!(lines[failed * 2 + 1], "exit=1", "{output}");
    }
    assert_eq!(lines[8], "exit=0", "{output}");
    assert_eq!(paths(&machine), ["café.txt", "ok.txt"]);
    assert_eq!(
        drives::requests(&machine.engine, "upload_content", DRIVE).len(),
        2,
        "only ok.txt and the NFD file were uploaded"
    );
    case.observe(format!(
        "the command wrote {output:?}; the drive holds café.txt in NFC and ok.txt"
    ));
    case.pass();
}

/// A command that prints whether the mount shows `path` as executable. A Mac
/// mount grants every access check, so there the mode shows the bit.
fn shows_executable(path: &str) -> String {
    if cfg!(target_os = "macos") {
        format!(
            "case \"$(stat -f %Sp {})\" in ???x*) echo true;; *) echo false;; esac",
            in_drive(DRIVE, path)
        )
    } else {
        format!(
            "if test -x {}; then echo true; else echo false; fi",
            in_drive(DRIVE, path)
        )
    }
}

#[test]
fn the_executable_bit_is_committed_by_chmod_and_follows_the_drives_records() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "run.sh written by another writer, its bit set and cleared alone by that writer, made executable by a command, replaced, deleted and written again by another writer",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(engine, DRIVE, &[write("run.sh", "echo one")]);
    });
    let shows = || machine.output(&shows_executable("run.sh"));
    assert_eq!(shows(), "false");
    commit(
        &machine.engine,
        DRIVE,
        &[write_executable("run.sh", "echo one")],
    );
    assert_eq!(shows(), "true");
    commit(&machine.engine, DRIVE, &[write("run.sh", "echo one")]);
    assert_eq!(shows(), "false");
    case.observe("another writer's change of the bit alone, with the same content, showed on the mount both ways");

    machine.output(&format!("chmod +x {}", in_drive(DRIVE, "run.sh")));
    assert_eq!(
        drives::commits(&machine.engine, DRIVE),
        [json!([{
            "change": "write", "path": "run.sh", "base_sha256": sha256(b"echo one"),
            "content_sha256": sha256(b"echo one"), "executable": true,
        }])]
    );
    assert_eq!(shows(), "true");
    case.observe("chmod +x committed a write of the same content with executable true");

    commit(
        &machine.engine,
        DRIVE,
        &[write_executable("run.sh", "echo two")],
    );
    assert_eq!(shows(), "true");
    assert_eq!(
        machine.output(&format!("cat {}", in_drive(DRIVE, "run.sh"))),
        "echo two"
    );
    commit(&machine.engine, DRIVE, &[delete("run.sh")]);
    commit(&machine.engine, DRIVE, &[write("run.sh", "echo three")]);
    assert_eq!(shows(), "false");
    case.observe("the mount showed the bit after an executable replacement and showed none after a deletion and a new write without it");
    case.pass();
}

#[test]
fn a_file_another_writer_committed_executable_runs_directly_on_the_mount() {
    let record = tetral_tests::record!();
    let mut case =
        record.case("a command that runs ./run.sh, committed executable by another writer");
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(
            engine,
            DRIVE,
            &[write_executable("bin/run.sh", "#!/bin/sh\necho ran\n")],
        );
    });
    assert_eq!(
        machine.output(&format!("cd {} && ./run.sh", in_drive(DRIVE, "bin"))),
        "ran"
    );
    case.observe("./run.sh printed ran");
    case.pass();
}

#[test]
fn a_never_synced_path_follows_the_drive_until_the_machine_writes_it_and_is_never_committed() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "node_modules/x.js read after two versions of another writer, written by a command, and read after a third",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(engine, DRIVE, &[write("node_modules/x.js", "v1")]);
    });
    let read = || machine.output(&format!("cat {}", in_drive(DRIVE, "node_modules/x.js")));
    assert_eq!(read(), "v1");
    commit(&machine.engine, DRIVE, &[write("node_modules/x.js", "v2")]);
    assert_eq!(read(), "v2");
    machine.output(&format!(
        "printf mine > {}",
        in_drive(DRIVE, "node_modules/x.js")
    ));
    commit(&machine.engine, DRIVE, &[write("node_modules/x.js", "v3")]);
    assert_eq!(read(), "mine");
    assert_eq!(
        drives::requests(&machine.engine, "upload_content", DRIVE),
        [] as [Value; 0]
    );
    assert_eq!(
        drives::requests(&machine.engine, "commit", DRIVE),
        [] as [Value; 0]
    );
    case.observe("the machine read v1 and v2, then its own write also after v3; it sent no upload and no commit");
    case.pass();
}

#[test]
fn an_empty_folder_stays_on_the_machine_and_a_folder_without_files_leaves_the_mount() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that makes an empty folder, then another writer's deletion of the only file of a folder",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(engine, DRIVE, &[write("full/only.txt", "only")]);
    });
    let list = || machine.output(&format!("ls -A {}", in_drive(DRIVE, "")));
    assert_eq!(list(), "full");
    machine.output(&format!("mkdir {}", in_drive(DRIVE, "empty")));
    assert_eq!(list(), "empty\nfull");
    commit(&machine.engine, DRIVE, &[delete("full/only.txt")]);
    assert_eq!(list(), "empty");
    assert_eq!(
        drives::requests(&machine.engine, "commit", DRIVE),
        [] as [Value; 0]
    );
    assert_eq!(paths(&machine), [] as [&str; 0]);
    case.observe("the empty folder is in no commit; full left the mount with its last file");
    case.pass();
}

#[test]
fn creating_changing_and_deleting_a_file_are_three_commits_with_their_bases() {
    let record = tetral_tests::record!();
    let mut case = record.case("three commands that create, change and delete notes.txt");
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    let file = in_drive(DRIVE, "notes.txt");
    machine.output(&format!("printf one > {file}"));
    machine.output(&format!("printf two > {file}"));
    let terminal = machine.command(&format!("rm {file}"));
    assert_eq!(outcome(&terminal).2["deleted"], 1, "{terminal}");
    assert_eq!(
        drives::commits(&machine.engine, DRIVE),
        [
            json!([{"change": "write", "path": "notes.txt", "base_sha256": null,
                    "content_sha256": sha256(b"one"), "executable": false}]),
            json!([{"change": "write", "path": "notes.txt", "base_sha256": sha256(b"one"),
                    "content_sha256": sha256(b"two"), "executable": false}]),
            json!([{"change": "delete", "path": "notes.txt", "base_sha256": sha256(b"two")}]),
        ]
    );
    case.observe("the three commits carry a write without base, a write based on the first content and a deletion based on the second");
    case.pass();
}

#[test]
fn files_a_command_deleted_and_another_writer_restored_are_back_without_an_upload() {
    const FILES: usize = 3_000;
    /// A file operation through a mount is stored before it returns, which
    /// takes milliseconds on a disk: each step of 3,000 gets minutes.
    const LARGE_WORK: Duration = Duration::from_secs(600);
    let record = tetral_tests::record!();
    let mut case = record.case(
        "3,000 files written into a mount outside any command while a command runs, a command after their uploads, one that deletes them all, a later write, another writer's commit that writes the 3,000 again, and a command that lists and reads them",
    );
    let restore: Vec<Value> = (0..FILES)
        .map(|index| write(&format!("dir/f{index:04}"), &format!("{index:04}")))
        .collect();
    let machine = DriveMachine::start_seeded_in_memory(&mut case, &[(DRIVE, READ_WRITE)], |_| {});
    let uploaded = || {
        drives::requests(&machine.engine, "upload_content", DRIVE)
            .iter()
            .filter(|request| request["status"] == 200 || request["status"] == 201)
            .count()
    };
    let stop = machine.project.join("stop");
    let checked = machine.machine().seams.count(WAIT_CHECKED);
    let running = machine.start_background(
        &format!("while [ ! -e '{}' ]; do sleep 0.05; done", stop.display()),
        1_000,
    );
    machine.wait_for_start(checked);
    machine.step(Duration::from_secs(1));
    machine.report(&running, "running");
    let mount = machine.mount(DRIVE);
    let writing = Instant::now();
    std::fs::create_dir(mount.join("dir")).unwrap();
    for index in 0..FILES {
        std::fs::write(
            mount.join(format!("dir/f{index:04}")),
            format!("{index:04}"),
        )
        .unwrap();
    }
    let written = writing.elapsed();
    // The uploads start two seconds of the App Server's clock after the
    // last write.
    machine.step(Duration::from_secs(2));
    wait_until_within(LARGE_WORK, "an accepted upload of each file", || {
        uploaded() == FILES
    });
    assert!(drives::commits(&machine.engine, DRIVE).is_empty());
    let terminal = machine.command_within(LARGE_WORK, "true");
    assert_eq!(outcome(&terminal).2["committed"], FILES, "{terminal}");
    let commits = drives::commits(&machine.engine, DRIVE);
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].as_array().map(Vec::len), Some(FILES));
    case.observe(format!(
        "the 3,000 writes took {written:.0?}; each file's upload was accepted with no commit, and the next command's drive_sync has committed 3000 in one commit, {:.0?} after the first write",
        writing.elapsed()
    ));

    let deleting = Instant::now();
    let terminal =
        machine.command_within(LARGE_WORK, &format!("rm -rf {}", in_drive(DRIVE, "dir")));
    let (exit, _, sync) = outcome(&terminal);
    assert_eq!(exit, 0, "{terminal}");
    assert_eq!(sync["deleted"], FILES, "{terminal}");
    let deleted = deleting.elapsed();
    let terminal = machine.command(&format!("printf later > {}", in_drive(DRIVE, "later.txt")));
    assert_eq!(outcome(&terminal).2["committed"], 1, "{terminal}");
    assert_eq!(paths(&machine), ["later.txt"]);
    case.observe(format!(
        "the deleting command's drive_sync has deleted 3000, {deleted:.0?} after it was issued; later.txt was committed after it"
    ));

    let uploads = drives::requests(&machine.engine, "upload_content", DRIVE).len();
    commit(&machine.engine, DRIVE, &restore);
    assert_eq!(
        machine.output(&format!(
            "cd {} && ls dir | wc -l | tr -d ' ' && cat dir/f1234 later.txt",
            in_drive(DRIVE, "")
        )),
        "3000\n1234later"
    );
    assert_eq!(paths(&machine).len(), FILES + 1);
    assert_eq!(
        drives::requests(&machine.engine, "upload_content", DRIVE).len(),
        uploads
    );
    case.observe("after the restoring commit the next command listed 3,000 files and read one of them and later.txt; the machine uploaded nothing more");
    std::fs::write(&stop, "").unwrap();
    machine.terminal(&running);
    case.pass();
}
