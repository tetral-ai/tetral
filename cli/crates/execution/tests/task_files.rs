//! Real-filesystem checks of the task-owned cleanup boundary.
use execution::store::TaskFiles;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};

#[test]
fn cleanup_removes_read_only_task_directories_without_following_links() {
    let root = tempfile::tempdir().unwrap();
    let logs = root.path().join("logs");
    let temporary = root.path().join("temporary");
    let outside = root.path().join("project");
    fs::create_dir_all(temporary.join("read-only/nested")).unwrap();
    fs::create_dir(&logs).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(temporary.join("read-only/nested/file"), "task data").unwrap();
    fs::write(outside.join("kept"), "project data").unwrap();
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o500)).unwrap();
    symlink(&outside, temporary.join("link")).unwrap();
    fs::set_permissions(
        temporary.join("read-only/nested"),
        fs::Permissions::from_mode(0o000),
    )
    .unwrap();
    fs::set_permissions(
        temporary.join("read-only"),
        fs::Permissions::from_mode(0o500),
    )
    .unwrap();
    let files = TaskFiles::new(logs.clone(), temporary.clone(), 0);
    for (part, result) in files.remove() {
        result.unwrap_or_else(|error| panic!("{part}: {error}"));
    }
    assert!(!temporary.exists());
    assert!(!logs.exists());
    assert_eq!(
        fs::read_to_string(outside.join("kept")).unwrap(),
        "project data"
    );
    assert_eq!(
        fs::metadata(&outside).unwrap().permissions().mode() & 0o777,
        0o500
    );
    fs::set_permissions(outside, fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn a_temporary_cleanup_failure_does_not_skip_log_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let logs = root.path().join("logs");
    fs::create_dir(&logs).unwrap();
    fs::write(logs.join("stdout.log"), "output").unwrap();
    let parent = root.path().join("not-a-directory");
    fs::write(&parent, "file").unwrap();
    let files = TaskFiles::new(logs.clone(), parent.join("temporary"), 0);
    let [(part, temporary), (_, logs_result)] = files.remove();
    assert_eq!(part, "temporary");
    assert_eq!(
        temporary.unwrap_err().source.kind(),
        std::io::ErrorKind::NotADirectory
    );
    logs_result.unwrap();
    assert!(!logs.exists());
    assert_eq!(fs::read_to_string(parent).unwrap(), "file");
}

#[test]
fn a_task_parent_must_be_private_to_its_owner() {
    let root = tempfile::tempdir().unwrap();
    for (name, mode, accepted) in [("private", 0o700, true), ("shared", 0o755, false)] {
        let parent = root.path().join(name);
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(mode)).unwrap();
        let files = TaskFiles::new(
            root.path().join(format!("logs-{name}")),
            parent.join("task"),
            0,
        );
        assert_eq!(files.create().is_ok(), accepted, "{name}");
    }
}
