//! Drive mounts on macOS through the App Server's NFS server: file handles
//! across a restart and after a detach, synchronous writes, reads waiting
//! for a download, the top folder's time, exports of another port, changes
//! that record no task, AppleDouble names, file locks, a stale mount whose
//! server is stopped, read-only mounts and their exports, and directory
//! listings in pages while their entries change. Requests go through real
//! mounts and through `nfs3_client`.
#![cfg(target_os = "macos")]

mod support;

#[allow(dead_code)]
#[path = "../../../tests/src/seams.rs"]
mod seams;

use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::Duration;

use drive::mount::{Bound, Mounting};
use drive::state::Write;
use nfs3_client::nfs3_types::mount::mountstat3;
use nfs3_client::nfs3_types::nfs3::{
    CREATE3args, GETATTR3args, LOOKUP3args, MKDIR3args, Nfs3Option, Nfs3Result, READ3args,
    READDIRPLUS3args, REMOVE3args, RENAME3args, RMDIR3args, SETATTR3args, SYMLINK3args, WRITE3args,
    cookieverf3, createhow3, createverf3, diropargs3, fattr3, nfs_fh3, nfsstat3, nfstime3, sattr3,
    stable_how, symlinkdata3,
};
use nfs3_client::nfs3_types::xdr_codec::Opaque;
use nfs3_client::tokio::{TokioConnector, TokioIo};
use nfs3_client::{ConnectError, MountError, Nfs3Connection, Nfs3ConnectionBuilder};
use protocol::id::{Id, SafeId};
use serde_json::{Value, json};
use support::{DRIVE, Env, Session, Unmount, key, outcome, task};

const SESSION: &str = "sess_a";
/// Set in the helper process: the mount point it mounts its own NFS server
/// at.
const HELPER: &str = "TETRAL_TEST_NFS_HELPER";
const MOUNTED: &str = "helper mounted";

type Client = Nfs3Connection<TokioIo<tokio::net::TcpStream>>;

fn bound(read_only: bool) -> Vec<Bound> {
    vec![Bound {
        drive_id: SafeId::new(DRIVE).unwrap(),
        read_only,
        exclude: Vec::new(),
    }]
}

fn port(env: &Env) -> u16 {
    match &env.mounting {
        Mounting::Nfs(server) => server.port(),
        other => panic!("no NFS server: {other:?}"),
    }
}

fn export(port: u16) -> String {
    format!("/{port}/{SESSION}/{DRIVE}")
}

/// `P` of `DRIVE`.
fn point(env: &Env) -> PathBuf {
    env.session_dir(SESSION).join("drives").join(DRIVE)
}

/// The Session with `DRIVE` mounted from a new NFS server, as after an App
/// Server start with drives enabled; `fresh` makes its sync state.
async fn mounted(env: &mut Env, fresh: bool, read_only: bool) -> Session {
    env.mounting = drive::mount_macos::probe(true);
    let session = if fresh {
        env.session(SESSION, Write::default(), false).await
    } else {
        env.reopen(SESSION, false).await
    };
    let failures = tokio::time::timeout(
        Duration::from_secs(20),
        session.sync.mount_drives(bound(read_only)),
    )
    .await
    .expect("the mount returns");
    assert_eq!(failures, []);
    session.caught_up().await;
    session
}

async fn connect(port: u16, path: &str) -> Result<Client, ConnectError> {
    Nfs3ConnectionBuilder::new(TokioConnector, "127.0.0.1", path)
        .connect_from_privileged_port(false)
        .mount_port(port)
        .nfs3_port(port)
        .mount()
        .await
}

fn at(dir: &nfs_fh3, name: &str) -> diropargs3<'static> {
    diropargs3 {
        dir: dir.clone(),
        name: name.as_bytes().to_vec().into(),
    }
}

fn result<T, E>(result: Nfs3Result<T, E>) -> Result<T, nfsstat3> {
    match result {
        Nfs3Result::Ok(ok) => Ok(ok),
        Nfs3Result::Err((status, _)) => Err(status),
    }
}

async fn lookup(client: &mut Client, dir: &nfs_fh3, name: &str) -> Result<nfs_fh3, nfsstat3> {
    let args = LOOKUP3args {
        what: at(dir, name),
    };
    result(client.lookup(&args).await.unwrap()).map(|ok| ok.object)
}

async fn getattr(client: &mut Client, file: &nfs_fh3) -> Result<fattr3, nfsstat3> {
    let args = GETATTR3args {
        object: file.clone(),
    };
    result(client.getattr(&args).await.unwrap()).map(|ok| ok.obj_attributes)
}

async fn mtime(client: &mut Client, file: &nfs_fh3) -> (u32, u32) {
    let nfstime3 { seconds, nseconds } = getattr(client, file).await.unwrap().mtime;
    (seconds, nseconds)
}

async fn read(client: &mut Client, file: &nfs_fh3) -> Result<Vec<u8>, nfsstat3> {
    let args = READ3args {
        file: file.clone(),
        offset: 0,
        count: 4_096,
    };
    result(client.read(&args).await.unwrap()).map(|ok| ok.data.to_vec())
}

/// WRITE of `data` at the start of `file`; how the reply says it is stable.
async fn write(client: &mut Client, file: &nfs_fh3, data: &[u8]) -> Result<stable_how, nfsstat3> {
    let args = WRITE3args {
        file: file.clone(),
        offset: 0,
        count: data.len() as u32,
        stable: stable_how::UNSTABLE,
        data: Opaque::borrowed(data),
    };
    result(client.write(&args).await.unwrap()).map(|ok| ok.committed)
}

/// A new file `name` in `dir`: its handle and `file_id`.
async fn create(client: &mut Client, dir: &nfs_fh3, name: &str) -> (nfs_fh3, i64) {
    let args = CREATE3args {
        where_: at(dir, name),
        how: createhow3::GUARDED(sattr3::default()),
    };
    let made = result(client.create(&args).await.unwrap()).unwrap();
    let (Nfs3Option::Some(file), Nfs3Option::Some(attr)) = (made.obj, made.obj_attributes) else {
        panic!("CREATE answers the handle and attributes");
    };
    let file_id = attr.fileid & ((1 << drive::nfs::FILE_ID_BITS) - 1);
    (file, i64::try_from(file_id).unwrap())
}

/// One READDIRPLUS page of `dir` after `cookie`: its names with their
/// cookies, its verifier and `eof`.
async fn page(
    client: &mut Client,
    dir: &nfs_fh3,
    cookie: u64,
    cookieverf: cookieverf3,
) -> Result<(Vec<(String, u64)>, cookieverf3, bool), nfsstat3> {
    let args = READDIRPLUS3args {
        dir: dir.clone(),
        cookie,
        cookieverf,
        dircount: 4_096,
        maxcount: 8_192,
    };
    result(client.readdirplus(&args).await.unwrap()).map(|ok| {
        let names = ok.reply.entries.0.iter().map(|entry| {
            (
                String::from_utf8_lossy(entry.name.as_ref()).into_owned(),
                entry.cookie,
            )
        });
        (names.collect(), ok.cookieverf, ok.reply.eof)
    })
}

/// Runs `work` on a blocking thread, since NFS requests of this process
/// are served by tasks of the test's runtime.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handle_answers_io_once_detached_stale_once_closed_and_reads_after_a_restart() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    env.stub.list("a.txt", "alpha");
    let session = mounted(&mut env, true, false).await;
    let old_port = port(&env);
    let mut old = connect(old_port, &export(old_port)).await.unwrap();
    let top = old.root_nfs_fh3();
    let file = lookup(&mut old, &top, "a.txt").await.unwrap();
    assert_eq!(read(&mut old, &file).await, Ok(b"alpha".to_vec()));

    // The App Server's shutdown detaches the mount: nothing is stored.
    session.sync.detach_all();
    let before = session.tables().await;
    assert_eq!(
        drive::mount_macos::listed(&point(&env)).unwrap(),
        None,
        "the detach unmounted P"
    );
    assert_eq!(
        getattr(&mut old, &file).await.err(),
        Some(nfsstat3::NFS3ERR_IO)
    );
    assert_eq!(
        write(&mut old, &file, b"lost").await,
        Err(nfsstat3::NFS3ERR_IO)
    );
    assert_eq!(session.tables().await, before);

    // Once the drive sync ends, the server knows the generation no more.
    session.sync.close().await;
    assert_eq!(read(&mut old, &file).await, Err(nfsstat3::NFS3ERR_STALE));

    // The next App Server serves the cursor's generation again.
    let _session = mounted(&mut env, false, false).await;
    assert_ne!(port(&env), old_port);
    let mut client = connect(port(&env), &export(port(&env))).await.unwrap();
    assert_eq!(read(&mut client, &file).await, Ok(b"alpha".to_vec()));
    assert_eq!(client.root_nfs_fh3(), top);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_is_stored_before_its_reply_and_entries_change_the_top_folders_time() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    let session = mounted(&mut env, true, false).await;
    let mut client = connect(port(&env), &export(port(&env))).await.unwrap();
    let top = client.root_nfs_fh3();

    let listed = mtime(&mut client, &top).await;
    let (file, file_id) = create(&mut client, &top, "new.txt").await;
    let created = mtime(&mut client, &top).await;
    assert!(created > listed, "CREATE: {created:?} after {listed:?}");

    assert_eq!(
        write(&mut client, &file, b"written").await,
        Ok(stable_how::FILE_SYNC)
    );
    let content = env
        .session_dir(SESSION)
        .join("content")
        .join(DRIVE)
        .join(file_id.to_string());
    assert_eq!(std::fs::read(&content).unwrap(), b"written");

    let args = RENAME3args {
        from: at(&top, "new.txt"),
        to: at(&top, "moved.txt"),
    };
    result(client.rename(&args).await.unwrap()).unwrap();
    let renamed = mtime(&mut client, &top).await;
    assert!(renamed > created, "RENAME: {renamed:?} after {created:?}");

    let args = REMOVE3args {
        object: at(&top, "moved.txt"),
    };
    result(client.remove(&args).await.unwrap()).unwrap();
    let removed = mtime(&mut client, &top).await;
    assert!(removed > renamed, "REMOVE: {removed:?} after {renamed:?}");
    // The removed entry's handle names no file.
    assert_eq!(read(&mut client, &file).await, Err(nfsstat3::NFS3ERR_STALE));
    assert!(session.tables().await.pending_changes.is_empty());
}

/// A separate process gives the sync observer its own seam environment.
/// The owner clock stays still, so background uploads cannot supply a sync.
#[test]
fn writes_sync_ordinary_and_local_only_content_before_reply() {
    const CHILD: &str = "TETRAL_TEST_NFS_SYNC_HELPER";
    const REPLIED: &str = "nfs.write_replied";
    const RETURNED: &str = "nfs.write_returned";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut env = Env::new();
                let _unmount = Unmount(env.root.path().to_path_buf());
                let session = mounted(&mut env, true, false).await;
                let mut client = connect(port(&env), &export(port(&env))).await.unwrap();
                let top = client.root_nfs_fh3();
                for name in ["ordinary.txt", "._metadata"] {
                    let (file, _) = create(&mut client, &top, name).await;
                    assert_eq!(
                        write(&mut client, &file, b"persisted bytes").await,
                        Ok(stable_how::FILE_SYNC)
                    );
                    state::seams::count(REPLIED);
                    state::seams::barrier(RETURNED).await;
                }
                assert_eq!(env.stub.puts_most.load(Ordering::SeqCst), 0);
                session.sync.close().await;
            });
        return;
    }

    // A short socket path fits Darwin's sockaddr_un. This also owns every
    // child mount, including cleanup if the child is killed on an assertion.
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    struct ChildMounts(PathBuf);
    impl Drop for ChildMounts {
        fn drop(&mut self) {
            // Env creates its root inside TMPDIR. Unmount expects that
            // actual root, not the parent containing all child roots.
            if let Ok(entries) = std::fs::read_dir(&self.0) {
                for entry in entries.flatten().filter(|entry| entry.path().is_dir()) {
                    drop(Unmount(entry.path()));
                }
            }
        }
    }
    let _unmount = ChildMounts(directory.path().to_owned());
    let seams = seams::Seams::serve(directory.path(), Duration::ZERO);
    seams.hold(RETURNED);
    let mut child = Helper(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "writes_sync_ordinary_and_local_only_content_before_reply",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("TMPDIR", directory.path())
            .env(seams::SOCKET_VARIABLE, seams.socket())
            .spawn()
            .unwrap(),
    );
    for replies in 1..=2 {
        seams.wait_for_count(REPLIED, replies);
        seams.wait_until_waiting(RETURNED, 1);
        assert_eq!(
            seams.count("drive.content_synced"),
            replies,
            "each WRITE must synchronize its backing content before replying"
        );
        seams.release_one(RETURNED);
    }
    let deadline = std::time::Instant::now() + seams::WAIT_LIMIT;
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "sync observer child: {status}");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sync observer child did not exit"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_waiting_for_its_download_is_answered_jukebox_after_three_seconds_and_its_retry_joins_the_download()
 {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    env.stub.list("a.txt", "alpha");
    let hash = support::sha256(b"alpha");
    env.stub.held_downloads.lock().unwrap().insert(hash.clone());
    let _session = mounted(&mut env, true, false).await;
    let mut client = connect(port(&env), &export(port(&env))).await.unwrap();
    let top = client.root_nfs_fh3();
    let file = lookup(&mut client, &top, "a.txt").await.unwrap();

    let reading = tokio::spawn(async move {
        let answer = read(&mut client, &file).await;
        (client, file, answer)
    });
    let stub = env.stub.clone();
    support::eventually(move || stub.downloads_started.load(Ordering::SeqCst) == 1).await;
    env.advance(Duration::from_millis(2_999)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!reading.is_finished(), "answered before the wait ended");
    env.advance(Duration::from_millis(1)).await;
    let (mut client, file, answer) = tokio::time::timeout(Duration::from_secs(5), reading)
        .await
        .expect("an answer once the wait ended")
        .unwrap();
    assert_eq!(answer, Err(nfsstat3::NFS3ERR_JUKEBOX));

    // The download went on; the client's retry gets its content.
    env.stub.held_downloads.lock().unwrap().remove(&hash);
    assert_eq!(read(&mut client, &file).await, Ok(b"alpha".to_vec()));
    assert_eq!(env.stub.downloads_started.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_request_names_an_export_of_this_port() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    let _session = mounted(&mut env, true, false).await;
    let port = port(&env);
    let other = port.checked_add(1).unwrap_or(port - 1);
    let refused = connect(port, &format!("/{other}/{SESSION}/{DRIVE}"))
        .await
        .err();
    assert!(
        matches!(
            refused,
            Some(ConnectError::Mount(MountError::Denied(
                mountstat3::MNT3ERR_NOENT
            )))
        ),
        "{refused:?}"
    );
    assert!(connect(port, &export(port)).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_records_no_task_and_waits_for_a_sync_point_or_a_new_connection() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    let session = mounted(&mut env, true, false).await;
    // A command runs while the changes are made, so their content uploads.
    session.task_started(1);
    let mut client = connect(port(&env), &export(port(&env))).await.unwrap();
    let top = client.root_nfs_fh3();
    let (file, _) = create(&mut client, &top, "new.txt").await;
    write(&mut client, &file, b"new").await.unwrap();
    let pending = session.tables().await.pending_changes[&key("new.txt")].clone();
    assert_eq!(pending.task_id, None);

    // Uploaded after its quiet period, and not committed then.
    env.advance(Duration::from_secs(2)).await;
    env.call().await.uploaded();
    session
        .until(|tables| tables.pending_changes[&key("new.txt")].uploaded)
        .await;
    env.advance(Duration::from_secs(2)).await;
    env.no_call(Duration::from_millis(200)).await;

    // The next sync point of any task commits it.
    let point = session.sync_point(Some(task(1)), Duration::ZERO, None);
    let commit = env.call().await;
    assert_eq!(
        support::summary(commit.changes()),
        [("new.txt".to_owned(), "write", None)]
    );
    commit.accepted();
    assert_eq!(outcome(point).await.committed, 1);

    // A rejection that no result reports names no task.
    let (file, _) = create(&mut client, &top, "other.txt").await;
    write(&mut client, &file, b"other").await.unwrap();
    env.advance(Duration::from_secs(2)).await;
    env.call().await.uploaded();
    session
        .until(|tables| tables.pending_changes[&key("other.txt")].uploaded)
        .await;
    session.sync.binding_current(drive::transfer::Binding {
        connection_id: Id::new("conn_b").unwrap(),
        binding_id: Id::new("bind_b").unwrap(),
    });
    env.call().await.answer(vec![
        json!({ "status": "rejected", "reason": "path_rule", "rule": "no such names" }),
    ]);
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    let report = env.reports.sent.lock().unwrap()[0].clone();
    assert_eq!(report.get("task_id"), None, "{report}");
    let rejected: &Vec<Value> = report["drive_sync"]["rejected"].as_array().unwrap();
    assert_eq!(rejected.len(), 1);
    assert!(
        rejected[0]["path"]
            .as_str()
            .unwrap()
            .ends_with("/other.txt")
    );
}

/// Kills the helper when the test ends.
struct Helper(Child);

impl Drop for Helper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// In the helper process: an NFS server of its own mounted at the point
/// it is given, until it is killed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flock_and_fcntl_locks_on_mounted_files_are_taken() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    env.stub.list("a.txt", "alpha");
    env.stub.list("b.txt", "beta");
    let _session = mounted(&mut env, true, false).await;
    let point = point(&env);
    let locked = tokio::time::timeout(
        Duration::from_secs(10),
        blocking(move || {
            let open = |name: &str| {
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(point.join(name))
                    .unwrap()
            };
            let exclusive = rustix::fs::FlockOperation::NonBlockingLockExclusive;
            (
                rustix::fs::flock(open("a.txt"), exclusive),
                rustix::fs::fcntl_lock(open("b.txt"), exclusive),
            )
        }),
    )
    .await
    .expect("both locks answer");
    assert_eq!(locked, (Ok(()), Ok(())));
}

#[test]
fn helper_server() {
    let Some(point) = std::env::var_os(HELPER) else {
        return;
    };
    let server = drive::nfs::TestDrive::start().unwrap();
    drive::mount_macos::mount_nfs(server.port(), &server.source(), Path::new(&point), false)
        .unwrap();
    println!("{MOUNTED}");
    loop {
        std::thread::sleep(Duration::from_secs(3_600));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_whose_server_is_stopped_is_force_unmounted_and_mounted_again() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    let point = point(&env);
    std::fs::create_dir_all(&point).unwrap();
    let mut helper = Helper(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "helper_server",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HELPER, &point)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = helper.0.stdout.take().unwrap();
    let ready = tokio::time::timeout(
        Duration::from_secs(30),
        blocking(move || {
            BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
                .any(|line| line.ends_with(MOUNTED))
        }),
    )
    .await;
    assert_eq!(ready, Ok(true), "the helper mounted P");
    let stale = drive::mount_macos::listed(&point).unwrap().unwrap();
    let pid = rustix::process::Pid::from_raw(helper.0.id() as i32).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::STOP).unwrap();

    let session = mounted(&mut env, true, false).await;
    let Mounting::Nfs(server) = &env.mounting else {
        unreachable!("mounted starts an NFS server");
    };
    let source = server.source(SESSION, DRIVE);
    assert_ne!(stale, source);
    assert_eq!(drive::mount_macos::listed(&point).unwrap(), Some(source));
    session.caught_up().await;
    drop(helper);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_mount_refuses_writes_with_erofs_and_its_export_every_change_with_rofs() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    env.stub.list("a.txt", "alpha");
    env.stub.list("dir/b.txt", "beta");
    let session = mounted(&mut env, true, true).await;
    let before = session.tables().await;

    // Sent to the export directly, as by a second mount without `rdonly`.
    let mut client = connect(port(&env), &export(port(&env))).await.unwrap();
    let top = client.root_nfs_fh3();
    let file = lookup(&mut client, &top, "a.txt").await.unwrap();
    let mode = sattr3 {
        mode: Nfs3Option::Some(0o600),
        ..sattr3::default()
    };
    let answers = [
        (
            "SETATTR",
            result(
                client
                    .setattr(&SETATTR3args {
                        object: file.clone(),
                        new_attributes: mode.clone(),
                        guard: Nfs3Option::None,
                    })
                    .await
                    .unwrap(),
            )
            .err(),
        ),
        ("WRITE", write(&mut client, &file, b"lost").await.err()),
        (
            "CREATE",
            result(
                client
                    .create(&CREATE3args {
                        where_: at(&top, "new.txt"),
                        how: createhow3::GUARDED(sattr3::default()),
                    })
                    .await
                    .unwrap(),
            )
            .err(),
        ),
        (
            "CREATE exclusive",
            result(
                client
                    .create(&CREATE3args {
                        where_: at(&top, "new.txt"),
                        how: createhow3::EXCLUSIVE(createverf3([1; 8])),
                    })
                    .await
                    .unwrap(),
            )
            .err(),
        ),
        (
            "MKDIR",
            result(
                client
                    .mkdir(&MKDIR3args {
                        where_: at(&top, "new"),
                        attributes: sattr3::default(),
                    })
                    .await
                    .unwrap(),
            )
            .err(),
        ),
        (
            "SYMLINK",
            result(
                client
                    .symlink(&SYMLINK3args {
                        where_: at(&top, "link"),
                        symlink: symlinkdata3 {
                            symlink_attributes: sattr3::default(),
                            symlink_data: b"a.txt".to_vec().into(),
                        },
                    })
                    .await
                    .unwrap(),
            )
            .err(),
        ),
        (
            "REMOVE",
            result(
                client
                    .remove(&REMOVE3args {
                        object: at(&top, "a.txt"),
                    })
                    .await
                    .unwrap(),
            )
            .err(),
        ),
        (
            "RMDIR",
            result(
                client
                    .rmdir(&RMDIR3args {
                        object: at(&top, "dir"),
                    })
                    .await
                    .unwrap(),
            )
            .err(),
        ),
        (
            "RENAME",
            result(
                client
                    .rename(&RENAME3args {
                        from: at(&top, "a.txt"),
                        to: at(&top, "moved.txt"),
                    })
                    .await
                    .unwrap(),
            )
            .err(),
        ),
    ];
    for (procedure, answer) in answers {
        assert_eq!(answer, Some(nfsstat3::NFS3ERR_ROFS), "{procedure}");
    }
    assert_eq!(read(&mut client, &file).await, Ok(b"alpha".to_vec()));
    assert_eq!(session.tables().await, before);

    let point = point(&env);
    let (new, existing, read) = blocking(move || {
        (
            std::fs::write(point.join("new.txt"), "new"),
            std::fs::OpenOptions::new()
                .write(true)
                .open(point.join("a.txt")),
            std::fs::read_to_string(point.join("a.txt")),
        )
    })
    .await;
    let erofs = Some(rustix::io::Errno::ROFS.raw_os_error());
    assert_eq!(new.unwrap_err().raw_os_error(), erofs);
    assert_eq!(existing.unwrap_err().raw_os_error(), erofs);
    assert_eq!(read.unwrap(), "alpha");
    assert!(session.tables().await.pending_changes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listing_in_pages_lists_every_other_entry_once_after_its_cookie_entry_is_removed_or_renamed()
 {
    for rename in [false, true] {
        let mut env = Env::new();
        let _unmount = Unmount(env.root.path().to_path_buf());
        let all: Vec<String> = (0..300).map(|index| format!("f{index:04}.txt")).collect();
        for name in &all {
            env.stub.list(name, "x");
        }
        let _session = mounted(&mut env, true, false).await;
        let port = port(&env);
        let mut client = connect(port, &export(port)).await.unwrap();
        let top = client.root_nfs_fh3();
        let (first, mut verifier, eof) = page(&mut client, &top, 0, cookieverf3::default())
            .await
            .unwrap();
        assert!(!eof, "the folder spans more than one page");
        let (cookie_entry, mut cookie) = first.last().cloned().unwrap();
        let changed = if rename {
            let args = RENAME3args {
                from: at(&top, &cookie_entry),
                to: at(&top, "renamed.txt"),
            };
            result(client.rename(&args).await.unwrap()).map(|_| ())
        } else {
            let args = REMOVE3args {
                object: at(&top, &cookie_entry),
            };
            result(client.remove(&args).await.unwrap()).map(|_| ())
        };
        changed.unwrap();
        let mut names: Vec<String> = first.into_iter().map(|(name, _)| name).collect();
        loop {
            let (entries, next, eof) = page(&mut client, &top, cookie, verifier)
                .await
                .unwrap_or_else(|status| panic!("after {} names: {status:?}", names.len()));
            verifier = next;
            if let Some((_, last)) = entries.last() {
                cookie = *last;
            }
            names.extend(entries.into_iter().map(|(name, _)| name));
            if eof {
                break;
            }
        }
        names.retain(|name| *name != cookie_entry && name != "renamed.txt");
        names.sort();
        let others: Vec<&String> = all.iter().filter(|name| **name != cookie_entry).collect();
        assert_eq!(names.iter().collect::<Vec<_>>(), others, "rename {rename}");
    }
}

/// `remove_dir_all` reads the folder while it removes the folder's entries,
/// as many programs do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_dir_all_removes_a_folder_of_a_thousand_files_through_the_mount() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    for index in 0..1_000 {
        env.stub.list(&format!("big/f{index:04}.txt"), "x");
    }
    let _session = mounted(&mut env, true, false).await;
    let big = point(&env).join("big");
    let (removed, left) = blocking(move || (std::fs::remove_dir_all(&big), big.exists())).await;
    assert!(removed.is_ok(), "{removed:?}");
    assert!(!left);
}

/// Assert every transfer, including content hashes, rather than filtering
/// unwanted names out of a summary after they have already been uploaded.
async fn settle_native(
    env: &mut Env,
    session: &Session,
    expected: &[(&str, &str)],
    allowed_uploads: &[String],
) -> drive::SyncOutcome {
    let mut point = session.sync_point(None, Duration::from_secs(30), None);
    let mut changes = Vec::new();
    let result = loop {
        tokio::select! {
            result = &mut point => break result.unwrap(),
            call = env.call() => match call {
                call @ support::Call::Upload { .. } => {
                    assert!(allowed_uploads.iter().any(|hash| hash == call.sha256()), "unexpected upload {}", call.sha256());
                    eprintln!("native upload {}", call.sha256());
                    call.uploaded();
                }
                call @ support::Call::Commit { .. } => {
                    for (path, change, _) in support::summary(call.changes()) {
                        eprintln!("native commit {change} {path}");
                        assert!(!path.split('/').any(|name| name.starts_with("._") || name.starts_with(".nfs.")), "local path committed: {path}");
                        changes.push((path, change.to_owned()));
                    }
                    call.accepted();
                }
            }
        }
    };
    let mut expected: Vec<_> = expected
        .iter()
        .map(|(path, change)| ((*path).to_owned(), (*change).to_owned()))
        .collect();
    expected.sort();
    changes.sort();
    assert_eq!(changes, expected);
    assert!(session.tables().await.pending_changes.is_empty());
    env.no_call(Duration::from_millis(100)).await;
    result
}

/// xattr's hex mode avoids text encoding, newline and FinderInfo formatting
/// ambiguities. Decode it before comparing exact attribute bytes.
fn attribute(path: &Path, name: &str) -> Vec<u8> {
    let output = Command::new("/usr/bin/xattr")
        .args(["-px", name])
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "attribute {name} on {}: {output:?}",
        path.display()
    );
    let hex: String = String::from_utf8(output.stdout)
        .unwrap()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    assert_eq!(hex.len() % 2, 0);
    eprintln!("native xattr {} {name}: {hex}", path.display());
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
        .collect()
}

fn set_attribute(path: &Path, name: &str, bytes: &[u8]) {
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    let output = Command::new("/usr/bin/xattr")
        .args(["-wx", name, &hex])
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success(), "set {name}: {output:?}");
    assert_eq!(
        attribute(path, name),
        bytes,
        "the source attribute is positively established"
    );
}

fn native(program: &str, args: &[&Path]) {
    let output = Command::new(program).args(args).output().unwrap();
    assert!(output.status.success(), "{program}: {output:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_copies_preserve_exact_local_metadata_and_remount_without_metadata_transfers() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    let session = mounted(&mut env, true, false).await;
    let mount = point(&env);
    let sources = tempfile::tempdir().unwrap();
    let source_dir = sources.path().to_path_buf();
    let copy_mount = mount.clone();
    let (paths, hashes, retained) = blocking(move || {
        let attrs: Vec<(&str, Vec<u8>)> = vec![
            ("com.example.k", b"custom\0bytes\n".to_vec()),
            ("com.apple.metadata:kMDItemWhereFroms", b"https://example.test/source".to_vec()),
            ("com.apple.TextEncoding", b"utf-8;134217984".to_vec()),
            ("com.apple.FinderInfo", [b"TEXTttxt".as_slice(), &[0; 24]].concat()),
            ("com.apple.quarantine", b"0081;65f00000;Safari;".to_vec()),
        ];
        let mut paths = Vec::new();
        let mut hashes = Vec::new();
        let mut retained = Vec::new();
        for (index, (attribute_name, bytes)) in attrs.into_iter().enumerate() {
            for no_xattrs in [false, true] {
                let name = format!("copy-{index}-{}.txt", if no_xattrs { "X" } else { "plain" });
                let source = source_dir.join(&name);
                let target = copy_mount.join(&name);
                let content = format!("ordinary content {index} {no_xattrs}\n").into_bytes();
                std::fs::write(&source, &content).unwrap();
                set_attribute(&source, attribute_name, &bytes);
                let mut cp = Command::new("/bin/cp");
                if no_xattrs { cp.arg("-X"); }
                let output = cp.arg(&source).arg(&target).output().unwrap();
                assert!(output.status.success(), "copy {name}: {output:?}");
                assert_eq!(std::fs::read(&target).unwrap(), content);
                if !no_xattrs || attribute_name == "com.apple.quarantine" {
                    assert_eq!(attribute(&target, attribute_name), bytes);
                    retained.push((name.clone(), attribute_name.to_owned(), bytes.clone()));
                }
                eprintln!("native copy {name} {attribute_name}: exact content and applicable attribute bytes");
                paths.push(name);
                hashes.push(support::sha256(&content));
            }
        }
        (paths, hashes, retained)
    }).await;
    let expected: Vec<_> = paths.iter().map(|path| (path.as_str(), "write")).collect();
    settle_native(&mut env, &session, &expected, &hashes).await;
    let tables = session.tables().await;
    let sidecars: Vec<_> = tables
        .files
        .values()
        .filter(|row| row.present && row.path.split('/').any(|name| name.starts_with("._")))
        .map(|row| (row.path.clone(), row.file_id))
        .collect();
    assert!(
        !sidecars.is_empty(),
        "native copying exercised AppleDouble files"
    );
    let mut metadata = Vec::new();
    for (path, id) in sidecars {
        let row = &tables.files[&key(&path)];
        assert_eq!(row.local_only.as_deref(), Some("macos_metadata"));
        assert_eq!(row.content.as_deref(), Some("session"));
        metadata.push((
            path,
            session
                .sync
                .read(&session.drive, id as u64, 0, row.size.unwrap() as u32)
                .await
                .unwrap(),
        ));
    }
    session.sync.detach_all();
    session.sync.close().await;
    assert_eq!(drive::mount_macos::listed(&mount).unwrap(), None);
    let reopened = mounted(&mut env, false, false).await;
    for (path, content) in metadata {
        let ino = reopened
            .sync
            .lookup(&reopened.drive, drive::ops::TOP_INODE, path.as_bytes())
            .await
            .unwrap()
            .ino;
        assert_eq!(
            reopened
                .sync
                .read(&reopened.drive, ino, 0, content.len() as u32)
                .await
                .unwrap(),
            content
        );
    }
    blocking(move || {
        for (path, name, bytes) in retained {
            assert_eq!(
                attribute(&mount.join(path), &name),
                bytes,
                "metadata survives a clean remount"
            );
        }
    })
    .await;
    settle_native(&mut env, &reopened, &[], &[]).await;
    reopened.sync.detach_all();
    reopened.sync.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_copy_overwrite_move_and_remove_keep_metadata_local() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    let session = mounted(&mut env, true, false).await;
    let mount = point(&env);
    let sources = tempfile::tempdir().unwrap();
    let a = sources.path().join("a");
    let b = sources.path().join("b");
    let target = mount.join("copied");
    let moved = mount.join("moved");
    std::fs::write(&a, b"ordinary A").unwrap();
    std::fs::write(&b, b"ordinary B").unwrap();
    for (source, suffix) in [(&a, "A"), (&b, "B")] {
        set_attribute(
            source,
            "com.example.k",
            format!("custom-{suffix}").as_bytes(),
        );
        set_attribute(
            source,
            "com.apple.quarantine",
            format!("0081;65f00000;Test{suffix};").as_bytes(),
        );
    }
    let (a_copy, target_copy) = (a.clone(), target.clone());
    blocking(move || native("/bin/cp", &[&a_copy, &target_copy])).await;
    settle_native(
        &mut env,
        &session,
        &[("copied", "write")],
        &[support::sha256(b"ordinary A")],
    )
    .await;
    let (b_copy, target_copy) = (b.clone(), target.clone());
    blocking(move || {
        native("/bin/cp", &[&b_copy, &target_copy]);
        assert_eq!(std::fs::read(&target_copy).unwrap(), b"ordinary B");
        for name in ["com.example.k", "com.apple.quarantine"] {
            assert_eq!(attribute(&target_copy, name), attribute(&b_copy, name));
        }
    })
    .await;
    settle_native(
        &mut env,
        &session,
        &[("copied", "write")],
        &[support::sha256(b"ordinary B")],
    )
    .await;
    let (target_copy, moved_copy, b_copy) = (target.clone(), moved.clone(), b.clone());
    blocking(move || {
        native("/bin/mv", &[&target_copy, &moved_copy]);
        assert!(!target_copy.exists());
        assert_eq!(std::fs::read(&moved_copy).unwrap(), b"ordinary B");
        for name in ["com.example.k", "com.apple.quarantine"] {
            assert_eq!(attribute(&moved_copy, name), attribute(&b_copy, name));
        }
    })
    .await;
    settle_native(
        &mut env,
        &session,
        &[("copied", "delete"), ("moved", "write")],
        &[support::sha256(b"ordinary B")],
    )
    .await;
    let before = session.tables().await;
    let moved_copy = moved.clone();
    blocking(move || {
        native("/bin/rm", &[&moved_copy]);
        assert!(!moved_copy.exists());
    })
    .await;
    settle_native(&mut env, &session, &[("moved", "delete")], &[]).await;
    let after = session.tables().await;
    for row in before
        .files
        .values()
        .filter(|row| row.present && row.path.starts_with("._"))
    {
        let remains = after
            .files
            .get(&key(&row.path))
            .is_some_and(|row| row.present);
        eprintln!(
            "native companion {}: {}",
            row.path,
            if remains {
                "unpaired local residue"
            } else {
                "removed by client"
            }
        );
        if remains {
            assert_eq!(
                after.files[&key(&row.path)].local_only.as_deref(),
                Some("macos_metadata")
            );
        } else {
            assert!(
                !session
                    .directory
                    .join("content")
                    .join(DRIVE)
                    .join(row.file_id.to_string())
                    .exists(),
                "client-removed sidecar backing content released"
            );
        }
    }
    session.sync.detach_all();
    session.sync.close().await;
}

fn native_temporary(directory: &Path) -> PathBuf {
    let entries: Vec<_> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            let bytes = name.as_bytes();
            bytes.len() == 18
                && bytes.starts_with(b".nfs.")
                && bytes[13] == b'.'
                && bytes[5..13].iter().all(u8::is_ascii_hexdigit)
                && bytes[14..].iter().all(u8::is_ascii_hexdigit)
        })
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "one observed native open-file temporary name"
    );
    entries[0].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_open_unlink_and_open_target_replacement_keep_temporary_bytes_local() {
    use std::io::{Read, Seek, SeekFrom, Write as _};
    for replacing in [false, true] {
        let mut env = Env::new();
        let _unmount = Unmount(env.root.path().to_path_buf());
        let session = mounted(&mut env, true, false).await;
        let directory = point(&env).join("out");
        let copy = directory.clone();
        blocking(move || {
            std::fs::create_dir(&copy).unwrap();
            std::fs::write(copy.join("target"), b"original target").unwrap();
            if replacing {
                std::fs::write(copy.join("source"), b"replacement source").unwrap();
            }
        })
        .await;
        let initial = if replacing {
            vec![("out/source", "write"), ("out/target", "write")]
        } else {
            vec![("out/target", "write")]
        };
        settle_native(
            &mut env,
            &session,
            &initial,
            &[
                support::sha256(b"original target"),
                support::sha256(b"replacement source"),
            ],
        )
        .await;
        let original_id = session.tables().await.files[&key("out/target")].file_id;
        let copy = directory.clone();
        let (held, temporary) = blocking(move || {
            let mut held = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(copy.join("target"))
                .unwrap();
            if replacing {
                std::fs::rename(copy.join("source"), copy.join("target")).unwrap();
                assert_eq!(
                    std::fs::read(copy.join("target")).unwrap(),
                    b"replacement source"
                );
            } else {
                std::fs::remove_file(copy.join("target")).unwrap();
                assert!(!copy.join("target").exists());
            }
            let temporary = native_temporary(&copy);
            let mut old = Vec::new();
            held.read_to_end(&mut old).unwrap();
            assert_eq!(old, b"original target");
            held.seek(SeekFrom::Start(0)).unwrap();
            held.write_all(b"temporary-only bytes").unwrap();
            held.sync_all().unwrap();
            held.seek(SeekFrom::Start(0)).unwrap();
            let mut changed = Vec::new();
            held.read_to_end(&mut changed).unwrap();
            assert_eq!(changed, b"temporary-only bytes");
            if !replacing {
                assert_eq!(
                    std::fs::remove_dir(&copy).unwrap_err().raw_os_error(),
                    Some(libc::ENOTEMPTY)
                );
            }
            (held, temporary)
        })
        .await;
        let relative = temporary
            .strip_prefix(point(&env))
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        eprintln!("native temporary {relative}, replacing={replacing}");
        let tables = session.tables().await;
        let row = &tables.files[&key(&relative)];
        assert_eq!(
            row.file_id, original_id,
            "open NFS handle retains its content identity"
        );
        assert_eq!(row.local_only.as_deref(), Some("nfs_temporary"));
        assert!(!tables.pending_changes.contains_key(&key(&relative)));
        let expected = if replacing {
            vec![("out/source", "delete"), ("out/target", "write")]
        } else {
            vec![("out/target", "delete")]
        };
        settle_native(&mut env, &session, &expected, &[]).await;
        let copy = directory.clone();
        blocking(move || {
            drop(held);
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while temporary.exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "client removes its temporary name on last close"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            if !replacing {
                std::fs::remove_dir(copy).unwrap();
            }
        })
        .await;
        session
            .until(|tables| {
                !tables
                    .files
                    .get(&key(&relative))
                    .is_some_and(|row| row.present)
            })
            .await;
        settle_native(&mut env, &session, &[], &[]).await;
        session.sync.detach_all();
        session.sync.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mounted_taskless_symlink_and_git_notices_reach_the_next_result_once() {
    let mut env = Env::new();
    let _unmount = Unmount(env.root.path().to_path_buf());
    let session = mounted(&mut env, true, false).await;
    let mount = point(&env);
    let copy = mount.clone();
    blocking(move || {
        std::fs::create_dir(copy.join("repo")).unwrap();
        std::fs::create_dir(copy.join("repo/.git")).unwrap();
        std::os::unix::fs::symlink("repo", copy.join("link")).unwrap();
    })
    .await;
    let result = settle_native(&mut env, &session, &[], &[]).await;
    let mut notices: Vec<_> = result
        .not_synced
        .iter()
        .map(|notice| (notice.path.as_str().to_owned(), notice.reason))
        .collect();
    notices.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        notices,
        vec![
            (
                mount.join("link").to_string_lossy().into_owned(),
                protocol::tool::NotSyncedReason::Symlink
            ),
            (
                mount.join("repo").to_string_lossy().into_owned(),
                protocol::tool::NotSyncedReason::GitRepository
            ),
        ]
    );
    assert!(
        settle_native(&mut env, &session, &[], &[])
            .await
            .not_synced
            .is_empty()
    );
    session.sync.detach_all();
    session.sync.close().await;
}
