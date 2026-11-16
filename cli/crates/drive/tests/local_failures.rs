//! Local storage failures are recorded at the Drive owner with correlation;
//! SQLite errors do not become successful settlement or lose pending writes.
mod support;

use drive::state::Write;
use sqlx::Connection;
use std::time::Duration;
use support::{Env, Records, key, outcome, task};

const TRACE: &str = "00-11111111111111111111111111111111-2222222222222222-01";

#[tokio::test]
async fn a_failed_commit_state_transaction_records_its_drive_and_initiating_trace_once() {
    let records = Records::start();
    let mut env = Env::new();
    let session = env.session("sess_failure", Write::default(), true).await;
    session.write("file", "pending", 1).await;
    let point = session.sync_point(Some(task(1)), Duration::from_secs(5), Some(TRACE));
    env.call().await.uploaded();
    let commit = env.call().await;
    let options =
        sqlx::sqlite::SqliteConnectOptions::new().filename(session.directory.join("sync-state.db"));
    let mut db = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_settlement BEFORE INSERT ON files BEGIN SELECT RAISE(ABORT, 'storage failure'); END").execute(&mut db).await.unwrap();
    commit.accepted();
    outcome(point).await;
    let failures: Vec<_> = records
        .lines()
        .into_iter()
        .filter(|line| line["event"] == "drive.state_write_failed")
        .collect();
    assert_eq!(failures.len(), 1, "{failures:#?}");
    let failure = &failures[0];
    for (field, value) in [
        ("session.id", "sess_failure"),
        ("drive.id", support::DRIVE),
        ("binding.id", "bind_a"),
        ("connection.id", "conn_a"),
        ("trace_id", "11111111111111111111111111111111"),
        ("error.code", "sync_state_write_failed"),
        ("level", "WARN"),
    ] {
        assert_eq!(failure[field], value, "{field}: {failure}");
    }
    assert!(
        failure["error.message"]
            .as_str()
            .is_some_and(|cause| cause.ends_with("storage failure")),
        "{failure}"
    );
    assert!(
        session
            .tables()
            .await
            .pending_changes
            .contains_key(&key("file"))
    );
    assert_eq!(session.read("file").await.unwrap(), "pending");
    sqlx::raw_sql("DROP TRIGGER fail_settlement")
        .execute(&mut db)
        .await
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_background_file_write_records_identity_without_an_instruction_trace() {
    let records = Records::start();
    let env = Env::new();
    let session = env.session("sess_background", Write::default(), true).await;
    session.write("file", "before", 1).await;
    let before = session.tables().await;
    let options =
        sqlx::sqlite::SqliteConnectOptions::new().filename(session.directory.join("sync-state.db"));
    let mut db = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_edit BEFORE INSERT ON files BEGIN SELECT RAISE(ABORT, 'storage failure'); END").execute(&mut db).await.unwrap();
    let inode = before.files[&key("file")].file_id as u64;
    assert!(
        session
            .sync
            .write(&session.drive, inode, 0, b"after!", Some(1))
            .await
            .is_err()
    );
    assert_eq!(session.tables().await, before);
    let failures: Vec<_> = records
        .lines()
        .into_iter()
        .filter(|line| line["event"] == "drive.state_write_failed")
        .collect();
    assert_eq!(failures.len(), 1, "{failures:#?}");
    assert_eq!(failures[0]["session.id"], "sess_background");
    assert_eq!(failures[0]["drive.id"], support::DRIVE);
    assert_eq!(failures[0]["binding.id"], "bind_a");
    assert!(
        failures[0]["error.message"]
            .as_str()
            .is_some_and(|cause| cause.ends_with("storage failure")),
        "{:?}",
        failures[0]
    );
    assert!(failures[0].get("trace_id").is_none());
    db.close().await.unwrap();
}
