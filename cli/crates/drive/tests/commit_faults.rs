//! A batch failure is recorded once by the owner with request correlation.
mod support;
use drive::state::Write;
use drive::transfer::Ended;
use support::RESULT_WAIT;
use support::{Env, NO_TASK, outcome};

#[tokio::test]
async fn a_permanent_commit_failure_has_one_correlated_record_for_all_its_changes() {
    let records = support::Records::start();
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    for path in ["a", "b", "c"] {
        session.write(path, path, NO_TASK).await;
    }
    let trace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let point = session.sync_point(None, RESULT_WAIT, Some(trace));
    for _ in 0..3 {
        env.call().await.uploaded();
    }
    let commit = env.call().await;
    assert_eq!(commit.changes().len(), 3);
    commit.commit_ended(Ended::Permanent {
        status: 400,
        code: "invalid_message",
    });
    assert_eq!(outcome(point).await.not_synced.len(), 3);
    let lines = records.lines();
    let faults: Vec<_> = lines
        .iter()
        .filter(|line| line["event"] == "drive.upload_fault")
        .collect();
    assert_eq!(faults.len(), 1, "{lines:?}");
    let fault = faults[0];
    assert_eq!(fault["drive_transfer.changes"], 3);
    assert_eq!(fault["drive_transfer.operation"], "commit");
    assert_eq!(fault["http.response.status_code"], 400);
    assert_eq!(fault["error.code"], "invalid_message");
    assert!(fault.get("error.message").is_none(), "{fault}");
    assert_eq!(fault["session.id"], "sess_a");
    assert_eq!(fault["drive.id"], support::DRIVE);
    assert_eq!(fault["binding.id"], "bind_a");
    assert_eq!(fault["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
    session.sync.close().await;
}
