//! The contract of `timeline`: a failing test leaves what the other
//! processes saw. One test, as the timeline and the failure's records are
//! the test process's.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};

use rustix::process::{Pid, Signal, WaitId, WaitIdOptions, kill_process, waitid};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine, seconds,
};
use tetral_tests::run::Tetral;
use tetral_tests::{record, timeline};

#[test]
fn a_failing_test_leaves_the_app_server_s_output_the_engine_s_traffic_and_the_clock_on_one_timeline()
 {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a worker's panic and then the test's, after an App Server connected to the reference control server, the test moved its clock and another process it started was killed",
    );
    let directory = tempfile::tempdir().unwrap();
    let engine = one_credential_engine();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    // The status reads "online" before the App Server's log line has come
    // through its stderr pipe.
    wait_until("the App Server's connection line on the timeline", || {
        timeline::holds(
            "tetral server run 1 stderr",
            r#""event":"connection.state_changed""#,
        )
    });
    machine.advance(seconds(1.5));
    let sleeper = Tetral::at(Path::new("/bin/sleep"), ["600"]).spawn();
    let pid = Pid::from_raw(sleeper.id() as i32).unwrap();
    kill_process(pid, Signal::KILL).unwrap();
    // Waits for its exit and leaves it unreaped, as a test that never
    // looked at the process would.
    waitid(
        WaitId::Pid(pid),
        WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
    )
    .unwrap();
    let worker = std::thread::Builder::new()
        .name("worker".to_owned())
        .spawn(|| panic!("a deliberate failure in the worker"))
        .unwrap();
    let failed = catch_unwind(AssertUnwindSafe(|| worker.join().unwrap()));
    assert!(failed.is_err());

    let stem = record::current().unwrap().with_extension("");
    let path = |suffix: &str| PathBuf::from(format!("{}.{suffix}", stem.display()));
    let timeline = std::fs::read_to_string(path("timeline.log")).unwrap();
    let both: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path("reference-1.json")).unwrap()).unwrap();
    // Each line's time, source and text, in the order written.
    let lines: Vec<(f64, &str)> = timeline
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (time, rest) = line.trim_start().split_once(" s  ").unwrap();
            (time.parse().unwrap(), rest)
        })
        .collect();
    let first = |source: &str, text: &str| {
        lines
            .iter()
            .position(|(_, line)| line.starts_with(source) && line.contains(text))
            .unwrap_or_else(|| panic!("no line of {source} with {text:?} in\n{timeline}"))
    };
    let connected = first(
        "tetral server run 1 stderr",
        r#""event":"connection.state_changed""#,
    );
    let ready = first("reference 1", "POST /v1/client/ready arrives");
    let answered = first("reference 1", "POST /v1/client/ready ends with status 200");
    let stream = first("reference 1", "GET /v1/client/events");
    let moved = first("seams 1", "the test moves the clock by 1.5s to 1001.500 s");
    let started = first("sleep 600 1", "starts as process");
    let in_the_worker = first(
        "test",
        "thread 'worker' panics: panicked at tests/tests/timeline.rs",
    );
    let on_its_join = first(
        "test",
        &format!(
            "thread '{}' panics: panicked at tests/tests/timeline.rs",
            std::thread::current().name().unwrap_or("<unnamed>")
        ),
    );
    assert!(
        lines.windows(2).all(|pair| pair[0].0 <= pair[1].0),
        "the lines are in the order of their times:\n{timeline}"
    );
    assert!(stream < ready && ready < answered && answered < moved && moved < started);
    assert!(connected < moved);
    assert!(started < in_the_worker && in_the_worker < on_its_join);
    assert!(
        lines[in_the_worker]
            .1
            .contains("a deliberate failure in the worker")
    );
    assert!(lines[on_its_join].1.contains("Any { .. }"));
    assert!(
        first("reference 1", "GET /v1/client/events?")
            < first("reference 1", "is still being served"),
        "the open stream is listed as served at the failure"
    );
    assert!(first("seams 1", "stands at 1001.500 s") > on_its_join);
    assert!(first("tetral server run 1", "still runs") > on_its_join);
    assert!(
        first(
            "sleep 600 1",
            "has exited, before the test waited for it: signal: 9"
        ) > on_its_join
    );
    assert!(
        both["traffic"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["path"] == "/v1/client/ready"
                && entry["outcome"] == "answered"
                && entry["status"] == 200)
    );
    assert_eq!(both["state"]["connections"][0]["ready_accepted"], true);
    drop(sleeper);
    for suffix in ["timeline.log", "reference-1.json"] {
        std::fs::remove_file(path(suffix)).unwrap();
    }
    case.observe(
        "the timeline held the App Server's connection.state_changed, the ready's arrival and its 200, the open stream, the clock's step, the start of the killed process, the worker's panic and then the test's on its join, each with its thread; at the failure, the App Server still ran and the killed process had exited by signal 9; the reference control server's file held its traffic and state",
    );
    case.pass();
}
