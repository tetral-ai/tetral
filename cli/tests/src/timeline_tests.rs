//! The contract of `timeline`: a failing test leaves what the other
//! processes saw.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine, seconds,
};

#[test]
fn a_failing_test_leaves_the_app_server_s_output_the_engine_s_traffic_and_the_clock_on_one_timeline()
 {
    let record = crate::record!();
    let mut case = record.case(
        "a panic after an App Server connected to the reference control server and the test moved its clock",
    );
    let directory = tempfile::tempdir().unwrap();
    let engine = one_credential_engine();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    machine.advance(seconds(1.5));
    let failed = catch_unwind(AssertUnwindSafe(|| panic!("a deliberate failure")));
    assert!(failed.is_err());

    let stem = crate::record::current().unwrap().with_extension("");
    let path = |suffix: &str| std::path::PathBuf::from(format!("{}.{suffix}", stem.display()));
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
    let panicked = first("test", "a deliberate failure");
    assert!(
        lines.windows(2).all(|pair| pair[0].0 <= pair[1].0),
        "the lines are in the order of their times:\n{timeline}"
    );
    assert!(stream < ready && ready < answered && answered < moved && moved < panicked);
    assert!(connected < moved);
    assert!(
        first("reference 1", "GET /v1/client/events?")
            < first("reference 1", "is still being served"),
        "the open stream is listed as served at the failure"
    );
    assert!(first("seams 1", "stands at 1001.500 s") > panicked);
    assert!(
        both["traffic"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["path"] == "/v1/client/ready" && entry["status"] == 200)
    );
    assert_eq!(both["state"]["connections"][0]["ready_accepted"], true);
    for suffix in ["timeline.log", "reference-1.json"] {
        std::fs::remove_file(path(suffix)).unwrap();
    }
    case.observe(
        "the timeline held the App Server's connection.state_changed, the ready's arrival and its 200, the open stream, the clock's step and the panic in that order; the reference control server's file held its traffic and state",
    );
    case.pass();
}
