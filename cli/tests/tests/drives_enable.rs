//! LA-12 on Linux: `tetral drives enable` succeeds with `{}` and changes
//! nothing, and fails with `server_running` while an App Server owns the
//! root.
#![cfg(target_os = "linux")]

use serde_json::json;
use tetral_tests::machine::{CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine};
use tetral_tests::run::Tetral;
use tetral_tests::snapshot::snapshot;
use tetral_tests::socket_directory;

const APP: &str = "app_one";

#[test]
fn drives_enable_changes_nothing_and_needs_the_root_free() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("drives enable while an App Server owns the root");
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    let root = machine.root.clone();
    let run = Tetral::new(["drives", "enable", "--app", APP])
        .home(&root)
        .run();
    case.command("tetral drives enable --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(1), "{}", run.exit());
    assert_eq!(run.error_code(), "server_running");
    case.observe("server_running with exit 1");
    case.pass();

    let mut case = record.case("drives enable on a root no App Server owns");
    machine.app_server.kill();
    let before = snapshot(&root);
    let run = Tetral::new(["drives", "enable", "--app", APP])
        .home(&root)
        .run();
    case.command("tetral drives enable --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), json!({}));
    assert_eq!(snapshot(&root), before, "the root is unchanged");
    case.observe("{} with exit 0; the root is unchanged");
    case.pass();
}
