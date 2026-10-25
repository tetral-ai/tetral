//! The `tool_execution` that ready advertises from the App Server's own
//! startup checks, with no test fragment: the protocol's full fragment on a
//! host with a shell, and the file tools alone where neither `/bin/bash`
//! nor `/bin/sh` exists.

use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine, tool_execution_fragment,
    wait_for_received,
};
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;

/// The `tool_execution` of the first ready that `engine` received.
fn advertised(engine: &ReferenceServer) -> Value {
    wait_for_received(engine, "ready", 1);
    engine.received("ready")[0]["body"]["capabilities"]["tool_execution"].clone()
}

/// The directories that hold `/bin/bash` and `/bin/sh`, which an empty file
/// system over each hides.
fn shell_directories() -> Vec<PathBuf> {
    let mut directories: Vec<PathBuf> = ["/bin/bash", "/bin/sh"]
        .into_iter()
        .filter_map(|shell| Path::new(shell).canonicalize().ok())
        .filter_map(|shell| shell.parent().map(Path::to_path_buf))
        .collect();
    directories.sort();
    directories.dedup();
    directories
}

#[test]
fn the_ready_advertises_the_tools_whose_startup_checks_pass() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();

    let mut case = record.case("an App Server on a host with /bin/bash and /bin/sh");
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let _machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    let fragment: Value = serde_json::from_str(&tool_execution_fragment()).unwrap();
    assert_eq!(advertised(&engine), fragment);
    case.observe("tool_execution equals the fixture ready_direct_and_disabled's");

    let mut case = record.case(
        "an App Server whose mount namespace has empty file systems over the directories of \
         /bin/bash and /bin/sh",
    );
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let mut hiding = vec!["--dev-bind".into(), "/".into(), "/".into()];
    for shells in shell_directories() {
        hiding.extend(["--tmpfs".into(), shells.into_os_string()]);
    }
    let _machine = Machine::start_under(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        "bwrap",
        &hiding,
    );
    let without_shell = advertised(&engine);
    assert_eq!(without_shell["tools"], json!(["edit", "view_image"]));
    let mut expected = fragment;
    expected["tools"] = json!(["edit", "view_image"]);
    assert_eq!(without_shell, expected);
    case.observe(
        "tools are edit and view_image; controls, policy versions, network modes and limits \
         are the fixture's",
    );
}
