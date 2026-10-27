//! A release build of `tetral` holds no test seam: the marker that names
//! the seam protocol, which exists only under the feature `test-seams`, is
//! absent from it and present in the harness's build.

use std::process::Command;

use tetral_tests::{binaries, workspace_dir};

const MARKER: &[u8] = b"TETRAL_TEST_SEAMS_V1";
const SOCKET_PARENT: &str = "TETRAL_TEST_SOCKET_PARENT";

fn holds_marker(binary: &[u8]) -> bool {
    binary.windows(MARKER.len()).any(|window| window == MARKER)
}

#[test]
fn a_release_build_holds_no_test_seam() {
    let mut case = tetral_tests::record!().case("a release build holds no test seam");
    let release_command =
        "cargo build --locked --release -p tetral --target-dir target/release-seams";
    let status = Command::new(binaries::cargo())
        .args([
            "build",
            "--locked",
            "--release",
            "-p",
            "tetral",
            "--target-dir",
            "target/release-seams",
        ])
        .current_dir(workspace_dir())
        .status()
        .expect("cargo starts");
    case.command(release_command, &status.to_string());
    assert!(status.success(), "{release_command}: {status}");

    let release = std::fs::read(workspace_dir().join("target/release-seams/release/tetral"))
        .expect("the release build");
    assert!(
        !holds_marker(&release),
        "the release build holds the test seams"
    );
    assert!(
        !release
            .windows(SOCKET_PARENT.len())
            .any(|window| window == SOCKET_PARENT.as_bytes()),
        "the release build reads the test socket-parent override"
    );
    case.observe("target/release-seams/release/tetral does not hold TETRAL_TEST_SEAMS_V1");

    let directory = tempfile::tempdir().unwrap();
    let missing_root = directory.path().join("absent-root");
    let result = Command::new(workspace_dir().join("target/release-seams/release/tetral"))
        .args(["server", "status", "--app", "app_one"])
        .env_clear()
        .env("TETRAL_HOME", &missing_root)
        .env(SOCKET_PARENT, "not-an-absolute-directory")
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(result["error"]["code"], "server_not_running");
    assert!(!missing_root.exists());
    case.observe("the release status command ignores an invalid test socket parent and reports server_not_running for a missing root");

    let with_seams = std::fs::read(binaries::tetral()).expect("the test-seams build");
    case.command(
        "cargo build --locked -p tetral --features test-seams --target-dir target/test-seams",
        "exit status: 0",
    );
    assert!(
        holds_marker(&with_seams),
        "the test-seams build lacks its marker"
    );
    assert!(
        with_seams
            .windows(SOCKET_PARENT.len())
            .any(|window| window == SOCKET_PARENT.as_bytes()),
        "the harness build lacks its socket-parent override"
    );
    case.observe("target/test-seams/debug/tetral holds TETRAL_TEST_SEAMS_V1");
    case.pass();
}
