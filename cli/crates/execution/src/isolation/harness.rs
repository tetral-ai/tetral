//! A test entry of `tetral` built with `test-seams`: one command run through
//! the admission of its policy, the availability check and the launch, as a
//! task supervisor runs them, so that cross-process tests can start real
//! restricted processes through this path.
//!
//! Its request is one JSON object: `policy`, the translation inputs other
//! than the trusted programs, which it finds as admission does
//! (`state_root`, `socket_directory`, `task_directories`,
//! `task_temporary_directory` and `drives`), `cwd`, `argv` and `env`. Its answer is one JSON line: `{"rejected": {code, message}}`,
//! `{"spawn_error": message}` or `{"exit_code", "stdout", "stderr"}`.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use protocol::connection::DriveAccess;
use serde_json::{Value, json};

use super::{Command, Isolation, LaunchError};
use crate::policy::{DriveMount, SessionDrives, TaskPermissions, TranslationInputs};

/// The `argv[0]` file name that selects this entry.
pub const PROGRAM: &str = "tetral-isolation-harness";

pub fn is_harness(argv0: &OsStr) -> bool {
    Path::new(argv0).file_name() == Some(OsStr::new(PROGRAM))
}

/// The answer line to the request `input`, with its newline.
pub fn answer(input: &[u8]) -> String {
    let answer = match serde_json::from_slice(input) {
        Ok(request) => run(&request),
        Err(error) => json!({"spawn_error": format!("invalid request: {error}")}),
    };
    format!("{answer}\n")
}

fn run(request: &Value) -> Value {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return json!({"spawn_error": error.to_string()}),
    };
    runtime.block_on(run_request(request))
}

async fn run_request(request: &Value) -> Value {
    let path = |name: &str| PathBuf::from(request[name].as_str().unwrap_or_default());
    let drives = request["drives"].as_object().map(|drives| SessionDrives {
        directory: PathBuf::from(drives["directory"].as_str().unwrap_or_default()),
        mounts: drives["mounts"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|mount| DriveMount {
                path: PathBuf::from(mount["path"].as_str().unwrap_or_default()),
                access: serde_json::from_value(mount["access"].clone())
                    .unwrap_or(DriveAccess::ReadOnly),
            })
            .collect(),
    });
    let cwd = path("cwd");
    let tetral = match std::env::current_exe().and_then(|exe| exe.canonicalize()) {
        Ok(tetral) => tetral,
        Err(error) => return json!({"spawn_error": error.to_string()}),
    };
    let inputs = TranslationInputs {
        state_root: path("state_root"),
        socket_directory: path("socket_directory"),
        task_directories: path("task_directories"),
        task_temporary_directory: path("task_temporary_directory"),
        drives,
        trusted_programs: super::trusted_programs(&tetral, None, &cwd),
    };
    let mut argv = vec![
        tetral.display().to_string(),
        crate::child::ENTRY.to_owned(),
        crate::child::exec::KIND.to_owned(),
        cwd.display().to_string(),
    ];
    argv.extend(strings(&request["argv"]).map(str::to_owned));
    let command = Command {
        argv,
        env: Some(
            request["env"]
                .as_object()
                .into_iter()
                .flatten()
                .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_owned())))
                .collect::<HashMap<_, _>>(),
        ),
        directory: Some(cwd.clone()),
    };

    let permissions = match TaskPermissions::admit(request.get("policy"), &inputs) {
        Ok(permissions) => permissions,
        Err(rejection) => return rejected(rejection),
    };
    let isolation = Isolation::new();
    let available = match isolation.check(&permissions, &cwd, &tetral).await {
        Ok(available) => available,
        Err(rejection) => return rejected(rejection),
    };
    let status = match std::fs::OpenOptions::new().write(true).open("/dev/null") {
        Ok(status) => status,
        Err(error) => return json!({"spawn_error": error.to_string()}),
    };
    let mut process = match isolation
        .launch(
            &available,
            command,
            &permissions,
            &cwd,
            &tetral,
            Some(status.into()),
        )
        .await
    {
        Ok(process) => process,
        Err(LaunchError::Rejected(rejection)) => return rejected(rejection),
        Err(LaunchError::Spawn(error)) => return json!({"spawn_error": error.to_string()}),
    };
    drop(process.child.stdin.take());
    let collect = |stream: Option<Box<dyn tokio::io::AsyncRead + Unpin + Send>>| async move {
        let mut bytes = Vec::new();
        if let Some(mut stream) = stream {
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut bytes).await;
        }
        String::from_utf8_lossy(&bytes).into_owned()
    };
    let stdout = process
        .child
        .stdout
        .take()
        .map(|stream| Box::new(stream) as _);
    let stderr = process
        .child
        .stderr
        .take()
        .map(|stream| Box::new(stream) as _);
    let (stdout, stderr) = tokio::join!(collect(stdout), collect(stderr));
    let exit_code = match process.child.wait().await {
        Ok(status) => status
            .code()
            .or_else(|| {
                std::os::unix::process::ExitStatusExt::signal(&status).map(|signal| 128 + signal)
            })
            .unwrap_or(-1),
        Err(_) => -1,
    };
    json!({"exit_code": exit_code, "stdout": stdout, "stderr": stderr})
}

fn strings(value: &Value) -> impl Iterator<Item = &str> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
}

fn rejected(rejection: crate::policy::PolicyRejection) -> Value {
    json!({"rejected": {"code": rejection.code, "message": rejection.message}})
}
