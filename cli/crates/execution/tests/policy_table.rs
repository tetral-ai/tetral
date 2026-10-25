//! The policy table: each policy with its exact rejection code, and the
//! profile table: each policy with the exact entries of its profile and the
//! access that Codex's own resolution of that profile gives.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use codex_protocol::models::{ManagedFileSystemPermissions, PermissionProfile};
use codex_protocol::permissions::{
    FileSystemAccessMode, FileSystemPath, FileSystemSandboxEntry, FileSystemSpecialPath,
    NetworkSandboxPolicy,
};
use codex_utils_absolute_path::AbsolutePathBuf;
use execution::policy::{
    DriveMount, PolicyRejection, SessionDrives, TaskPermissions, TranslationInputs, validate,
};
use protocol::connection::DriveAccess;
use protocol::tool::RejectionCode;
use serde_json::{Value, json};

fn fixtures() -> HashMap<String, Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../protocol/tool-execution-v1.fixtures.json");
    let document: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    document
        .as_array()
        .unwrap()
        .iter()
        .map(|fixture| {
            (
                fixture["name"].as_str().unwrap().to_owned(),
                fixture["value"].clone(),
            )
        })
        .collect()
}

fn policy(rules: Value, network: &str) -> Value {
    json!({"schema_version": 1, "filesystem": {"rules": rules}, "network": {"mode": network}})
}

/// A managed policy of the fixtures changed the way the contract checker
/// changes it for one semantic check.
fn managed(fixtures: &HashMap<String, Value>, change: impl FnOnce(&mut Value)) -> Value {
    let mut policy = fixtures["managed_explicit_rules"].clone();
    change(&mut policy["network"]["rules"]);
    policy
}

/// A change of a policy's managed network rules.
type Change = fn(&mut Value);

#[test]
fn each_policy_gets_its_exact_rejection_code() {
    use RejectionCode::{InvalidInput, UnsupportedPolicy};
    let fixtures = fixtures();
    let downstream = |name: &str| fixtures[name]["payload"].get("sandbox_policy").cloned();
    let mut rows: Vec<(&str, Option<Value>, RejectionCode)> = vec![
        ("missing_policy", downstream("missing_policy"), InvalidInput),
        ("null_policy", downstream("null_policy"), InvalidInput),
    ];
    for name in [
        "missing_network",
        "unknown_policy_field",
        "relative_permission_path",
        "unknown_network_mode",
        "managed_without_rules",
    ] {
        rows.push((name, Some(fixtures[name].clone()), InvalidInput));
    }
    rows.push((
        "managed_explicit_rules",
        Some(fixtures["managed_explicit_rules"].clone()),
        UnsupportedPolicy,
    ));
    // The contract checker's semantic checks of a policy: a policy it accepts
    // passes validation and is refused only for being managed.
    let semantic: [(&str, Change, RejectionCode); 8] = [
        (
            "reversed_network_ports",
            |rules| rules["allow"][0]["ports"][0] = json!({"from": 444, "to": 443}),
            InvalidInput,
        ),
        (
            "URL_is_not_host_rule",
            |rules| rules["allow"][0]["destination"]["value"] = json!("https://example.com/path"),
            InvalidInput,
        ),
        (
            "unknown_CIDR",
            |rules| {
                rules["allow"][0]["destination"] = json!({"kind": "cidr", "value": "10.0.0.1/99"})
            },
            InvalidInput,
        ),
        (
            "host_UTS46_full_stop_case_and_trailing_dot",
            |rules| rules["allow"][0]["destination"]["value"] = json!("B\u{fc}cher\u{3002}DE."),
            UnsupportedPolicy,
        ),
        (
            "host_with_zero_width_joiner",
            |rules| rules["allow"][0]["destination"]["value"] = json!("a\u{200d}b.com"),
            InvalidInput,
        ),
        (
            "CIDR_netmask_form",
            |rules| {
                rules["allow"][0]["destination"] =
                    json!({"kind": "cidr", "value": "10.0.0.0/255.0.0.0"})
            },
            InvalidInput,
        ),
        (
            "CIDR_prefix_with_leading_zero",
            |rules| {
                rules["allow"][0]["destination"] = json!({"kind": "cidr", "value": "10.0.0.0/08"})
            },
            InvalidInput,
        ),
        (
            "listen_address_with_zone",
            |rules| rules["listen"] = json!([{"address": "fe80::1%a", "transport": "tcp", "ports": [{"from": 8080, "to": 8080}]}]),
            InvalidInput,
        ),
    ];
    for (name, change, code) in semantic {
        rows.push((name, Some(managed(&fixtures, change)), code));
    }
    let mut version_2 = policy(json!([]), "disabled");
    version_2["schema_version"] = json!(2);
    let mut version_text = policy(json!([]), "disabled");
    version_text["schema_version"] = json!("1");
    rows.extend([
        ("schema_version 2", Some(version_2), UnsupportedPolicy),
        ("schema_version \"1\"", Some(version_text), InvalidInput),
        (
            "/a read with /a/ read_write",
            Some(policy(
                json!([{"path": "/a", "access": "read"}, {"path": "/a/", "access": "read_write"}]),
                "disabled",
            )),
            InvalidInput,
        ),
    ]);

    for (name, policy, code) in rows {
        let rejection = validate(policy.as_ref()).expect_err(name);
        assert_eq!(rejection.code, code, "{name}: {}", rejection.message);
    }
}

const ROOT: &str = "/home/user/.tetral";
const SOCKETS: &str = "/tmp/tetral-1000";
const TASKS: &str = "/var/tmp/tetral-tasks";
const TASK_TMP: &str = "/var/tmp/tetral-tasks/task_1";
const TETRAL: &str = "/usr/local/bin/tetral";
const PROJECT: &str = "/srv/project";
const DRIVES: &str = "/home/user/.tetral/sessions/session_1/drives";
const MOUNT: &str = "/home/user/.tetral/sessions/session_1/drives/drive_1";

fn inputs(drive: Option<DriveAccess>) -> TranslationInputs {
    TranslationInputs {
        state_root: ROOT.into(),
        socket_directory: SOCKETS.into(),
        task_directories: TASKS.into(),
        task_temporary_directory: TASK_TMP.into(),
        drives: drive.map(|access| SessionDrives {
            directory: DRIVES.into(),
            mounts: vec![DriveMount {
                path: MOUNT.into(),
                access,
            }],
        }),
        trusted_programs: vec![TETRAL.into()],
    }
}

fn admit(
    policy: Option<&Value>,
    drive: Option<DriveAccess>,
) -> Result<TaskPermissions, PolicyRejection> {
    TaskPermissions::admit(policy, &inputs(drive))
}

#[test]
fn two_spellings_of_one_path_with_one_access_count_as_one_rule() {
    let permissions = admit(
        Some(&policy(
            json!([{"path": "/a/./b/../c", "access": "read"}, {"path": "/a/c", "access": "read"}]),
            "disabled",
        )),
        None,
    )
    .unwrap();
    assert_entries(&permissions, vec![read("/a/c")]);
}

fn path(path: &str) -> FileSystemPath {
    FileSystemPath::from(AbsolutePathBuf::from_absolute_path(path).unwrap())
}

fn read(at: &str) -> FileSystemSandboxEntry {
    FileSystemSandboxEntry::new(path(at), FileSystemAccessMode::Read)
}

fn write(at: &str) -> FileSystemSandboxEntry {
    FileSystemSandboxEntry::new(path(at), FileSystemAccessMode::Write)
}

fn deny(at: &str) -> FileSystemSandboxEntry {
    FileSystemSandboxEntry::new(path(at), FileSystemAccessMode::Deny)
}

/// Write access for a path that need not exist.
fn write_if_present(at: &str) -> FileSystemSandboxEntry {
    FileSystemSandboxEntry::skip_missing_path(path(at), FileSystemAccessMode::Write)
}

/// The entries of every profile under these inputs without drives: the
/// platform's minimal reads and the mandatory protections.
fn protections() -> Vec<FileSystemSandboxEntry> {
    vec![
        FileSystemSandboxEntry::new(
            FileSystemPath::Special {
                value: FileSystemSpecialPath::Minimal,
            },
            FileSystemAccessMode::Read,
        ),
        deny(ROOT),
        deny(SOCKETS),
        deny(TASKS),
        write(TASK_TMP),
        write_if_present("/var/tmp/tetral-tasks/task_1/.git"),
        write_if_present("/var/tmp/tetral-tasks/task_1/.agents"),
        write_if_present("/var/tmp/tetral-tasks/task_1/.codex"),
        write_if_present("/var/tmp/tetral-tasks/task_1/.aws"),
        read(TETRAL),
    ]
}

fn entries(permissions: &TaskPermissions) -> &[FileSystemSandboxEntry] {
    match permissions.profile() {
        PermissionProfile::Managed {
            file_system: ManagedFileSystemPermissions::Restricted { entries, .. },
            ..
        } => entries,
        other => panic!("not a restricted managed profile: {other:?}"),
    }
}

/// The profile holds exactly the protections and `extra`, each once.
fn assert_entries(permissions: &TaskPermissions, extra: Vec<FileSystemSandboxEntry>) {
    let count = |entries: Vec<FileSystemSandboxEntry>| {
        let mut counts = HashMap::new();
        for entry in entries {
            *counts.entry(entry).or_insert(0) += 1;
        }
        counts
    };
    let expected = protections().into_iter().chain(extra).collect();
    assert_eq!(count(entries(permissions).to_vec()), count(expected));
}

/// The access that Codex's own resolution gives a path: `deny`, `read` or
/// `write`.
fn access(permissions: &TaskPermissions, at: &str) -> &'static str {
    let policy = permissions.profile().file_system_sandbox_policy();
    let at = Path::new(at);
    let cwd = Path::new(PROJECT);
    if policy.can_write_local_path_with_cwd(at, cwd) {
        "write"
    } else if policy.can_read_local_path_with_cwd(at, cwd) {
        "read"
    } else {
        "deny"
    }
}

struct Row {
    name: &'static str,
    rules: Value,
    drive: Option<DriveAccess>,
    extra: Vec<FileSystemSandboxEntry>,
    resolved: Vec<(&'static str, &'static str)>,
}

#[test]
fn each_policy_gets_its_exact_profile_and_resolved_access() {
    let project = |at: &str| format!("{PROJECT}{at}");
    let rows = [
        Row {
            name: "no rules",
            rules: json!([]),
            drive: None,
            extra: vec![],
            resolved: vec![("/srv/project/a", "deny")],
        },
        Row {
            name: "/ read with the project read_write",
            rules: json!([{"path": "/", "access": "read"}, {"path": PROJECT, "access": "read_write"}]),
            drive: None,
            extra: vec![
                read("/"),
                write(PROJECT),
                write_if_present(&project("/.agents")),
                write_if_present(&project("/.codex")),
                write_if_present(&project("/.aws")),
            ],
            resolved: vec![
                ("/srv/project/.git/config", "read"),
                ("/srv/project/.codex/x", "write"),
            ],
        },
        Row {
            name: "/ read_write",
            rules: json!([{"path": "/", "access": "read_write"}]),
            drive: None,
            extra: vec![
                write("/"),
                write_if_present("/.agents"),
                write_if_present("/.codex"),
                write_if_present("/.aws"),
            ],
            resolved: vec![
                ("/.git", "read"),
                ("/home/user/.tetral/credentials.json", "deny"),
                ("/var/tmp/tetral-tasks/task_2/out", "deny"),
                ("/var/tmp/tetral-tasks/task_1/out", "write"),
                (TETRAL, "read"),
            ],
        },
        Row {
            name: "the project read_write with its .aws denied",
            rules: json!([
                {"path": PROJECT, "access": "read_write"},
                {"path": "/srv/project/.aws", "access": "deny"},
            ]),
            drive: None,
            extra: vec![
                write(PROJECT),
                deny(&project("/.aws")),
                write_if_present(&project("/.agents")),
                write_if_present(&project("/.codex")),
            ],
            resolved: vec![("/srv/project/.aws/credentials", "deny")],
        },
        Row {
            name: "a rule inside the state root",
            rules: json!([{"path": "/home/user/.tetral/sessions", "access": "read_write"}]),
            drive: None,
            extra: vec![],
            resolved: vec![(
                "/home/user/.tetral/sessions/session_2/drives/drive_2/f",
                "deny",
            )],
        },
        Row {
            name: "a read_only binding",
            rules: json!([{"path": PROJECT, "access": "read_write"}]),
            drive: Some(DriveAccess::ReadOnly),
            extra: vec![
                write(PROJECT),
                write_if_present(&project("/.agents")),
                write_if_present(&project("/.codex")),
                write_if_present(&project("/.aws")),
                read(DRIVES),
                read(MOUNT),
            ],
            resolved: vec![(
                "/home/user/.tetral/sessions/session_1/drives/drive_1/report.md",
                "read",
            )],
        },
        Row {
            name: "a read_write binding without a read_write rule",
            rules: json!([{"path": "/", "access": "read"}]),
            drive: Some(DriveAccess::ReadWrite),
            extra: vec![read("/"), read(DRIVES), read(MOUNT)],
            resolved: vec![(
                "/home/user/.tetral/sessions/session_1/drives/drive_1/report.md",
                "read",
            )],
        },
        Row {
            name: "a read_write binding with a read_write rule",
            rules: json!([{"path": PROJECT, "access": "read_write"}]),
            drive: Some(DriveAccess::ReadWrite),
            extra: vec![
                write(PROJECT),
                write_if_present(&project("/.agents")),
                write_if_present(&project("/.codex")),
                write_if_present(&project("/.aws")),
                read(DRIVES),
                write(MOUNT),
                write_if_present(&format!("{MOUNT}/.git")),
                write_if_present(&format!("{MOUNT}/.agents")),
                write_if_present(&format!("{MOUNT}/.codex")),
                write_if_present(&format!("{MOUNT}/.aws")),
            ],
            resolved: vec![
                (
                    "/home/user/.tetral/sessions/session_1/drives/drive_1/report.md",
                    "write",
                ),
                (
                    "/home/user/.tetral/sessions/session_1/drives/drive_1/.git/config",
                    "write",
                ),
            ],
        },
    ];

    for row in rows {
        let permissions = admit(Some(&policy(row.rules, "disabled")), row.drive)
            .unwrap_or_else(|rejection| panic!("{}: {}", row.name, rejection.message));
        assert_entries(&permissions, row.extra);
        for (at, expected) in row.resolved {
            assert_eq!(access(&permissions, at), expected, "{}: {at}", row.name);
        }
    }
}

#[test]
fn disabled_and_direct_become_the_restricted_and_the_enabled_network() {
    for (mode, network) in [
        ("disabled", NetworkSandboxPolicy::Restricted),
        ("direct", NetworkSandboxPolicy::Enabled),
    ] {
        let permissions = admit(Some(&policy(json!([]), mode)), None).unwrap();
        assert_eq!(
            permissions.profile().network_sandbox_policy(),
            network,
            "{mode}"
        );
    }
}
