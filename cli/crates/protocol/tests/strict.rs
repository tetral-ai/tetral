//! What the decoders refuse beyond the fixtures: input that two JSON readers
//! could read two ways, which the strict pass refuses before any type sees it;
//! a member sent as `null` where its schema refuses `null`; an identity that
//! is empty or holds U+0000; and an unknown member in an object that the
//! schemas close.

mod common;

use std::collections::{BTreeSet, HashMap};

use common::{FILES, Fixture, by_name, decode_fixture, fixtures, read_json, validator};
use jsonschema::Validator;
use protocol::connection::Heartbeat;
use protocol::drive_transfer::ListFilesResponse;
use protocol::{DecodeError, StrictErrorKind, decode, strict};
use serde_json::Value;

#[test]
fn the_strict_reader_refuses_ambiguous_json() {
    use StrictErrorKind::*;
    let rows: [(&str, &str, Option<StrictErrorKind>); 11] = [
        (
            "repeated key at depth 1",
            r#"{"a":1,"a":1}"#,
            Some(DuplicateMember),
        ),
        (
            "repeated key at depth 3",
            r#"{"a":{"b":{"c":1,"c":2}}}"#,
            Some(DuplicateMember),
        ),
        (
            "repeated key spelled with an escape",
            r#"{"a":1,"\u0061":2}"#,
            Some(DuplicateMember),
        ),
        ("lone high surrogate", r#""\ud800""#, Some(LoneSurrogate)),
        ("lone low surrogate", r#""\udc00""#, Some(LoneSurrogate)),
        ("escaped surrogate pair", r#""\ud83d\ude00""#, None),
        (
            "number beyond a 64-bit float",
            "1e400",
            Some(NonFiniteNumber),
        ),
        (
            "integer above the signed 64-bit range",
            "9223372036854775808",
            Some(IntegerOutOfRange),
        ),
        (
            "integer below the signed 64-bit range",
            "-9223372036854775809",
            Some(IntegerOutOfRange),
        ),
        ("largest signed 64-bit integer", "9223372036854775807", None),
        (
            "smallest signed 64-bit integer",
            "-9223372036854775808",
            None,
        ),
    ];
    for (case, input, refused) in rows {
        let outcome = strict::check(input.as_bytes()).map_err(|error| error.kind);
        assert_eq!(outcome.err(), refused, "{case}");
    }
}

#[test]
fn the_strict_pass_and_the_typed_decoder_accept_the_same_nesting() {
    let nested = |levels: usize| "[".repeat(levels) + &"]".repeat(levels);
    let deepest = nested(strict::MAX_NESTING);
    assert_eq!(strict::check(deepest.as_bytes()), Ok(()));
    assert!(serde_json::from_str::<serde_json::Value>(&deepest).is_ok());
    let deeper = nested(strict::MAX_NESTING + 1);
    assert_eq!(
        strict::check(deeper.as_bytes()).map_err(|error| error.kind),
        Err(StrictErrorKind::TooDeep)
    );
    assert!(serde_json::from_str::<serde_json::Value>(&deeper).is_err());
}

#[test]
fn the_decoder_reads_through_the_strict_pass_and_keeps_integers_exact() {
    let heartbeat = |seq: &str| {
        format!(
            r#"{{"protocol_version":1,"connection_id":"c","message_id":"m","request_message_id":"r","heartbeat_seq":{seq}}}"#
        )
    };
    // 2^53 + 1 is the first integer that a 64-bit float cannot hold.
    for (input, seq) in [
        (heartbeat("9007199254740993"), 9_007_199_254_740_993),
        (heartbeat("9223372036854775807"), i64::MAX),
    ] {
        let decoded: Heartbeat = decode(input.as_bytes()).expect("the sequence decodes");
        assert_eq!(decoded.heartbeat_seq.get(), seq);
    }
    let listing: ListFilesResponse =
        decode(br#"{"files":[],"has_more":false,"start_seq":-0}"#).expect("the integer -0 decodes");
    assert_eq!(listing.start_seq.map(|seq| seq.get()), Some(0));
    let repeated = heartbeat("1,\"heartbeat_seq\":2");
    assert!(matches!(
        decode::<Heartbeat>(repeated.as_bytes()),
        Err(DecodeError::Strict(error)) if error.kind == StrictErrorKind::DuplicateMember
    ));
}

const TOOL: &str = "tool-execution-v1";
const CONNECTION: &str = "connection-v1";
const TRANSFER: &str = "drive-transfer-v1";

/// The fixture files and schema validators that the table rows below use.
struct Protocol {
    documents: HashMap<&'static str, Value>,
    validators: HashMap<(String, String), Validator>,
}

impl Protocol {
    fn read() -> Self {
        Protocol {
            documents: FILES
                .iter()
                .map(|(file, ..)| (*file, read_json(&format!("{file}.fixtures.json"))))
                .collect(),
            validators: HashMap::new(),
        }
    }

    /// The file that holds the valid fixture `name`, which no other file
    /// holds.
    fn file_holding(&self, name: &str) -> &'static str {
        let files: Vec<&'static str> = FILES
            .iter()
            .map(|(file, ..)| *file)
            .filter(|file| {
                fixtures(&self.documents[file])
                    .iter()
                    .any(|fixture| fixture.name == name && fixture.valid)
            })
            .collect();
        match files[..] {
            [file] => file,
            _ => panic!("valid fixture {name} in {files:?}"),
        }
    }

    /// The value of the valid fixture `name`, with its schema's pointer.
    fn fixture(&self, file: &str, name: &str) -> (Value, String) {
        let fixtures = fixtures(&self.documents[file]);
        let fixture = fixtures
            .iter()
            .find(|fixture| fixture.name == name && fixture.valid)
            .unwrap_or_else(|| panic!("no valid fixture {file} {name}"));
        (fixture.value.clone(), format!("/$defs/{}", fixture.schema))
    }

    /// Decodes `value` in place of fixture `name`'s value, with the status,
    /// endpoint and paired fixtures that `name` gives.
    fn decode(&self, file: &str, name: &str, value: &Value) -> Result<Value, DecodeError> {
        let fixtures = fixtures(&self.documents[file]);
        let by_name = by_name(&fixtures);
        let fixture = by_name[name];
        let edited = Fixture {
            name: fixture.name,
            schema: fixture.schema,
            valid: fixture.valid,
            value,
            raw: fixture.raw,
        };
        decode_fixture(file, &edited, &by_name)
    }

    fn passes(&mut self, file: &str, pointer: &str, value: &Value) -> bool {
        self.validators
            .entry((file.to_owned(), pointer.to_owned()))
            .or_insert_with(|| validator(file, pointer))
            .is_valid(value)
    }
}

/// Sets the member or item at `pointer`, adding a member that is not there.
fn put(value: &mut Value, pointer: &str, new: Value) {
    let (parent, last) = pointer.rsplit_once('/').expect("a member pointer");
    match value.pointer_mut(parent) {
        Some(Value::Object(members)) => {
            members.insert(last.to_owned(), new);
        }
        Some(Value::Array(items)) => items[last.parse::<usize>().expect("an item index")] = new,
        _ => panic!("no object or array at {parent:?}"),
    }
}

fn remove(value: &mut Value, pointer: &str) {
    let (parent, last) = pointer.rsplit_once('/').expect("a member pointer");
    let members = value
        .pointer_mut(parent)
        .and_then(Value::as_object_mut)
        .unwrap_or_else(|| panic!("no object at {parent:?}"));
    assert!(members.remove(last).is_some(), "no member {pointer}");
}

/// Whether typed decoding refused the message for the reason that `word`
/// names, rather than for another one.
fn refused_naming(decoded: &Result<Value, DecodeError>, word: &str) -> bool {
    matches!(decoded, Err(DecodeError::Shape(text)) if text.contains(word))
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Presence {
    Required,
    Optional,
}

#[test]
fn a_member_sent_as_null_where_its_schema_refuses_null_is_refused() {
    use Presence::*;
    #[rustfmt::skip]
    let rows: [(&str, &str, &str, Presence); 50] = [
        (TOOL, "ack", "/message_id", Required),
        (TOOL, "ack", "/accepted", Required),
        (TOOL, "protocol_error_without_request_id", "/error/code", Required),
        (TOOL, "execute_edit", "/payload/tool_use_event_id", Required),
        (TOOL, "execute_edit", "/payload/input", Required),
        (TOOL, "execute_edit", "/payload/sandbox_policy/filesystem/rules/0/path", Required),
        (TOOL, "drive_sync_report", "/drive_sync/committed", Required),
        (CONNECTION, "heartbeat_report", "/heartbeat_seq", Required),
        (CONNECTION, "connected_resumed", "/type", Required),
        (CONNECTION, "connected_resumed", "/payload", Required),
        (CONNECTION, "drive_changed", "/payload/drive_id", Required),
        (CONNECTION, "events_query_new", "/protocol_version", Required),
        (CONNECTION, "error_rate_limited", "/code", Required),
        (CONNECTION, "ready_no_tools", "/capabilities/session_preparation", Required),
        (CONNECTION, "mcp_tools_empty_listing", "/server_info/name", Required),
        (CONNECTION, "closed_report", "/status", Required),
        (TRANSFER, "rate_limited_headers", "/Retry-After", Required),
        (TRANSFER, "commit_request_delete_of_absent_file", "/changes/0/path", Required),
        (TRANSFER, "list_changes_page_from_beginning", "/records/0/version_id", Required),
        (TRANSFER, "error_drive_cursor_expired", "/message", Required),
        (TRANSFER, "list_files_empty_drive", "/has_more", Required),
        (TOOL, "execute_with_traceparent", "/traceparent", Optional),
        (TOOL, "drive_sync_report_with_task", "/task_id", Optional),
        (TOOL, "ready_with_local_mcp", "/local_mcp", Optional),
        (TOOL, "partial_stdin_error_preserves_result", "/error", Optional),
        (TOOL, "partial_stdin_error_preserves_result", "/result", Optional),
        (TOOL, "not_synced_uploading", "/result/drive_sync", Optional),
        (TOOL, "mcp_cancel_reply", "/result/result/cancelled", Optional),
        (TOOL, "mcp_cancel_reply", "/result/result/was_running", Optional),
        (TOOL, "mcp_terminal", "/result/result/isError", Optional),
        (TOOL, "mcp_terminal", "/result/result/structuredContent", Optional),
        (CONNECTION, "events_query_resume", "/previous_connection_id", Optional),
        (CONNECTION, "events_query_resume", "/resume", Optional),
        (CONNECTION, "session_close_with_traceparent", "/traceparent", Optional),
        (CONNECTION, "protocol_error_report", "/request_message_id", Optional),
        (CONNECTION, "ready_with_startup", "/startup", Optional),
        (CONNECTION, "ready_local_mcp_without_tools", "/capabilities/local_mcp", Optional),
        (CONNECTION, "prepared_report", "/repository_failures", Optional),
        (CONNECTION, "applied_report", "/drive_mount_failures", Optional),
        (CONNECTION, "applied_report", "/lost_changes", Optional),
        (CONNECTION, "mcp_tools_listing", "/tools/0/description", Optional),
        (CONNECTION, "mcp_tools_listing", "/tools/0/annotations", Optional),
        (CONNECTION, "mcp_tools_listing", "/tools/1/title", Optional),
        (CONNECTION, "mcp_tools_listing", "/tools/1/outputSchema", Optional),
        (TRANSFER, "list_files_next_page_query", "/after_path", Optional),
        (TRANSFER, "list_changes_query_inside_commit", "/after_index", Optional),
        (TRANSFER, "list_files_empty_drive", "/start_seq", Optional),
        (TRANSFER, "upload_headers_empty_file", "/Content-Length", Optional),
        (TRANSFER, "download_headers_open_range", "/Range", Optional),
        (TRANSFER, "commit_response", "/results/2/current", Optional),
    ];
    let mut protocol = Protocol::read();
    for (file, name, member, presence) in rows {
        let case = format!("{name} {member}");
        let (value, schema) = protocol.fixture(file, name);
        let mut null = value.clone();
        put(&mut null, member, Value::Null);
        let mut without = value;
        remove(&mut without, member);
        // Each row is what it claims: the schema refuses the null, and lets
        // the member be absent exactly when the row says it is optional.
        assert!(
            !protocol.passes(file, &schema, &null),
            "{case}: the schema allows null"
        );
        assert_eq!(
            protocol.passes(file, &schema, &without),
            presence == Optional,
            "{case}: not {presence:?}"
        );
        let decoded = protocol.decode(file, name, &null);
        assert!(
            refused_naming(&decoded, "null"),
            "{case}: null decoded as {decoded:?}"
        );
    }
}

#[test]
fn an_identity_that_is_empty_or_holds_nul_is_refused() {
    #[rustfmt::skip]
    let rows: [(&str, &str, &str); 6] = [
        (CONNECTION, "connected_resumed", "/message_id"),
        (CONNECTION, "events_query_new", "/instance_id"),
        (TOOL, "ack", "/message_id"),
        (TOOL, "execute_edit", "/payload/tool_use_event_id"),
        (TRANSFER, "list_files_next_page_query", "/binding_id"),
        (TRANSFER, "list_changes_page_from_beginning", "/records/0/version_id"),
    ];
    let mut protocol = Protocol::read();
    for (file, name, member) in rows {
        let (value, schema) = protocol.fixture(file, name);
        for identity in ["", "id\u{0}tail"] {
            let case = format!("{name} {member} {identity:?}");
            let mut edited = value.clone();
            put(&mut edited, member, Value::String(identity.to_owned()));
            assert!(
                !protocol.passes(file, &schema, &edited),
                "{case}: the schema allows it"
            );
            let decoded = protocol.decode(file, name, &edited);
            assert!(
                refused_naming(&decoded, "nonempty string without U+0000"),
                "{case}: decoded as {decoded:?}"
            );
        }
    }
}

/// Closed definitions that no message holds, so no message can carry an
/// unknown member in them: the drive transfer limits and endpoint tables.
const TABLES: [(&str, &str); 2] = [
    (TRANSFER, "/$defs/DriveTransferLimits"),
    (TRANSFER, "/$defs/DriveTransferEndpoints"),
];

/// Places in the schemas, as file and JSON pointer.
type Locations = BTreeSet<(String, String)>;

/// The schema locations with `additionalProperties: false`, and the targets
/// of every `$ref`.
fn schema_walk() -> (Locations, Locations) {
    fn walk(
        file: &str,
        node: &Value,
        pointer: String,
        closed: &mut Locations,
        targets: &mut Locations,
    ) {
        match node {
            Value::Object(members) => {
                if members.get("additionalProperties") == Some(&Value::Bool(false)) {
                    closed.insert((file.to_owned(), pointer.clone()));
                }
                if let Some(Value::String(target)) = members.get("$ref") {
                    let (target_file, target_pointer) =
                        target.split_once('#').expect("a $ref with a fragment");
                    let target_file = target_file.strip_suffix(".schema.json").unwrap_or(file);
                    targets.insert((target_file.to_owned(), target_pointer.to_owned()));
                }
                for (name, member) in members {
                    walk(file, member, format!("{pointer}/{name}"), closed, targets);
                }
            }
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    walk(file, item, format!("{pointer}/{index}"), closed, targets);
                }
            }
            _ => {}
        }
    }
    let (mut closed, mut targets) = (BTreeSet::new(), BTreeSet::new());
    for (file, ..) in FILES {
        let schema = read_json(&format!("{file}.schema.json"));
        walk(file, &schema, String::new(), &mut closed, &mut targets);
    }
    (closed, targets)
}

type Edits = &'static [(&'static str, &'static str)];

#[test]
fn an_unknown_member_in_each_closed_object_is_refused() {
    // Each row names a closed schema location under `$defs`, the valid
    // fixture that holds an object of it, edits that put one there where no
    // fixture does, and the object's pointer. The decoder reads the network
    // modes `disabled` and `direct` as two types, so `NetworkPolicy/oneOf/0`
    // has a row for each.
    const DIRECT: Edits = &[("/payload/sandbox_policy/network", r#"{"mode":"direct"}"#)];
    const CIDR: Edits = &[(
        "/network/rules/allow/0/destination",
        r#"{"kind":"cidr","value":"10.0.0.0/8"}"#,
    )];
    const LISTEN: Edits = &[(
        "/network/rules/listen",
        r#"[{"address":"127.0.0.1","transport":"tcp","ports":[{"from":8080,"to":8080}]}]"#,
    )];
    const MANAGED: Edits = &[
        (
            "/tool_execution/network_modes",
            r#"["disabled","direct","managed"]"#,
        ),
        (
            "/tool_execution/managed_network",
            r#"{"transports":["tcp"],"listen":true}"#,
        ),
    ];
    const DOWNLOAD: Edits = &[(
        "/drive_sync/downloading",
        r#"[{"path":"/tetral/sessions/id_session_id/drives/id_drive_id/data/input.csv","downloaded_bytes":10,"size_bytes":20}]"#,
    )];
    const SERVICE_ACCOUNT: Edits = &[(
        "/drive_sync/rejected/0/current_writer",
        r#"{"type":"service_account_actor","service_account_id":"id_service_account_id"}"#,
    )];
    #[rustfmt::skip]
    let rows: [(&str, &str, &str, Edits, &str); 29] = [
        (TOOL, "SandboxPolicy", "execute_edit", &[], "/payload/sandbox_policy"),
        (TOOL, "FilesystemPolicy", "execute_edit", &[], "/payload/sandbox_policy/filesystem"),
        (TOOL, "FilesystemRule", "execute_edit", &[], "/payload/sandbox_policy/filesystem/rules/0"),
        (TOOL, "NetworkPolicy/oneOf/0", "execute_edit", &[], "/payload/sandbox_policy/network"),
        (TOOL, "NetworkPolicy/oneOf/0", "execute_edit", DIRECT, DIRECT[0].0),
        (TOOL, "NetworkPolicy/oneOf/1", "managed_explicit_rules", &[], "/network"),
        (TOOL, "ManagedNetworkRules", "managed_explicit_rules", &[], "/network/rules"),
        (TOOL, "NetworkRule", "managed_explicit_rules", &[], "/network/rules/allow/0"),
        (TOOL, "NetworkDestination/oneOf/0", "managed_explicit_rules", &[], "/network/rules/allow/0/destination"),
        (TOOL, "NetworkDestination/oneOf/1", "managed_explicit_rules", CIDR, CIDR[0].0),
        (TOOL, "PortRange", "managed_explicit_rules", &[], "/network/rules/allow/0/ports/0"),
        (TOOL, "NetworkListen", "managed_explicit_rules", LISTEN, "/network/rules/listen/0"),
        (TOOL, "TransportLimits", "ready_direct_and_disabled", &[], "/tool_execution/limits"),
        (TOOL, "ManagedNetworkCapability", "ready_direct_and_disabled", MANAGED, MANAGED[1].0),
        (TOOL, "DriveSyncOutcome", "drive_sync_report", &[], "/drive_sync"),
        (TOOL, "DriveRejection", "drive_sync_report", &[], "/drive_sync/rejected/0"),
        (TOOL, "DriveNotSynced", "not_synced_uploading", &[], "/result/drive_sync/not_synced/0"),
        (TOOL, "DriveDownload", "drive_sync_report", DOWNLOAD, "/drive_sync/downloading/0"),
        (TOOL, "DriveMountFailure", "applied_report", &[], "/drive_mount_failures/0"),
        (TOOL, "DriveWriter/oneOf/0", "commit_response_rejections", &[], "/results/0/current/writer"),
        (TOOL, "DriveWriter/oneOf/1", "drive_sync_report", &[], "/drive_sync/rejected/1/current_writer"),
        (TOOL, "DriveWriter/oneOf/2", "drive_sync_report", SERVICE_ACCOUNT, SERVICE_ACCOUNT[0].0),
        (TOOL, "DriveWriter/oneOf/3", "commit_response", &[], "/results/2/current/writer"),
        (CONNECTION, "McpTool", "mcp_tools_2048_tools", &[], "/tools/0"),
        (TRANSFER, "DrivePathParams", "drive_path_params", &[], ""),
        (TRANSFER, "ContentPathParams", "content_path_params", &[], ""),
        (TRANSFER, "CommitRequest", "commit_request", &[], ""),
        (TRANSFER, "DeleteChange", "commit_request", &[], "/changes/0"),
        (TRANSFER, "WriteChange", "commit_request", &[], "/changes/1"),
    ];
    let mut protocol = Protocol::read();
    for (file, location, name, edits, object) in rows {
        let case = format!("{file}#/$defs/{location}");
        let holder = protocol.file_holding(name);
        let (mut value, schema) = protocol.fixture(holder, name);
        for (pointer, edit) in edits {
            put(
                &mut value,
                pointer,
                serde_json::from_str(edit).expect("an edit is JSON"),
            );
        }
        // The edited fixture is still valid and decodes, and the object at
        // the row's pointer is one of the row's location.
        assert!(
            protocol.passes(holder, &schema, &value),
            "{case}: the edits break the fixture"
        );
        let decoded = protocol.decode(holder, name, &value);
        assert!(
            decoded.is_ok(),
            "{case}: the edited fixture is refused: {decoded:?}"
        );
        let held = value
            .pointer(object)
            .cloned()
            .expect("an object at the pointer");
        assert!(
            protocol.passes(file, &format!("/$defs/{location}"), &held),
            "{case}: {object:?} is not this object"
        );
        put(
            &mut value,
            &format!("{object}/unexpected_member"),
            Value::from(1),
        );
        assert!(
            !protocol.passes(holder, &schema, &value),
            "{case}: the schema allows the member"
        );
        let decoded = protocol.decode(holder, name, &value);
        assert!(
            refused_naming(&decoded, "unexpected_member"),
            "{case}: decoded as {decoded:?}"
        );
    }
    let distinct: BTreeSet<_> = rows.iter().collect();
    assert_eq!(distinct.len(), rows.len(), "a row repeats");
    let (closed, targets) = schema_walk();
    let mut listed: Locations = rows
        .iter()
        .map(|(file, location, ..)| ((*file).to_owned(), format!("/$defs/{location}")))
        .collect();
    for (file, table) in TABLES {
        assert!(
            !targets
                .iter()
                .any(|(target_file, target)| target_file == file && target.starts_with(table)),
            "{file}#{table} is held by a message"
        );
        listed.insert((file.to_owned(), table.to_owned()));
    }
    assert_eq!(listed, closed);
}
