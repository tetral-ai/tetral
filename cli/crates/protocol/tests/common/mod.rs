//! The protocol's schema and fixture files as the tests read them, and the
//! decoder that each fixture's schema names.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use jsonschema::{Retrieve, Uri, Validator};
use protocol::drive_transfer as transfer;
use protocol::errors::{self, DriveTransferEndpoint};
use protocol::{Answer, DecodeError, Message, connection, decode, decode_answer, encode, tool};
use serde_json::{Value, json};

/// The fixture files with their fixture, valid and invalid counts.
pub const FILES: [(&str, usize, usize, usize); 3] = [
    ("tool-execution-v1", 61, 29, 32),
    ("connection-v1", 239, 61, 178),
    ("drive-transfer-v1", 126, 49, 77),
];

fn protocol_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../protocol")
}

pub fn read_json(file: &str) -> Value {
    let path = protocol_dir().join(file);
    let text = std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_json::from_slice(&text).unwrap_or_else(|error| panic!("{file}: {error}"))
}

/// Serves each schema file under its own file name, whatever the base URI.
struct SchemaFiles(HashMap<String, Value>);

impl Retrieve for SchemaFiles {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let name = uri.path().as_str().rsplit('/').next().unwrap_or_default();
        self.0
            .get(name)
            .cloned()
            .ok_or_else(|| format!("no schema file {name}").into())
    }
}

/// Validates against the schema at `pointer` inside a schema file, such as
/// `/$defs/Ready`, with the cross-file `$ref`s resolved by file name.
pub fn validator(file: &str, pointer: &str) -> Validator {
    let files = FILES
        .iter()
        .map(|(name, ..)| {
            let name = format!("{name}.schema.json");
            let schema = read_json(&name);
            (name, schema)
        })
        .collect();
    jsonschema::options()
        .with_base_uri("https://schemas.invalid/")
        .with_retriever(SchemaFiles(files))
        .build(&json!({ "$ref": format!("{file}.schema.json#{pointer}") }))
        .unwrap_or_else(|error| panic!("{file}#{pointer}: {error}"))
}

pub struct Fixture<'a> {
    pub name: &'a str,
    pub schema: &'a str,
    pub valid: bool,
    pub value: &'a Value,
    pub raw: &'a Value,
}

impl Fixture<'_> {
    fn bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self.value).expect("a fixture value encodes")
    }

    fn status(&self, member: &str) -> u16 {
        self.raw[member]
            .as_u64()
            .and_then(|status| u16::try_from(status).ok())
            .unwrap_or_else(|| panic!("{}: no {member}", self.name))
    }
}

fn reencoded<M: Message>(message: &M) -> Value {
    serde_json::from_slice(&encode(message)).expect("an encoded message is JSON")
}

fn round_trip<M: Message>(bytes: &[u8]) -> Result<Value, DecodeError> {
    decode::<M>(bytes).map(|message| reencoded(&message))
}

fn answer_trip<M: Answer<R>, R>(bytes: &[u8], request: &R) -> Result<Value, DecodeError> {
    decode_answer::<M, R>(bytes, request).map(|message| reencoded(&message))
}

/// Each fixture of a fixture file, in file order.
pub fn fixtures(document: &Value) -> Vec<Fixture<'_>> {
    document
        .as_array()
        .expect("a fixture file holds a list")
        .iter()
        .map(|raw| Fixture {
            name: raw["name"].as_str().expect("a fixture has a name"),
            schema: raw["schema"].as_str().expect("a fixture names its schema"),
            valid: raw["valid"]
                .as_bool()
                .expect("a fixture says whether it is valid"),
            value: &raw["value"],
            raw,
        })
        .collect()
}

/// The fixtures of one file by name, for the requests that `pair` names.
pub type ByName<'a> = HashMap<&'a str, &'a Fixture<'a>>;

pub fn by_name<'a>(fixtures: &'a [Fixture<'a>]) -> ByName<'a> {
    fixtures
        .iter()
        .map(|fixture| (fixture.name, fixture))
        .collect()
}

struct Pairs<'a> {
    fixture: &'a Fixture<'a>,
    by_name: &'a ByName<'a>,
}

impl Pairs<'_> {
    fn get<M: Message>(&self, role: &str) -> Option<M> {
        let name = self.fixture.raw["pair"][role].as_str()?;
        let paired = self.by_name[name];
        Some(
            decode::<M>(&paired.bytes()).unwrap_or_else(|error| {
                panic!("{}: paired {role} {name}: {error}", self.fixture.name)
            }),
        )
    }
}

/// Decodes a fixture as the type of its schema, with the context the fixture
/// gives, and returns the type's re-encoding.
pub fn decode_fixture(
    file: &str,
    fixture: &Fixture<'_>,
    by_name: &ByName<'_>,
) -> Result<Value, DecodeError> {
    let pairs = Pairs { fixture, by_name };
    let bytes = fixture.bytes();
    let bytes = bytes.as_slice();
    match (file, fixture.schema) {
        ("tool-execution-v1", "Downstream") => round_trip::<connection::ToolDownstream>(bytes),
        ("tool-execution-v1", "ReadyExtension") => round_trip::<tool::ReadyExtension>(bytes),
        ("tool-execution-v1", "SandboxPolicy") => round_trip::<tool::SandboxPolicy>(bytes),
        (_, "ToolStatus") => round_trip::<tool::ToolStatus>(bytes),
        (_, "DriveSyncReport") => round_trip::<tool::DriveSyncReport>(bytes),
        (_, "Acknowledgement") => round_trip::<tool::Acknowledgement>(bytes),
        (_, "ProtocolErrorReport") => round_trip::<tool::ProtocolErrorReport>(bytes),
        ("connection-v1", "EventsQuery") => round_trip::<connection::EventsQuery>(bytes),
        ("connection-v1", "Downstream") => round_trip::<connection::Downstream>(bytes),
        ("connection-v1", "Ready") => round_trip::<connection::Ready>(bytes),
        ("connection-v1", "Heartbeat") => round_trip::<connection::Heartbeat>(bytes),
        ("connection-v1", "GrantAcknowledgement") => {
            round_trip::<connection::GrantAcknowledgement>(bytes)
        }
        ("connection-v1", "SessionStatus") => round_trip::<connection::SessionStatus>(bytes),
        ("connection-v1", "McpTools") => round_trip::<connection::McpTools>(bytes),
        ("connection-v1", "CredentialRequest") => {
            round_trip::<connection::CredentialRequest>(bytes)
        }
        ("connection-v1", "CredentialResponse") => {
            round_trip::<connection::CredentialResponse>(bytes)
        }
        ("connection-v1", "GitTicketRequest") => round_trip::<connection::GitTicketRequest>(bytes),
        ("connection-v1", "GitTicketResponse") => {
            round_trip::<connection::GitTicketResponse>(bytes)
        }
        ("connection-v1", "ErrorBody") => {
            errors::decode_client_error(fixture.status("http_status"), bytes)
                .map(|body| reencoded(&body))
        }
        ("drive-transfer-v1", "ListFilesQuery") => round_trip::<transfer::ListFilesQuery>(bytes),
        ("drive-transfer-v1", "ListChangesQuery") => {
            round_trip::<transfer::ListChangesQuery>(bytes)
        }
        ("drive-transfer-v1", "CommonQuery") => round_trip::<transfer::CommonQuery>(bytes),
        ("drive-transfer-v1", "DrivePathParams") => round_trip::<transfer::DrivePathParams>(bytes),
        ("drive-transfer-v1", "ContentPathParams") => {
            round_trip::<transfer::ContentPathParams>(bytes)
        }
        ("drive-transfer-v1", "UploadContentRequestHeaders") => {
            round_trip::<transfer::UploadContentRequestHeaders>(bytes)
        }
        ("drive-transfer-v1", "DownloadContentRequestHeaders") => {
            round_trip::<transfer::DownloadContentRequestHeaders>(bytes)
        }
        ("drive-transfer-v1", "PartialContentResponseHeaders") => {
            match pairs.get::<transfer::DownloadContentRequestHeaders>("request_headers") {
                Some(request) => {
                    answer_trip::<transfer::PartialContentResponseHeaders, _>(bytes, &request)
                }
                None => round_trip::<transfer::PartialContentResponseHeaders>(bytes),
            }
        }
        ("drive-transfer-v1", "RateLimitedResponseHeaders") => {
            round_trip::<transfer::RateLimitedResponseHeaders>(bytes)
        }
        ("drive-transfer-v1", "ListFilesResponse") => match pairs.get("query") {
            Some(query) => {
                answer_trip::<transfer::ListFilesResponse, transfer::ListFilesQuery>(bytes, &query)
            }
            None => round_trip::<transfer::ListFilesResponse>(bytes),
        },
        ("drive-transfer-v1", "ListChangesResponse") => match pairs.get("query") {
            Some(query) => {
                answer_trip::<transfer::ListChangesResponse, transfer::ListChangesQuery>(
                    bytes, &query,
                )
            }
            None => round_trip::<transfer::ListChangesResponse>(bytes),
        },
        ("drive-transfer-v1", "DriveChanged") => round_trip::<transfer::DriveChanged>(bytes),
        ("drive-transfer-v1", "UploadContentResponse") => {
            let path = pairs.get::<transfer::ContentPathParams>("path_params");
            let headers = pairs.get::<transfer::UploadContentRequestHeaders>("request_headers");
            match (&path, &headers) {
                (Some(path), Some(headers)) => answer_trip::<transfer::UploadContentResponse, _>(
                    bytes,
                    &transfer::UploadRequest { path, headers },
                ),
                (None, None) => round_trip::<transfer::UploadContentResponse>(bytes),
                _ => panic!(
                    "{}: an upload pairs with its path and its headers",
                    fixture.name
                ),
            }
        }
        ("drive-transfer-v1", "CommitRequest") => round_trip::<transfer::CommitRequest>(bytes),
        ("drive-transfer-v1", "CommitResponse") => match pairs.get("request") {
            Some(request) => {
                answer_trip::<transfer::CommitResponse, transfer::CommitRequest>(bytes, &request)
            }
            None => round_trip::<transfer::CommitResponse>(bytes),
        },
        ("drive-transfer-v1", "Error") => {
            let endpoint: DriveTransferEndpoint =
                serde_json::from_value(fixture.raw["endpoint"].clone())
                    .unwrap_or_else(|error| panic!("{}: endpoint: {error}", fixture.name));
            errors::decode_drive_transfer_error(endpoint, fixture.status("status"), bytes)
                .map(|body| reencoded(&body))
        }
        (file, schema) => panic!("{}: no decoder for {file} {schema}", fixture.name),
    }
}
