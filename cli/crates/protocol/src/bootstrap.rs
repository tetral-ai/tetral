//! The bootstrap: the one JSON object that a launcher gives `tetral configure`
//! on stdin, and that the CLI keeps as `credentials.json`.
//!
//! It is read under the strict pass of [`crate::strict`]; unknown members
//! are ignored, and a missing, `null` or malformed member fails. A failure
//! names the member and its rule and never quotes a value, so a key cannot
//! reach an error message.

use std::fmt;

use serde::Serialize;
use serde_json::{Map, Value};

use crate::field::{NonEmpty, NulFree, Text, TextRule};
use crate::id::Id;
use crate::strict::{self, StrictError};
use crate::time::{Rfc3339Utc, Utc};

pub const BOOTSTRAP_MAX_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Bootstrap {
    pub server_url: ServerUrl,
    pub instance_id: Id,
    pub credential_id: Id,
    pub connection_key: BootstrapKey,
    pub expires_at: Rfc3339Utc,
}

/// Engine's base URL: an absolute `https` URL, or `http` to an IP literal in
/// `127.0.0.0/8` or `[::1]`, with no user information, query or fragment,
/// kept without a trailing `/`.
pub type ServerUrl = Text<ServerUrlRule>;

pub enum ServerUrlRule {}

impl TextRule for ServerUrlRule {
    const NAME: &'static str = "https URL, or http URL to an IP address in 127.0.0.0/8 or [::1], \
        without user information, query or fragment";
    fn accepts(text: &str) -> bool {
        server_url_holds(text)
    }
}

/// The bootstrap's connection key: any nonempty text, the rule of the
/// bootstrap input, where a key that Engine issues has the stricter
/// [`ConnectionKey`](crate::connection::ConnectionKey). It never appears in
/// `Debug` or `Display` output.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct BootstrapKey(Text<NonEmpty>);

impl BootstrapKey {
    /// The key's plaintext, for the `Authorization` header only; the
    /// credential file gets it from [`Bootstrap::encode`].
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&crate::connection::ConnectionKey> for BootstrapKey {
    /// A key that Engine issued, which the bootstrap's rule always accepts.
    fn from(key: &crate::connection::ConnectionKey) -> BootstrapKey {
        BootstrapKey(Text::new(key.expose()).expect("an issued key is nonempty"))
    }
}

impl fmt::Debug for BootstrapKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Display for BootstrapKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Why a bootstrap was refused. It names a member and its rule, never a
/// value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BootstrapError {
    #[error("the bootstrap is larger than {} bytes", grouped(*.limit))]
    TooLarge { limit: usize },
    #[error("the bootstrap is not valid JSON: {0}")]
    Strict(#[from] StrictError),
    #[error("the bootstrap is not valid JSON")]
    InvalidJson,
    #[error("the bootstrap is not a JSON object")]
    NotAnObject,
    #[error("{member} is missing")]
    Missing { member: &'static str },
    #[error("{member} is null")]
    Null { member: &'static str },
    #[error("{member} is not a string")]
    NotAString { member: &'static str },
    #[error("{member} is not a valid {rule}")]
    Invalid {
        member: &'static str,
        rule: &'static str,
    },
}

/// `65536` as `65,536`.
fn grouped(bytes: usize) -> String {
    let digits = bytes.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

impl Bootstrap {
    pub fn decode(input: &[u8]) -> Result<Bootstrap, BootstrapError> {
        Bootstrap::decode_with_limit(input, BOOTSTRAP_MAX_BYTES)
    }

    /// `decode` with another size limit, for tests.
    pub fn decode_with_limit(input: &[u8], max_bytes: usize) -> Result<Bootstrap, BootstrapError> {
        if input.len() > max_bytes {
            return Err(BootstrapError::TooLarge { limit: max_bytes });
        }
        strict::check(input)?;
        let value: Value =
            serde_json::from_slice(input).map_err(|_| BootstrapError::InvalidJson)?;
        let Value::Object(members) = value else {
            return Err(BootstrapError::NotAnObject);
        };
        Ok(Bootstrap {
            server_url: member(&members, "server_url", ServerUrlRule::NAME, |text| {
                ServerUrl::new(text.trim_end_matches('/')).ok()
            })?,
            instance_id: member(&members, "instance_id", NulFree::NAME, |text| {
                Id::new(text).ok()
            })?,
            credential_id: member(&members, "credential_id", NulFree::NAME, |text| {
                Id::new(text).ok()
            })?,
            connection_key: member(&members, "connection_key", NonEmpty::NAME, |text| {
                Text::new(text).ok().map(BootstrapKey)
            })?,
            expires_at: member(&members, "expires_at", Utc::NAME, |text| {
                Rfc3339Utc::new(text).ok()
            })?,
        })
    }

    /// The bootstrap as compact JSON with the key in plain text, the form
    /// `credentials.json` holds.
    pub fn encode(&self) -> Vec<u8> {
        crate::encode(self)
    }
}

fn member<T>(
    members: &Map<String, Value>,
    member: &'static str,
    rule: &'static str,
    read: impl FnOnce(&str) -> Option<T>,
) -> Result<T, BootstrapError> {
    match members.get(member) {
        None => Err(BootstrapError::Missing { member }),
        Some(Value::Null) => Err(BootstrapError::Null { member }),
        Some(Value::String(text)) => read(text).ok_or(BootstrapError::Invalid { member, rule }),
        Some(_) => Err(BootstrapError::NotAString { member }),
    }
}

fn server_url_holds(text: &str) -> bool {
    if text.ends_with('/')
        || !text.bytes().all(|byte| byte.is_ascii_graphic())
        || text.contains(['?', '#'])
    {
        return false;
    }
    let (https, rest) = if let Some(rest) = text.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = text.strip_prefix("http://") {
        (false, rest)
    } else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.contains('@') {
        return false;
    }
    let (host, port) = if let Some(inside) = authority.strip_prefix('[') {
        let Some(end) = inside.find(']') else {
            return false;
        };
        let (host, after) = authority.split_at(end + 2);
        match after {
            "" => (host, None),
            _ => match after.strip_prefix(':') {
                Some(port) => (host, Some(port)),
                None => return false,
            },
        }
    } else {
        match authority.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        }
    };
    if let Some(port) = port
        && !(port.bytes().all(|byte| byte.is_ascii_digit())
            && port.parse::<u16>().is_ok_and(|port| port > 0))
    {
        return false;
    }
    let ipv6 = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .map(|address| address.parse::<std::net::Ipv6Addr>());
    match ipv6 {
        Some(Ok(address)) => https || address.is_loopback(),
        Some(Err(_)) => false,
        None if https => is_hostname(host),
        None => ipv4_octets(host).is_some_and(|octets| octets[0] == 127),
    }
}

/// The four octets of a dotted-quad IPv4 address with no leading zeros.
fn ipv4_octets(host: &str) -> Option<[u8; 4]> {
    let parts: Vec<&str> = host.split('.').collect();
    let [a, b, c, d] = parts.as_slice() else {
        return None;
    };
    let octet = |part: &str| -> Option<u8> {
        let canonical = !part.is_empty()
            && part.bytes().all(|byte| byte.is_ascii_digit())
            && (part == "0" || !part.starts_with('0'));
        canonical.then(|| part.parse().ok()).flatten()
    };
    Some([octet(a)?, octet(b)?, octet(c)?, octet(d)?])
}

/// The longest host name and the longest label of one, in bytes, as DNS
/// allows them.
const HOST_NAME_MAX_BYTES: usize = 253;
const LABEL_MAX_BYTES: usize = 63;

/// Dot-separated labels of letters, digits and inner hyphens.
fn is_hostname(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= HOST_NAME_MAX_BYTES
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= LABEL_MAX_BYTES
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn valid() -> Value {
        json!({
            "server_url": "https://engine.example.com",
            "instance_id": "inst_1",
            "credential_id": "ccred_1",
            "connection_key": "tetral_ck_secret",
            "expires_at": "2026-12-31T23:59:59Z",
        })
    }

    fn with(member: &str, value: Value) -> Vec<u8> {
        let mut bootstrap = valid();
        bootstrap[member] = value;
        serde_json::to_vec(&bootstrap).unwrap()
    }

    fn without(member: &str) -> Vec<u8> {
        let mut bootstrap = valid();
        bootstrap.as_object_mut().unwrap().remove(member);
        serde_json::to_vec(&bootstrap).unwrap()
    }

    fn padded_to(size: usize) -> Vec<u8> {
        let base = with("padding", json!("")).len();
        let padded = with("padding", json!("x".repeat(size - base)));
        assert_eq!(padded.len(), size);
        padded
    }

    #[test]
    fn each_rule_of_the_bootstrap_holds() {
        let invalid = |member, rule| BootstrapError::Invalid { member, rule };
        let refused: Vec<(&str, Vec<u8>, BootstrapError)> = vec![
            (
                "larger than 65,536 bytes",
                padded_to(65_537),
                BootstrapError::TooLarge { limit: 65_536 },
            ),
            (
                "not JSON",
                b"{\"server_url\": ".to_vec(),
                BootstrapError::Strict(strict::check(b"{\"server_url\": ").unwrap_err()),
            ),
            (
                "a repeated member",
                b"{\"instance_id\": \"a\", \"instance_id\": \"b\"}".to_vec(),
                BootstrapError::Strict(
                    strict::check(b"{\"instance_id\": \"a\", \"instance_id\": \"b\"}").unwrap_err(),
                ),
            ),
            ("an array", b"[]".to_vec(), BootstrapError::NotAnObject),
            (
                "plain http to another host",
                with("server_url", json!("http://10.0.0.1")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "plain http to an IPv6 address that is not loopback",
                with("server_url", json!("http://[2001:db8::1]:8080")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "plain http to a host name",
                with("server_url", json!("http://localhost:8080")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "another scheme",
                with("server_url", json!("ftp://engine.example.com")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "no scheme",
                with("server_url", json!("engine.example.com")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "user information",
                with("server_url", json!("https://user@engine.example.com")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "a query",
                with("server_url", json!("https://engine.example.com/?a=b")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "a fragment",
                with("server_url", json!("https://engine.example.com/#a")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "port 0",
                with("server_url", json!("https://engine.example.com:0")),
                invalid("server_url", ServerUrlRule::NAME),
            ),
            (
                "an empty instance_id",
                with("instance_id", json!("")),
                invalid("instance_id", NulFree::NAME),
            ),
            (
                "a credential_id holding U+0000",
                with("credential_id", json!("c\u{0}")),
                invalid("credential_id", NulFree::NAME),
            ),
            (
                "an empty connection_key",
                with("connection_key", json!("")),
                invalid("connection_key", NonEmpty::NAME),
            ),
            (
                "a day that does not exist",
                with("expires_at", json!("2026-02-30T00:00:00Z")),
                invalid("expires_at", Utc::NAME),
            ),
            (
                "a time with an offset",
                with("expires_at", json!("2026-12-31T23:59:59+01:00")),
                invalid("expires_at", Utc::NAME),
            ),
            (
                "expires_at null",
                with("expires_at", Value::Null),
                BootstrapError::Null {
                    member: "expires_at",
                },
            ),
            (
                "instance_id a number",
                with("instance_id", json!(7)),
                BootstrapError::NotAString {
                    member: "instance_id",
                },
            ),
        ];
        for (case, input, error) in refused {
            assert_eq!(Bootstrap::decode(&input), Err(error), "{case}");
        }
        for member in [
            "server_url",
            "instance_id",
            "credential_id",
            "connection_key",
            "expires_at",
        ] {
            assert_eq!(
                Bootstrap::decode(&without(member)),
                Err(BootstrapError::Missing { member }),
                "{member} missing"
            );
        }

        let accepted = [
            (
                "a trailing / is dropped",
                with("server_url", json!("https://engine.example.com/v1/")),
                "https://engine.example.com/v1",
            ),
            (
                "a port",
                with("server_url", json!("https://engine.example.com:8443")),
                "https://engine.example.com:8443",
            ),
            (
                "plain http to 127.0.0.0/8",
                with("server_url", json!("http://127.1.2.3:8080")),
                "http://127.1.2.3:8080",
            ),
            (
                "plain http to [::1]",
                with("server_url", json!("http://[::1]:8080/")),
                "http://[::1]:8080",
            ),
            (
                "an unknown member",
                padded_to(65_536),
                "https://engine.example.com",
            ),
        ];
        for (case, input, server_url) in accepted {
            let bootstrap =
                Bootstrap::decode(&input).unwrap_or_else(|error| panic!("{case}: {error}"));
            assert_eq!(bootstrap.server_url.as_str(), server_url, "{case}");
        }
    }

    #[test]
    fn the_key_never_reaches_debug_display_or_an_error() {
        let bootstrap = Bootstrap::decode(&serde_json::to_vec(&valid()).unwrap()).unwrap();
        assert_eq!(bootstrap.connection_key.expose(), "tetral_ck_secret");
        assert!(!format!("{bootstrap:?} {}", bootstrap.connection_key).contains("secret"));
        let refused =
            Bootstrap::decode(&with("expires_at", json!("tetral_ck_secret"))).unwrap_err();
        assert!(!refused.to_string().contains("secret"));
        let reread = Bootstrap::decode(&bootstrap.encode()).unwrap();
        assert_eq!(reread, bootstrap);
    }
}
