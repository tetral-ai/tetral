//! The validation of a call's `sandbox_policy`, read from its JSON value so
//! that a later `schema_version` is never judged by version 1's members. The
//! first failing check decides the rejection.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};

use idna::uts46::{AsciiDenyList, DnsLength, Hyphens, Uts46};
use protocol::tool::{FilesystemAccess, PORT_MAX, RejectionCode};
use serde_json::{Map, Value};

use super::PolicyRejection;

/// A policy that the CLI can enforce: each rule at its normalized path, every
/// path once, and a network mode other than `managed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidPolicy {
    pub rules: BTreeMap<String, FilesystemAccess>,
    pub network: Network,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Disabled,
    Direct,
}

const DNS_NAME_MAX: usize = 253;
const DNS_LABEL_MAX: usize = 63;
const IPV4_PREFIX_MAX: u32 = 32;
const IPV6_PREFIX_MAX: u32 = 128;

/// Validates `sandbox_policy`, given as the payload's member, absent or not.
pub fn validate(policy: Option<&Value>) -> Result<ValidPolicy, PolicyRejection> {
    let Some(Value::Object(policy)) = policy else {
        return Err(invalid("sandbox_policy is missing or not an object"));
    };
    match policy.get("schema_version") {
        Some(Value::Number(version)) if version.as_i64() == Some(1) => {}
        Some(Value::Number(version)) if version.is_i64() || version.is_u64() => {
            return Err(PolicyRejection {
                code: RejectionCode::UnsupportedPolicy,
                message: "sandbox_policy.schema_version is not supported".to_owned(),
            });
        }
        _ => {
            return Err(invalid(
                "sandbox_policy.schema_version is missing or not an integer",
            ));
        }
    }
    closed(
        policy,
        &["schema_version", "filesystem", "network"],
        "sandbox_policy has a member it does not define",
    )?;
    let filesystem = object(
        required(policy, "filesystem", "sandbox_policy.filesystem is missing")?,
        "sandbox_policy.filesystem is not an object",
    )?;
    closed(
        filesystem,
        &["rules"],
        "sandbox_policy.filesystem has a member it does not define",
    )?;
    let Value::Array(rules) = required(
        filesystem,
        "rules",
        "sandbox_policy.filesystem.rules is missing",
    )?
    else {
        return Err(invalid("sandbox_policy.filesystem.rules is not an array"));
    };
    for rule in rules {
        complete(
            rule,
            &["path", "access"],
            "sandbox_policy.filesystem.rules holds a rule that is not an object of path and access",
        )?;
    }
    let network = object(
        required(policy, "network", "sandbox_policy.network is missing")?,
        "sandbox_policy.network is not an object",
    )?;
    closed(
        network,
        &["mode", "rules"],
        "sandbox_policy.network has a member it does not define",
    )?;
    required(network, "mode", "sandbox_policy.network.mode is missing")?;

    let rules = filesystem_rules(rules)?;
    let network = network_mode(network)?;
    Ok(ValidPolicy { rules, network })
}

fn filesystem_rules(
    rules: &[Value],
) -> Result<BTreeMap<String, FilesystemAccess>, PolicyRejection> {
    let mut normalized = BTreeMap::new();
    for rule in rules {
        let path = match &rule["path"] {
            Value::String(path) if path.starts_with('/') && !path.contains('\0') => path,
            _ => {
                return Err(invalid(
                    "sandbox_policy.filesystem.rules holds a path that is not absolute or holds U+0000",
                ));
            }
        };
        let access = match rule["access"].as_str() {
            Some("deny") => FilesystemAccess::Deny,
            Some("read") => FilesystemAccess::Read,
            Some("read_write") => FilesystemAccess::ReadWrite,
            _ => {
                return Err(invalid(
                    "sandbox_policy.filesystem.rules holds an access other than deny, read or read_write",
                ));
            }
        };
        if *normalized.entry(normalize(path)).or_insert(access) != access {
            return Err(invalid(
                "sandbox_policy.filesystem.rules gives one path two different accesses",
            ));
        }
    }
    Ok(normalized)
}

/// Normalizes an absolute path lexically: repeated `/` collapse, `.`
/// segments and a trailing `/` go, and each `..` removes the segment before
/// it. No symbolic link is resolved, and `*`, `?` and `[` are ordinary
/// characters.
fn normalize(path: &str) -> String {
    let mut segments = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            segment => segments.push(segment),
        }
    }
    format!("/{}", segments.join("/"))
}

fn network_mode(network: &Map<String, Value>) -> Result<Network, PolicyRejection> {
    let mode = match network["mode"].as_str() {
        Some("disabled") => Network::Disabled,
        Some("direct") => Network::Direct,
        Some("managed") => {
            let rules = object(
                required(
                    network,
                    "rules",
                    "sandbox_policy.network.rules is missing for managed",
                )?,
                "sandbox_policy.network.rules is not an object",
            )?;
            managed_rules(rules)?;
            // The CLI never advertises `managed`, so a managed policy never
            // runs, not even as `direct`.
            return Err(PolicyRejection {
                code: RejectionCode::UnsupportedPolicy,
                message: "sandbox_policy.network.mode managed is not supported".to_owned(),
            });
        }
        _ => {
            return Err(invalid(
                "sandbox_policy.network.mode is not disabled, direct or managed",
            ));
        }
    };
    if network.contains_key("rules") {
        return Err(invalid(
            "sandbox_policy.network.rules is only allowed for managed",
        ));
    }
    Ok(mode)
}

fn managed_rules(rules: &Map<String, Value>) -> Result<(), PolicyRejection> {
    complete_object(
        rules,
        &["allow", "deny", "listen"],
        "sandbox_policy.network.rules is not exactly allow, deny and listen",
    )?;
    for list in ["allow", "deny"] {
        for entry in array(&rules[list])? {
            let entry = complete(
                entry,
                &["destination", "transport", "ports"],
                "sandbox_policy.network.rules holds an entry that is not an object of destination, transport and ports",
            )?;
            let destination = complete(
                &entry["destination"],
                &["kind", "value"],
                "sandbox_policy.network.rules holds a destination that is not an object of kind and value",
            )?;
            let valid = match (destination["kind"].as_str(), destination["value"].as_str()) {
                (Some("host"), Some(value)) => is_host(value),
                (Some("cidr"), Some(value)) => is_cidr(value),
                _ => false,
            };
            if !valid {
                return Err(invalid(
                    "sandbox_policy.network.rules holds a destination that is not a host or a CIDR",
                ));
            }
            transport_and_ports(entry)?;
        }
    }
    for entry in array(&rules["listen"])? {
        let entry = complete(
            entry,
            &["address", "transport", "ports"],
            "sandbox_policy.network.rules.listen holds an entry that is not an object of address, transport and ports",
        )?;
        if !entry["address"]
            .as_str()
            .is_some_and(|address| address.parse::<IpAddr>().is_ok())
        {
            return Err(invalid(
                "sandbox_policy.network.rules.listen holds an address that is not an IP address",
            ));
        }
        transport_and_ports(entry)?;
    }
    Ok(())
}

fn transport_and_ports(entry: &Map<String, Value>) -> Result<(), PolicyRejection> {
    if !matches!(entry["transport"].as_str(), Some("tcp" | "udp")) {
        return Err(invalid(
            "sandbox_policy.network.rules holds a transport other than tcp or udp",
        ));
    }
    let ports = match &entry["ports"] {
        Value::Array(ports) if !ports.is_empty() => ports,
        _ => {
            return Err(invalid(
                "sandbox_policy.network.rules holds ports that are not a nonempty array",
            ));
        }
    };
    for range in ports {
        let range = complete(
            range,
            &["from", "to"],
            "sandbox_policy.network.rules holds a port range that is not an object of from and to",
        )?;
        let valid = match (range["from"].as_i64(), range["to"].as_i64()) {
            (Some(from), Some(to)) => 1 <= from && from <= to && to <= PORT_MAX,
            _ => false,
        };
        if !valid {
            return Err(invalid(
                "sandbox_policy.network.rules holds a port range outside 1 to 65535 or with from above to",
            ));
        }
    }
    Ok(())
}

/// A host of the network rules: `*`, a name, or `*.` and a name, the name
/// converted as a whole with UTS #46 ToASCII (the `idna` crate's processing is
/// nontransitional and always checks bidi and joiners) and one trailing dot
/// removed.
fn is_host(value: &str) -> bool {
    if value == "*" {
        return true;
    }
    if value.contains(['/', ':', '@', '?', '#']) {
        return false;
    }
    let name = value.strip_prefix("*.").unwrap_or(value);
    let Ok(ascii) = Uts46::new().to_ascii(
        name.as_bytes(),
        AsciiDenyList::STD3,
        Hyphens::Check,
        DnsLength::Ignore,
    ) else {
        return false;
    };
    let ascii = ascii.strip_suffix('.').unwrap_or(&ascii);
    !ascii.is_empty()
        && ascii.len() <= DNS_NAME_MAX
        && ascii.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= DNS_LABEL_MAX
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        && ascii.parse::<Ipv4Addr>().is_err()
}

/// A CIDR of the network rules: an address, `/` and a decimal prefix length
/// without leading zeros, with no host bits set. The standard library's
/// address parsers refuse leading zeros in IPv4 and zones in IPv6.
fn is_cidr(value: &str) -> bool {
    let Some((address, prefix)) = value.split_once('/') else {
        return false;
    };
    if prefix.is_empty()
        || !prefix.bytes().all(|byte| byte.is_ascii_digit())
        || (prefix.len() > 1 && prefix.starts_with('0'))
    {
        return false;
    }
    let Ok(prefix) = prefix.parse::<u32>() else {
        return false;
    };
    match address.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => {
            prefix <= IPV4_PREFIX_MAX && u32::from(address).checked_shl(prefix).unwrap_or(0) == 0
        }
        Ok(IpAddr::V6(address)) => {
            prefix <= IPV6_PREFIX_MAX && u128::from(address).checked_shl(prefix).unwrap_or(0) == 0
        }
        Err(_) => false,
    }
}

fn object<'a>(
    value: &'a Value,
    message: &'static str,
) -> Result<&'a Map<String, Value>, PolicyRejection> {
    value.as_object().ok_or_else(|| invalid(message))
}

fn required<'a>(
    object: &'a Map<String, Value>,
    member: &str,
    message: &'static str,
) -> Result<&'a Value, PolicyRejection> {
    object.get(member).ok_or_else(|| invalid(message))
}

fn array(value: &Value) -> Result<&Vec<Value>, PolicyRejection> {
    value.as_array().ok_or_else(|| {
        invalid("sandbox_policy.network.rules holds an allow, deny or listen that is not an array")
    })
}

/// An object with exactly the given members.
fn complete<'a>(
    value: &'a Value,
    members: &[&str],
    message: &'static str,
) -> Result<&'a Map<String, Value>, PolicyRejection> {
    let object = object(value, message)?;
    complete_object(object, members, message)?;
    Ok(object)
}

fn complete_object(
    object: &Map<String, Value>,
    members: &[&str],
    message: &'static str,
) -> Result<(), PolicyRejection> {
    closed(object, members, message)?;
    if members.iter().any(|member| !object.contains_key(*member)) {
        return Err(invalid(message));
    }
    Ok(())
}

/// Every object of a policy is closed, so a misspelled member is refused, not
/// read as a grant.
fn closed(
    object: &Map<String, Value>,
    members: &[&str],
    message: &'static str,
) -> Result<(), PolicyRejection> {
    if object.keys().any(|key| !members.contains(&key.as_str())) {
        return Err(invalid(message));
    }
    Ok(())
}

fn invalid(message: &'static str) -> PolicyRejection {
    PolicyRejection {
        code: RejectionCode::InvalidInput,
        message: message.to_owned(),
    }
}
