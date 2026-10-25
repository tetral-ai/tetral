//! The policy table: each policy with its exact rejection code.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use execution::policy::validate;
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
