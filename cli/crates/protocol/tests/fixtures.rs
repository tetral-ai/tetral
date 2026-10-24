//! Every fixture of the protocol's three fixture files against the protocol
//! types: a valid fixture decodes, re-encodes to its own value and still
//! passes its schema; an invalid one is refused.

mod common;

use std::collections::HashMap;

use common::{FILES, Fixture, by_name, decode_fixture, fixtures, read_json, validator};
use jsonschema::Validator;
use protocol::errors::DriveTransferEndpoint;
use serde_json::{Map, Value};

/// Members of valid fixtures that no type knows, which decoding drops.
const UNKNOWN_MEMBERS: [(&str, &[&str]); 2] = [
    (
        "connected_with_unknown_optional_field",
        &["/payload/hub", "/trace"],
    ),
    (
        "ready_with_unknown_optional_field",
        &["/hostname", "/capabilities/gpu"],
    ),
];

/// The fixture's value without the members that decoding drops.
fn expected_encoding(fixture: &Fixture<'_>) -> Value {
    let mut expected = fixture.value.clone();
    let dropped = UNKNOWN_MEMBERS
        .iter()
        .find(|(name, _)| *name == fixture.name)
        .map_or(&[][..], |(_, pointers)| *pointers);
    for pointer in dropped {
        let (parent, member) = pointer.rsplit_once('/').expect("a member pointer");
        let object: &mut Map<String, Value> = expected
            .pointer_mut(parent)
            .and_then(Value::as_object_mut)
            .unwrap_or_else(|| panic!("{}: no object at {parent:?}", fixture.name));
        assert!(
            object.remove(member).is_some(),
            "{}: no member {pointer}",
            fixture.name
        );
    }
    expected
}

#[test]
fn every_protocol_fixture_round_trips_or_is_rejected() {
    let (mut total, mut valid, mut invalid) = (0, 0, 0);
    for (file, file_total, file_valid, file_invalid) in FILES {
        let document = read_json(&format!("{file}.fixtures.json"));
        let fixtures = fixtures(&document);
        let by_name = by_name(&fixtures);
        let mut validators: HashMap<&str, Validator> = HashMap::new();
        let (mut seen_valid, mut seen_invalid) = (0, 0);
        for fixture in &fixtures {
            let decoded = decode_fixture(file, fixture, &by_name);
            if fixture.valid {
                seen_valid += 1;
                let encoded = decoded.unwrap_or_else(|error| panic!("{}: {error}", fixture.name));
                assert_eq!(encoded, expected_encoding(fixture), "{}", fixture.name);
                let validator = validators
                    .entry(fixture.schema)
                    .or_insert_with(|| validator(file, &format!("/$defs/{}", fixture.schema)));
                let errors: Vec<String> = validator
                    .iter_errors(&encoded)
                    .map(|error| error.to_string())
                    .collect();
                assert!(
                    errors.is_empty(),
                    "{}: re-encoding fails its schema: {errors:?}",
                    fixture.name
                );
            } else {
                seen_invalid += 1;
                assert!(
                    decoded.is_err(),
                    "{}: invalid fixture accepted as {:?}",
                    fixture.name,
                    decoded
                );
            }
        }
        assert_eq!(
            (fixtures.len(), seen_valid, seen_invalid),
            (file_total, file_valid, file_invalid),
            "{file}"
        );
        total += fixtures.len();
        valid += seen_valid;
        invalid += seen_invalid;
    }
    assert_eq!((total, valid, invalid), (426, 139, 287));
}

#[test]
fn drive_transfer_endpoint_table_matches_the_schema() {
    let schema = read_json("drive-transfer-v1.schema.json");
    let endpoints = &schema["$defs"]["DriveTransferEndpoints"]["properties"];
    for endpoint in DriveTransferEndpoint::ALL {
        let name = serde_json::to_value(endpoint).expect("an endpoint name encodes");
        let row = &endpoints[name.as_str().expect("an endpoint name is a string")]["const"];
        let spec = endpoint.spec();
        assert_eq!(row["method"], spec.method, "{name}");
        assert_eq!(row["path"], spec.path, "{name}");
        let mut errors: Vec<String> = spec
            .errors
            .iter()
            .map(|code| {
                let name = serde_json::to_value(code).expect("a code encodes");
                format!("{} {}", code.status(), name.as_str().unwrap_or_default())
            })
            .collect();
        errors.sort();
        let mut listed: Vec<String> = row["errors"]
            .as_array()
            .expect("an endpoint lists its errors")
            .iter()
            .map(|pair| pair.as_str().unwrap_or_default().to_owned())
            .collect();
        listed.sort();
        assert_eq!(errors, listed, "{name}");
    }
    assert_eq!(
        endpoints.as_object().map(Map::len),
        Some(DriveTransferEndpoint::ALL.len())
    );
}
