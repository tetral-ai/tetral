//! Content fingerprints: what makes two JSON messages the same content.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// The lowercase hexadecimal SHA-256 of `value` re-encoded with object
/// members sorted by name and no whitespace, so that member order and
/// whitespace are no change while an omitted member or another value is.
/// The sorting is done here, not left to the JSON library, whose maps keep
/// insertion order when a dependency turns that on.
pub fn fingerprint(value: &Value) -> String {
    let mut encoded = Vec::new();
    canonical(value, &mut encoded);
    Sha256::digest(&encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(members) => {
            let mut names: Vec<&String> = members.keys().collect();
            names.sort();
            out.push(b'{');
            for (index, name) in names.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, name).expect("a string encodes");
                out.push(b':');
                canonical(&members[name], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                canonical(item, out);
            }
            out.push(b']');
        }
        scalar => serde_json::to_writer(&mut *out, scalar).expect("a scalar encodes"),
    }
}
