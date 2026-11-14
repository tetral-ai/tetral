//! The behaviour cases of `protocol/behaviour-v1.cases.json`
//! (`protocol/README.md`, "Behaviour cases"): the file's cases of one
//! direction, and the matching of a message that the reference control
//! server sent or accepted against what a case writes.
//!
//! [`Bound`] implements what the file's CLI-direction cases use: references
//! inside strings, `$capture`, `$value` and `$absent`. Any other member that
//! starts with `$` fails the match, so that a case with a matcher this
//! harness lacks fails its run instead of passing unchecked. A reference is
//! bound by a state or by a message the server wrote, never by one the CLI
//! sent: there an unbound reference fails.

use std::collections::BTreeMap;

use serde_json::Value;

/// The cases of `direction` (`engine` or `cli`), in file order.
pub fn of_direction(direction: &str) -> Vec<Value> {
    let path = crate::repository_root().join("protocol/behaviour-v1.cases.json");
    let file: Value = serde_json::from_slice(
        &std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display())),
    )
    .expect("the case file is JSON");
    assert_eq!(file["version"], 1, "the case file's version");
    file["cases"]
        .as_array()
        .expect("the case file's cases")
        .iter()
        .filter(|case| case["direction"] == direction)
        .cloned()
        .collect()
}

/// What one run of a case has bound: each symbol's identity, each captured
/// value and the connection of the latest `connected`.
#[derive(Default)]
pub struct Bound {
    pub connection: Option<String>,
    symbols: BTreeMap<String, String>,
    values: BTreeMap<String, Value>,
}

/// The kinds of symbol a case names.
const KINDS: [&str; 5] = ["instance", "credential", "session", "binding", "drive"];

enum Lookup {
    Unbound(String),
    Failed(String),
}

impl Bound {
    /// Gives the symbol of `kind` its identity, as the state that creates
    /// the thing does.
    pub fn bind(&mut self, kind: &str, symbol: &str, identity: &str) {
        assert!(KINDS.contains(&kind), "no kind of symbol {kind}");
        self.symbols
            .insert(format!("{kind}:{symbol}"), identity.to_owned());
    }

    /// Holds `got`, a message the server wrote, to `want` as the case file
    /// writes it, and binds what `want` names: a `$capture`, and a string
    /// that is one reference not bound before. The error names the place of
    /// the first difference.
    pub fn matches(&mut self, at: &str, want: &Value, got: &Value) -> Result<(), String> {
        self.check(at, want, got, true)
    }

    /// Holds `got`, a message the CLI sent, to `want`: as [`Bound::matches`],
    /// but a reference not bound before fails.
    pub fn holds(&mut self, at: &str, want: &Value, got: &Value) -> Result<(), String> {
        self.check(at, want, got, false)
    }

    fn check(&mut self, at: &str, want: &Value, got: &Value, binds: bool) -> Result<(), String> {
        match want {
            Value::String(text) => self.check_text(at, text, got, binds),
            Value::Array(items) => {
                let received = got
                    .as_array()
                    .filter(|received| received.len() == items.len())
                    .ok_or_else(|| format!("{at}: got {got}, want an array of {}", items.len()))?;
                for (index, (item, one)) in items.iter().zip(received).enumerate() {
                    self.check(&format!("{at}/{index}"), item, one, binds)?;
                }
                Ok(())
            }
            Value::Object(members) => {
                if let Some(operator) = members.keys().find(|name| name.starts_with('$')) {
                    let operand = &members[operator];
                    return match (operator.as_str(), operand.as_str(), members.len()) {
                        ("$capture", Some(name), 1) => {
                            self.values.insert(name.to_owned(), got.clone());
                            Ok(())
                        }
                        ("$value", Some(name), 1) => match self.values.get(name) {
                            Some(captured) if captured == got => Ok(()),
                            Some(captured) => Err(format!("{at}: got {got}, want {captured}")),
                            None => Err(format!("{at}: $value {name} is not bound")),
                        },
                        _ => Err(format!("{at}: the matcher {operator} is not implemented")),
                    };
                }
                let received = got
                    .as_object()
                    .ok_or_else(|| format!("{at}: got {got}, want an object"))?;
                for (name, member) in members {
                    let place = format!("{at}/{name}");
                    if member.as_object().is_some_and(|absent| {
                        absent.len() == 1 && absent.get("$absent") == Some(&Value::Bool(true))
                    }) {
                        if let Some(present) = received.get(name) {
                            return Err(format!("{place}: got {present}, want no such member"));
                        }
                        continue;
                    }
                    let one = received
                        .get(name)
                        .ok_or_else(|| format!("{place}: missing in {got}"))?;
                    self.check(&place, member, one, binds)?;
                }
                Ok(())
            }
            scalar if scalar == got => Ok(()),
            scalar => Err(format!("{at}: got {got}, want {scalar}")),
        }
    }

    fn check_text(&mut self, at: &str, text: &str, got: &Value, binds: bool) -> Result<(), String> {
        let expanded = match (self.expand(text), got.as_str()) {
            (Err(Lookup::Unbound(_)), Some(received)) if binds && whole_reference(text) => {
                self.capture(&text[2..text.len() - 1], received);
                return Ok(());
            }
            (Err(Lookup::Unbound(reference)), _) => {
                return Err(format!("{at}: {reference} is not bound"));
            }
            (Err(Lookup::Failed(reason)), _) => return Err(format!("{at}: {reason}")),
            (Ok(expanded), _) => expanded,
        };
        if got.as_str() == Some(expanded.as_str()) {
            Ok(())
        } else {
            Err(format!("{at}: got {got}, want {expanded:?}"))
        }
    }

    fn capture(&mut self, reference: &str, received: &str) {
        match reference.split_once(':') {
            Some((kind, symbol)) => self.bind(kind, symbol, received),
            None if reference == "connection" => self.connection = Some(received.to_owned()),
            None => {
                self.values
                    .insert(reference.to_owned(), Value::String(received.to_owned()));
            }
        }
    }

    /// `text` with each `${...}` replaced.
    fn expand(&self, text: &str) -> Result<String, Lookup> {
        let mut expanded = String::new();
        let mut rest = text;
        while let Some(start) = rest.find("${") {
            let end = rest[start..]
                .find('}')
                .ok_or_else(|| Lookup::Failed(format!("{text:?} has an open reference")))?
                + start;
            expanded.push_str(&rest[..start]);
            expanded.push_str(&self.lookup(&rest[start + 2..end])?);
            rest = &rest[end + 1..];
        }
        expanded.push_str(rest);
        Ok(expanded)
    }

    fn lookup(&self, reference: &str) -> Result<String, Lookup> {
        let unbound = || Lookup::Unbound(format!("${{{reference}}}"));
        match reference.split_once(':') {
            Some((kind, _)) if KINDS.contains(&kind) => {
                self.symbols.get(reference).cloned().ok_or_else(unbound)
            }
            Some(_) => Err(Lookup::Failed(format!(
                "the reference ${{{reference}}} is not implemented"
            ))),
            None if reference == "connection" => self.connection.clone().ok_or_else(unbound),
            None if reference == "index" => Err(Lookup::Failed(
                "the reference ${index} is not implemented".to_owned(),
            )),
            None => match self.values.get(reference) {
                Some(Value::String(text)) => Ok(text.clone()),
                Some(Value::Number(number)) => Ok(number.to_string()),
                Some(other) => Err(Lookup::Failed(format!(
                    "${{{reference}}} is {other}, neither a string nor a number"
                ))),
                None => Err(unbound()),
            },
        }
    }
}

/// Whether `text` is one reference and nothing else.
fn whole_reference(text: &str) -> bool {
    text.starts_with("${") && text.find('}') == Some(text.len() - 1)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::Bound;

    #[test]
    fn a_match_binds_captures_and_holds_later_values_to_them() {
        let mut bound = Bound::default();
        bound.bind("session", "S", "sess_1");
        let event = json!({
            "type": "session.close", "message_id": "msg_7", "connection_id": "conn_1",
            "payload": {"session_id": "sess_1", "binding_id": "bind_1", "count": 3},
        });
        let want = json!({
            "type": "session.close", "message_id": {"$capture": "instruction"},
            "connection_id": "${connection}",
            "payload": {
                "session_id": "${session:S}", "binding_id": "${binding:B}",
                "count": {"$capture": "count"}, "ticket": {"$absent": true},
            },
        });
        bound.matches("event", &want, &event).unwrap();
        assert_eq!(bound.connection.as_deref(), Some("conn_1"));

        let report = json!({
            "request_message_id": "msg_7", "binding_id": "bind_1", "count": 3,
            "note": "for msg_7 of sess_1",
        });
        let want = json!({
            "request_message_id": "${instruction}", "binding_id": "${binding:B}",
            "count": {"$value": "count"}, "note": "for ${instruction} of ${session:S}",
        });
        bound.holds("body", &want, &report).unwrap();
    }

    #[test]
    fn a_match_names_the_first_difference_and_fails_on_what_it_lacks() {
        let mut bound = Bound::default();
        bound.bind("session", "S", "sess_1");
        bound
            .matches(
                "event",
                &json!({"count": {"$capture": "count"}}),
                &json!({"count": 3}),
            )
            .unwrap();
        // Whether the CLI sent the message: there no reference is bound.
        let (written, sent) = (false, true);
        let rows = [
            (
                written,
                json!({"a": {"b": 1}}),
                json!({"a": {"b": 2}}),
                "body/a/b: got 2, want 1",
            ),
            (
                written,
                json!({"a": [1]}),
                json!({"a": [1, 2]}),
                "body/a: got [1,2], want an array of 1",
            ),
            (
                written,
                json!({"a": [1, 2]}),
                json!({"a": [1, 3]}),
                "body/a/1: got 3, want 2",
            ),
            (
                written,
                json!({"a": 1}),
                json!({"b": 1}),
                "body/a: missing in {\"b\":1}",
            ),
            (
                written,
                json!({"a": {"$absent": true}}),
                json!({"a": null}),
                "body/a: got null, want no such member",
            ),
            (
                written,
                json!({"a": "${session:S}"}),
                json!({"a": "sess_2"}),
                "body/a: got \"sess_2\", want \"sess_1\"",
            ),
            (
                written,
                json!({"a": "${count}"}),
                json!({"a": "4"}),
                "body/a: got \"4\", want \"3\"",
            ),
            (
                written,
                json!({"a": {"$value": "count"}}),
                json!({"a": 4}),
                "body/a: got 4, want 3",
            ),
            (
                written,
                json!({"a": "${name}"}),
                json!({"a": 1}),
                "body/a: ${name} is not bound",
            ),
            (
                written,
                json!({"a": "x ${name}"}),
                json!({"a": "x y"}),
                "body/a: ${name} is not bound",
            ),
            (
                written,
                json!({"a": "${name} x"}),
                json!({"a": "y x"}),
                "body/a: ${name} is not bound",
            ),
            (
                sent,
                json!({"a": "${name}"}),
                json!({"a": "y"}),
                "body/a: ${name} is not bound",
            ),
            (
                sent,
                json!({"a": "${binding:B}"}),
                json!({"a": "bind_1"}),
                "body/a: ${binding:B} is not bound",
            ),
            (
                written,
                json!({"a": {"$value": "name"}}),
                json!({"a": 1}),
                "body/a: $value name is not bound",
            ),
            (
                written,
                json!({"a": {"$capture": "name", "b": 1}}),
                json!({"a": {"b": 1}}),
                "body/a: the matcher $capture is not implemented",
            ),
            (
                written,
                json!({"a": {"$between": [1, 2]}}),
                json!({"a": 1}),
                "body/a: the matcher $between is not implemented",
            ),
            (
                written,
                json!({"a": "${sha256:hello}"}),
                json!({"a": "x"}),
                "body/a: the reference ${sha256:hello} is not implemented",
            ),
        ];
        for (by_the_cli, want, got, reason) in rows {
            let failed = if by_the_cli {
                bound.holds("body", &want, &got)
            } else {
                bound.matches("body", &want, &got)
            };
            assert_eq!(failed, Err(reason.to_owned()), "{want} against {got}");
        }
    }
}
