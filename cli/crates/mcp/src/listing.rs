//! One listing of a server's tools: `tools/list` without a cursor, then with
//! each `nextCursor`, until an answer without one. Each answer is checked as
//! a `2025-06-18` `ListToolsResult` in its raw JSON, and each tool keeps only
//! `name`, `title`, `description`, `inputSchema`, `outputSchema` and
//! `annotations`, as the values received. The listing fails at the first
//! violation of the limits and sends nothing more.

use std::collections::HashSet;
use std::time::Duration;

use protocol::connection::McpTool;
use serde_json::{Map, Value};

use crate::limits::Limits;
use crate::transport::RawAnswer;

/// The members of a tool that a list keeps.
const KEPT: [&str; 6] = [
    "name",
    "title",
    "description",
    "inputSchema",
    "outputSchema",
    "annotations",
];

/// What the listing does after an answer.
#[derive(Debug, PartialEq)]
pub(crate) enum Step {
    /// Requests the next page with this cursor.
    Next(String),
    /// The list is complete.
    Done(Vec<McpTool>),
    /// The listing failed with this message.
    Failed(String),
}

#[derive(Debug)]
pub(crate) struct Listing {
    /// When it fails if it has not ended, on the seam clock.
    pub deadline: Duration,
    /// The tag of the request whose answer it waits for.
    pub awaiting: u64,
    pages: usize,
    cursors: HashSet<String>,
    tools: Vec<Map<String, Value>>,
    names: HashSet<String>,
    /// The compact JSON size of the kept tools as an array.
    bytes: usize,
    limits: Limits,
}

impl Listing {
    /// A listing whose first request, tagged `first`, goes out at `now`.
    pub fn new(now: Duration, first: u64, limits: Limits) -> Listing {
        Listing {
            deadline: now + limits.listing,
            awaiting: first,
            pages: 0,
            cursors: HashSet::new(),
            tools: Vec::new(),
            names: HashSet::new(),
            bytes: "[]".len(),
            limits,
        }
    }

    pub fn timed_out() -> String {
        "tools/list did not finish within 30 seconds".to_owned()
    }

    /// Takes the answer to the awaited request.
    pub fn answer(&mut self, answer: RawAnswer) -> Step {
        match self.page(answer) {
            Ok(Some(cursor)) => Step::Next(cursor),
            Ok(None) => Step::Done(
                std::mem::take(&mut self.tools)
                    .into_iter()
                    .map(|tool| {
                        serde_json::from_value(Value::Object(tool))
                            .expect("a checked tool is a list entry")
                    })
                    .collect(),
            ),
            Err(message) => Step::Failed(message),
        }
    }

    fn page(&mut self, answer: RawAnswer) -> Result<Option<String>, String> {
        let result = match answer {
            RawAnswer::Result(result) => result,
            RawAnswer::Error(error) => {
                return Err(format!("tools/list failed: {}", describe_error(&error)));
            }
        };
        self.pages += 1;
        let (tools, cursor) = valid(result).map_err(|violation| {
            format!("tools/list answer is not a valid ListToolsResult: {violation}")
        })?;
        for tool in tools {
            self.keep(tool)?;
        }
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        if cursor.len() > self.limits.cursor_bytes {
            return Err(format!(
                "nextCursor over {} bytes",
                self.limits.cursor_bytes
            ));
        }
        if !self.cursors.insert(cursor.clone()) {
            return Err("nextCursor repeated".to_owned());
        }
        if self.pages >= self.limits.pages {
            return Err(format!(
                "tools/list needs more than {} pages",
                self.limits.pages
            ));
        }
        Ok(Some(cursor))
    }

    fn keep(&mut self, mut tool: Map<String, Value>) -> Result<(), String> {
        if self.tools.len() == self.limits.tools {
            return Err(format!("more than {} tools", self.limits.tools));
        }
        tool.retain(|member, _| KEPT.contains(&member.as_str()));
        let name = tool["name"]
            .as_str()
            .expect("a checked tool has a name")
            .to_owned();
        if !self.names.insert(name.clone()) {
            return Err(format!("two tools named {name}"));
        }
        let size = serde_json::to_vec(&tool)
            .expect("a JSON object encodes")
            .len();
        self.bytes += size + usize::from(!self.tools.is_empty());
        if self.bytes > self.limits.report_bytes {
            return Err(format!(
                "tool list report over {} bytes",
                self.limits.report_bytes
            ));
        }
        self.tools.push(tool);
        Ok(())
    }
}

/// `code message` of a JSON-RPC error object, as far as it has them.
fn describe_error(error: &Value) -> String {
    let code = error.get("code").map_or_else(String::new, Value::to_string);
    let message = error.get("message").and_then(Value::as_str).unwrap_or("");
    format!("{code} {message}")
}

/// One page's tools, each a JSON object, and its `nextCursor`.
type Page = (Vec<Map<String, Value>>, Option<String>);

/// The page of a valid `ListToolsResult`, or its first violation.
fn valid(result: Value) -> Result<Page, String> {
    let Value::Object(mut result) = result else {
        return Err("the result is not an object".to_owned());
    };
    let Some(Value::Array(tools)) = result.remove("tools") else {
        return Err("tools is missing or not an array".to_owned());
    };
    let mut checked = Vec::with_capacity(tools.len());
    for (index, tool) in tools.into_iter().enumerate() {
        let Value::Object(tool) = tool else {
            return Err(format!("tools[{index}] is not an object"));
        };
        if !tool
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| !name.is_empty())
        {
            return Err(format!("tools[{index}] has no nonempty string name"));
        }
        if !tool.get("inputSchema").is_some_and(Value::is_object) {
            return Err(format!("tools[{index}] has no object inputSchema"));
        }
        for member in ["title", "description"] {
            if tool.get(member).is_some_and(|value| !value.is_string()) {
                return Err(format!("tools[{index}].{member} is not a string"));
            }
        }
        for member in ["outputSchema", "annotations"] {
            if tool.get(member).is_some_and(|value| !value.is_object()) {
                return Err(format!("tools[{index}].{member} is not an object"));
            }
        }
        checked.push(tool);
    }
    let cursor = match result.remove("nextCursor") {
        None => None,
        Some(Value::String(cursor)) => Some(cursor),
        Some(_) => return Err("nextCursor is not a string".to_owned()),
    };
    Ok((checked, cursor))
}
