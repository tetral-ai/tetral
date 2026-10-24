//! The crate-graph rules of `cli/README.md`, checked over `cargo metadata`.
//! [`Violation::rule`] numbers them in the order that file states them:
//! layering, paths outside the workspace, AGPL licences, test code in the
//! release graph, and test seams.
//!
//! Beside the graph, no library crate prints: only the `tetral` binary may
//! write to stdout, and every record goes through `observe`. Library crates
//! also deny the print lints, but no build runs clippy, so this test reads
//! their sources.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};
use tetral_tests::record::Case;

const TEST_PACKAGE: &str = "tetral-tests";

/// The layer table of `cli/README.md`: each workspace crate with the workspace
/// crates it may depend on.
const ALLOWED_DEPENDENCIES: &[(&str, &[&str])] = &[
    (
        "tetral",
        &[
            "appserver",
            "sdk",
            "connection",
            "session",
            "execution",
            "drive",
            "mcp",
            "protocol",
            "state",
            "observe",
        ],
    ),
    (
        "appserver",
        &[
            "connection",
            "session",
            "execution",
            "drive",
            "mcp",
            "protocol",
            "state",
            "observe",
        ],
    ),
    ("sdk", &["protocol"]),
    (
        "connection",
        &["execution", "drive", "mcp", "protocol", "state", "observe"],
    ),
    (
        "session",
        &["execution", "drive", "mcp", "protocol", "state", "observe"],
    ),
    ("execution", &["protocol", "state", "observe"]),
    ("drive", &["protocol", "state", "observe"]),
    ("mcp", &["protocol", "state", "observe"]),
    ("protocol", &[]),
    ("state", &[]),
    ("observe", &[]),
];

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Violation {
    LayerEdge { from: String, to: String },
    PathOutsideWorkspace { package: String, path: PathBuf },
    AgplLicence { package: String },
    TestCodeInRelease { package: String },
    TestSeamsByDefault { package: String },
    TestSeamsEnabled { package: String, dependency: String },
}

impl Violation {
    fn rule(&self) -> u8 {
        match self {
            Violation::LayerEdge { .. } => 1,
            Violation::PathOutsideWorkspace { .. } => 2,
            Violation::AgplLicence { .. } => 3,
            Violation::TestCodeInRelease { .. } => 4,
            Violation::TestSeamsByDefault { .. } | Violation::TestSeamsEnabled { .. } => 5,
        }
    }
}

fn workspace_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the test package lies inside the workspace")
        .to_path_buf()
}

/// The workspace's `cargo metadata`, noted in the case's record.
fn metadata(case: &mut Case) -> Value {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let output = Command::new(cargo)
        .args(["metadata", "--format-version", "1"])
        .current_dir(workspace_dir())
        .output()
        .expect("cargo metadata runs");
    case.command(
        "cargo metadata --format-version 1",
        &output.status.to_string(),
    );
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("cargo metadata prints JSON")
}

fn str_value(value: &Value) -> &str {
    value.as_str().unwrap_or_default()
}

fn packages(metadata: &Value) -> &Vec<Value> {
    metadata["packages"]
        .as_array()
        .expect("metadata lists packages")
}

fn workspace_names(metadata: &Value) -> BTreeSet<String> {
    let members: BTreeSet<&str> = metadata["workspace_members"]
        .as_array()
        .expect("metadata lists workspace members")
        .iter()
        .map(str_value)
        .collect();
    packages(metadata)
        .iter()
        .filter(|package| members.contains(str_value(&package["id"])))
        .map(|package| str_value(&package["name"]).to_owned())
        .collect()
}

/// Removes `.` and `..` without touching the filesystem.
fn normalize_lexically(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            other => result.push(other),
        }
    }
    result
}

const TEST_SEAMS: &str = "test-seams";

/// The features of workspace crates that turn on together, followed through
/// each crate's feature table and the dependencies its features enable, in
/// every form a table entry takes: `name`, `dep:name`, `name/feature` and
/// `name?/feature`. A weak `name?/feature` counts as enabled, so the check
/// errs on the strict side.
struct FeatureGraph<'a> {
    members: HashMap<&'a str, &'a Value>,
}

impl<'a> FeatureGraph<'a> {
    fn new(metadata: &'a Value) -> Self {
        let ids: BTreeSet<&str> = metadata["workspace_members"]
            .as_array()
            .expect("metadata lists workspace members")
            .iter()
            .map(str_value)
            .collect();
        let members = packages(metadata)
            .iter()
            .filter(|package| ids.contains(str_value(&package["id"])))
            .map(|package| (str_value(&package["name"]), package))
            .collect();
        FeatureGraph { members }
    }

    /// The workspace crate a dependency names: a path dependency on a member,
    /// never a registry package that shares a member's name.
    fn member(&self, dependency: &'a Value) -> Option<&'a str> {
        let name = str_value(&dependency["name"]);
        (dependency["path"].is_string() && self.members.contains_key(name)).then_some(name)
    }

    /// The workspace crate that `package` names `key` in its manifest.
    fn target(&self, package: &'a Value, key: &str) -> Option<&'a str> {
        package["dependencies"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|dependency| manifest_key(dependency) == key)
            .and_then(|dependency| self.member(dependency))
    }

    fn enable_dependency(
        &self,
        package: &'a Value,
        key: &str,
        pending: &mut Vec<(&'a str, &'a str)>,
    ) {
        for dependency in package["dependencies"].as_array().into_iter().flatten() {
            let Some(name) = self.member(dependency) else {
                continue;
            };
            if manifest_key(dependency) != key {
                continue;
            }
            pending.extend(dependency_features(dependency).map(|feature| (name, feature)));
            if dependency["uses_default_features"].as_bool() == Some(true) {
                pending.push((name, "default"));
            }
        }
    }

    fn reaches_test_seams(&self, start: impl IntoIterator<Item = (&'a str, &'a str)>) -> bool {
        let mut pending: Vec<(&str, &str)> = start.into_iter().collect();
        let mut enabled = BTreeSet::new();
        while let Some((name, feature)) = pending.pop() {
            if feature == TEST_SEAMS {
                return true;
            }
            if !enabled.insert((name, feature)) {
                continue;
            }
            let Some(package) = self.members.get(name).copied() else {
                continue;
            };
            let entries = package["features"][feature]
                .as_array()
                .into_iter()
                .flatten();
            for entry in entries.map(str_value) {
                if let Some(key) = entry.strip_prefix("dep:") {
                    self.enable_dependency(package, key, &mut pending);
                } else if let Some((key, wanted)) = entry.split_once('/') {
                    let strong = key.strip_suffix('?').is_none();
                    let key = key.trim_end_matches('?');
                    if let Some(target) = self.target(package, key) {
                        pending.push((target, wanted));
                    }
                    if strong {
                        self.enable_dependency(package, key, &mut pending);
                    }
                } else {
                    pending.push((name, entry));
                    self.enable_dependency(package, entry, &mut pending);
                }
            }
        }
        false
    }
}

/// The name a manifest gives a dependency: its rename, or the package's name.
fn manifest_key(dependency: &Value) -> &str {
    dependency["rename"]
        .as_str()
        .unwrap_or(str_value(&dependency["name"]))
}

fn dependency_features(dependency: &Value) -> impl Iterator<Item = &str> {
    dependency["features"]
        .as_array()
        .into_iter()
        .flatten()
        .map(str_value)
}

fn violations(metadata: &Value) -> Vec<Violation> {
    let members = workspace_names(metadata);
    let root = normalize_lexically(Path::new(str_value(&metadata["workspace_root"])));
    let allowed: HashMap<&str, &[&str]> = ALLOWED_DEPENDENCIES.iter().copied().collect();
    let features = FeatureGraph::new(metadata);
    let mut found = Vec::new();

    for package in packages(metadata) {
        let name = str_value(&package["name"]);
        let is_member = members.contains(name);
        for dependency in package["dependencies"].as_array().into_iter().flatten() {
            let target = str_value(&dependency["name"]);
            // A default that reaches the seams is the target crate's own breach.
            let enables_seams = dependency["kind"].is_null()
                && features.reaches_test_seams(
                    dependency_features(dependency).map(|feature| (target, feature)),
                );
            if enables_seams {
                found.push(Violation::TestSeamsEnabled {
                    package: name.to_owned(),
                    dependency: target.to_owned(),
                });
            }
            let Some(path) = dependency["path"].as_str() else {
                continue;
            };
            let path = normalize_lexically(Path::new(path));
            if !path.starts_with(&root) {
                found.push(Violation::PathOutsideWorkspace {
                    package: name.to_owned(),
                    path,
                });
                continue;
            }
            let permitted = name == TEST_PACKAGE
                || allowed
                    .get(name)
                    .is_some_and(|crates| crates.contains(&target));
            if is_member && members.contains(target) && !permitted {
                found.push(Violation::LayerEdge {
                    from: name.to_owned(),
                    to: target.to_owned(),
                });
            }
        }
        if package["source"].is_null() {
            let manifest = normalize_lexically(Path::new(str_value(&package["manifest_path"])));
            if !manifest.starts_with(&root) {
                found.push(Violation::PathOutsideWorkspace {
                    package: name.to_owned(),
                    path: manifest.parent().unwrap_or(&manifest).to_path_buf(),
                });
            }
        }
        if str_value(&package["license"])
            .to_ascii_uppercase()
            .contains("AGPL")
        {
            found.push(Violation::AgplLicence {
                package: name.to_owned(),
            });
        }
        if is_member && features.reaches_test_seams([(name, "default")]) {
            found.push(Violation::TestSeamsByDefault {
                package: name.to_owned(),
            });
        }
    }

    found.extend(test_code_in_release(metadata));
    found.sort_by(|a, b| a.rule().cmp(&b.rule()).then_with(|| a.cmp(b)));
    found
}

/// Follows the normal dependencies of `tetral` through the resolved graph.
fn test_code_in_release(metadata: &Value) -> Vec<Violation> {
    let names: HashMap<&str, &str> = packages(metadata)
        .iter()
        .map(|package| (str_value(&package["id"]), str_value(&package["name"])))
        .collect();
    let nodes: HashMap<&str, &Value> = metadata["resolve"]["nodes"]
        .as_array()
        .expect("metadata holds the resolved graph")
        .iter()
        .map(|node| (str_value(&node["id"]), node))
        .collect();
    let mut pending: Vec<&str> = names
        .iter()
        .filter(|(_, name)| **name == "tetral")
        .map(|(id, _)| *id)
        .collect();
    let mut reached = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !reached.insert(id) {
            continue;
        }
        for dependency in nodes[id]["deps"].as_array().into_iter().flatten() {
            let normal = dependency["dep_kinds"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|kind| kind["kind"].is_null());
            if normal {
                pending.push(str_value(&dependency["pkg"]));
            }
        }
    }
    reached
        .into_iter()
        .map(|id| names[id])
        .filter(|name| *name == TEST_PACKAGE || *name == "jsonschema")
        .map(|name| Violation::TestCodeInRelease {
            package: name.to_owned(),
        })
        .collect()
}

const CRATES_IO: &str = "registry+https://github.com/rust-lang/crates.io-index";

/// A package from crates.io, as Cargo lists one.
fn registry_package(name: &str, license: &str) -> Value {
    json!({
        "name": name,
        "id": format!("{name} 1.0.0"),
        "license": license,
        "source": CRATES_IO,
        "manifest_path": format!("/registry/{name}-1.0.0/Cargo.toml"),
        "dependencies": [],
        "features": {},
    })
}

fn package_mut<'a>(metadata: &'a mut Value, name: &str) -> &'a mut Value {
    metadata["packages"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|package| package["name"] == name)
        .unwrap_or_else(|| panic!("package {name} is in the metadata"))
}

fn node_mut<'a>(metadata: &'a mut Value, id: &str) -> &'a mut Value {
    metadata["resolve"]["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == id)
        .unwrap_or_else(|| panic!("node {id} is in the resolved graph"))
}

/// Declares a dependency as Cargo would list it: in the package's
/// dependencies and, when it names a package of the graph, as an edge of the
/// resolved graph with its kind.
fn add_dependency(
    metadata: &mut Value,
    from: &str,
    to: &str,
    kind: Value,
    features: Value,
    path: &Path,
) {
    let from_id = str_value(&package_mut(metadata, from)["id"]).to_owned();
    package_mut(metadata, from)["dependencies"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "name": to,
            "kind": kind,
            "features": features,
            "path": path,
            "source": null,
            "rename": null,
            "uses_default_features": true,
        }));
    let to_id = packages(metadata)
        .iter()
        .find(|package| package["name"] == to)
        .map(|package| str_value(&package["id"]).to_owned());
    if let Some(to_id) = to_id {
        node_mut(metadata, &from_id)["deps"]
            .as_array_mut()
            .unwrap()
            .push(json!({"pkg": to_id, "dep_kinds": [{"kind": kind, "target": null}]}));
    }
}

#[test]
fn the_workspace_holds_the_layered_crates_and_the_test_package() {
    let mut case =
        tetral_tests::record!().case("the workspace holds the layered crates and the test package");
    let mut expected: BTreeSet<String> = ALLOWED_DEPENDENCIES
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect();
    expected.insert(TEST_PACKAGE.to_owned());
    assert_eq!(workspace_names(&metadata(&mut case)), expected);
    case.observe(format!("the members are exactly {expected:?}"));
}

#[test]
fn the_workspace_graph_breaks_no_rule() {
    let mut case = tetral_tests::record!().case("the workspace graph breaks no rule");
    assert_eq!(violations(&metadata(&mut case)), Vec::new());
    case.observe("no violation of the five rules");
}

#[test]
fn each_injected_breach_is_reported_once_by_its_rule() {
    let mut case =
        tetral_tests::record!().case("each injected breach is reported once by its rule");
    let mut metadata = metadata(&mut case);
    let root = workspace_dir();

    add_dependency(
        &mut metadata,
        "connection",
        "session",
        Value::Null,
        json!([]),
        &root.join("crates/session"),
    );
    add_dependency(
        &mut metadata,
        "protocol",
        TEST_PACKAGE,
        json!("dev"),
        json!([]),
        &root.join("tests"),
    );
    add_dependency(
        &mut metadata,
        "state",
        "internal",
        Value::Null,
        json!([]),
        &root.join("../internal"),
    );
    metadata["packages"]
        .as_array_mut()
        .unwrap()
        .push(registry_package("copyleft", "AGPL-3.0"));
    let protocol = str_value(&package_mut(&mut metadata, "protocol")["id"]).to_owned();
    metadata["packages"]
        .as_array_mut()
        .unwrap()
        .push(registry_package("jsonschema", "MIT"));
    metadata["resolve"]["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id": "jsonschema 1.0.0", "deps": []}));
    node_mut(&mut metadata, &protocol)["deps"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "pkg": "jsonschema 1.0.0",
            "dep_kinds": [{"kind": null, "target": null}],
        }));
    package_mut(&mut metadata, "tetral")["features"]["default"] = json!(["test-seams"]);
    add_dependency(
        &mut metadata,
        "tetral",
        "session",
        Value::Null,
        json!(["test-seams"]),
        &root.join("crates/session"),
    );

    assert_eq!(
        violations(&metadata),
        vec![
            Violation::LayerEdge {
                from: "connection".to_owned(),
                to: "session".to_owned()
            },
            Violation::LayerEdge {
                from: "protocol".to_owned(),
                to: TEST_PACKAGE.to_owned()
            },
            Violation::PathOutsideWorkspace {
                package: "state".to_owned(),
                path: normalize_lexically(&root.join("../internal")),
            },
            Violation::AgplLicence {
                package: "copyleft".to_owned()
            },
            Violation::TestCodeInRelease {
                package: "jsonschema".to_owned()
            },
            Violation::TestSeamsByDefault {
                package: "tetral".to_owned()
            },
            Violation::TestSeamsEnabled {
                package: "tetral".to_owned(),
                dependency: "session".to_owned()
            },
        ]
    );
    case.observe("seven injected breaches, each reported once by its rule, in order");
}

#[test]
fn breaches_reached_through_features_or_patches_are_reported() {
    let mut case =
        tetral_tests::record!().case("breaches reached through features or patches are reported");
    type Injection = fn(&mut Value, &Path);
    let cases: [(&str, Injection, Vec<Violation>); 5] = [
        (
            "a default feature that turns on test-seams",
            |metadata, _| {
                package_mut(metadata, "session")["features"] =
                    json!({"default": ["fixtures"], "fixtures": ["test-seams"]});
            },
            vec![Violation::TestSeamsByDefault {
                package: "session".to_owned(),
            }],
        ),
        (
            "a default feature that names a dependency's test-seams",
            |metadata, _| {
                package_mut(metadata, "appserver")["features"] =
                    json!({"default": ["session/test-seams"]});
            },
            vec![Violation::TestSeamsByDefault {
                package: "appserver".to_owned(),
            }],
        ),
        (
            "a default feature that turns on an optional dependency with a feature that turns on test-seams",
            |metadata, _| {
                package_mut(metadata, "session")["features"] = json!({"fixtures": ["test-seams"]});
                let appserver = package_mut(metadata, "appserver");
                appserver["features"] = json!({"default": ["dep:session"]});
                let session = appserver["dependencies"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|dependency| dependency["name"] == "session")
                    .unwrap();
                session["optional"] = json!(true);
                session["features"] = json!(["fixtures"]);
            },
            vec![
                Violation::TestSeamsByDefault {
                    package: "appserver".to_owned(),
                },
                Violation::TestSeamsEnabled {
                    package: "appserver".to_owned(),
                    dependency: "session".to_owned(),
                },
            ],
        ),
        (
            "a normal dependency on a feature that turns on test-seams",
            |metadata, root| {
                package_mut(metadata, "session")["features"] = json!({"fixtures": ["test-seams"]});
                add_dependency(
                    metadata,
                    "tetral",
                    "session",
                    Value::Null,
                    json!(["fixtures"]),
                    &root.join("crates/session"),
                );
            },
            vec![Violation::TestSeamsEnabled {
                package: "tetral".to_owned(),
                dependency: "session".to_owned(),
            }],
        ),
        (
            "a path package from outside the workspace, as a patch brings in",
            |metadata, root| {
                let mut patched = registry_package("itoa", "MIT");
                patched["source"] = Value::Null;
                patched["manifest_path"] = json!(root.join("../vendored/itoa/Cargo.toml"));
                metadata["packages"].as_array_mut().unwrap().push(patched);
            },
            vec![Violation::PathOutsideWorkspace {
                package: "itoa".to_owned(),
                path: normalize_lexically(&workspace_dir().join("../vendored/itoa")),
            }],
        ),
    ];

    let root = workspace_dir();
    for (name, inject, expected) in cases {
        let mut metadata = metadata(&mut case);
        inject(&mut metadata, &root);
        assert_eq!(violations(&metadata), expected, "{name}");
        let rules: Vec<u8> = expected.iter().map(Violation::rule).collect();
        case.observe(format!("{name}: reported by rule {rules:?}"));
    }
}

const PRINT_MACROS: &[&str] = &["print", "println", "eprint", "eprintln", "dbg"];

/// The sources of every library crate: every workspace crate but `tetral`.
fn library_sources(crates: &Path) -> Vec<(PathBuf, String)> {
    fn walk(directory: &Path, sources: &mut Vec<(PathBuf, String)>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let text = fs::read_to_string(&path).unwrap();
                sources.push((path, text));
            }
        }
    }
    let mut sources = Vec::new();
    for name in library_crates() {
        walk(&crates.join(name).join("src"), &mut sources);
    }
    sources
}

fn library_crates() -> impl Iterator<Item = &'static str> {
    ALLOWED_DEPENDENCIES
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| *name != "tetral")
}

/// Each line that calls a print macro, read with comments and the contents
/// of string and character literals blanked out.
fn print_calls(sources: &[(PathBuf, String)]) -> Vec<(PathBuf, usize)> {
    let mut calls = Vec::new();
    for (path, text) in sources {
        for (index, code) in code_only(text).lines().enumerate() {
            let prints = PRINT_MACROS.iter().any(|name| {
                code.match_indices(&format!("{name}!")).any(|(at, _)| {
                    let before = code[..at].chars().next_back();
                    !before.is_some_and(|c| c.is_alphanumeric() || c == '_')
                })
            });
            if prints {
                calls.push((path.clone(), index + 1));
            }
        }
    }
    calls
}

/// `text` with every character of a comment or inside a string, raw string or
/// character literal replaced by a space, newlines kept, so that lines and
/// columns stay where they were.
fn code_only(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut code = String::with_capacity(text.len());
    let is_word = |c: char| c.is_alphanumeric() || c == '_';
    let blank = |code: &mut String, c: char| code.push(if c == '\n' { '\n' } else { ' ' });
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '/' && next == Some('/') {
            while i < chars.len() && chars[i] != '\n' {
                blank(&mut code, chars[i]);
                i += 1;
            }
        } else if c == '/' && next == Some('*') {
            let mut depth = 0;
            loop {
                if i >= chars.len() {
                    break;
                }
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    code.push_str("  ");
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    code.push_str("  ");
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    blank(&mut code, chars[i]);
                    i += 1;
                }
            }
        } else if c == '"' {
            // A raw string is `r`, any number of `#`, then the quote, with no
            // word character before the `r` but a `b` or `c` prefix.
            let hashes = chars[..i].iter().rev().take_while(|&&c| c == '#').count();
            let r = i.checked_sub(hashes + 1).filter(|&r| chars[r] == 'r');
            let raw = r.is_some_and(|r| {
                let before = r.checked_sub(1).map(|b| chars[b]);
                match before {
                    Some('b' | 'c') => !r.checked_sub(2).is_some_and(|b| is_word(chars[b])),
                    Some(before) => !is_word(before),
                    None => true,
                }
            });
            code.push('"');
            i += 1;
            while i < chars.len() {
                if raw {
                    if chars[i] == '"'
                        && chars[i + 1..]
                            .iter()
                            .take(hashes)
                            .filter(|&&c| c == '#')
                            .count()
                            == hashes
                    {
                        break;
                    }
                } else if chars[i] == '\\' {
                    blank(&mut code, chars[i]);
                    i += 1;
                    if i < chars.len() {
                        blank(&mut code, chars[i]);
                        i += 1;
                    }
                    continue;
                } else if chars[i] == '"' {
                    break;
                }
                blank(&mut code, chars[i]);
                i += 1;
            }
            if i < chars.len() {
                code.push('"');
                i += 1;
            }
        } else if c == '\'' && next == Some('\\') {
            code.push('\'');
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                if chars[i] == '\\' {
                    blank(&mut code, chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    blank(&mut code, chars[i]);
                    i += 1;
                }
            }
            if i < chars.len() {
                code.push('\'');
                i += 1;
            }
        } else if c == '\'' && chars.get(i + 2) == Some(&'\'') {
            // A one-character literal; `'a` without a closing quote is a
            // lifetime and stays code.
            code.push_str("' '");
            i += 3;
        } else {
            code.push(c);
            i += 1;
        }
    }
    code
}

#[test]
fn no_library_crate_prints() {
    let _case = tetral_tests::record!().case("no library crate prints");
    let sources = library_sources(&workspace_dir().join("crates"));
    for name in library_crates() {
        let lib = workspace_dir().join("crates").join(name).join("src/lib.rs");
        assert!(
            sources.iter().any(|(path, _)| *path == lib),
            "{} was not read",
            lib.display()
        );
    }
    assert_eq!(print_calls(&sources), Vec::new());
}

#[test]
fn each_injected_print_call_is_reported_once() {
    let _case = tetral_tests::record!().case("each injected print call is reported once");
    for call in [
        "print!(\"done\")",
        "println!(\"done\")",
        "eprint!(\"done\")",
        "eprintln!(\"done\")",
        "dbg!(done)",
    ] {
        // Every library crate's sources, with the call in a nested module.
        let root = tempfile::tempdir().unwrap();
        let crates = root.path().join("crates");
        for name in library_crates() {
            let src = crates.join(name).join("src");
            fs::create_dir_all(&src).unwrap();
            fs::write(src.join("lib.rs"), "mod tasks;\n").unwrap();
        }
        let injected = crates.join("session/src/tasks/report.rs");
        fs::create_dir_all(injected.parent().unwrap()).unwrap();
        fs::write(
            &injected,
            format!("fn report() {{\n    {call};\n    // {call}\n    my_{call};\n}}\n"),
        )
        .unwrap();
        assert_eq!(
            print_calls(&library_sources(&crates)),
            vec![(injected.clone(), 2)],
            "{call}"
        );
    }
}

#[test]
fn a_print_call_is_found_after_any_literal_and_never_inside_a_comment_or_literal() {
    let _case = tetral_tests::record!()
        .case("a print call is found after any literal and never inside a comment or literal");
    let injected = PathBuf::from("injected.rs");
    let rows: &[(&str, &[usize])] = &[
        ("let url = \"http://example\"; println!(\"{url}\");", &[1]),
        ("let quote = '\"'; println!(\"{quote}\");", &[1]),
        ("let slash = '\\\\'; let s = \"//\"; dbg!(s);", &[1]),
        ("let raw = r#\"a \" // b\"#; eprintln!(\"{raw}\");", &[1]),
        ("fn f<'a>(x: &'a str) { print!(\"{x}\") }", &[1]),
        ("let text = \"println!(\\\"x\\\")\";", &[]),
        ("let raw = br##\"print!(\"x\")\"##;", &[]),
        ("/* outer /* inner */ println!(\"x\") */ let x = 1;", &[]),
        ("/// println!(\"x\")\nlet x = 1;", &[]),
        (
            "let s = \"a\nprintln!(\\\"x\\\")\";\nprint!(\"{s}\");",
            &[3],
        ),
    ];
    for (source, lines) in rows {
        let found: Vec<usize> = print_calls(&[(injected.clone(), source.to_string())])
            .into_iter()
            .map(|(_, line)| line)
            .collect();
        assert_eq!(found, *lines, "{source}");
    }
}
