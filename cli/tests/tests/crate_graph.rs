//! The crate-graph rules of `cli/README.md`, checked over `cargo metadata`.
//! [`Violation::rule`] numbers them in the order that file states them:
//! layering, paths outside the workspace, AGPL licences, test code in the
//! release graph, and test seams.

use std::collections::{BTreeSet, HashMap};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

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

fn metadata() -> Value {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let output = Command::new(cargo)
        .args(["metadata", "--format-version", "1"])
        .current_dir(workspace_dir())
        .output()
        .expect("cargo metadata runs");
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
    let mut expected: BTreeSet<String> = ALLOWED_DEPENDENCIES
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect();
    expected.insert(TEST_PACKAGE.to_owned());
    assert_eq!(workspace_names(&metadata()), expected);
}

#[test]
fn the_workspace_graph_breaks_no_rule() {
    assert_eq!(violations(&metadata()), Vec::new());
}

#[test]
fn each_injected_breach_is_reported_once_by_its_rule() {
    let mut metadata = metadata();
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
}

#[test]
fn breaches_reached_through_features_or_patches_are_reported() {
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
    for (case, inject, expected) in cases {
        let mut metadata = metadata();
        inject(&mut metadata, &root);
        assert_eq!(violations(&metadata), expected, "{case}");
    }
}
