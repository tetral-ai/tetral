#!/usr/bin/env python3
"""Project the default Helm objects into their raw and service-owned fragments.

Requires the repository's selected Helm executable. Existing fragment ownership
and composition order stay stable; new shared routing/security objects remain in
shared deployment files. This renders locally and never accesses a cluster.
"""
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parent.parent

def documents(text):
    for block in re.split(r"(?m)^---\s*$", text):
        block = re.sub(r"(?m)^# Source:.*\n", "", block).strip()
        if block:
            yield block

def identity(block):
    api = re.search(r"(?m)^apiVersion: (.+)$", block)
    kind = re.search(r"(?m)^kind: (.+)$", block)
    metadata = re.search(r"(?ms)^metadata:\n(.*?)(?=^\S|\Z)", block)
    if not api or not kind or not metadata:
        raise ValueError("rendered document lacks an object identity")
    name = re.search(r"(?m)^  name: (.+)$", metadata[1])
    namespace = re.search(r"(?m)^  namespace: (.+)$", metadata[1])
    if not name:
        raise ValueError("rendered object lacks a name")
    return (api[1], kind[1], namespace[1] if namespace else "", name[1])

def write(path, blocks):
    path.write_text("\n---\n".join(blocks) + "\n")

rendered = subprocess.check_output(["helm", "template", "tetral", str(ROOT / "deploy/helm/tetral")], text=True)
objects = {}
for block in documents(rendered):
    key = identity(block)
    if key in objects:
        raise ValueError(f"duplicate rendered identity: {key}")
    objects[key] = block
used = set()
for path in sorted((ROOT / "deploy/kubernetes").glob("*.yaml")):
    if path.name in ("internal-routing.yaml", "internal-security.yaml", "database-budget.yaml"):
        continue
    keys = [identity(block) for block in documents(path.read_text())]
    write(path, [objects[key] for key in keys])
    used.update(keys)
for path in sorted((ROOT / "services").glob("*/k8s/**/*.yaml")):
    if path.name == "secret.example.yaml" or "profiles" in path.parts:
        continue
    keys = [identity(block) for block in documents(path.read_text())]
    write(path, [objects[key] for key in keys])
new = {key: block for key, block in objects.items() if key not in used}
routing = [block for key, block in new.items() if key[1] in ("DestinationRule", "VirtualService")]
security = [block for key, block in new.items() if key[1] in ("PeerAuthentication", "AuthorizationPolicy", "NetworkPolicy")]
budget = [block for key, block in new.items() if key[1] == "ConfigMap" and key[3] == "tetral-database-connection-budget"]
if len(routing) + len(security) + len(budget) != len(new):
    raise ValueError("new object needs an explicit owning projection")
write(ROOT / "deploy/kubernetes/database-budget.yaml", budget)
write(ROOT / "deploy/kubernetes/internal-routing.yaml", routing)
write(ROOT / "deploy/kubernetes/internal-security.yaml", security)

# Alternate profiles replace the default set; they are never added to it.
hardened = subprocess.check_output(["helm", "template", "tetral", str(ROOT / "deploy/helm/tetral"), "--set", "transport.profile=hardened"], text=True)
profile = ROOT / "deploy/kubernetes/profiles/hardened"
profile.mkdir(parents=True, exist_ok=True)
write(profile / "workloads.yaml", list(documents(hardened)))
