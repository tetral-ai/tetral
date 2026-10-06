#!/usr/bin/env python3
"""Project the default Helm objects into their raw and service-owned fragments.

Requires the repository's selected Helm executable. Existing fragment ownership
and composition order stay stable; new shared routing/security objects remain in
shared deployment files. This renders locally and never accesses a cluster.
"""
from pathlib import Path
import re
import json
import subprocess

ROOT = Path(__file__).resolve().parent.parent

from manifest_projection import documents, identity

def write(path, blocks):
    path.write_text("\n---\n".join("\n".join(line.rstrip() for line in block.splitlines()) for block in blocks) + "\n")

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
    missing = [key for key in keys if key not in objects]
    if any(key[0] != "cilium.io/v2" or key[1] != "CiliumNetworkPolicy" for key in missing):
        raise ValueError(f"owned fragment contains an unexpected missing resource: {path}: {missing}")
    keys = [key for key in keys if key in objects]
    write(path, [objects[key] for key in keys])
    used.update(keys)
for path in sorted((ROOT / "services").glob("*/k8s/**/*.yaml")):
    if path.name == "secret.example.yaml" or "profiles" in path.parts:
        continue
    keys = [identity(block) for block in documents(path.read_text())]
    missing = [key for key in keys if key not in objects]
    if any(key[0] != "cilium.io/v2" or key[1] != "CiliumNetworkPolicy" for key in missing):
        raise ValueError(f"owned fragment contains an unexpected missing resource: {path}: {missing}")
    keys = [key for key in keys if key in objects]
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

# The public edge is optional application-owned configuration, distinct from
# independently installed Gateway API/Envoy Gateway controllers and CRDs.
edge_rendered = subprocess.check_output(["helm", "template", "tetral", str(ROOT / "deploy/helm/tetral"), "--set", "edge.enabled=true"], text=True)
edge_kinds = {"Gateway", "EnvoyProxy", "HTTPRoute", "SecurityPolicy", "ClientTrafficPolicy", "BackendTrafficPolicy", "EnvoyPatchPolicy", "BackendTLSPolicy"}
edge_blocks = [block for block in documents(edge_rendered) if identity(block)[1] in edge_kinds]
edge_path = ROOT / "deploy/kubernetes/edge-gateway/envoy-gateway.yaml"
edge_path.parent.mkdir(parents=True, exist_ok=True)
write(edge_path, edge_blocks)
hardened_edge = subprocess.check_output(["helm", "template", "tetral", str(ROOT / "deploy/helm/tetral"), "--set", "edge.enabled=true", "--set", "transport.profile=hardened"], text=True)
write(profile / "edge-gateway.yaml", [block for block in documents(hardened_edge) if identity(block)[1] in edge_kinds])
# The optional provider-specific Cilium policies have an explicit projection;
# portable defaults do not require this CNI or its CRDs.
cilium = subprocess.check_output(["helm", "template", "tetral", str(ROOT / "deploy/helm/tetral"), "--set", "cilium.enabled=true"], text=True)
cilium_profile = ROOT / "deploy/kubernetes/profiles/cilium"
cilium_profile.mkdir(parents=True, exist_ok=True)
write(cilium_profile / "apiserver-policies.yaml", [block for block in documents(cilium) if identity(block)[1] == "CiliumNetworkPolicy"])

# Portable, exact identities for a private target adapter to bind and compare.
# Installation overrides render their own inventory from these same owners.
def resource_list(blocks):
    return [dict(zip(("apiVersion", "kind", "namespace", "name"), key)) for key in sorted(identity(block) for block in blocks)]
managed = ROOT / "deploy/managed"
managed.mkdir(parents=True, exist_ok=True)
inventory = {
    "schema": "tetral.managed-resource-inventory/v1",
    "dependencyLock": "deploy/dependencies.lock.json",
    "resourceSets": {
        "standard-routed": resource_list(documents(rendered)),
        "hardened": resource_list(documents(hardened)),
        "public-edge-standard": resource_list(edge_blocks),
        "public-edge-hardened": resource_list(block for block in documents(hardened_edge) if identity(block)[1] in edge_kinds),
        "cilium-optional": resource_list(block for block in documents(cilium) if identity(block)[1] == "CiliumNetworkPolicy"),
    },
    "independentPrerequisites": [
        {"owner": "istio", "renderer": "deploy/istio/render.py"},
        {"owner": "envoy-gateway", "renderer": "deploy/envoy-gateway/render.py"},
        {"owner": "cert-manager", "renderer": "deploy/cert-manager/render.py", "requiredWhen": "nativeCertificates.enabled"},
    ],
    "previousResourceDispositions": "deploy/managed/previous-resource-dispositions.json",
    "removals": json.loads((managed / "previous-resource-dispositions.json").read_text())["retirements"],
    "preserve": ["unrelated controllers and Gateway API CRD ownership", "stores, databases, object namespaces and unrelated applications"],
}
(managed / "resource-inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")

native = subprocess.check_output(["helm", "template", "tetral", str(ROOT / "deploy/helm/tetral"), "--set", "transport.profile=hardened", "--set", "nativeCertificates.enabled=true"], text=True)
inventory["resourceSets"]["native-certificates-optional"] = resource_list(block for block in documents(native) if identity(block)[1] == "Certificate")
(managed / "resource-inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")
