#!/usr/bin/env python3
"""Verify the official locked NATS chart and render one independent release.

This is local Helm rendering only. No Kubernetes API or credentials are read.
The generated values keep broker replica count and route references together.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--charts-dir", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--values-output", type=Path)
parser.add_argument("--replicas", type=int, default=1)
parser.add_argument("--profile", choices=["standard-routed", "hardened"], default="standard-routed")
args = parser.parse_args()
if args.replicas < 1:
    parser.error("replicas must be positive")
root = Path(__file__).resolve().parent.parent.parent
lock = json.loads((root / "deploy/dependencies.lock.json").read_text())
if lock["schema"] != "tetral.deployment-dependencies/v1" or lock["nats"]["version"] != "2.15.0":
    parser.error("unsupported dependency lock")
chart = lock["nats"]["charts"][0]
archive = args.charts_dir / f'{chart["name"]}-{chart["version"]}.tgz'
if hashlib.sha256(archive.read_bytes()).hexdigest() != chart["sha256"]:
    parser.error("archive digest differs for nats")
images = {image["component"]: image["reference"] for image in lock["nats"]["images"]}
routes = [f"<< $NATS_ROUTE_{i} >>" for i in range(args.replicas)]
route_env = {f"NATS_ROUTE_{i}": {"valueFrom": {"secretKeyRef": {"name": "tetral-nats-cluster-credentials", "key": f"route_{i}"}}} for i in range(args.replicas)}
overrides = {
    "container": {"image": {"fullImageName": images["server"]}, "env": route_env},
    "reloader": {"image": {"fullImageName": images["reloader"]}},
    "promExporter": {"image": {"fullImageName": images["exporter"]}},
    "config": {"cluster": {"replicas": args.replicas, "merge": {"routes": routes}}},
}
# YAML accepts JSON. No secret values enter these generated references.
with tempfile.TemporaryDirectory(prefix="tetral-nats-render-") as temporary:
    values = Path(temporary) / "locked-values.json"
    values.write_text(json.dumps(overrides))
    command = ["helm", "template", "tetral-nats", str(archive), "--namespace", "tetral-system", "--values", str(root / "deploy/nats/values.yaml")]
    if args.profile == "hardened":
        command += ["--values", str(root / "deploy/nats/values-hardened.yaml")]
    command += ["--values", str(values)]
    rendered = subprocess.check_output(command, text=True)
    if args.values_output:
        args.values_output.parent.mkdir(parents=True, exist_ok=True)
        args.values_output.write_text(json.dumps(overrides, indent=2) + "\n")
for reference in images.values():
    if reference not in rendered:
        parser.error("rendered image differs from lock")
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(rendered.rstrip() + "\n---\n" + (root / "deploy/nats/network.yaml").read_text())
