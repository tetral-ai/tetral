#!/usr/bin/env python3
"""Verify locked chart archives and render mandatory Istiod trust references.

Only local Helm rendering is performed. No install, cluster or kubeconfig is
used. The upstream optional cacerts mount is made mandatory with its complete
intermediate/key/chain/root inventory; missing operator material blocks startup.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--charts-dir", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--trust-domain", default="cluster.local")
args = parser.parse_args()
if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", args.trust_domain):
    parser.error("trust domain must be a concrete DNS name")
root = Path(__file__).resolve().parent.parent.parent
lock = json.loads((root / "deploy/dependencies.lock.json").read_text())
if lock["schema"] != "tetral.deployment-dependencies/v1" or lock["istio"]["version"] != "1.31.1":
    parser.error("unsupported dependency lock")
parts = []
for chart in lock["istio"]["charts"]:
    archive = args.charts_dir / f'{chart["name"]}-{chart["version"]}.tgz'
    if hashlib.sha256(archive.read_bytes()).hexdigest() != chart["sha256"]:
        parser.error(f'archive digest differs for {chart["name"]}')
    command = ["helm", "template", "istio-" + chart["name"], str(archive), "--namespace", "istio-system", "--values", str(root / "deploy/istio" / (chart["name"] + "-values.yaml"))]
    if chart["name"] == "istiod":
        command += ["--set", "meshConfig.trustDomain=" + args.trust_domain]
    rendered = subprocess.check_output(command, text=True)
    if chart["name"] == "istiod":
        pattern = r"(?m)^(\s*)- name: cacerts\n\s*secret:\n\s*secretName: cacerts\n\s*optional: true$"
        def mandatory(match):
            indent = match[1]
            return indent + "- name: cacerts\n" + indent + "  secret:\n" + indent + "    secretName: cacerts\n" + indent + "    optional: false\n" + indent + "    items:\n" + "\n".join(indent + "      - key: " + key + "\n" + indent + "        path: " + key for key in ("ca-cert.pem", "ca-key.pem", "root-cert.pem", "cert-chain.pem"))
        rendered, count = re.subn(pattern, mandatory, rendered)
        if count != 1:
            parser.error("locked chart's single cacerts volume shape changed")
        if 'image: "' + next(image["reference"] for image in lock["istio"]["images"] if image["component"] == "pilot") + '"' not in rendered:
            parser.error("rendered discovery image differs from lock")
    parts.append(rendered.strip())
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text("\n---\n".join(parts) + "\n")
