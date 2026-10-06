#!/usr/bin/env python3
"""Bind expected installation identities to an actual local Helm render."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root / 'deploy'))
from manifest_projection import documents, identity

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--profile', choices=['standard-routed', 'hardened'], required=True)
parser.add_argument('--values', type=Path)
parser.add_argument('--output-dir', type=Path, required=True)
parser.add_argument('--public-edge', action='store_true')
parser.add_argument('--cilium', action='store_true')
parser.add_argument('--git-fqdn-policy', action='store_true')
parser.add_argument('--native-certificates', action='store_true')
args = parser.parse_args()
command = ['helm', 'template', 'tetral', str(root / 'deploy/helm/tetral')]
source_values = None
if args.values:
    source_values = hashlib.sha256(args.values.read_bytes()).hexdigest()
    command.extend(['--values', str(args.values)])
for key, value in [
    ('transport.profile', args.profile),
    ('edge.enabled', str(args.public_edge).lower()),
    ('cilium.enabled', str(args.cilium).lower()),
    ('cilium.gitProxyFQDNPolicy', str(args.git_fqdn_policy).lower()),
    ('nativeCertificates.enabled', str(args.native_certificates).lower()),
]:
    command.extend(['--set', key + '=' + value])
rendered = subprocess.check_output(command)
resources = sorted(identity(block) for block in documents(rendered.decode()))
if len(set(resources)) != len(resources):
    parser.error('actual render repeats a resource identity')
args.output_dir.mkdir(parents=True, exist_ok=True)
(args.output_dir / 'rendered-resources.yaml').write_bytes(rendered)
artifact = {
    'schema': 'tetral.rendered-resource-inventory/v1',
    'profile': args.profile,
    'renderedSHA256': hashlib.sha256(rendered).hexdigest(),
    'valuesSHA256': source_values,
    'expectedResources': [dict(zip(('apiVersion', 'kind', 'namespace', 'name'), resource)) for resource in resources],
}
(args.output_dir / 'expected-resources.json').write_text(json.dumps(artifact, indent=2) + '\n')
