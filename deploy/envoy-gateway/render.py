#!/usr/bin/env python3
"""Verify selected controller/CRD bytes and render locally, without a cluster.

Gateway API CRDs are optional in the output: an installation with a compatible
provider-owned set must preserve that ownership. Envoy Gateway CRDs and its
controller are independently owned prerequisites, never application resources.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--charts-dir', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--include-gateway-api-crds', action='store_true')
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent.parent
lock = json.loads((root / 'deploy/dependencies.lock.json').read_text())
eg = lock['envoy_gateway']
if lock['schema'] != 'tetral.deployment-dependencies/v1' or eg['version'] != '1.9.2' or lock['gateway_api']['version'] != '1.6.1':
    parser.error('unsupported gateway dependency lock')
def verified(path, sha, role):
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != sha:
        parser.error(f'archive digest differs for {role}')
    return body.decode() if path.suffix == '.yaml' else body
chart = eg['charts'][0]
archive = args.charts_dir / f'{chart["name"]}-{chart["version"]}.tgz'
verified(archive, chart['sha256'], chart['name'])
parts = []
if args.include_gateway_api_crds:
    parts.append(verified(root / 'deploy/envoy-gateway/gateway-api/standard-install-v1.6.1.yaml', lock['gateway_api']['standard_crds']['sha256'], 'Gateway API'))
parts.append(verified(root / 'deploy/envoy-gateway/gateway-api/envoy-gateway-crds-v1.9.2.yaml', eg['crds']['sha256'], 'Envoy Gateway CRDs'))
images = {image['component']: image['reference'] for image in eg['images']}
if set(images) != {'controller', 'proxy'} or any('@sha256:' not in ref for ref in images.values()):
    parser.error('incomplete immutable gateway image inventory')
rendered = subprocess.check_output(['helm', 'template', 'envoy-gateway', str(archive), '--namespace', 'envoy-gateway-system', '--values', str(root / 'deploy/envoy-gateway/controller-values.yaml'), '--set-string', 'global.images.envoyGateway.image=' + images['controller'], '--set-string', 'global.images.envoyProxy.image=' + images['proxy']], text=True)
for ref in images.values():
    if ref not in rendered:
        parser.error('rendered gateway image differs from lock')
if 'enableEnvoyPatchPolicy: true' not in rendered:
    parser.error('controller does not enable the reviewed raw-header patch')
parts.extend([rendered, (root / 'deploy/envoy-gateway/gateway-class.yaml').read_text()])
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text('\n---\n'.join(part.strip() for part in parts) + '\n')
