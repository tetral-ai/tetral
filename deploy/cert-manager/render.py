#!/usr/bin/env python3
"""Render the independently owned native certificate prerequisite locally."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--charts-dir', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent.parent
lock = json.loads((root / 'deploy/dependencies.lock.json').read_text())
selected = lock['cert_manager']
if lock['schema'] != 'tetral.deployment-dependencies/v1' or selected['version'] != '1.21.2':
    parser.error('unsupported certificate dependency lock')
chart = selected['charts'][0]
archive = args.charts_dir / f'{chart["name"]}-{chart["version"]}.tgz'
if hashlib.sha256(archive.read_bytes()).hexdigest() != chart['sha256']:
    parser.error('certificate chart archive digest differs from lock')
images = {entry['component']: entry['reference'] for entry in selected['images']}
if set(images) != {'controller', 'webhook', 'cainjector', 'acmesolver', 'startupapicheck'}:
    parser.error('incomplete certificate controller image inventory')
command = ['helm', 'template', 'cert-manager', str(archive), '--namespace', 'cert-manager',
           '--set', 'crds.enabled=true', '--set', 'crds.keep=true']
for component, reference in images.items():
    tagged, delimiter, digest = reference.partition('@')
    repository, tag = tagged.rsplit(':', 1)
    if delimiter != '@' or not digest.startswith('sha256:') or len(digest) != 71:
        parser.error('certificate controller image is not immutable')
    prefix = 'image' if component == 'controller' else component + '.image'
    for key, value in [('repository', repository), ('tag', tag), ('digest', digest)]:
        command.extend(['--set-string', prefix + '.' + key + '=' + value])
rendered = subprocess.check_output(command, text=True)
for reference in images.values():
    if reference not in rendered:
        parser.error('rendered certificate image differs from lock')
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(rendered)
