#!/usr/bin/env python3
"""Validate an observed resource list locally against a selected portable set.

This command reads a previously collected JSON list; it never contacts a cluster
or deletes resources. Supply all selected controller/app resources to establish
complete installation evidence; controller prerequisites retain separate owners.

An observed object is Tetral-owned when it carries app.kubernetes.io/part-of=tetral
or when its identity is declared by any resource set, the optional Auth issuer
policy or the bound installation render. Chart-owned edge, routing and security
objects carry no ownership label, so the declared identity is what recognizes a
leftover object outside the selected set. Undeclared, unlabeled objects are
preserved. The feature flags select canonical default sets only; a bound render
already fixes its feature set, so --expected-dir rejects them.
"""
import argparse
import json
import hashlib
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--profile', choices=['standard-routed', 'hardened'], required=True)
parser.add_argument('--observed', type=Path, required=True)
parser.add_argument('--expected-dir', type=Path)
parser.add_argument('--public-edge', action='store_true')
parser.add_argument('--cilium', action='store_true')
parser.add_argument('--native-certificates', action='store_true')
parser.add_argument('--auth-issuer-network', action='store_true')
parser.add_argument('--require-complete', action='store_true')
args = parser.parse_args()
if args.expected_dir and (args.public_edge or args.cilium or args.native_certificates or args.auth_issuer_network):
    parser.error('feature flags are fixed by the bound render; omit them with --expected-dir')
root = Path(__file__).resolve().parent.parent.parent
inventory = json.loads((root / 'deploy/managed/resource-inventory.json').read_text())
if inventory['schema'] != 'tetral.managed-resource-inventory/v1':
    parser.error('unsupported resource inventory')
if args.native_certificates and args.profile != 'hardened':
    parser.error('native certificate automation requires hardened profile')

def identity(resource):
    metadata = resource.get('metadata', resource)
    return tuple(str(resource.get(key, metadata.get(key, ''))) for key in
                 ('apiVersion', 'kind', 'namespace', 'name'))

sets = [args.profile]
if args.public_edge:
    sets.append('public-edge-hardened' if args.profile == 'hardened' else 'public-edge-standard')
if args.cilium:
    sets.append('cilium-optional')
if args.native_certificates:
    sets.append('native-certificates-optional')
expected = {identity(resource) for name in sets for resource in inventory['resourceSets'][name]}
expected_source = 'canonical_defaults'
auth_issuer_policy = ('networking.k8s.io/v1', 'NetworkPolicy', 'tetral-system', 'auth-issuer-https')
declared = {identity(resource) for resources in inventory['resourceSets'].values() for resource in resources}
declared.add(auth_issuer_policy)
if args.auth_issuer_network:
    expected.add(auth_issuer_policy)
if args.expected_dir:
    sys.path.insert(0, str(root / 'deploy'))
    from manifest_projection import documents, identity as rendered_identity
    artifact = json.loads((args.expected_dir / 'expected-resources.json').read_text())
    rendered = (args.expected_dir / 'rendered-resources.yaml').read_bytes()
    if artifact.get('schema') != 'tetral.rendered-resource-inventory/v1' or artifact.get('profile') != args.profile:
        parser.error('expected render profile/schema differs')
    if hashlib.sha256(rendered).hexdigest() != artifact.get('renderedSHA256'):
        parser.error('expected render bytes differ from bound digest')
    actual_rendered_ids = [rendered_identity(block) for block in documents(rendered.decode())]
    bound_ids = [identity(resource) for resource in artifact['expectedResources']]
    if len(set(actual_rendered_ids)) != len(actual_rendered_ids) or sorted(actual_rendered_ids) != sorted(bound_ids):
        parser.error('expected identity inventory differs from actual rendered resources')
    expected = set(bound_ids)
    declared.update(bound_ids)
    expected_source = 'bound_installation_render'
if expected.intersection(identity(resource) for resource in inventory['removals']):
    parser.error('expected installation contains a superseded resource identity')
body = json.loads(args.observed.read_text())
observed = body.get('items', []) if isinstance(body, dict) else body
if not isinstance(observed, list):
    parser.error('observed input must be a resource array or Kubernetes List')
seen = set()
failures = []
retirements = {identity(resource): resource for resource in inventory['removals']}
for resource in observed:
    key = identity(resource)
    if key in seen:
        failures.append({'reason': 'duplicate_resource_identity', 'resource': key})
    seen.add(key)
    labels = resource.get('metadata', {}).get('labels', {})
    retired = retirements.get(key)
    if retired and all(labels.get(name) == value for name, value in retired['requiredLabels'].items()):
        failures.append({'reason': 'superseded_owned_resource_survives', 'resource': key})
    elif (labels.get('app.kubernetes.io/part-of') == 'tetral' or key in declared) and key not in expected:
        failures.append({'reason': 'unexpected_owned_resource', 'resource': key})
if args.require_complete:
    failures.extend({'reason': 'missing_expected_resource', 'resource': key} for key in sorted(expected - seen))
print(json.dumps({'schema': 'tetral.managed-resource-check/v1', 'profile': args.profile,
                  'expectedCount': len(expected), 'observedCount': len(seen),
                  'expectedSource': expected_source,
                  'coverage': 'complete_resource_set' if args.require_complete else 'partial_no_completeness_claim',
                  'failures': failures, 'status': 'FAIL' if failures else 'PASS'}, indent=2))
raise SystemExit(1 if failures else 0)
