#!/usr/bin/env python3
"""Stateful remote CLI fixture; real release scripts, OCI layouts and Go validators run unchanged."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

root = Path(os.environ['PUBLICATION_REMOTE'])
state_path = root / 'state.json'
state = json.loads(state_path.read_text())
args = sys.argv[1:]
tool = Path(sys.argv[0]).name


def option(name):
    return args[args.index(name) + 1]


def emit(value):
    print(json.dumps(value))


def write(event):
    state_path.write_text(json.dumps(state))
    with (root / 'writes').open('a') as log:
        log.write(event + '\n')
    if os.environ.get('FAIL_AFTER') == event:
        sys.exit(97)


def layout(digest):
    return root / digest.replace(':', '-')


def resolve(ref):
    if '@' in ref:
        return ref.rsplit('@', 1)[1]
    if ref not in state['tags']:
        sys.exit(1)
    return state['tags'][ref]


if tool == 'git' and args[:1] == ['fetch']:
    pass
elif tool == 'git' and args[:2] == ['tag', '-l']:
    print('\n'.join(state.get('git_tags', [])))
elif tool == 'oras' and args[:1] == ['logout']:
    pass
elif tool == 'oras' and args[:2] == ['repo', 'tags']:
    emit({'tags': state.get('reservation_tags', [])})
elif tool == 'oras' and args[:2] == ['manifest', 'fetch']:
    digest = resolve(args[-1])
    if '--format' in args:
        print(digest)
    else:
        sys.stdout.buffer.write((layout(digest) / 'blobs' / 'sha256' / digest.split(':')[1]).read_bytes())
elif tool == 'oras' and args[0] == 'cp':
    source, destination = args[-2:]
    if '--to-oci-layout' in args:
        shutil.copytree(layout(resolve(source)), destination.rsplit(':', 1)[0], dirs_exist_ok=True)
    else:
        directory, digest = source.rsplit('@', 1)
        shutil.copytree(directory, layout(digest), dirs_exist_ok=True)
        state['tags'][destination] = digest
        assert ':authorization-' in destination
        write('authorization')
elif tool == 'docker' and args[:3] == ['buildx', 'imagetools', 'create']:
    state['tags'][option('--tag')] = args[-1].rsplit('@', 1)[1]
    write('image:' + option('--tag').split('/')[-1].split(':')[0])
elif tool == 'helm' and args[0] == 'push':
    package = Path(args[1]).read_bytes()
    manifest = {'schemaVersion': 2, 'config': {'mediaType': 'application/vnd.cncf.helm.config.v1+json'},
                'layers': [{'mediaType': 'application/vnd.cncf.helm.chart.content.v1.tar+gzip',
                            'digest': 'sha256:' + hashlib.sha256(package).hexdigest()}]}
    body = json.dumps(manifest).encode()
    digest = 'sha256:' + hashlib.sha256(body).hexdigest()
    blob = layout(digest) / 'blobs' / 'sha256' / digest.split(':')[1]
    blob.parent.mkdir(parents=True)
    blob.write_bytes(body)
    state['tags']['ghcr.io/tetral-ai/charts/tetral:' + os.environ['VERSION']] = digest
    write('chart')
elif tool == 'gh' and args[0] == 'api':
    endpoint = next(a for a in args if a.startswith('repos/'))
    if endpoint.endswith('/approvals'):
        emit([{'state': 'approved', 'user': {'id': 7}, 'environments': [{'name': 'release'}]}])
    elif '/statuses?' in endpoint:
        emit([{'log_url': 'https://github.com/tetral-ai/tetral/actions/runs/42/job/99'}])
    elif '/deployments?' in endpoint:
        emit([[{'id': 11, 'sha': os.environ['WORKFLOW_SHA']}]] )
    elif endpoint.endswith('/git/tags'):
        state['tag_target'] = next(a.split('=', 1)[1] for a in args if a.startswith('object='))
        state['tag_object'] = 'a' * 40
        write('tag-object')
        print(state['tag_object'])
    elif endpoint.endswith('/git/refs'):
        assert 'sha=' + state['tag_object'] in args
        state['tag'] = state['tag_target']
        write('tag')
    elif '/git/ref/tags/' in endpoint:
        if not state.get('tag'):
            sys.exit(1)
        emit({'object': {'type': 'tag', 'sha': state['tag_object']}})
    elif '/git/tags/' in endpoint:
        assert endpoint.endswith('/' + state['tag_object'])
        assert option('--jq') == '.object.sha'
        print(state['tag_target'])
    else:
        raise AssertionError(args)
elif tool == 'gh' and args[:2] == ['release', 'view']:
    if state.get('release') is None:
        sys.exit(1)
    emit(dict(state['release'], assets=[{'name': name} for name in state['assets']]))
elif tool == 'gh' and args[:2] == ['release', 'create']:
    assert '--draft' in args and '--prerelease' in args
    assert state.get('release') is None
    state['release'] = {'isDraft': True, 'isPrerelease': True}
    write('draft')
elif tool == 'gh' and args[:2] == ['release', 'upload']:
    path = Path(args[3])
    assert path.name not in state['assets']
    state['assets'][path.name] = path.read_text()
    write('asset:' + path.name)
elif tool == 'gh' and args[:2] == ['release', 'download']:
    body = state['assets'][option('--pattern')].encode()
    assert option('--output') == '-'
    sys.stdout.buffer.write(body)
elif tool == 'gh' and args[:2] == ['release', 'edit']:
    assert '--draft=false' in args and '--prerelease' in args
    assert set(state['assets']) == {'candidate.json', 'evidence.json', 'authorization.json'}
    state['release'] = {'isDraft': False, 'isPrerelease': True}
    write('published')
else:
    raise AssertionError((tool, args))
