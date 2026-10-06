#!/usr/bin/env python3
"""Scan the separate pinned upstream translator module and its reachable symbols."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
lock = json.loads((root / 'deploy/dependencies.lock.json').read_text())['envoy_gateway']['secret_helper']
if lock != {
    'module_directory': 'integration/envoy-gateway-secret-helper',
    'upstream_module': 'github.com/envoyproxy/gateway',
    'upstream_version': 'v1.9.2',
    'go_toolchain': 'go1.26.8',
}:
    raise SystemExit('unsupported SecretType helper dependency lock')
helper = root / lock['module_directory']
environment = dict(os.environ, GOTOOLCHAIN=lock['go_toolchain'])
# Build the same govulncheck tool pinned by the Engine module. Execute it from
# the nested module so both scans inspect that distinct graph, not Engine's.
with tempfile.TemporaryDirectory(prefix='tetral-secret-helper-vuln-') as directory:
    binary = Path(directory) / 'govulncheck'
    subprocess.run(['go', 'build', '-mod=readonly', '-p', '2', '-o', str(binary),
                    'golang.org/x/vuln/cmd/govulncheck'], cwd=root, env=environment, check=True)
    # Module reports inventory every advisory in the selected graph, including
    # packages this program never imports or calls. Keep those reports intact;
    # only the scanner's normal finding exit may continue to the symbol gate.
    module = subprocess.run([str(binary), '-scan', 'module'], cwd=helper, env=environment)
    if module.returncode == 0:
        print('helper module inventory: no advisories', flush=True)
    elif module.returncode == 3:
        print('helper module inventory: advisories reported; symbol gate pending', flush=True)
    else:
        module.check_returncode()
    subprocess.run([str(binary), '-scan', 'symbol', './...'], cwd=helper, env=environment, check=True)
    print('helper symbol gate: passed', flush=True)
