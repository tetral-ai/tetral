package testinfra

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSecretHelperSecurityGatePreservesInventoryAndRejectsScannerErrors(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repositoryRootForTest(t), "scripts/check-envoy-secret-helper.py"))
	if err != nil {
		t.Fatal(err)
	}
	lock, err := os.ReadFile(filepath.Join(repositoryRootForTest(t), "deploy/dependencies.lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                     string
		build, module, symbol    int
		wantScans                []string
		wantSuccess, wantFinding bool
	}{
		{"clean", 0, 0, 0, []string{"module", "symbol"}, true, false},
		{"inventory_findings", 0, 3, 0, []string{"module", "symbol"}, true, true},
		{"module_syntax_error", 0, 2, 0, []string{"module"}, false, false},
		{"module_infrastructure_error", 0, 1, 0, []string{"module"}, false, false},
		{"module_unexpected_exit", 0, 4, 0, []string{"module"}, false, false},
		{"symbol_findings", 0, 3, 3, []string{"module", "symbol"}, false, true},
		{"symbol_error", 0, 0, 2, []string{"module", "symbol"}, false, false},
		{"build_error", 1, 0, 0, nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for _, directory := range []string{"scripts", "deploy", "bin", "integration/envoy-gateway-secret-helper"} {
				if err := os.MkdirAll(filepath.Join(root, directory), 0700); err != nil {
					t.Fatal(err)
				}
			}
			for name, body := range map[string][]byte{"scripts/check-envoy-secret-helper.py": script, "deploy/dependencies.lock.json": lock, "bin/go": []byte(secretHelperScannerFixture)} {
				if err := os.WriteFile(filepath.Join(root, name), body, 0700); err != nil {
					t.Fatal(err)
				}
			}
			transcript := filepath.Join(root, "calls.jsonl")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", filepath.Join(root, "scripts/check-envoy-secret-helper.py"))
			command.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+":"+os.Getenv("PATH"), "FIXTURE_TRANSCRIPT="+transcript,
				"FIXTURE_BUILD_EXIT="+strconv.Itoa(tc.build), "FIXTURE_MODULE_EXIT="+strconv.Itoa(tc.module), "FIXTURE_SYMBOL_EXIT="+strconv.Itoa(tc.symbol))
			output, runErr := command.CombinedOutput()
			if ctx.Err() != nil || (runErr == nil) != tc.wantSuccess {
				t.Fatalf("gate success=%v want=%v context=%v output=%s", runErr == nil, tc.wantSuccess, ctx.Err(), output)
			}
			body, err := os.ReadFile(transcript)
			if err != nil {
				t.Fatal(err)
			}
			var scans []string
			var binary string
			for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
				var call struct {
					Kind, CWD, Toolchain string
					Args                 []string
				}
				if err := json.Unmarshal([]byte(line), &call); err != nil {
					t.Fatal(err)
				}
				if call.Toolchain != "go1.26.9" {
					t.Fatal("gate changed the selected scanner toolchain")
				}
				if call.Kind == "build" {
					if call.CWD != root || len(call.Args) != 7 || !slices.Equal(call.Args[:5], []string{"build", "-mod=readonly", "-p", "2", "-o"}) || call.Args[6] != "golang.org/x/vuln/cmd/govulncheck" {
						t.Fatal("gate changed the pinned readonly scanner build")
					}
					binary = call.Args[5]
					continue
				}
				if call.CWD != filepath.Join(root, "integration/envoy-gateway-secret-helper") || len(call.Args) < 2 {
					t.Fatal("scanner ran outside the nested module")
				}
				scan := call.Args[1]
				want := []string{"-scan", "module"}
				if scan == "symbol" {
					want = []string{"-scan", "symbol", "./..."}
				}
				if !slices.Equal(call.Args, want) {
					t.Fatal("scanner patterns or scan scope changed")
				}
				scans = append(scans, scan)
			}
			if !slices.Equal(scans, tc.wantScans) {
				t.Fatalf("executed scans=%v want=%v", scans, tc.wantScans)
			}
			if len(scans) > 0 && !strings.Contains(string(output), "UNFILTERED_MODULE_REPORT") {
				t.Fatal("gate hid the scanner's module report")
			}
			if tc.wantFinding && !strings.Contains(string(output), "helper module inventory: advisories reported; symbol gate pending") {
				t.Fatal("gate omitted the inventory finding disposition")
			}
			if strings.Contains(string(output), "helper symbol gate: passed") != tc.wantSuccess {
				t.Fatal("gate reported successful symbol coverage after failure")
			}
			if _, err := os.Stat(filepath.Dir(binary)); !os.IsNotExist(err) {
				t.Fatal("temporary scanner directory was not removed")
			}
		})
	}
}

// The fixture controls process exits, not advisory content or production graph.
// It runs the owning script unchanged and records its actual child boundaries.
const secretHelperScannerFixture = `#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
kind = 'build' if args[0] == 'build' else 'scan'
with open(os.environ['FIXTURE_TRANSCRIPT'], 'a') as stream:
    stream.write(json.dumps(dict(Kind=kind, CWD=os.getcwd(), Toolchain=os.environ.get('GOTOOLCHAIN'), Args=args)) + '\n')
if kind == 'build':
    code = int(os.environ['FIXTURE_BUILD_EXIT'])
    if code == 0:
        binary = pathlib.Path(args[args.index('-o') + 1])
        binary.write_bytes(pathlib.Path(__file__).read_bytes())
        binary.chmod(0o700)
else:
    scan = args[1]
    print('UNFILTERED_' + scan.upper() + '_REPORT', flush=True)
    code = int(os.environ['FIXTURE_' + scan.upper() + '_EXIT'])
sys.exit(code)
`
