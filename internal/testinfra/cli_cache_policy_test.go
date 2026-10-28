package testinfra

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIBuildCacheWorkflowPolicy(t *testing.T) {
	root := testRepositoryRoot(t)
	const producer = "workflows/cli-build-cache.yml"
	const consumer = "actions/cli-build-cache/action.yml"
	tests := []struct{ name, file, old, new, want string }{
		{name: "repository"},
		{"PR writer", producer, "on:\n  push:", "on:\n  pull_request:\n  push:", "main pushes and dispatch only"},
		{"off main", producer, "if: github.ref == 'refs/heads/main'", "if: always()", "limited to main"},
		{"writable token", producer, "contents: read", "contents: write", "read-only contents"},
		{"cancels upload", producer, "cancel-in-progress: false", "cancel-in-progress: true", "without cancelling"},
		{"wrong revision", producer, "persist-credentials: false", "persist-credentials: false\n          ref: untrusted", "exact revision"},
		{"implicit Go cache", producer, "cache: false", "cache: true", "without its own cache"},
		{"different build", producer, "build common producer", "build all producer", "shared build recipe"},
		{"failed build accepted", producer, "id: build-common", "id: build-common\n        continue-on-error: true", "shared build recipe"},
		{"lookup downloads", producer, "lookup-only: true", "lookup-only: false", "look up each exact key"},
		{"save key changed", producer, "name: Save common\n        if: steps.lookup-common.outputs.cache-hit != 'true'\n        uses: actions/cache/save" + pinnedCLICache + "\n        with:\n          path: ${{ steps.prepare.outputs.common-paths }}\n          key: ${{ steps.prepare.outputs.common-key }}", "name: Save common\n        if: steps.lookup-common.outputs.cache-hit != 'true'\n        uses: actions/cache/save" + pinnedCLICache + "\n        with:\n          path: ${{ steps.prepare.outputs.common-paths }}\n          key: stale", "shared compatible keys"},
		{"extra execution", producer, "      - name: Compile common", "      - run: cargo test\n      - name: Compile common", "reviewed setup"},
		{"consumer implicit save", consumer, "actions/cache/restore" + pinnedCLICache, "actions/cache" + pinnedCLICache, "restore only common"},
		{"consumer wrong component", consumer, "path: ${{ steps.prepare.outputs.common-paths }}", "path: cli/target", "component paths"},
		{"consumer incompatible fallback", consumer, "restore-keys: ${{ steps.prepare.outputs.common-prefix }}", "restore-keys: cli-build-v1-", "compatible identity"},
		{"consumer digest for every profile", consumer, " && steps.prepare.outputs.variant == 'digest'", "", "selected variant"},
		{"consumer no disk guard", consumer, "if: steps.prepare.outputs.restore-allowed == 'true'", "if: always()", "disk guard"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := t.TempDir()
			if err := os.CopyFS(filepath.Join(fixture, ".github"), os.DirFS(filepath.Join(root, ".github"))); err != nil {
				t.Fatal(err)
			}
			if tc.old != "" {
				path := filepath.Join(fixture, ".github", tc.file)
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				changed := strings.Replace(string(body), tc.old, tc.new, 1)
				if changed == string(body) {
					t.Fatal("mutation did not match")
				}
				if err := os.WriteFile(path, []byte(changed), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := VerifyCLIBuildCacheWorkflows(fixture)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCLICacheGlobalWriterBoundary(t *testing.T) {
	for _, kind := range []string{"save", "implicit", "PR override"} {
		t.Run(kind, func(t *testing.T) {
			root := testRepositoryRoot(t)
			fixture := t.TempDir()
			if err := os.CopyFS(filepath.Join(fixture, ".github"), os.DirFS(filepath.Join(root, ".github"))); err != nil {
				t.Fatal(err)
			}
			body := "on:\n  workflow_dispatch:\njobs:\n  unrecognized:\n    steps:\n      - uses: actions/cache/save" + pinnedCLICache + "\n"
			want := "saves to the Actions cache"
			if kind == "implicit" {
				body = strings.Replace(body, "actions/cache/save", "actions/cache", 1)
				want = "saves in a post step"
			}
			if kind == "PR override" {
				body = "on:\n  pull_request:\ncache-mode: read\njobs:\n  unrecognized:\n    cache-mode: write\n    steps: []\n"
				want = "overrides the pull request cache-mode"
			}
			if err := os.WriteFile(filepath.Join(fixture, ".github/workflows/unrecognized.yml"), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			if err := VerifyGoBuildCacheWorkflows(fixture); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("got %v, want %s", err, want)
			}
		})
	}
}

func cliCacheScriptFixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{"scripts", "cli", "bin", "evidence"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	body, err := os.ReadFile(filepath.Join(testRepositoryRoot(t), "scripts/cli-build-cache.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "scripts/cli-build-cache.sh")
	if err := os.WriteFile(script, body, 0700); err != nil {
		t.Fatal(err)
	}
	cargo := `#!/bin/bash
printf '%s|%s|%s\n' "$PWD" "${CODEX_BWRAP_SHA256:-none}" "$*" >> "$TRANSCRIPT"
if [[ "$*" == *"${FAIL_PHASE:-never-match}"* ]]; then exit 31; fi
`
	for name, content := range map[string]string{"cargo": cargo, "sha256sum": "#!/bin/bash\nif [[ \"$1\" == /usr/bin/bwrap ]]; then echo \"${BWRAP_DIGEST:-host-digest}  /usr/bin/bwrap\"; else exec /usr/bin/sha256sum \"$@\"; fi\n"} {
		if err := os.WriteFile(filepath.Join(root, "bin", name), []byte(content), 0700); err != nil {
			t.Fatal(err)
		}
	}
	return root, script
}

func TestCLICacheCompileRecipe(t *testing.T) {
	normal := []string{"build --locked", "test --locked --no-run", "build --locked -p tetral --features test-seams --target-dir target/test-seams"}
	seams := normal[2]
	digest := "build --locked -p tetral --features test-seams --target-dir target/test-seams-bwrap-digest"
	release := "build --locked --release -p tetral --target-dir target/release-seams"
	for _, tc := range []struct {
		profile  string
		commands []string
	}{
		{"common", normal}, {"functional", append(append([]string{}, normal...), digest)},
		{"release", []string{"test --locked -p tetral-tests --test release_seams --no-run", seams, release}},
		{"task-capacity", []string{"test --locked -p tetral-tests --test tool_admission --no-run", seams}},
		{"digest", []string{digest}}, {"release-only", []string{release}},
		{"all", append(append([]string{}, normal...), digest, release)},
	} {
		t.Run(tc.profile, func(t *testing.T) {
			root, script := cliCacheScriptFixture(t)
			transcript := filepath.Join(root, "commands")
			command := exec.Command("bash", script, "build", tc.profile, "cold", filepath.Join(root, "evidence"))
			command.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+":"+os.Getenv("PATH"), "TRANSCRIPT="+transcript, "CODEX_BWRAP_SHA256=ambient-wrong-digest")
			if out, err := command.CombinedOutput(); err != nil {
				t.Fatalf("%v: %s", err, out)
			}
			body, err := os.ReadFile(transcript)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(body)), "\n")
			if len(lines) != len(tc.commands) {
				t.Fatalf("got commands %q", lines)
			}
			for i, args := range tc.commands {
				digestEnv := "none"
				if args == digest {
					digestEnv = "host-digest"
				}
				want := filepath.Join(root, "cli") + "|" + digestEnv + "|" + args
				if lines[i] != want {
					t.Fatalf("got %q, want %q", lines[i], want)
				}
			}
		})
	}
}

func TestCLICacheCompileFailureEvidence(t *testing.T) {
	root, script := cliCacheScriptFixture(t)
	command := exec.Command("bash", script, "build", "common", "cold", filepath.Join(root, "evidence"))
	command.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+":"+os.Getenv("PATH"), "TRANSCRIPT="+filepath.Join(root, "commands"), "FAIL_PHASE=test --locked")
	if err := command.Run(); err == nil {
		t.Fatal("build failure was accepted")
	}
	body, err := os.ReadFile(filepath.Join(root, "evidence/build-phases.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "\t31\n") || strings.Contains(string(body), "\tseams\t") {
		t.Fatalf("failure evidence = %s", body)
	}
}

func TestCLICacheComponentPaths(t *testing.T) {
	root := testRepositoryRoot(t)
	for component, want := range map[string]string{
		"common":  "~/.cargo/registry/index\n~/.cargo/registry/cache\n~/.cargo/registry/src\n~/.cargo/git/db\n~/.cargo/git/checkouts\ncli/target/debug\ncli/target/test-seams\n!cli/target/debug/incremental\n!cli/target/test-seams/debug/incremental\n",
		"digest":  "cli/target/test-seams-bwrap-digest/debug\n!cli/target/test-seams-bwrap-digest/debug/incremental\n",
		"release": "cli/target/release-seams/release\n!cli/target/release-seams/release/incremental\n",
	} {
		t.Run(component, func(t *testing.T) {
			out, err := exec.Command("bash", filepath.Join(root, "scripts/cli-build-cache.sh"), "paths", component).CombinedOutput()
			if err != nil || string(out) != want {
				t.Fatalf("got %q (%v), want %q", out, err, want)
			}
		})
	}
}

func TestCLICacheIdentityAndDiskFallback(t *testing.T) {
	root, script := cliCacheScriptFixture(t)
	for name, body := range map[string]string{
		"git":   "#!/bin/bash\ncase \"$3\" in ls-files) printf 'cli/Cargo.toml\\0cli/Cargo.lock\\0';; rev-parse) echo \"${TEST_REVISION:-commit-a}\";; *) exit 2;; esac\n",
		"rustc": "#!/bin/bash\necho rustc-pinned\n", "go": "#!/bin/bash\necho go-pinned\n", "cc": "#!/bin/bash\necho \"${COMPILER_ID:-compiler-a}\"\n",
		"df": "#!/bin/bash\nprintf 'Filesystem 1-blocks Used Available Capacity Mounted\\n/dev/fixture 999 1 %s 1%% /\\n' \"${FREE_BYTES:-999999999999}\"\n",
	} {
		if err := os.WriteFile(filepath.Join(root, "bin", name), []byte(body), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for name, body := range map[string]string{"Cargo.toml": "[workspace]\n", "Cargo.lock": "version = 4\n"} {
		if err := os.WriteFile(filepath.Join(root, "cli", name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	prepare := func(profile string, env ...string) map[string]string {
		t.Helper()
		output := filepath.Join(t.TempDir(), "outputs")
		command := exec.Command("bash", script, "prepare", profile, filepath.Join(root, "evidence"))
		command.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+":"+os.Getenv("PATH"), "TRANSCRIPT="+filepath.Join(root, "commands"), "GITHUB_OUTPUT="+output)
		command.Env = append(command.Env, env...)
		if out, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		body, err := os.ReadFile(output)
		if err != nil {
			t.Fatal(err)
		}
		result := map[string]string{}
		for _, line := range strings.Split(string(body), "\n") {
			key, value, ok := strings.Cut(line, "=")
			if ok {
				result[key] = value
			}
		}
		return result
	}
	a := prepare("functional")
	if a["variant"] != "digest" || a["restore-allowed"] != "true" {
		t.Fatalf("unexpected preparation: %v", a)
	}
	b := prepare("release")
	if b["variant"] != "release" || a["common-key"] != b["common-key"] || a["release-key"] != b["release-key"] {
		t.Fatalf("profile changed compatible keys: %v %v", a, b)
	}
	b = prepare("task-capacity", "FREE_BYTES=1")
	if b["variant"] != "none" || b["restore-allowed"] != "false" {
		t.Fatalf("low disk must skip restoration: %v", b)
	}
	b = prepare("task-capacity", "FREE_BYTES=unknown")
	if b["restore-allowed"] != "false" {
		t.Fatal("unmeasurable disk must skip restoration")
	}
	b = prepare("functional", "TEST_REVISION=commit-b")
	if a["common-key"] == b["common-key"] || a["common-prefix"] != b["common-prefix"] {
		t.Fatal("revision must refresh the key within its compatible prefix")
	}
	b = prepare("functional", "BWRAP_DIGEST=new-host-digest")
	if a["digest-prefix"] == b["digest-prefix"] || a["common-key"] != b["common-key"] || a["release-key"] != b["release-key"] {
		t.Fatal("bubblewrap changes must isolate the digest fixture identity")
	}
	b = prepare("functional", "COMPILER_ID=compiler-b")
	for _, component := range []string{"common", "digest", "release"} {
		if a[component+"-prefix"] == b[component+"-prefix"] {
			t.Fatalf("%s must invalidate on compiler changes", component)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "cli/Cargo.lock"), []byte("version = 5\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b = prepare("functional")
	for _, component := range []string{"common", "digest", "release"} {
		if a[component+"-prefix"] == b[component+"-prefix"] {
			t.Fatalf("%s must invalidate on dependency changes", component)
		}
	}
}
