package testinfra

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestFastPlanStartsNoDurableDependency(t *testing.T) {
	root := repositoryRootForTest(t)
	plan, err := BuildPlan(root, ProfileFast, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Dependencies) != 0 {
		t.Fatalf("Fast dependencies = %v; want none", plan.Dependencies)
	}
	calls := map[string]int{}
	starters := dependencyStarters{
		postgresql: func(context.Context, *dependencyManager) error { calls["postgresql"]++; return nil },
		minio:      func(context.Context, *dependencyManager) error { calls["minio"]++; return nil },
		nats:       func(context.Context, *dependencyManager) error { calls["nats"]++; return nil },
		docker:     func(context.Context) error { calls["docker"]++; return nil },
	}
	manager, err := startDependenciesWith(context.Background(), plan.Dependencies, nil, starters)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 || len(manager.containers) != 0 || len(manager.evidence) != 0 {
		t.Fatalf("Fast dependency startup had side effects: calls=%v containers=%v evidence=%v", calls, manager.containers, manager.evidence)
	}
}

func TestSetupCommandErrorEndsWithItsOutputTail(t *testing.T) {
	longLine := "registry " + strings.Repeat("x", 1000)
	failed := []string{"exit status 3"}
	for line := 33; line < 50; line++ {
		failed = append(failed, fmt.Sprintf("  | output %d", line))
	}
	failed = append(failed, "  | "+longLine[:300]+" ...", "  | unreachable", "  | ::error::forged")
	for _, tc := range []struct {
		name, script string
		want         []string
	}{
		{
			name:   "a failing command adds its last 20 lines, cut and safe for the job log, as it exits while a descendant still holds its output",
			script: `(sleep 5; echo late) & line=10; while [ $line -lt 50 ]; do echo "output $line"; line=$((line + 1)); done; echo "$1"; printf 'unreachable\r::error::forged\a\n' >&2; exit 3`,
			want:   failed,
		},
		{
			name:   "a successful command adds nothing",
			script: `echo progress; echo warning >&2`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			if err := runQuietInDir(context.Background(), t.TempDir(), "sh", "-c", tc.script, "sh", longLine); err != nil {
				got = strings.Split(err.Error(), "\n")
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("error lines:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(tc.want, "\n"))
			}
		})
	}
}

func TestNextRunnerRemovesDependencyContainerOwnedByDeadProcess(t *testing.T) {
	if os.Getenv("TETRAL_TEST_DOCKER_AVAILABLE") == "" {
		t.Skip("requires runner-owned Docker dependency")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name, err := dependencyContainerName("orphan-proof")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = removeContainer(context.Background(), name) }()
	if err := runQuiet(ctx, "docker", "run", "-d", "--rm", "--name", name,
		"--label", "tetral.test.owner=testinfra",
		"--label", "tetral.test.owner-pid=999999999",
		"--label", "tetral.test.owner-start=dead",
		postgresImage, "sh", "-c", "sleep 300"); err != nil {
		t.Fatal(err)
	}
	// Full runs independent package processes. Their orphan collectors may
	// observe the same dead owner and overlap Docker's in-progress removal.
	start := make(chan struct{})
	results := make(chan error, 8)
	for range cap(results) {
		go func() {
			<-start
			results <- cleanupOrphanedDependencyContainers(ctx)
		}()
	}
	close(start)
	for range cap(results) {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	output, err := commandOutput(ctx, "docker", "ps", "-aq", "--filter", "name="+name)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output) != "" {
		t.Fatal("dead runner's dependency container remained")
	}
}

// The injected starters create real run-owned containers and wait on each
// other's progress, so a Keycloak or egctl start that ran in the serial order
// would block until the deadline instead of passing.
func TestDependencyStartupRunsKeycloakAndEGCTLInBackground(t *testing.T) {
	if os.Getenv("TETRAL_TEST_DOCKER_AVAILABLE") == "" {
		t.Skip("requires runner-owned Docker dependency")
	}
	type proof struct {
		kind string
		// serialRunning closes once the first serial starter has done its part.
		serialRunning     chan struct{}
		backgroundStarted chan struct{}
		backgroundDone    chan struct{}
		natsCalls         atomic.Int32
		observed          sync.Map
		// stopCaller cancels the context the caller passed to startup.
		stopCaller context.CancelCauseFunc
	}
	wait := func(ctx context.Context, channel <-chan struct{}) error {
		select {
		case <-channel:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	startContainer := func(ctx context.Context, m *dependencyManager, p *proof, dependency string) error {
		name, err := dependencyContainerName(p.kind)
		if err != nil {
			return err
		}
		arguments := append([]string{"run", "-d", "--rm", "--name", name}, dependencyContainerLabels(p.kind, m.runID)...)
		arguments = append(arguments, postgresImage, "sh", "-c", "sleep 300")
		return m.publishContainer(ctx, dependency, name, postgresImage, nil, func(map[int]int) []string { return arguments }, nil)
	}
	startRecorded := func(ctx context.Context, m *dependencyManager, p *proof, dependency string) error {
		if err := startContainer(ctx, m, p, dependency); err != nil {
			return err
		}
		m.environment = append(m.environment, "PROOF_"+strings.ToUpper(dependency)+"=1")
		m.evidence = append(m.evidence, DependencyEvidence{Name: dependency, Source: "proof"})
		return nil
	}
	// envoy and nats start in order and keycloak and egctl in the background;
	// nats, when reached, starts a container.
	type behaviors struct {
		envoy, keycloak, egctl func(context.Context, *dependencyManager, *proof) error
	}
	injected := errors.New("injected starter failure")
	callerStopped := errors.New("caller stopped")
	tests := []struct {
		name      string
		behaviors behaviors
		wantErr   error
		// containers counts the proof's containers once startup returned.
		containers int
		check      func(*testing.T, *dependencyManager, *proof)
	}{
		{
			name: "keycloak and egctl start beside the serial starters, whose live containers a sibling's orphan cleanup keeps",
			behaviors: behaviors{
				envoy: func(ctx context.Context, m *dependencyManager, p *proof) error {
					if err := startRecorded(ctx, m, p, "envoy"); err != nil {
						return err
					}
					close(p.serialRunning)
					for range 2 {
						if err := wait(ctx, p.backgroundStarted); err != nil {
							return err
						}
					}
					return nil
				},
				keycloak: func(ctx context.Context, m *dependencyManager, p *proof) error {
					if err := wait(ctx, p.serialRunning); err != nil {
						return err
					}
					if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
						return err
					}
					return startRecorded(ctx, m, p, "keycloak")
				},
				egctl: func(ctx context.Context, m *dependencyManager, p *proof) error {
					if err := wait(ctx, p.serialRunning); err != nil {
						return err
					}
					m.environment = append(m.environment, "PROOF_EGCTL=1")
					m.evidence = append(m.evidence, DependencyEvidence{Name: "egctl", Source: "proof"})
					return nil
				},
			},
			containers: 3,
			check: func(t *testing.T, manager *dependencyManager, _ *proof) {
				for _, dependency := range []string{"egctl", "envoy", "keycloak", "nats"} {
					found := slices.IndexFunc(manager.evidence, func(item DependencyEvidence) bool { return item.Name == dependency })
					if found < 0 || manager.evidence[found].SetupElapsed <= 0 {
						t.Fatalf("evidence %+v lacks %s with its setup time", manager.evidence, dependency)
					}
					if !slices.Contains(manager.environment, "PROOF_"+strings.ToUpper(dependency)+"=1") {
						t.Fatalf("environment %v lacks the variable %s set", manager.environment, dependency)
					}
				}
				if inherited := slices.DeleteFunc(slices.Clone(manager.environment), func(variable string) bool { return variable != "INHERITED=1" }); len(inherited) != 1 {
					t.Fatalf("environment %v holds the inherited variable %d times; want once", manager.environment, len(inherited))
				}
			},
		},
		{
			name: "a failing serial starter cancels the background ones and removes what they start after it",
			behaviors: behaviors{
				envoy: func(ctx context.Context, m *dependencyManager, p *proof) error {
					close(p.serialRunning)
					if err := wait(ctx, p.backgroundStarted); err != nil {
						return err
					}
					return injected
				},
				keycloak: func(ctx context.Context, m *dependencyManager, p *proof) error {
					<-ctx.Done()
					p.observed.Store("keycloak", ctx.Err())
					// The daemon can still create a container whose dispatch began before the cancellation.
					if err := startContainer(context.WithoutCancel(ctx), m, p, "keycloak"); err != nil {
						return err
					}
					return ctx.Err()
				},
				egctl: func(ctx context.Context, _ *dependencyManager, _ *proof) error {
					<-ctx.Done()
					return ctx.Err()
				},
			},
			wantErr: injected,
			check: func(t *testing.T, _ *dependencyManager, p *proof) {
				if observed, _ := p.observed.Load("keycloak"); observed != context.Canceled {
					t.Fatalf("keycloak starter observed %v; want the cancellation", observed)
				}
			},
		},
		{
			name: "a failing background starter cancels the serial one and removes its container",
			behaviors: behaviors{
				envoy: func(ctx context.Context, m *dependencyManager, p *proof) error {
					if err := startContainer(ctx, m, p, "envoy"); err != nil {
						return err
					}
					close(p.serialRunning)
					<-ctx.Done()
					p.observed.Store("envoy", ctx.Err())
					return ctx.Err()
				},
				keycloak: func(ctx context.Context, _ *dependencyManager, p *proof) error {
					if err := wait(ctx, p.serialRunning); err != nil {
						return err
					}
					return injected
				},
				egctl: func(ctx context.Context, _ *dependencyManager, _ *proof) error {
					<-ctx.Done()
					return ctx.Err()
				},
			},
			wantErr: injected,
			check: func(t *testing.T, _ *dependencyManager, p *proof) {
				if observed, _ := p.observed.Load("envoy"); observed != context.Canceled {
					t.Fatalf("serial starter observed %v; want the cancellation", observed)
				}
				if calls := p.natsCalls.Load(); calls != 0 {
					t.Fatalf("NATS started %d times after the failure", calls)
				}
			},
		},
		{
			name: "a caller that stops startup is reported as the cause, not a starter's symptom",
			behaviors: behaviors{
				envoy: func(ctx context.Context, _ *dependencyManager, p *proof) error {
					close(p.serialRunning)
					p.stopCaller(callerStopped)
					<-ctx.Done()
					return ctx.Err()
				},
				keycloak: func(ctx context.Context, _ *dependencyManager, _ *proof) error {
					<-ctx.Done()
					return errors.New("keycloak did not become ready")
				},
				egctl: func(ctx context.Context, _ *dependencyManager, _ *proof) error {
					<-ctx.Done()
					return ctx.Err()
				},
			},
			wantErr: callerStopped,
			check:   func(*testing.T, *dependencyManager, *proof) {},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			suffix, err := randomIdentity(4)
			if err != nil {
				t.Fatal(err)
			}
			p := &proof{kind: "startup-proof-" + suffix, serialRunning: make(chan struct{}), backgroundStarted: make(chan struct{}, 2), backgroundDone: make(chan struct{}, 2)}
			containers := func() []string {
				t.Helper()
				output, err := commandOutput(context.Background(), "docker", "ps", "-aq", "--filter", "label=tetral.test.kind="+p.kind)
				if err != nil {
					t.Fatal(err)
				}
				return strings.Fields(output)
			}
			t.Cleanup(func() {
				for _, container := range containers() {
					_ = removeContainer(context.Background(), container)
				}
			})
			tracked := func(behavior func(context.Context, *dependencyManager, *proof) error, background bool) func(context.Context, *dependencyManager) error {
				return func(ctx context.Context, m *dependencyManager) error {
					if background {
						defer func() { p.backgroundDone <- struct{}{} }()
						p.backgroundStarted <- struct{}{}
					}
					return behavior(ctx, m, p)
				}
			}
			starters := dependencyStarters{
				keycloak: tracked(tc.behaviors.keycloak, true),
				tool:     tracked(tc.behaviors.egctl, true),
				image: func(ctx context.Context, m *dependencyManager, _ string) error {
					return tracked(tc.behaviors.envoy, false)(ctx, m)
				},
				nats: tracked(func(ctx context.Context, m *dependencyManager, p *proof) error {
					p.natsCalls.Add(1)
					return startRecorded(ctx, m, p, "nats")
				}, false),
			}
			deadline, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			ctx, stop := context.WithCancelCause(deadline)
			defer stop(nil)
			p.stopCaller = stop
			manager, err := startDependenciesWith(ctx, []string{"egctl", "envoy", "keycloak", "nats"}, []string{"INHERITED=1"}, starters)
			// Both background starters run in every row; one that outlived
			// startup would show what it leaves behind.
			for range 2 {
				_ = wait(deadline, p.backgroundDone)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("startup error = %v; want %v", err, tc.wantErr)
			}
			if got := len(containers()); got != tc.containers {
				t.Fatalf("%d proof containers exist after startup returned; want %d", got, tc.containers)
			}
			tc.check(t, manager, p)
			if manager != nil {
				if err := manager.stopBounded(); err != nil {
					t.Fatal(err)
				}
				if remaining := containers(); len(remaining) != 0 {
					t.Fatalf("teardown left containers %v", remaining)
				}
			}
		})
	}
}

func TestRunnerOwnedPostgreSQLIsHostReadyWithoutDurability(t *testing.T) {
	if os.Getenv("TETRAL_TEST_DOCKER_AVAILABLE") == "" {
		t.Skip("requires runner-owned Docker dependency")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	manager := &dependencyManager{environment: os.Environ()}
	if err := manager.startPostgreSQL(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.stopBounded(); err != nil {
			t.Errorf("stop PostgreSQL dependency: %v", err)
		}
	}()
	if err := verifyPostgreSQLConnection(ctx, manager.postgresDSN); err != nil {
		t.Fatalf("PostgreSQL dependency was not reachable through its published DSN: %v", err)
	}
	connection, err := pgx.Connect(ctx, manager.postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	want := map[string]string{"max_connections": "300", "fsync": "off", "synchronous_commit": "off", "full_page_writes": "off"}
	recorded := map[string]string{}
	for _, setting := range strings.Split(manager.evidence[0].ConfigurationIdentity, ",") {
		name, value, _ := strings.Cut(setting, "=")
		recorded[name] = value
	}
	if !maps.Equal(recorded, want) {
		t.Fatalf("evidence records settings %v; want %v", recorded, want)
	}
	for name, value := range want {
		var actual string
		if err := connection.QueryRow(ctx, "SHOW "+name).Scan(&actual); err != nil {
			t.Fatal(err)
		}
		if actual != value {
			t.Fatalf("server runs %s = %s; want %s", name, actual, value)
		}
	}
}

func TestSDKInstallUsesPinnedPackageManager(t *testing.T) {
	for _, manager := range []string{"pnpm@10.34.5", "pnpm@11.4.0+sha512.fixture"} {
		t.Run(manager, func(t *testing.T) {
			directory := t.TempDir()
			binary := filepath.Join(directory, "bun")
			if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > arguments\n"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "package.json"), []byte(fmt.Sprintf(`{"packageManager":%q}`, manager)), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
			if err := installSDKDependencies(t.Context(), directory); err != nil {
				t.Fatal(err)
			}
			actual, err := os.ReadFile(filepath.Join(directory, "arguments"))
			if err != nil {
				t.Fatal(err)
			}
			version, _, _ := strings.Cut(manager, "+")
			want := "x\n" + version + "\ninstall\n--frozen-lockfile\n--ignore-scripts\n"
			if string(actual) != want {
				t.Fatalf("installer arguments = %q; want %q", actual, want)
			}
		})
	}
}
