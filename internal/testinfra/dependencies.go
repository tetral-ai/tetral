package testinfra

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

const (
	postgresImage         = "ghcr.io/tetral-ai/mirror/postgres:18-alpine"
	minioImage            = "ghcr.io/tetral-ai/mirror/minio:RELEASE.2025-09-07T16-13-09Z"
	forkSDKCommit         = "e69a6e78357047ecc6224aa54cc8b3e448dcf9e7"
	dependencyStopTimeout = 30 * time.Second
)

// runnerPostgreSQLSettings configure the runner's disposable database. No test
// restarts it or reads its write-ahead log, so it runs without durability and
// the checkpoint every clone drop requests no longer waits on fsync.
var runnerPostgreSQLSettings = []string{"max_connections=300", "fsync=off", "synchronous_commit=off", "full_page_writes=off"}

var dockerPublishing sync.Mutex

type dependencyManager struct {
	environment           []string
	containers            []string
	containerDependencies []string
	directories           []string
	directoryDependencies []string
	evidence              []DependencyEvidence
	postgresDSN           string
	runID                 string
	root                  string
}

type dependencyStarters struct {
	postgresql func(context.Context, *dependencyManager) error
	minio      func(context.Context, *dependencyManager) error
	nats       func(context.Context, *dependencyManager) error
	keycloak   func(context.Context, *dependencyManager) error
	docker     func(context.Context) error
	sdk        func(context.Context, *dependencyManager) error
	image      func(context.Context, *dependencyManager, string) error
	tool       func(context.Context, *dependencyManager) error
}

var productionDependencyStarters = dependencyStarters{
	postgresql: func(ctx context.Context, manager *dependencyManager) error { return manager.startPostgreSQL(ctx) },
	minio:      func(ctx context.Context, manager *dependencyManager) error { return manager.startMinIO(ctx) },
	nats:       func(ctx context.Context, manager *dependencyManager) error { return manager.startNATS(ctx) },
	keycloak:   func(ctx context.Context, manager *dependencyManager) error { return manager.startKeycloak(ctx) },
	docker:     dockerAvailable,
	sdk:        func(ctx context.Context, manager *dependencyManager) error { return manager.startSDK(ctx) },
	tool:       func(ctx context.Context, manager *dependencyManager) error { return manager.prepareEGCTL(ctx) },
	image: func(ctx context.Context, manager *dependencyManager, name string) error {
		return manager.preparePinnedImage(ctx, name)
	},
}

func (m *dependencyManager) environmentForProcess() ([]string, string, error) {
	environment := withoutEnvironmentVariable(m.environment, storagetest.EnvTestRunID)
	if m.postgresDSN == "" {
		return environment, "", nil
	}
	runID, err := randomIdentity(16)
	if err != nil {
		return nil, "", err
	}
	environment = append(environment, storagetest.EnvTestRunID+"="+runID)
	return environment, runID, nil
}

func withoutEnvironmentVariable(environment []string, name string) []string {
	prefix := name + "="
	result := make([]string, 0, len(environment))
	for _, item := range environment {
		if !strings.HasPrefix(item, prefix) {
			result = append(result, item)
		}
	}
	return result
}

func (m *dependencyManager) closeProcessRun(runID string) error {
	if m.postgresDSN == "" || runID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return storagetest.CloseRun(ctx, m.postgresDSN, runID)
}

func startDependencies(ctx context.Context, dependencies []string, root string) (*dependencyManager, error) {
	return startDependenciesWithRoot(ctx, dependencies, os.Environ(), productionDependencyStarters, root)
}

func startDependenciesWith(ctx context.Context, dependencies, environment []string, starters dependencyStarters) (*dependencyManager, error) {
	return startDependenciesWithRoot(ctx, dependencies, environment, starters, "")
}

func startDependenciesWithRoot(ctx context.Context, dependencies, environment []string, starters dependencyStarters, root string) (*dependencyManager, error) {
	manager := &dependencyManager{environment: environment, root: root}
	// Keycloak and egctl mostly wait, on a JVM start or on a download and a
	// helper build, so each starts in the background on its own manager. That
	// manager shares no state with the serial starts and is adopted only after
	// every starter has returned.
	backgroundStarters := map[string]func(context.Context, *dependencyManager) error{"keycloak": starters.keycloak, "egctl": starters.tool}
	caller := ctx
	ctx, cancel := context.WithCancel(caller)
	defer cancel()
	var failure error
	var failed sync.Once
	fail := func(err error) {
		failed.Do(func() {
			failure = err
			cancel()
		})
	}
	inherited := slices.Clone(environment)
	var background []*dependencyManager
	var starting sync.WaitGroup
	for _, dependency := range dependencies {
		start, ok := backgroundStarters[dependency]
		if !ok {
			continue
		}
		if start == nil {
			fail(fmt.Errorf("%s dependency starter is unavailable", dependency))
			break
		}
		if err := manager.ensureRunID(); err != nil {
			fail(err)
			break
		}
		starter := &dependencyManager{environment: slices.Clone(inherited), root: root, runID: manager.runID}
		background = append(background, starter)
		starting.Go(func() {
			started := time.Now()
			if err := start(ctx, starter); err != nil {
				fail(err)
				return
			}
			starter.recordDependencySetup(dependency, time.Since(started))
		})
	}
	for _, dependency := range dependencies {
		if _, ok := backgroundStarters[dependency]; ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			fail(err)
			break
		}
		if err := manager.startSerialDependency(ctx, dependency, starters); err != nil {
			fail(err)
			break
		}
	}
	starting.Wait()
	for _, starter := range background {
		manager.adopt(starter, inherited)
	}
	if failure != nil {
		_ = manager.stopBounded()
		// A starter stopped by the caller reports only its own symptom.
		if cause := context.Cause(caller); cause != nil && !errors.Is(failure, cause) {
			failure = fmt.Errorf("%w: %v", cause, failure)
		}
		return nil, failure
	}
	return manager, nil
}

func (m *dependencyManager) startSerialDependency(ctx context.Context, dependency string, starters dependencyStarters) error {
	started := time.Now()
	switch dependency {
	case "nats":
		if starters.nats == nil {
			return fmt.Errorf("NATS dependency starter is unavailable")
		}
		if err := starters.nats(ctx, m); err != nil {
			return err
		}
	case "postgresql":
		if dsn := os.Getenv(storagetest.EnvTestDatabaseURL); dsn == "" {
			if err := starters.postgresql(ctx, m); err != nil {
				return err
			}
		} else if err := m.recordExternalPostgreSQL(ctx, dsn); err != nil {
			return err
		}
	case "minio":
		if os.Getenv("TETRAL_TEST_MINIO_ENDPOINT") == "" {
			if err := starters.minio(ctx, m); err != nil {
				return err
			}
		} else {
			m.evidence = append(m.evidence, DependencyEvidence{Name: "minio", Source: "external", Identity: "external-minio"})
		}
	case "docker":
		if err := starters.docker(ctx); err != nil {
			return err
		}
		m.environment = append(withoutEnvironmentVariable(m.environment, "TETRAL_TEST_DOCKER_AVAILABLE"), "TETRAL_TEST_DOCKER_AVAILABLE=1")
		m.evidence = append(m.evidence, DependencyEvidence{Name: "docker", Source: "host-daemon", Identity: "available"})
	case "sdk":
		if err := starters.sdk(ctx, m); err != nil {
			return err
		}
	case "envoy", "edge-envoy", "bun-image":
		if starters.image == nil {
			return fmt.Errorf("pinned image dependency starter is unavailable")
		}
		if err := starters.image(ctx, m, dependency); err != nil {
			return err
		}
	case "bun-workspaces":
		if m.root == "" {
			return fmt.Errorf("bun workspace dependency requires a repository root")
		}
		identity, err := installBunWorkspaces(ctx, m.root)
		if err != nil {
			return err
		}
		m.evidence = append(m.evidence, DependencyEvidence{Name: "bun-workspaces", Source: "repository-lockfiles", Identity: identity})
	default:
		return fmt.Errorf("unknown test dependency %q", dependency)
	}
	m.recordDependencySetup(dependency, time.Since(started))
	if dependency == "postgresql" {
		templateStarted := time.Now()
		digest, err := storagetest.PrepareTemplate(ctx, m.postgresDSN)
		if err != nil {
			return fmt.Errorf("prepare PostgreSQL test template: %w", err)
		}
		for index := range m.evidence {
			if m.evidence[index].Name == dependency {
				m.evidence[index].TemplateIdentity = digest
				m.evidence[index].TemplateSetupElapsed = time.Since(templateStarted)
			}
		}
	}
	return nil
}

// adopt takes over what a background starter left once it has returned: its
// containers and directories for teardown, its evidence, and the environment
// variables it set over the inherited ones.
func (m *dependencyManager) adopt(starter *dependencyManager, inherited []string) {
	m.containers = append(m.containers, starter.containers...)
	m.containerDependencies = append(m.containerDependencies, starter.containerDependencies...)
	m.directories = append(m.directories, starter.directories...)
	m.directoryDependencies = append(m.directoryDependencies, starter.directoryDependencies...)
	m.evidence = append(m.evidence, starter.evidence...)
	for _, variable := range starter.environment {
		if slices.Contains(inherited, variable) {
			continue
		}
		name, _, _ := strings.Cut(variable, "=")
		m.environment = append(withoutEnvironmentVariable(m.environment, name), variable)
	}
}

func (m *dependencyManager) recordDependencySetup(name string, elapsed time.Duration) {
	for index := range m.evidence {
		if m.evidence[index].Name == name {
			m.evidence[index].SetupElapsed = elapsed
		}
	}
}

func installBunWorkspaces(ctx context.Context, root string) (string, error) {
	hash := sha256.New()
	for _, directory := range []string{"services/agent-runtime", "services/gateway"} {
		// Both directories are fixed repository-owned workspace roots.
		//nolint:gosec
		lockfile, err := os.ReadFile(filepath.Join(root, directory, "bun.lock"))
		if err != nil {
			return "", fmt.Errorf("read %s lockfile: %w", directory, err)
		}
		_, _ = hash.Write([]byte(directory + "\x00"))
		_, _ = hash.Write(lockfile)
		if err := runQuietInDir(ctx, filepath.Join(root, directory), "bun", "install", "--frozen-lockfile"); err != nil {
			return "", fmt.Errorf("install %s dependencies: %w", directory, err)
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (m *dependencyManager) startSDK(ctx context.Context) error {
	for _, executable := range []string{"bun", "node"} {
		if _, err := exec.LookPath(executable); err != nil {
			return fmt.Errorf("SDK tests require %s: %w", executable, err)
		}
	}
	candidates := []string{strings.TrimSpace(os.Getenv("TETRAL_ENGINE_SDK_ROOT"))}
	if m.root != "" {
		candidates = append(candidates, filepath.Join(filepath.Dir(m.root), "tetral-sdk-typescript"))
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if err := verifySDKCheckout(ctx, candidate); err == nil {
			if err := installSDKDependencies(ctx, candidate); err != nil {
				return err
			}
			m.environment = append(withoutEnvironmentVariable(m.environment, "TETRAL_ENGINE_SDK_ROOT"),
				"TETRAL_ENGINE_SDK_ROOT="+candidate,
				"TETRAL_RUN_GO_BUN_GRPC_INTEROP=1",
			)
			m.evidence = append(m.evidence, DependencyEvidence{Name: "sdk", Source: "existing-checkout", Identity: forkSDKCommit})
			return nil
		}
	}
	directory, err := os.MkdirTemp("", "tetral-sdk-")
	if err != nil {
		return err
	}
	m.directories = append(m.directories, directory)
	m.directoryDependencies = append(m.directoryDependencies, "sdk")
	commands := [][]string{
		{"git", "init", "-q", directory},
		{"git", "-C", directory, "remote", "add", "origin", "https://github.com/tetral-ai/tetral-sdk-typescript.git"},
		{"git", "-C", directory, "fetch", "--depth=1", "origin", forkSDKCommit},
		{"git", "-C", directory, "checkout", "--detach", "-q", "FETCH_HEAD"},
	}
	for _, arguments := range commands {
		if err := runQuiet(ctx, arguments[0], arguments[1:]...); err != nil {
			return fmt.Errorf("prepare pinned SDK evidence: %w", err)
		}
	}
	if err := installSDKDependencies(ctx, directory); err != nil {
		return err
	}
	if err := verifySDKCheckout(ctx, directory); err != nil {
		return err
	}
	m.environment = append(withoutEnvironmentVariable(m.environment, "TETRAL_ENGINE_SDK_ROOT"),
		"TETRAL_ENGINE_SDK_ROOT="+directory,
		"TETRAL_RUN_GO_BUN_GRPC_INTEROP=1",
	)
	m.evidence = append(m.evidence, DependencyEvidence{Name: "sdk", Source: "pinned-public-checkout", Identity: forkSDKCommit})
	return nil
}

func installSDKDependencies(ctx context.Context, directory string) error {
	//nolint:gosec // G304: directory is the runner-selected SDK checkout; package.json is a fixed filename.
	contents, err := os.ReadFile(filepath.Join(directory, "package.json"))
	if err != nil {
		return fmt.Errorf("read SDK package manager: %w", err)
	}
	var pkg struct {
		PackageManager string `json:"packageManager"`
	}
	if err := json.Unmarshal(contents, &pkg); err != nil {
		return fmt.Errorf("decode SDK package manager: %w", err)
	}
	manager, _, _ := strings.Cut(pkg.PackageManager, "+")
	if !strings.HasPrefix(manager, "pnpm@") || strings.TrimPrefix(manager, "pnpm@") == "" {
		return fmt.Errorf("SDK packageManager must specify a pnpm version")
	}
	if err := runQuietInDir(ctx, directory, "bun", "x", manager, "install", "--frozen-lockfile", "--ignore-scripts"); err != nil {
		return fmt.Errorf("install pinned SDK dependencies: %w", err)
	}
	return nil
}

func verifySDKCheckout(ctx context.Context, directory string) error {
	head, err := commandOutput(ctx, "git", "-C", directory, "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(head) != forkSDKCommit {
		return fmt.Errorf("SDK checkout is not the pinned revision")
	}
	status, err := commandOutput(ctx, "git", "-C", directory, "status", "--porcelain")
	if err != nil || strings.TrimSpace(status) != "" {
		return fmt.Errorf("SDK checkout is not clean")
	}
	return nil
}

func (m *dependencyManager) startPostgreSQL(ctx context.Context) error {
	if err := dockerAvailable(ctx); err != nil {
		return err
	}
	if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
		return err
	}
	if err := m.ensureRunID(); err != nil {
		return err
	}
	name, err := dependencyContainerName("postgres")
	if err != nil {
		return err
	}
	labels := dependencyContainerLabels("postgres", m.runID)
	arguments := []string{"run", "-d", "--rm", "--name", name}
	arguments = append(arguments, labels...)
	arguments = append(arguments,
		"-p", "127.0.0.1::5432", "-e", "POSTGRES_USER=tetral", "-e", "POSTGRES_PASSWORD=tetral",
		"-e", "POSTGRES_DB=tetral", postgresImage)
	for _, setting := range runnerPostgreSQLSettings {
		arguments = append(arguments, "-c", setting)
	}
	if err := m.publishContainer(ctx, "postgresql", name, postgresImage, nil, func(map[int]int) []string { return arguments }, nil); err != nil {
		return fmt.Errorf("start PostgreSQL dependency: %w", err)
	}
	for attempt := 0; attempt < 60; attempt++ {
		if runQuiet(ctx, "docker", "exec", name, "pg_isready", "-U", "tetral", "-d", "tetral") == nil {
			port, err := dockerPort(ctx, name, "5432/tcp")
			if err != nil {
				return err
			}
			dsn := "postgres://tetral:tetral@127.0.0.1:" + port + "/tetral?sslmode=disable"
			if err := verifyPostgreSQLConnection(ctx, dsn); err != nil {
				if err := sleepContext(ctx, 500*time.Millisecond); err != nil {
					return err
				}
				continue
			}
			identity, err := dockerImageDigest(ctx, postgresImage)
			if err != nil {
				return err
			}
			version, err := dockerOutput(ctx, "exec", name, "postgres", "--version")
			if err != nil {
				return err
			}
			m.postgresDSN = dsn
			m.environment = append(m.environment,
				storagetest.EnvTestDatabaseURL+"="+dsn,
				storagetest.EnvTestProvenance+"="+identity,
			)
			m.evidence = append(m.evidence, DependencyEvidence{Name: "postgresql", Source: "runner-container", Identity: identity, Version: version, ConfigurationIdentity: strings.Join(runnerPostgreSQLSettings, ","), RunID: m.runID})
			return nil
		}
		if err := sleepContext(ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("PostgreSQL dependency did not become ready")
}

func (m *dependencyManager) startMinIO(ctx context.Context) error {
	if err := dockerAvailable(ctx); err != nil {
		return err
	}
	if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
		return err
	}
	if err := m.ensureRunID(); err != nil {
		return err
	}
	name, err := dependencyContainerName("minio")
	if err != nil {
		return err
	}
	labels := dependencyContainerLabels("minio", m.runID)
	arguments := []string{"run", "-d", "--rm", "--name", name}
	arguments = append(arguments, labels...)
	arguments = append(arguments,
		"-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER=tetralminio", "-e", "MINIO_ROOT_PASSWORD=tetralminio123",
		minioImage, "server", "/data", "--address", ":9000")
	if err := m.publishContainer(ctx, "minio", name, minioImage, nil, func(map[int]int) []string { return arguments }, nil); err != nil {
		return fmt.Errorf("start MinIO dependency: %w", err)
	}
	port, err := dockerPort(ctx, name, "9000/tcp")
	if err != nil {
		return err
	}
	endpoint := "http://127.0.0.1:" + port
	client := http.Client{Timeout: time.Second}
	for attempt := 0; attempt < 60; attempt++ {
		response, requestErr := client.Get(endpoint + "/minio/health/ready")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				identity, err := dockerImageDigest(ctx, minioImage)
				if err != nil {
					return err
				}
				m.environment = append(m.environment,
					"TETRAL_TEST_MINIO_ENDPOINT="+endpoint,
					"TETRAL_TEST_MINIO_ACCESS_KEY=tetralminio",
					"TETRAL_TEST_MINIO_SECRET_KEY=tetralminio123",
					"TETRAL_TEST_MINIO_REGION=us-east-1",
				)
				m.evidence = append(m.evidence, DependencyEvidence{Name: "minio", Source: "runner-container", Identity: identity})
				return nil
			}
		}
		if err := sleepContext(ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("MinIO dependency did not become ready")
}

// publishContainer starts a run-owned container of image that publishes
// loopback ports. A reserved port is free from its release until Docker binds
// it, and Docker picks every 127.0.0.1::port binding from the same range, so
// each such start in this process runs from reservation to its verified
// binding under one lock. An absent image is pulled before the lock is taken,
// so no start waits on another's registry download. The name is registered
// before dispatch because the daemon can create the container even when the
// CLI is canceled first.
func (m *dependencyManager) publishContainer(ctx context.Context, dependency, name, image string, reserve []int, arguments func(bindings map[int]int) []string, verify func(bindings map[int]int) error) error {
	if runQuiet(ctx, "docker", "image", "inspect", image) != nil {
		if err := runQuiet(ctx, "docker", "pull", image); err != nil {
			return fmt.Errorf("pull %s: %w", image, err)
		}
	}
	dockerPublishing.Lock()
	defer dockerPublishing.Unlock()
	bindings, release, err := ReserveLoopbackPorts(reserve...)
	if err != nil {
		return err
	}
	command := arguments(bindings)
	m.containers = append(m.containers, name)
	m.containerDependencies = append(m.containerDependencies, dependency)
	release()
	if err := runQuiet(ctx, "docker", command...); err != nil {
		return err
	}
	if verify != nil {
		return verify(bindings)
	}
	return nil
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m *dependencyManager) ensureRunID() error {
	if m.runID != "" {
		return nil
	}
	runID, err := randomIdentity(16)
	if err != nil {
		return err
	}
	m.runID = runID
	return nil
}

func dependencyContainerLabels(kind, runID string) []string {
	pid, started := currentProcessIdentity()
	return []string{
		"--label", "tetral.test.owner=testinfra",
		"--label", "tetral.test.kind=" + kind,
		"--label", "tetral.test.run-id=" + runID,
		"--label", "tetral.test.owner-pid=" + strconv.Itoa(pid),
		"--label", "tetral.test.owner-start=" + started,
	}
}

func cleanupOrphanedDependencyContainers(ctx context.Context) error {
	output, err := commandOutput(ctx, "docker", "ps", "-aq", "--filter", "label=tetral.test.owner=testinfra")
	if err != nil {
		return fmt.Errorf("list owned test dependency containers: %w", err)
	}
	for _, container := range strings.Fields(output) {
		pidText, err := commandOutput(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"tetral.test.owner-pid\"}}", container)
		if err != nil {
			if exists, checkErr := dockerResourceExists(ctx, "container", "id", container); checkErr == nil && !exists {
				continue
			}
			return fmt.Errorf("inspect owned test dependency container: %w", err)
		}
		started, err := commandOutput(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"tetral.test.owner-start\"}}", container)
		if err != nil {
			if exists, checkErr := dockerResourceExists(ctx, "container", "id", container); checkErr == nil && !exists {
				continue
			}
			return fmt.Errorf("inspect owned test dependency container: %w", err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(pidText))
		if err != nil || processIdentityAlive(pid, strings.TrimSpace(started)) {
			continue
		}
		if err := removeContainer(ctx, container); err != nil {
			if removed, checkErr := waitDockerResourceRemoved(ctx, "container", "id", container); checkErr == nil && removed {
				continue
			}
			return fmt.Errorf("remove orphaned test dependency container: %w", err)
		}
	}
	return nil
}

func (m *dependencyManager) stop(ctx context.Context) error {
	var first error
	for index := len(m.containers) - 1; index >= 0; index-- {
		started := time.Now()
		if err := removeContainer(ctx, m.containers[index]); err != nil && first == nil {
			first = err
		}
		m.recordDependencyTeardown(m.containerDependencies[index], time.Since(started))
	}
	for index := len(m.directories) - 1; index >= 0; index-- {
		started := time.Now()
		if err := os.RemoveAll(m.directories[index]); err != nil && first == nil {
			first = err
		}
		m.recordDependencyTeardown(m.directoryDependencies[index], time.Since(started))
	}
	return first
}

func (m *dependencyManager) recordDependencyTeardown(name string, elapsed time.Duration) {
	for index := range m.evidence {
		if m.evidence[index].Name == name {
			m.evidence[index].TeardownElapsed += elapsed
		}
	}
}

func (m *dependencyManager) stopBounded() error {
	ctx, cancel := context.WithTimeout(context.Background(), dependencyStopTimeout)
	defer cancel()
	return m.stop(ctx)
}

func (m *dependencyManager) recordExternalPostgreSQL(ctx context.Context, dsn string) error {
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("external PostgreSQL descriptor is malformed")
	}
	parsed.User = nil
	parsed.RawQuery = ""
	identity := "external:" + parsed.Scheme + "://" + parsed.Host + parsed.EscapedPath()
	version, err := postgreSQLVersion(ctx, dsn)
	if err != nil {
		return fmt.Errorf("external PostgreSQL verification failed")
	}
	if err := m.ensureRunID(); err != nil {
		return err
	}
	m.postgresDSN = dsn
	m.environment = append(m.environment,
		storagetest.EnvTestProvenance+"="+identity,
	)
	m.evidence = append(m.evidence, DependencyEvidence{Name: "postgresql", Source: "external", Identity: identity, Version: version, RunID: m.runID})
	return nil
}

func verifyPostgreSQLConnection(ctx context.Context, dsn string) error {
	_, err := postgreSQLVersion(ctx, dsn)
	return err
}

func postgreSQLVersion(ctx context.Context, dsn string) (string, error) {
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", err
	}
	var version string
	queryErr := connection.QueryRow(ctx, "SHOW server_version_num").Scan(&version)
	closeErr := connection.Close(ctx)
	if queryErr != nil {
		return "", queryErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return version, nil
}

func dockerAvailable(ctx context.Context) error {
	if err := runQuiet(ctx, "docker", "info", "--format", "{{.ServerVersion}}"); err != nil {
		return fmt.Errorf("selected evidence requires Docker: %w", err)
	}
	return nil
}

func dockerPort(ctx context.Context, container, target string) (string, error) {
	// Container names and ports are generated by this package, and no shell is used.
	//nolint:gosec
	command := exec.CommandContext(ctx, "docker", "port", container, target)
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("resolve dependency port: %w", err)
	}
	line := strings.TrimSpace(string(output))
	separator := strings.LastIndexByte(line, ':')
	if separator < 0 {
		return "", fmt.Errorf("dependency port output is malformed")
	}
	return line[separator+1:], nil
}

func runQuiet(ctx context.Context, name string, arguments ...string) error {
	return runQuietInDir(ctx, "", name, arguments...)
}

func runQuietInDir(ctx context.Context, directory, name string, arguments ...string) error {
	// Callers select the directory, executable, and arguments from the closed dependency inventory.
	//nolint:gosec
	command := exec.CommandContext(ctx, name, arguments...)
	command.Dir = directory
	return runWithOutputTail(command)
}

// runWithOutputTail discards a command's output unless the command fails; then
// its error ends with the output's last lines. The output goes to a file, not a
// pipe, so Wait still returns when the command exits while a descendant, such
// as the node process that bun x starts, holds the output open.
func runWithOutputTail(command *exec.Cmd) error {
	output, err := os.CreateTemp("", "tetral-command-output-")
	if err != nil {
		return err
	}
	defer func() {
		_ = output.Close()
		_ = os.Remove(output.Name())
	}()
	command.Stdout, command.Stderr = output, output
	if err := command.Run(); err != nil {
		return fmt.Errorf("%w%s", err, commandOutputTail(output))
	}
	return nil
}

func dependencyContainerName(kind string) (string, error) {
	suffix, err := randomIdentity(6)
	if err != nil {
		return "", err
	}
	return "tetral-test-" + kind + "-" + suffix, nil
}

func randomIdentity(byteCount int) (string, error) {
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func dockerImageDigest(ctx context.Context, image string) (string, error) {
	output, err := dockerOutput(ctx, "image", "inspect", "--format", "{{json .RepoDigests}}", image)
	if err != nil {
		return "", err
	}
	var digests []string
	if err := json.Unmarshal([]byte(output), &digests); err != nil || len(digests) == 0 || !strings.Contains(digests[0], "@sha256:") {
		return "", fmt.Errorf("dependency image has no resolved OCI digest")
	}
	return digests[0], nil
}

func dockerOutput(ctx context.Context, arguments ...string) (string, error) {
	// Arguments are assembled by the runner's closed Docker dependency inventory.
	//nolint:gosec
	command := exec.CommandContext(ctx, "docker", arguments...)
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("inspect dependency: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func commandOutput(ctx context.Context, name string, arguments ...string) (string, error) {
	// Callers select executables and arguments from the runner's closed dependency inventory.
	//nolint:gosec
	command := exec.CommandContext(ctx, name, arguments...)
	output, err := command.Output()
	return string(output), err
}
