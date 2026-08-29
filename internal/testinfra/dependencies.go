package testinfra

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

const (
	postgresImage         = "ghcr.io/tetral-ai/mirror/postgres:18-alpine"
	minioImage            = "ghcr.io/tetral-ai/mirror/minio:RELEASE.2025-09-07T16-13-09Z"
	forkSDKCommit         = "83ad546898bf9ac0369a4d214463c63fd4502586"
	dependencyStopTimeout = 30 * time.Second
)

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
	docker     func(context.Context) error
	sdk        func(context.Context, *dependencyManager) error
}

var productionDependencyStarters = dependencyStarters{
	postgresql: func(ctx context.Context, manager *dependencyManager) error { return manager.startPostgreSQL(ctx) },
	minio:      func(ctx context.Context, manager *dependencyManager) error { return manager.startMinIO(ctx) },
	docker:     dockerAvailable,
	sdk:        func(ctx context.Context, manager *dependencyManager) error { return manager.startSDK(ctx) },
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
	for _, dependency := range dependencies {
		started := time.Now()
		evidenceStart := len(manager.evidence)
		switch dependency {
		case "postgresql":
			if dsn := os.Getenv(storagetest.EnvTestDatabaseURL); dsn == "" {
				if err := starters.postgresql(ctx, manager); err != nil {
					_ = manager.stopBounded()
					return nil, err
				}
			} else if err := manager.recordExternalPostgreSQL(ctx, dsn); err != nil {
				_ = manager.stopBounded()
				return nil, err
			}
		case "minio":
			if os.Getenv("TETRAL_TEST_MINIO_ENDPOINT") == "" {
				if err := starters.minio(ctx, manager); err != nil {
					_ = manager.stopBounded()
					return nil, err
				}
			} else {
				manager.evidence = append(manager.evidence, DependencyEvidence{Name: "minio", Source: "external", Identity: "external-minio"})
			}
		case "docker":
			if err := starters.docker(ctx); err != nil {
				_ = manager.stopBounded()
				return nil, err
			}
			manager.environment = append(withoutEnvironmentVariable(manager.environment, "TETRAL_TEST_DOCKER_AVAILABLE"), "TETRAL_TEST_DOCKER_AVAILABLE=1")
			manager.evidence = append(manager.evidence, DependencyEvidence{Name: "docker", Source: "host-daemon", Identity: "available"})
		case "sdk":
			if err := starters.sdk(ctx, manager); err != nil {
				_ = manager.stopBounded()
				return nil, err
			}
		case "bun-workspaces":
			if root == "" {
				_ = manager.stopBounded()
				return nil, fmt.Errorf("bun workspace dependency requires a repository root")
			}
			identity, err := installBunWorkspaces(ctx, root)
			if err != nil {
				_ = manager.stopBounded()
				return nil, err
			}
			manager.evidence = append(manager.evidence, DependencyEvidence{Name: "bun-workspaces", Source: "repository-lockfiles", Identity: identity})
		default:
			_ = manager.stopBounded()
			return nil, fmt.Errorf("unknown test dependency %q", dependency)
		}
		for index := evidenceStart; index < len(manager.evidence); index++ {
			if manager.evidence[index].Name == dependency {
				manager.evidence[index].SetupElapsed = time.Since(started)
			}
		}
		if dependency == "postgresql" {
			templateStarted := time.Now()
			digest, err := storagetest.PrepareTemplate(ctx, manager.postgresDSN)
			if err != nil {
				_ = manager.stopBounded()
				return nil, fmt.Errorf("prepare PostgreSQL test template: %w", err)
			}
			for index := evidenceStart; index < len(manager.evidence); index++ {
				if manager.evidence[index].Name == dependency {
					manager.evidence[index].TemplateIdentity = digest
					manager.evidence[index].TemplateSetupElapsed = time.Since(templateStarted)
				}
			}
		}
	}
	return manager, nil
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
	candidates := []string{strings.TrimSpace(os.Getenv("TETRAL_ENGINE_SDK_ROOT"))}
	if m.root != "" {
		candidates = append(candidates, filepath.Join(filepath.Dir(m.root), "tetral-sdk-typescript"))
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if err := verifySDKCheckout(ctx, candidate); err == nil {
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
	if err := runQuietInDir(ctx, directory, "bun", "install", "--no-save", "--ignore-scripts"); err != nil {
		return fmt.Errorf("install pinned SDK dependencies: %w", err)
	}
	for _, name := range []string{"bun.lock", "bun.lockb"} {
		if err := os.Remove(filepath.Join(directory, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove generated SDK lockfile: %w", err)
		}
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
		"-e", "POSTGRES_DB=tetral", postgresImage, "-c", "max_connections=300")
	if err := runQuiet(ctx, "docker", arguments...); err != nil {
		return fmt.Errorf("start PostgreSQL dependency: %w", err)
	}
	m.containers = append(m.containers, name)
	m.containerDependencies = append(m.containerDependencies, "postgresql")
	for attempt := 0; attempt < 60; attempt++ {
		if runQuiet(ctx, "docker", "exec", name, "pg_isready", "-U", "tetral", "-d", "tetral") == nil {
			port, err := dockerPort(ctx, name, "5432/tcp")
			if err != nil {
				return err
			}
			dsn := "postgres://tetral:tetral@127.0.0.1:" + port + "/tetral?sslmode=disable"
			if err := verifyPostgreSQLConnection(ctx, dsn); err != nil {
				time.Sleep(500 * time.Millisecond)
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
			m.evidence = append(m.evidence, DependencyEvidence{Name: "postgresql", Source: "runner-container", Identity: identity, Version: version, RunID: m.runID})
			return nil
		}
		time.Sleep(500 * time.Millisecond)
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
	if err := runQuiet(ctx, "docker", arguments...); err != nil {
		return fmt.Errorf("start MinIO dependency: %w", err)
	}
	m.containers = append(m.containers, name)
	m.containerDependencies = append(m.containerDependencies, "minio")
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
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("MinIO dependency did not become ready")
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
			return fmt.Errorf("inspect owned test dependency container: %w", err)
		}
		started, err := commandOutput(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"tetral.test.owner-start\"}}", container)
		if err != nil {
			return fmt.Errorf("inspect owned test dependency container: %w", err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(pidText))
		if err != nil || processIdentityAlive(pid, strings.TrimSpace(started)) {
			continue
		}
		if err := runQuiet(ctx, "docker", "rm", "-f", container); err != nil {
			return fmt.Errorf("remove orphaned test dependency container: %w", err)
		}
	}
	return nil
}

func (m *dependencyManager) stop(ctx context.Context) error {
	var first error
	for index := len(m.containers) - 1; index >= 0; index-- {
		started := time.Now()
		if err := runQuiet(ctx, "docker", "rm", "-f", m.containers[index]); err != nil && first == nil {
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
	// Callers select executables and arguments from the runner's closed dependency inventory.
	//nolint:gosec
	command := exec.CommandContext(ctx, name, arguments...)
	return command.Run()
}

func runQuietInDir(ctx context.Context, directory, name string, arguments ...string) error {
	// Callers select the directory, executable, and arguments from the closed dependency inventory.
	//nolint:gosec
	command := exec.CommandContext(ctx, name, arguments...)
	command.Dir = directory
	return command.Run()
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
