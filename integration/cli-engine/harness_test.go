package joint

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/blob/blobtest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/testinfra"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

func TestMain(m *testing.M) {
	testinfra.ListenLoopback = listenLoopback
	code := m.Run()
	builds.remove()
	os.Exit(code)
}

// inputs are the harness's environment variables, checked for one test.
type inputs struct {
	machineOS             string
	cliBin                string
	mcpTestServerBin      string
	cliBinMacOS           string
	mcpTestServerBinMacOS string
	macSSHDestination     string
	macSSHKey             string
	cliBinNext            string
	appExampleScript      string
	acceptanceDir         string
	// cliCommit names the parent of the directory holding TETRAL_CLI_BIN.
	cliCommit string
}

// needs is what a test requires beyond the variables every test needs.
type needs struct {
	// computerUse: the test enables Computer Use on its machines.
	computerUse bool
	// platforms limits the machine operating systems the test runs with;
	// empty is both.
	platforms []string
	// variables are further required variables, such as TETRAL_CLI_BIN_NEXT.
	variables []string
}

var fullCommit = regexp.MustCompile(`^[0-9a-f]{40}$`)

// requireInputs reads the variables t needs and fails it at once when one is
// unset, TETRAL_MACHINE_OS is neither linux nor macos or not one the test
// runs with, a binary input or the example script is not an executable
// file, or the CLI builds do not sit in directories named by one full commit
// hash.
func requireInputs(t *testing.T, n needs) inputs {
	t.Helper()
	// In list mode a test with a budget lists itself before it needs inputs.
	if listMode() {
		if slices.Contains(n.variables, "TETRAL_PERSON_AT_MAC") {
			fmt.Printf("joint-person\t%s\n", t.Name())
			t.SkipNow()
		}
		t.Fatalf("%s has no budget", t.Name())
	}
	in := inputs{
		machineOS:             os.Getenv("TETRAL_MACHINE_OS"),
		cliBin:                os.Getenv("TETRAL_CLI_BIN"),
		mcpTestServerBin:      os.Getenv("TETRAL_MCP_TEST_SERVER_BIN"),
		cliBinMacOS:           os.Getenv("TETRAL_CLI_BIN_MACOS"),
		mcpTestServerBinMacOS: os.Getenv("TETRAL_MCP_TEST_SERVER_BIN_MACOS"),
		macSSHDestination:     os.Getenv("TETRAL_MAC_SSH_DESTINATION"),
		macSSHKey:             os.Getenv("TETRAL_MAC_SSH_KEY"),
		cliBinNext:            os.Getenv("TETRAL_CLI_BIN_NEXT"),
		appExampleScript:      os.Getenv("TETRAL_APP_EXAMPLE_SCRIPT"),
		acceptanceDir:         os.Getenv("TETRAL_ACCEPTANCE_DIR"),
	}
	values := map[string]string{
		"TETRAL_MACHINE_OS": in.machineOS, "TETRAL_CLI_BIN": in.cliBin, "TETRAL_MCP_TEST_SERVER_BIN": in.mcpTestServerBin,
		"TETRAL_CLI_BIN_MACOS": in.cliBinMacOS, "TETRAL_MCP_TEST_SERVER_BIN_MACOS": in.mcpTestServerBinMacOS,
		"TETRAL_MAC_SSH_DESTINATION": in.macSSHDestination, "TETRAL_MAC_SSH_KEY": in.macSSHKey,
		"TETRAL_CLI_BIN_NEXT": in.cliBinNext, "TETRAL_APP_EXAMPLE_SCRIPT": in.appExampleScript, "TETRAL_ACCEPTANCE_DIR": in.acceptanceDir,
	}
	required := append([]string{"TETRAL_MACHINE_OS", "TETRAL_CLI_BIN", "TETRAL_ACCEPTANCE_DIR"}, n.variables...)
	if in.machineOS == "macos" {
		required = append(required, "TETRAL_CLI_BIN_MACOS", "TETRAL_MAC_SSH_DESTINATION", "TETRAL_MAC_SSH_KEY")
	}
	if n.computerUse {
		required = append(required, "TETRAL_MCP_TEST_SERVER_BIN")
		if in.machineOS == "macos" {
			required = append(required, "TETRAL_MCP_TEST_SERVER_BIN_MACOS")
		}
	}
	for _, name := range required {
		if os.Getenv(name) == "" {
			t.Fatalf("%s is required and unset", name)
		}
	}
	if in.machineOS != "linux" && in.machineOS != "macos" {
		t.Fatalf("TETRAL_MACHINE_OS is %q; want linux or macos", in.machineOS)
	}
	if len(n.platforms) > 0 && !slices.Contains(n.platforms, in.machineOS) {
		t.Fatalf("%s runs only with TETRAL_MACHINE_OS %s", t.Name(), strings.Join(n.platforms, " or "))
	}
	for _, name := range []string{"TETRAL_CLI_BIN", "TETRAL_MCP_TEST_SERVER_BIN", "TETRAL_CLI_BIN_MACOS", "TETRAL_MCP_TEST_SERVER_BIN_MACOS", "TETRAL_CLI_BIN_NEXT", "TETRAL_APP_EXAMPLE_SCRIPT"} {
		if slices.Contains(required, name) {
			requireExecutable(t, name, values[name])
		}
	}
	in.cliCommit = buildCommit(t, "TETRAL_CLI_BIN", in.cliBin)
	if slices.Contains(required, "TETRAL_CLI_BIN_MACOS") {
		if macCommit := buildCommit(t, "TETRAL_CLI_BIN_MACOS", in.cliBinMacOS); macCommit != in.cliCommit {
			t.Fatalf("TETRAL_CLI_BIN is a build of %s and TETRAL_CLI_BIN_MACOS of %s; want one CLI commit", in.cliCommit, macCommit)
		}
	}
	if err := os.MkdirAll(in.acceptanceDir, 0o700); err != nil { //nolint:gosec // G703: the operator's own record directory.
		t.Fatalf("create TETRAL_ACCEPTANCE_DIR: %v", err)
	}
	return in
}

func requireExecutable(t *testing.T, name, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("%s=%s names no executable file", name, path)
	}
}

// buildCommit is the name of the parent of the directory holding binary,
// which must be a full commit hash: <CLI commit>/<os>-<arch>/<binary>.
func buildCommit(t *testing.T, name, binary string) string {
	t.Helper()
	commit := filepath.Base(filepath.Dir(filepath.Dir(binary)))
	if !fullCommit.MatchString(commit) {
		t.Fatalf("%s=%s is not in a build directory <CLI commit>/<os>-<arch>/ named by a full commit hash", name, binary)
	}
	return commit
}

// acceptanceDir is the directory requireInputs checked and created.
func acceptanceDir() string { return os.Getenv("TETRAL_ACCEPTANCE_DIR") }

// repositoryRoot is the Engine checkout this module replaces its dependency
// with: two levels above this directory.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("no Engine checkout at %s: %v", root, err)
	}
	return root
}

// platformOf is the <os>-<arch> of the machines, as their ready names them.
func platformOf(in inputs) string {
	if in.machineOS == "macos" {
		return "macos-aarch64"
	}
	arch := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[runtime.GOARCH]
	return "linux-" + arch
}

// requireRestrictedProcesses runs the CLI's isolation availability probe on
// the Linux host and fails t at once when the host cannot run restricted
// processes.
func requireRestrictedProcesses(t *testing.T, cliBin string) {
	t.Helper()
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		bwrap = filepath.Join(filepath.Dir(cliBin), "bwrap")
	}
	output, err := exec.Command(bwrap, "--unshare-user", "--unshare-net", "--ro-bind", "/", "/", "/bin/true").CombinedOutput() //nolint:gosec // the CLI's own probe command
	if err != nil {
		t.Fatalf("this host cannot run restricted processes: %s --unshare-user --unshare-net --ro-bind / / /bin/true failed: %v: %s; it needs unprivileged user namespaces (kernel.unprivileged_userns_clone=1 and kernel.apparmor_restrict_unprivileged_userns=0) and a bwrap on PATH or beside TETRAL_CLI_BIN", bwrap, err, output)
	}
}

// serviceBuilds holds the Engine binaries every test of one go test process
// starts; they are built once from the checkout.
type serviceBuilds struct {
	once sync.Once
	dir  string
	err  error
	// shared: dir is TETRAL_ENGINE_BUILDS_DIR, built by the runner.
	shared bool
}

var builds serviceBuilds

func (b *serviceBuilds) path(t *testing.T, name string) string {
	t.Helper()
	b.once.Do(func() {
		// A runner that starts many test processes builds the services once
		// and names the directory; this process then neither builds nor
		// removes them.
		if dir := os.Getenv("TETRAL_ENGINE_BUILDS_DIR"); dir != "" {
			b.dir, b.shared = dir, true
			return
		}
		b.dir, b.err = os.MkdirTemp("", "tetral-engine-builds-")
		if b.err != nil {
			return
		}
		b.err = BuildServices(repositoryRoot(t), b.dir)
	})
	if b.err != nil {
		t.Fatal(b.err)
	}
	return filepath.Join(b.dir, name)
}

func (b *serviceBuilds) remove() {
	if b.dir != "" && !b.shared {
		_ = os.RemoveAll(b.dir)
	}
}

// engine is the real Engine of one test. Machines and test clients reach it
// only through its public edge, and git-proxy through its TLS front.
type engine struct {
	t      *testing.T
	record *acceptanceRecord
	// serverURL is the edge's loopback http origin, every machine's
	// server_url.
	serverURL string
	edgePort  string
	// gitProxyURL is the origin of git-proxy's TLS front, the git_proxy_url
	// of every git ticket; gitProxyPort is its port.
	gitProxyURL, gitProxyPort string
	github                    *github
	// apiKey is the workspace API key of the Controller and the developer.
	apiKey string
	// nats is the broker of the services that publish or subscribe.
	nats     testinfra.NATSFixture
	identity *identity
	edge     *edge
	// hubs are the computer-hub processes by pod name.
	hubs map[string]*serviceProcess
	// ownerURL and databaseCA reach PostgreSQL as the table owner, for what
	// a test writes or reads in-process in place of a service; owner is that
	// connection once opened.
	ownerRole, ownerURL, databaseCA string
	owner                           *dbconnect.Client
	cleanupEnv                      map[string]string
	// databaseEnv is the database settings of a workload's role and blobEnv
	// the blob store settings, for a service the test starts later.
	databaseEnv func(role string) map[string]string
	blobEnv     map[string]string
	// roleURL reaches PostgreSQL as a workload's role.
	roleURL func(role string) string
	// runtime is the test Runtime once a test asks for it.
	runtime *testRuntime
}

// engineOptions are the settings a test gives the Engine services. hubPods
// is the number of computer-hub processes, one when zero; streamMaxAge their
// stream age setting and uploadBudget the upload budget in bytes per minute
// of api and computer-hub, the defaults when empty.
type engineOptions struct {
	minCLIVersion, recommendedCLIVersion string
	hubPods                              int
	streamMaxAge                         string
	uploadBudget                         string
}

const (
	harnessWorkspace = "harness_workspace"
	hubPodPrefix     = "computer-hub"
	blobBucket       = "tetral-harness"
	postgresName     = "postgres.transport.test"
	minioName        = "minio.transport.test"
)

// startEngine starts the containers, prepares the database and the
// workspace, and starts auth, api and computer-hub and the public edge. Every
// part stops when t ends.
func startEngine(t *testing.T, record *acceptanceRecord, options engineOptions) *engine {
	t.Helper()
	ctx := t.Context()
	e := &engine{t: t, record: record, apiKey: randomHex(t, 32)}
	done := record.timed("engine: service builds")
	builds.path(t, "tetral-api")
	done()
	done = record.timed("engine: PostgreSQL container")
	postgres := transporttest.NewPostgreSQL(t)
	done()
	done = record.timed("engine: MinIO container")
	minio := transporttest.NewMinIO(t)
	done()
	blobConfig := blob.Config{Endpoint: minio.Endpoint, Region: "us-east-1", Bucket: blobBucket, AccessKey: "fixture-access", SecretKey: "fixture-password", TLSCAPath: filepath.Join(minio.Directory, "ca.pem"), TLSServerName: minioName}
	createBucket(t, blobConfig)
	done = record.timed("engine: NATS")
	nats, closeNATS, err := testinfra.NewNATSFixture(ctx, repositoryRoot(t))
	done()
	if err != nil {
		t.Fatalf("start NATS: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeNATS(ctx); err != nil {
			t.Errorf("stop NATS: %v", err)
		}
	})
	e.nats = nats
	e.identity = newIdentity(t)
	done = record.timed("engine: tetral-db-prepare and tetral-bootstrap")
	roleURL, migrationOwner := prepareDatabase(t, postgres)
	e.ownerRole, e.ownerURL, e.databaseCA = migrationOwner, roleURL(migrationOwner), filepath.Join(postgres.Directory, "ca.pem")
	databaseEnv := func(role string) map[string]string {
		return map[string]string{"TETRAL_DATABASE_URL": roleURL(role), "TETRAL_DATABASE_TLS_CA_PATH": filepath.Join(postgres.Directory, "ca.pem"), "TETRAL_DATABASE_TLS_SERVER_NAME": postgresName}
	}
	e.databaseEnv, e.roleURL = databaseEnv, roleURL
	runCommand(t, builds.path(t, "tetral-bootstrap"), databaseEnv(migrationOwner), nil, "--workspace-id", harnessWorkspace, "--name", "CLI and Engine")
	done()
	record.command("`tetral-db-prepare` and `tetral-bootstrap --workspace-id %s` on a fresh PostgreSQL", harnessWorkspace)

	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	publicKey := signer.PublicKeyBase64()
	blobEnv := map[string]string{blob.EnvEndpoint: blobConfig.Endpoint, blob.EnvRegion: blobConfig.Region, blob.EnvBucket: blobConfig.Bucket, blob.EnvAccessKey: blobConfig.AccessKey, blob.EnvSecretKey: blobConfig.SecretKey, blob.EnvTLSCAPath: blobConfig.TLSCAPath, blob.EnvTLSServerName: blobConfig.TLSServerName}

	e.cleanupEnv, e.blobEnv = merge(databaseEnv("cleanup"), blobEnv), blobEnv

	authAddresses := reserveAddresses(t, 3)
	authEnv := merge(databaseEnv("auth"), map[string]string{
		"TETRAL_AUTH_HTTP_ADDR":                          authAddresses[0],
		"TETRAL_AUTH_METRICS_ADDR":                       authAddresses[1],
		"TETRAL_AUTH_GRPC_ADDR":                          authAddresses[2],
		"TETRAL_AUTH_GRPC_TRANSPORT":                     "plaintext",
		"TETRAL_HTTP_TRANSPORT":                          "plaintext",
		"TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64": privateKey,
		"ENGINE_API_KEY":                                 e.apiKey,
		"ENGINE_BOOTSTRAP_WORKSPACE_ID":                  harnessWorkspace,
	})
	e.startService("auth", "auth", "tetral-auth", authEnv, authAddresses[0])

	vaultKey := randomHex(t, 32)
	apiAddresses := reserveAddresses(t, 2)
	dataDir := filepath.Join(t.TempDir(), "api-data")
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	apiEnv := merge(databaseEnv("api"), blobEnv, map[string]string{
		"TETRAL_API_HTTP_ADDR":                          apiAddresses[0],
		"TETRAL_API_METRICS_ADDR":                       apiAddresses[1],
		"TETRAL_HTTP_TRANSPORT":                         "plaintext",
		"ENGINE_VAULT_KEY":                              vaultKey,
		"ENGINE_DATA_DIR":                               dataDir,
		"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": publicKey,
		"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF":       "ghcr.io/tetral-ai/sandbox:0.0.0-dev",
	})
	if options.uploadBudget != "" {
		apiEnv["TETRAL_API_DRIVE_UPLOAD_BUDGET_BYTES_PER_MINUTE"] = options.uploadBudget
	}
	e.startService("api", "api", "tetral-api", apiEnv, apiAddresses[0])

	e.github = startGitHub(t)
	frontListener, err := listenLoopback()
	if err != nil {
		t.Fatal(err)
	}
	e.gitProxyURL = "https://" + frontListener.Addr().String()
	_, e.gitProxyPort, _ = net.SplitHostPort(frontListener.Addr().String())
	gitProxyAddresses := reserveAddresses(t, 2)
	e.startService("git-proxy", "git-proxy", "git-proxy", merge(databaseEnv("git_proxy"), map[string]string{
		"TETRAL_GIT_PROXY_HTTP_ADDR":       gitProxyAddresses[0],
		"TETRAL_GIT_PROXY_METRICS_ADDR":    gitProxyAddresses[1],
		"TETRAL_GIT_PROXY_PUBLIC_BASE_URL": e.gitProxyURL,
		"TETRAL_HTTP_TRANSPORT":            "plaintext",
		"ENGINE_VAULT_KEY":                 vaultKey,
		"HTTPS_PROXY":                      e.github.proxyURL,
		"SSL_CERT_FILE":                    e.github.caPath,
	}), gitProxyAddresses[1])
	e.github.startGitProxyFront(frontListener, gitProxyAddresses[0])

	pageTokenKey := randomHex(t, 32)
	// The edge's address is fixed before the hubs start: it is the
	// server_url of the bootstraps their claims return.
	edgeListener, err := listenLoopback()
	if err != nil {
		t.Fatal(err)
	}
	var hubs [][2]string
	e.hubs = map[string]*serviceProcess{}
	for i := range max(options.hubPods, 1) {
		pod := fmt.Sprintf("%s-%d", hubPodPrefix, i)
		hubAddresses := reserveAddresses(t, 2)
		hubEnv := merge(databaseEnv("computer_hub"), blobEnv, map[string]string{
			"TETRAL_COMPUTER_HUB_HTTP_ADDR":                 hubAddresses[0],
			"TETRAL_COMPUTER_HUB_METRICS_ADDR":              hubAddresses[1],
			"TETRAL_HTTP_TRANSPORT":                         "plaintext",
			"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": publicKey,
			"TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY":            pageTokenKey,
			"TETRAL_COMPUTER_HUB_POD_NAME":                  pod,
			"TETRAL_COMPUTER_HUB_SERVER_URL":                "http://" + edgeListener.Addr().String(),
			"TETRAL_COMPUTER_HUB_GIT_PROXY_URL":             e.gitProxyURL,
		})
		for key, value := range map[string]string{
			"TETRAL_COMPUTER_HUB_MIN_CLI_VERSION":                      options.minCLIVersion,
			"TETRAL_COMPUTER_HUB_RECOMMENDED_CLI_VERSION":              options.recommendedCLIVersion,
			"TETRAL_COMPUTER_HUB_STREAM_MAX_AGE":                       options.streamMaxAge,
			"TETRAL_COMPUTER_HUB_DRIVE_UPLOAD_BUDGET_BYTES_PER_MINUTE": options.uploadBudget,
		} {
			if value != "" {
				hubEnv[key] = value
			}
		}
		e.hubs[pod] = e.startService("computer-hub", pod, "tetral-computer-hub", hubEnv, hubAddresses[0])
		hubs = append(hubs, [2]string{pod, "http://" + hubAddresses[0]})
	}

	e.edge, e.serverURL, e.edgePort = startEdge(t, edgeListener, authAddresses[2], map[string][][2]string{
		"auth":         {{"auth", "http://" + authAddresses[0]}},
		"api":          {{"api", "http://" + apiAddresses[0]}},
		"computer-hub": hubs,
	})
	record.observe("Engine at %s through the public edge: auth, api and computer-hub (%s) as processes, PostgreSQL and MinIO containers, NATS; git-proxy as a process behind its TLS front at %s, reaching a GitHub stand-in through a CONNECT proxy", e.serverURL, strings.Join(slices.Sorted(maps.Keys(e.hubs)), ", "), e.gitProxyURL)
	return e
}

// killHub takes pod out of the edge's rotation and kills its computer-hub
// process at once, with no drain.
func (e *engine) killHub(pod string) {
	e.t.Helper()
	process := e.hubs[pod]
	e.edge.setDown(pod, true)
	process.killed.Store(true)
	_ = process.command.Process.Kill()
	<-process.exited
}

// ownerTx runs fn in a transaction of the harness workspace as the table
// owner.
func (e *engine) ownerTx(fn func(context.Context, *dbconnect.Tx) error) {
	e.t.Helper()
	e.ownerTxIn(harnessWorkspace, fn)
}

// ownerTxIn is ownerTx in the workspace ws.
func (e *engine) ownerTxIn(ws string, fn func(context.Context, *dbconnect.Tx) error) {
	e.t.Helper()
	ctx := e.t.Context()
	if e.owner == nil {
		opened, err := dbconnect.OpenProtectedDSN(ctx, e.ownerURL, e.databaseCA, postgresName)
		if err != nil {
			e.t.Fatalf("open the database as the table owner: %v", err)
		}
		e.owner = opened.Client
		e.t.Cleanup(func() { _ = e.owner.Close() })
	}
	if err := e.owner.WithWorkspaceTx(ctx, ws, "joint.owner_read_write", func(tx *dbconnect.Tx) error { return fn(ctx, tx) }); err != nil {
		e.t.Fatalf("owner transaction: %v", err)
	}
}

// runCleanupPhases runs one scheduled pass of the cleanup process's phases
// as the cleanup role, with env added to its settings.
func (e *engine) runCleanupPhases(env map[string]string) {
	e.t.Helper()
	runCommand(e.t, builds.path(e.t, "tetral-cleanup"), merge(e.cleanupEnv, env), nil)
	e.record.command("tetral-cleanup phases pass as the cleanup database role with %v", env)
}

// otherWorkspace bootstraps the workspace id, as the harness's own, with an
// API key of its own and returns its test Controller and developer clients.
func (e *engine) otherWorkspace(id string) (*controllerClient, *developerClient) {
	e.t.Helper()
	runCommand(e.t, builds.path(e.t, "tetral-bootstrap"), e.databaseEnv(e.ownerRole), nil, "--workspace-id", id, "--name", id)
	key := randomHex(e.t, 32)
	e.ownerTxIn(id, func(ctx context.Context, tx *dbconnect.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,created_at)
			VALUES($1,$2,'bootstrap',$3,$4,'bootstrap','independent_key',$5)`, "ak_"+randomHex(e.t, 8), id, auth.KeyPrefixFor(key), auth.DigestAPIKey(key), time.Now().UTC().Format(time.RFC3339))
		return err
	})
	e.record.command("`tetral-bootstrap --workspace-id %s` and a bootstrap API key of that workspace", id)
	return &controllerClient{t: e.t, baseURL: e.serverURL, apiKey: key, record: e.record},
		&developerClient{t: e.t, baseURL: e.serverURL, apiKey: key, record: e.record}
}

// forRow is the Engine as the table row that t runs sees it: failures,
// cleanups, the record and the clients it makes are the row's, while the
// processes, the edge and the Runtime's client are the table's.
func (e *engine) forRow(t *testing.T, record *acceptanceRecord) *engine {
	row := *e
	row.t, row.record = t, record
	if e.runtime != nil {
		row.runtime = &testRuntime{e: &row, client: e.runtime.client, sessions: e.runtime.sessions}
	}
	return &row
}

// controller is the test Controller client of the workspace.
func (e *engine) controller() *controllerClient {
	return &controllerClient{t: e.t, baseURL: e.serverURL, apiKey: e.apiKey, record: e.record}
}

// developer is the test developer client of the workspace.
func (e *engine) developer() *developerClient {
	return &developerClient{t: e.t, baseURL: e.serverURL, apiKey: e.apiKey, record: e.record}
}

func createBucket(t *testing.T, config blob.Config) {
	t.Helper()
	owner, err := transportsecurity.Open(t.Context(), transportsecurity.Config{CAPath: config.TLSCAPath})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	tlsConfig, err := owner.ClientTLSConfig(config.TLSServerName, "")
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsConfig}}
	if err := blobtest.CreateBucket(t.Context(), client, config); err != nil {
		t.Fatal(err)
	}
}

// prepareDatabase runs tetral-db-prepare as the superuser with one role per
// workload and returns the URL of a workload's role and the name of the
// migration owner's workload.
func prepareDatabase(t *testing.T, postgres *transporttest.PostgreSQL) (func(role string) string, string) {
	t.Helper()
	contract, err := database.LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	declarations := database.RoleDeclarations{Roles: map[string]database.RoleCredential{}}
	for _, name := range append(contract.WorkloadNames(), contract.MigrationOwner) {
		declarations.Roles[name] = database.RoleCredential{Name: "tetral_" + name, Password: randomHex(t, 16)}
	}
	input, err := json.Marshal(declarations)
	if err != nil {
		t.Fatal(err)
	}
	runCommand(t, builds.path(t, "tetral-db-prepare"), map[string]string{
		"TETRAL_DATABASE_ADMIN_URL":       postgres.URL,
		"TETRAL_DATABASE_TLS_CA_PATH":     filepath.Join(postgres.Directory, "ca.pem"),
		"TETRAL_DATABASE_TLS_SERVER_NAME": postgresName,
	}, input)
	admin, err := url.Parse(postgres.URL)
	if err != nil {
		t.Fatal(err)
	}
	return func(role string) string {
		credential, ok := declarations.Roles[role]
		if !ok {
			t.Fatalf("no database role %s", role)
		}
		u := *admin
		u.User = url.UserPassword(credential.Name, credential.Password)
		return u.String()
	}, contract.MigrationOwner
}

// runCommand runs binary with only env and PATH in its environment and
// fails t with its output when it fails.
func runCommand(t *testing.T, binary string, env map[string]string, stdin []byte, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...) //nolint:gosec // the harness's own builds
	command.Env = childEnvironment(env)
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s %s: %v: %s", filepath.Base(binary), strings.Join(args, " "), err, output)
	}
}

func childEnvironment(env map[string]string) []string {
	result := []string{"PATH=" + os.Getenv("PATH")}
	for name, value := range env {
		result = append(result, name+"="+value)
	}
	return result
}

func merge(maps ...map[string]string) map[string]string {
	result := map[string]string{}
	for _, m := range maps {
		for name, value := range m {
			result[name] = value
		}
	}
	return result
}

// serviceProcess is one Engine service running as its own process.
type serviceProcess struct {
	name    string
	command *exec.Cmd
	log     *lockedBuffer
	exited  chan struct{}
	err     error
	// tokenPath is the file holding the process's own workload token, which
	// a service that makes internal gRPC calls names in its token setting.
	tokenPath string
	// killed marks a process the test killed on purpose.
	killed atomic.Bool
}

// startService starts the build name as the process label of service with
// env, gives it its own workload token of the service's account and the
// TokenReview endpoint, and waits until its /ready on readyAddress answers
// 200. It stops with SIGTERM when t ends.
func (e *engine) startService(service, label, name string, env map[string]string, readyAddress string) *serviceProcess {
	t := e.t
	t.Helper()
	defer e.record.timed("engine: start %s until ready", label)()
	env = merge(env, e.identity.environment(), map[string]string{"TETRAL_DEPLOYMENT_ENVIRONMENT": "test", "TETRAL_SERVICE_VERSION": e.record.engineCommit})
	process := &serviceProcess{name: label, log: &lockedBuffer{}, exited: make(chan struct{}), tokenPath: e.identity.issue(t, service, label)}
	process.command = exec.Command(builds.path(t, name)) //nolint:gosec // the harness's own build
	process.command.Env = childEnvironment(env)
	process.command.Stdout, process.command.Stderr = process.log, process.log
	if err := process.command.Start(); err != nil {
		t.Fatalf("start %s: %v", label, err)
	}
	go func() {
		process.err = process.command.Wait()
		close(process.exited)
	}()
	t.Cleanup(func() { process.stop(t) })
	deadline := time.Now().Add(60 * time.Second)
	client := &http.Client{Timeout: time.Second}
	for {
		response, err := client.Get("http://" + readyAddress + "/ready")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return process
			}
		}
		select {
		case <-process.exited:
			t.Fatalf("%s exited before it was ready: %v\n%s", label, process.err, process.log.tail(40))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not ready within 60s\n%s", label, process.log.tail(40))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *serviceProcess) stop(t *testing.T) {
	if p.killed.Load() {
		return
	}
	select {
	case <-p.exited:
		t.Errorf("%s exited during the test: %v\n%s", p.name, p.err, p.log.tail(40))
		return
	default:
	}
	_ = p.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
		if p.err != nil {
			t.Errorf("%s did not shut down cleanly: %v\n%s", p.name, p.err, p.log.tail(40))
		}
	case <-time.After(20 * time.Second):
		_ = p.command.Process.Kill()
		<-p.exited
		t.Errorf("%s did not stop within 20s of SIGTERM", p.name)
	}
	if t.Failed() {
		t.Logf("%s log:\n%s", p.name, p.log.tail(80))
	}
}

// loopbackPorts is the range of loopback ports this test process takes
// every listener and reservation from when its runner names one,
// TETRAL_JOINT_PORTS=<first>-<last>. The runner gives each of the test
// processes it runs at once a range of its own, below the range the kernel
// hands out for port 0 on Linux and macOS: a port the harness
// reserves is closed until its service binds it, and from such a range no
// other process can be handed it meanwhile. Each port is used once.
var loopbackPorts struct {
	mu         sync.Mutex
	read       bool
	next, last int
}

// listenLoopback listens on a free loopback port: the next free one of the
// runner's range, or one the kernel picks when the runner names none.
func listenLoopback() (net.Listener, error) {
	p := &loopbackPorts
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.read {
		p.read = true
		if value := os.Getenv("TETRAL_JOINT_PORTS"); value != "" {
			if _, err := fmt.Sscanf(value, "%d-%d", &p.next, &p.last); err != nil || p.next <= 0 || p.last < p.next {
				return nil, fmt.Errorf("TETRAL_JOINT_PORTS=%q is no range <first>-<last>", value)
			}
		}
	}
	if p.last == 0 {
		return net.Listen("tcp", "127.0.0.1:0")
	}
	for ; p.next <= p.last; p.next++ {
		// A port in use outside the runner's processes is passed over.
		if listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p.next)); err == nil {
			p.next++
			return listener, nil
		}
	}
	return nil, fmt.Errorf("the ports of TETRAL_JOINT_PORTS=%s are used up", os.Getenv("TETRAL_JOINT_PORTS"))
}

// reserveAddresses returns n free loopback addresses.
func reserveAddresses(t *testing.T, n int) []string {
	t.Helper()
	addresses := make([]string, 0, n)
	listeners := make([]net.Listener, 0, n)
	for range n {
		listener, err := listenLoopback()
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	return addresses
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// lockedBuffer collects a process's output for the harness's reports.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// tail is the last n lines of the buffer.
func (b *lockedBuffer) tail(n int) string {
	lines := strings.Split(strings.TrimRight(b.String(), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// machines are the CLI machines of one test, on the platform its inputs name.
type machines struct {
	t      *testing.T
	in     inputs
	needs  needs
	engine *engine
	record *acceptanceRecord
	// probed: the restricted-process probe ran on the Linux host.
	probed bool
	mac    *macSession
}

func newMachines(t *testing.T, in inputs, n needs, e *engine, record *acceptanceRecord) *machines {
	return &machines{t: t, in: in, needs: n, engine: e, record: record}
}

// add starts the machine name of app on the platform TETRAL_MACHINE_OS names.
func (s *machines) add(name, app string) *machine {
	s.t.Helper()
	if s.in.machineOS == "macos" {
		return s.addMac(name, app)
	}
	return s.addLinux(name, app)
}

// addLinux starts the machine name of app on the Linux host from
// TETRAL_CLI_BIN.
func (s *machines) addLinux(name, app string) *machine {
	s.t.Helper()
	return s.linuxMachine(name, app, s.t.TempDir())
}

// linuxMachine starts the machine name of app from TETRAL_CLI_BIN in a new
// directory under base.
func (s *machines) linuxMachine(name, app, base string) *machine {
	t := s.t
	t.Helper()
	if !s.probed {
		done := s.record.timed("machine %s: restricted-process probe", name)
		requireRestrictedProcesses(t, s.in.cliBin)
		s.probed = true
		done()
	}
	dir := filepath.Join(base, name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Registered after the directory, this runs before its removal and after
	// the machine's App Servers stopped.
	t.Cleanup(func() {
		defer s.record.timed("cleanup: detach mounts left under machine %s", name)()
		s.detachLeftMounts(dir)
	})
	m := &machine{t: t, name: name, app: app, tetral: s.in.cliBin, home: filepath.Join(dir, "home"), gitCA: s.engine.github.caPath, host: linuxHost{}, record: s.record}
	m.version()
	return m
}

// addOnSmallVolume starts the machine name of app with its home on a volume
// of about 7 GiB and returns it with a directory on that volume outside the
// machine's directory: on Linux the directory TETRAL_SMALL_VOLUME
// names, on the Mac a disk image the test creates and attaches, which needs
// no root.
func (s *machines) addOnSmallVolume(name, app string) (*machine, string) {
	t := s.t
	t.Helper()
	if s.in.machineOS == "macos" {
		return s.addMacOnSmallVolume(name, app)
	}
	volume := os.Getenv("TETRAL_SMALL_VOLUME")
	if volume == "" {
		t.Fatal("TETRAL_SMALL_VOLUME must name a directory on a volume of 6 to 8 GiB")
	}
	base, err := os.MkdirTemp(volume, "joint-")
	if err != nil {
		t.Fatal(err)
	}
	// Registered first, this runs after the machine's own cleanups.
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	return s.linuxMachine(name, app, base), base
}

// addInMemory starts the machine name of app with its home on a volume held
// in memory: on Linux a directory under /dev/shm, on the Mac a RAM disk the
// test attaches, which needs no root. The CLI syncs each file and state
// change it records, so a row that writes thousands of files through a
// mount, and claims nothing about the disk, spends minutes on a disk and
// seconds in memory.
func (s *machines) addInMemory(name, app string) *machine {
	t := s.t
	t.Helper()
	if s.in.machineOS == "macos" {
		return s.addMacInMemory(name, app)
	}
	base, err := os.MkdirTemp("/dev/shm", "joint-")
	if err != nil {
		t.Fatal(err)
	}
	// Registered first, this runs after the machine's own cleanups.
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	s.record.command("machine %s with its home under /dev/shm, in memory", name)
	return s.linuxMachine(name, app, base)
}

// detachLeftMounts lazily unmounts every mount under dir. Shutdown leaves
// an App Server's drive mounts in place for the next App Server to detach,
// and a test's machine has no next App Server.
func (s *machines) detachLeftMounts(dir string) {
	t := s.t
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Errorf("read mountinfo: %v", err)
		return
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) < 5 || !strings.HasPrefix(fields[4], dir+"/") {
			continue
		}
		if out, err := exec.Command("fusermount3", "-u", "-z", fields[4]).CombinedOutput(); err != nil {
			// The CLI's own unmount can end between the read and the detach.
			if mounted(fields[4]) {
				t.Errorf("unmount %s: %v %s", fields[4], err, out)
			}
			continue
		}
		s.record.command("`fusermount3 -u -z %s`, a mount left after the App Server stopped", fields[4])
	}
}

// mounted is whether a mount sits at point in this process's mount table.
func mounted(point string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return true
	}
	for line := range strings.Lines(string(data)) {
		if fields := strings.Fields(line); len(fields) >= 5 && fields[4] == point {
			return true
		}
	}
	return false
}

// machine is one CLI machine: the tetral it runs and its state root.
type machine struct {
	t      *testing.T
	name   string
	app    string
	tetral string
	home   string
	// gitCA is the machine's copy of the test CA, which its App Server
	// trusts for git-proxy through GIT_SSL_CAINFO.
	gitCA  string
	host   machineHost
	record *acceptanceRecord
	// cliVersion is what the machine's tetral --version wrote.
	cliVersion string
	// withoutDrives keeps configure on a Mac from enabling drives.
	withoutDrives bool
}

// machineHost runs a machine's commands on its host.
type machineHost interface {
	// command is argv on the host with env in its environment; each value of
	// secrets reaches the environment without appearing in command text.
	command(ctx context.Context, env, secrets map[string]string, stdin io.Reader, argv []string) *exec.Cmd
	// signal sends sig to the started process.
	signal(ctx context.Context, p *cliProcess, sig syscall.Signal) error
	// pid is the host's process ID of p once it is known.
	pid(p *cliProcess) string
}

type linuxHost struct{}

func (linuxHost) command(ctx context.Context, env, secrets map[string]string, stdin io.Reader, argv []string) *exec.Cmd {
	command := exec.CommandContext(ctx, argv[0], argv[1:]...) //nolint:gosec // the CLI build under test
	command.Env = append(childEnvironment(merge(env, secrets)), "HOME="+os.Getenv("HOME"))
	command.Stdin = stdin
	command.Cancel = func() error { return command.Process.Signal(syscall.SIGKILL) }
	return command
}

func (linuxHost) signal(_ context.Context, p *cliProcess, sig syscall.Signal) error {
	return p.command.Process.Signal(sig)
}

func (linuxHost) pid(p *cliProcess) string { return fmt.Sprint(p.command.Process.Pid) }

// commandResult is what one finished tetral command wrote.
type commandResult struct {
	exitCode int
	stdout   []byte
	stderr   string
}

// object decodes the one JSON object the command wrote to stdout.
func (r commandResult) object(t *testing.T) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(r.stdout, &object); err != nil {
		t.Fatalf("stdout %q is not one JSON object: %v; stderr:\n%s", r.stdout, err, r.stderr)
	}
	return object
}

// run runs tetral with args (and --app) on the machine and waits for it.
func (m *machine) run(stdin []byte, args ...string) commandResult {
	m.t.Helper()
	ctx, cancel := context.WithTimeout(m.t.Context(), 2*time.Minute)
	defer cancel()
	var stdout, stderr bytes.Buffer
	var input io.Reader
	if stdin != nil {
		input = bytes.NewReader(stdin)
	}
	command := m.host.command(ctx, map[string]string{"TETRAL_HOME": m.home}, nil, input, append([]string{m.tetral}, m.args(args)...))
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	result := commandResult{stdout: stdout.Bytes(), stderr: stderr.String()}
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		result.exitCode = exit.ExitCode()
	case err != nil:
		m.t.Fatalf("run tetral %s on %s: %v\n%s", strings.Join(args, " "), m.name, err, stderr.String())
	}
	m.record.command("`tetral %s` on %s exited %d: `%s`", strings.Join(m.args(args), " "), m.name, result.exitCode, bytes.TrimSpace(result.stdout))
	return result
}

// args adds --app to every command but --version.
func (m *machine) args(args []string) []string {
	if len(args) == 1 && args[0] == "--version" {
		return args
	}
	return append(slices.Clone(args), "--app", m.app)
}

// version records the machine's tetral --version.
func (m *machine) version() {
	m.t.Helper()
	defer m.record.timed("machine %s: tetral --version", m.name)()
	result := m.run(nil, "--version")
	version, _ := result.object(m.t)["cli_version"].(string)
	if result.exitCode != 0 || version == "" {
		m.t.Fatalf("tetral --version on %s: exit %d, %s", m.name, result.exitCode, result.stdout)
	}
	m.cliVersion = version
	m.record.setCLIVersion(version)
}

// configure installs bootstrap with tetral configure --from-stdin and
// requires it to succeed. On a Mac it then enables drives, as the app does
// before the machine's first server run, unless the machine is to have none.
func (m *machine) configure(bootstrap []byte) {
	m.t.Helper()
	done := m.record.timed("machine %s: configure --from-stdin", m.name)
	result := m.run(bootstrap, "configure", "--from-stdin")
	done()
	if result.exitCode != 0 || string(bytes.TrimSpace(result.stdout)) != "{}" {
		m.t.Fatalf("configure on %s: exit %d, %s\n%s", m.name, result.exitCode, result.stdout, result.stderr)
	}
	if _, mac := m.host.(macHost); mac && !m.withoutDrives {
		m.enableDrives()
	}
}

// enableDrives runs tetral drives enable and requires it to succeed.
func (m *machine) enableDrives() {
	m.t.Helper()
	defer m.record.timed("machine %s: drives enable", m.name)()
	result := m.run(nil, "drives", "enable")
	if result.exitCode != 0 || string(bytes.TrimSpace(result.stdout)) != "{}" {
		m.t.Fatalf("drives enable on %s: exit %d, %s\n%s", m.name, result.exitCode, result.stdout, result.stderr)
	}
}

// status is the App Server's GET /v1/status through tetral server status.
func (m *machine) status() map[string]any {
	m.t.Helper()
	result := m.run(nil, "server", "status")
	if result.exitCode != 0 {
		m.t.Fatalf("server status on %s: exit %d, %s", m.name, result.exitCode, result.stdout)
	}
	return result.object(m.t)
}

// directory is the absolute path of name beside the machine's state root,
// on the machine's host.
func (m *machine) directory(name string) string {
	return filepath.Join(filepath.Dir(m.home), name)
}

// shell runs argv on the machine's host outside tetral, with the machine's
// git trust, for at most 2 minutes, and returns its exit code and trimmed
// stdout.
func (m *machine) shell(argv ...string) (int, string) {
	m.t.Helper()
	return m.shellWithin(2*time.Minute, argv...)
}

// shellWithin runs argv as shell does, for at most timeout.
func (m *machine) shellWithin(timeout time.Duration, argv ...string) (int, string) {
	m.t.Helper()
	ctx, cancel := context.WithTimeout(m.t.Context(), timeout)
	defer cancel()
	var stdout, stderr bytes.Buffer
	command := m.host.command(ctx, map[string]string{"GIT_SSL_CAINFO": m.gitCA, "GIT_TERMINAL_PROMPT": "0"}, nil, nil, argv)
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	var exit *exec.ExitError
	code := 0
	switch {
	case errors.As(err, &exit):
		code = exit.ExitCode()
	case err != nil:
		m.t.Fatalf("run %s on %s: %v\n%s", strings.Join(argv, " "), m.name, err, stderr.String())
	}
	m.record.command("`%s` on %s exited %d", strings.Join(argv, " "), m.name, code)
	return code, strings.TrimSpace(stdout.String())
}

// waitStatus reads the status until done accepts it or timeout passes.
func (m *machine) waitStatus(timeout time.Duration, what string, done func(map[string]any) bool) map[string]any {
	m.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		result := m.run(nil, "server", "status")
		if result.exitCode == 0 {
			if status := result.object(m.t); done(status) {
				return status
			}
		}
		if time.Now().After(deadline) {
			m.t.Fatalf("%s: %s not reached within %s; last status %s", m.name, what, timeout, result.stdout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// cliProcess is a started tetral command, such as server run.
type cliProcess struct {
	machine *machine
	command *exec.Cmd
	stdout  lockedBuffer
	stderr  *pidWriter
	exited  chan struct{}
	err     error
}

// serve starts tetral server run with args on the machine. A server still
// running when the test ends gets SIGTERM.
func (m *machine) serve(args ...string) *cliProcess {
	m.t.Helper()
	return m.serveWith(nil, args...)
}

// serveWith starts tetral server run as serve does, with env added to its
// environment.
func (m *machine) serveWith(env map[string]string, args ...string) *cliProcess {
	m.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := &cliProcess{machine: m, stderr: newPIDWriter(), exited: make(chan struct{})}
	p.command = m.host.command(ctx, merge(map[string]string{"TETRAL_HOME": m.home, "GIT_SSL_CAINFO": m.gitCA}, env), nil, nil, append([]string{m.tetral}, m.args(append([]string{"server", "run"}, args...))...))
	p.command.Stdout, p.command.Stderr = &p.stdout, p.stderr
	if err := p.command.Start(); err != nil {
		cancel()
		m.t.Fatalf("start server run on %s: %v", m.name, err)
	}
	go func() {
		p.err = p.command.Wait()
		close(p.exited)
	}()
	m.record.command("`tetral %s` on %s started", strings.Join(m.args(append([]string{"server", "run"}, args...)), " "), m.name)
	m.t.Cleanup(func() {
		defer cancel()
		select {
		case <-p.exited:
			return
		default:
		}
		defer m.record.timed("cleanup: SIGTERM to server run on %s until it exits", m.name)()
		if err := p.signal(syscall.SIGTERM); err != nil {
			m.t.Errorf("stop server run on %s: %v", m.name, err)
		}
		select {
		case <-p.exited:
		case <-time.After(15 * time.Second):
			m.t.Errorf("server run on %s did not exit within 15s of SIGTERM", m.name)
		}
		if m.t.Failed() {
			m.t.Logf("server run on %s stderr:\n%s", m.name, p.stderr.tail(80))
		}
	})
	return p
}

func (p *cliProcess) signal(sig syscall.Signal) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return p.machine.host.signal(ctx, p, sig)
}

// stop sends SIGTERM and returns the exit code and the status object the
// server wrote as of its exit.
func (p *cliProcess) stop() commandResult {
	t := p.machine.t
	t.Helper()
	if err := p.signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM to server run on %s: %v", p.machine.name, err)
	}
	select {
	case <-p.exited:
	case <-time.After(15 * time.Second):
		t.Fatalf("server run on %s did not exit within 15s of SIGTERM", p.machine.name)
	}
	result := commandResult{stdout: []byte(p.stdout.String()), stderr: p.stderr.String()}
	var exit *exec.ExitError
	if errors.As(p.err, &exit) {
		result.exitCode = exit.ExitCode()
	} else if p.err != nil {
		t.Fatalf("server run on %s: %v", p.machine.name, p.err)
	}
	p.machine.record.command("SIGTERM to `tetral server run` on %s: exited %d", p.machine.name, result.exitCode)
	return result
}

// pidWriter collects a process's stderr; a line "tetral-harness-pid <pid>"
// that a remote shell writes before it runs the command gives the process ID
// on the remote host and is not collected.
type pidWriter struct {
	lockedBuffer
	mu      sync.Mutex
	partial []byte
	pid     string
	known   chan struct{}
}

const pidMarker = "tetral-harness-pid "

func newPIDWriter() *pidWriter { return &pidWriter{known: make(chan struct{})} }

func (w *pidWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pid != "" {
		return w.lockedBuffer.Write(p)
	}
	w.partial = append(w.partial, p...)
	line, rest, found := bytes.Cut(w.partial, []byte("\n"))
	if !found {
		return len(p), nil
	}
	if pid, ok := strings.CutPrefix(string(line), pidMarker); ok {
		w.pid = pid
		close(w.known)
		_, _ = w.lockedBuffer.Write(rest)
	} else {
		w.pid = "-"
		close(w.known)
		_, _ = w.lockedBuffer.Write(w.partial)
	}
	w.partial = nil
	return len(p), nil
}

// remotePID waits up to timeout for the marker line.
func (w *pidWriter) remotePID(timeout time.Duration) (string, error) {
	select {
	case <-w.known:
	case <-time.After(timeout):
		return "", errors.New("the remote shell wrote no process ID")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pid == "-" {
		return "", errors.New("the remote shell wrote no process ID")
	}
	return w.pid, nil
}

// statusField reads one string field of a status object; JSON null is "".
func statusField(status map[string]any, name string) string {
	value, _ := status[name].(string)
	return value
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// lastErrorCode is the code of a status object's last_error, or "".
func lastErrorCode(status map[string]any) string {
	lastError, _ := status["last_error"].(map[string]any)
	code, _ := lastError["code"].(string)
	return code
}
