// Layer 2: each Engine command as its own built process against real
// PostgreSQL; the comparison with the manifests is layer 1.
package integration

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/runtimecontrol/resultnotify"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// commandDatabase is one database clone with the production roles installed.
type commandDatabase struct {
	admin     *sql.DB
	workloads *storagetest.WorkloadDB
}

func newCommandDatabase(t *testing.T) *commandDatabase {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	return &commandDatabase{admin: admin, workloads: storagetest.OpenWorkloadDB(t, admin, "job_runner")}
}

// env is the database environment of a command serving as workload, its
// URL in variable.
func (d *commandDatabase) env(t *testing.T, variable, workload string) map[string]string {
	t.Helper()
	return map[string]string{
		variable:                          storagetest.RuntimeDatabaseURL(t, d.workloads.OpenWorkload(t, workload, nil)),
		"TETRAL_DATABASE_TLS_CA_PATH":     filepath.Join(os.Getenv(envIsolatedTLSPGCerts), "ca.pem"),
		"TETRAL_DATABASE_TLS_SERVER_NAME": "postgres.transport.test",
	}
}

// outputRecord is the record with msg in a command's output.
func outputRecord(t *testing.T, output, msg string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == msg {
			return record
		}
	}
	t.Fatalf("no %s record in: %s", msg, output)
	return nil
}

// outputRecords counts the records with msg in a command's output.
func outputRecords(output, msg string) (count int) {
	for _, line := range strings.Split(output, "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == msg {
			count++
		}
	}
	return count
}

// commandBlobEnvironment is a protected blob store nothing listens at. A
// command builds its client without a request, so it serves with it as long
// as no content moves.
func commandBlobEnvironment() map[string]string {
	return map[string]string{
		"TETRAL_BLOB_ENDPOINT":        "https://127.0.0.1:9",
		"TETRAL_BLOB_REGION":          "us-east-1",
		"TETRAL_BLOB_BUCKET":          "unused",
		"TETRAL_BLOB_ACCESS_KEY":      "unused",
		"TETRAL_BLOB_SECRET_KEY":      "unused-secret",
		"TETRAL_BLOB_ALLOW_INSECURE":  "false",
		"TETRAL_BLOB_LOCAL_TEST_MODE": "false",
		"TETRAL_BLOB_TLS_CA_PATH":     filepath.Join(os.Getenv(envIsolatedTLSPGCerts), "ca.pem"),
		"TETRAL_BLOB_TLS_SERVER_NAME": "blob.transport.test",
	}
}

// commandExit is one start that ends on its own: its environment, the status
// it exits with (1, a refused start, unless succeeds) and what it writes
// first.
type commandExit struct {
	name string
	// set replaces variables of the command's good environment.
	set map[string]string
	// arguments are the command's arguments.
	arguments []string
	// prepare changes the database before the start.
	prepare  func(t *testing.T, db *commandDatabase)
	succeeds bool
	// record is the msg of the record that names the failure; the default
	// is startup.failed. want are fields of that record.
	record string
	want   map[string]string
	// messageHas are parts of the record's error.message; absent are parts
	// the whole output must not hold.
	messageHas, absent []string
}

// serviceCommand is one command of the table: how it is built and started,
// what it refuses, and what holds while it serves.
type serviceCommand struct {
	name, pkg, workload string
	// bun is the entry of a command Bun runs from source, in place of pkg.
	bun string
	// component is the name the command's records carry, when it is not
	// name; databaseVariable names its database URL, when it is not
	// TETRAL_DATABASE_URL.
	component, databaseVariable string
	// refusal is the record a refused start writes, when it is not
	// startup.failed of class startup_error.
	refusal, refusalClass string
	// unreachable are the fields by which that record names a database it
	// cannot reach, when it is not a Go command's startup.cause.
	unreachable map[string]string
	// readyPath is the path that answers once ready, when it is not /ready.
	readyPath string
	// env is the command's own environment for its reserved addresses; the
	// database, deployment and version variables are added to it.
	env func(t *testing.T, addresses []string) map[string]string
	// ready is the index of the address whose /ready answers.
	ready int
	exits []commandExit
	// serving runs once the process is ready and returns what must hold
	// after its exit.
	serving func(t *testing.T, db *commandDatabase, addresses []string) (ended func())
	// neverServes names a command that is started for its exits alone: a
	// scheduled run.
	neverServes bool
	// pod names a command that serves only inside a cluster: it serves as
	// its production build in a container laid out as its pod.
	pod bool
}

// commandDataDir is a new directory called name with mode.
func commandDataDir(t *testing.T, name string, mode os.FileMode) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if err := os.Mkdir(dir, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, mode); err != nil { //nolint:gosec // G302: a row's directory is open to others on purpose.
		t.Fatal(err)
	}
	return dir
}

const commandVaultKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func serviceCommands(t *testing.T) []serviceCommand {
	configuration := map[string]string{"error.class": "config_error", "startup.cause": "configuration"}
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	get := func(url string, header http.Header) int {
		t.Helper()
		request, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header = header
		response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		return response.StatusCode
	}
	return []serviceCommand{
		{name: "git-proxy", pkg: "./services/git-proxy/cmd/git-proxy", workload: "git_proxy", ready: 1,
			env: func(_ *testing.T, addresses []string) map[string]string {
				return map[string]string{
					"TETRAL_GIT_PROXY_HTTP_ADDR":       addresses[0],
					"TETRAL_GIT_PROXY_METRICS_ADDR":    addresses[1],
					"TETRAL_GIT_PROXY_PUBLIC_BASE_URL": "https://git.tetral.test",
					"TETRAL_HTTP_TRANSPORT":            "plaintext",
					"ENGINE_VAULT_KEY":                 commandVaultKey,
				}
			},
			exits: []commandExit{
				{name: "vault key of the wrong length, before the database opens",
					set:  map[string]string{"ENGINE_VAULT_KEY": "short-secret-sentinel", "TETRAL_DATABASE_URL": refusedDatabaseURL},
					want: configuration, messageHas: []string{"ENGINE_VAULT_KEY must be 64 hex characters"}, absent: []string{"short-secret-sentinel"}},
			}},
		{name: "queue", pkg: "./services/queue/cmd/tetral-queue", workload: "queue", ready: 0,
			env: func(_ *testing.T, addresses []string) map[string]string {
				return map[string]string{"TETRAL_QUEUE_HTTP_ADDR": addresses[0], "TETRAL_QUEUE_GRPC_ADDR": addresses[1]}
			},
			exits: []commandExit{
				{name: "reclaim limit of zero", set: map[string]string{"TETRAL_QUEUE_LEASE_RECLAIM_LIMIT": "0"},
					want: configuration, messageHas: []string{"TETRAL_QUEUE_LEASE_RECLAIM_LIMIT must be a positive integer"}},
			}},
		{name: "api", pkg: "./services/api/cmd/tetral-api", workload: "api", ready: 0,
			env: func(t *testing.T, addresses []string) map[string]string {
				env := map[string]string{
					"TETRAL_API_HTTP_ADDR":                          addresses[0],
					"TETRAL_API_METRICS_ADDR":                       addresses[1],
					"TETRAL_HTTP_TRANSPORT":                         "plaintext",
					"ENGINE_VAULT_KEY":                              commandVaultKey,
					"ENGINE_DATA_DIR":                               commandDataDir(t, "data", 0o700),
					"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": signer.PublicKeyBase64(),
					"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF":       "ghcr.io/tetral-ai/sandbox:0.0.0-dev",
				}
				maps.Copy(env, commandBlobEnvironment())
				return env
			},
			exits: []commandExit{
				{name: "vault key of the wrong length, before the database opens",
					set:  map[string]string{"ENGINE_VAULT_KEY": "tetral_sk_short_vault_key_sentinel", "TETRAL_DATABASE_URL": refusedDatabaseURL},
					want: map[string]string{"error.class": "config_error"}, messageHas: []string{"ENGINE_VAULT_KEY must be 64 hex characters (32 bytes), got 34 characters"}, absent: []string{"tetral_sk_short_vault_key_sentinel"}},
				{name: "vault key of the right length that is not hexadecimal",
					set:  map[string]string{"ENGINE_VAULT_KEY": "tetralsk_zz_vault_sentinel_0000000000000000000000000000000000000"},
					want: map[string]string{"error.class": "config_error"}, messageHas: []string{"ENGINE_VAULT_KEY must be 64 hexadecimal characters (32 bytes); it contains non-hexadecimal characters"},
					absent: []string{"tetralsk_zz_vault_sentinel", "invalid hex key"}},
				{name: "data directory others can read", set: map[string]string{"ENGINE_DATA_DIR": commandDataDir(t, "tetral_sk_data_dir_sentinel", 0o755)},
					want: map[string]string{"error.class": "config_error"}, messageHas: []string{"ENGINE_DATA_DIR", "0755"}, absent: []string{"tetral_sk_data_dir_sentinel"}},
				{name: "event page limit of zero", set: map[string]string{"TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST": "0"},
					messageHas: []string{"runtime control configuration: TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST must be a positive integer"}},
				{name: "listen address that names no port", set: map[string]string{"TETRAL_API_HTTP_ADDR": "127.0.0.1:bad"},
					record: "workload.listener.failed", want: map[string]string{"listener.transport": "tcp", "startup.cause": "listener"}, messageHas: []string{"listen tcp: lookup tcp/bad: unknown port"}, absent: []string{`"listener.address"`}},
				{name: "environment stored in a shape this version cannot read",
					prepare: func(t *testing.T, db *commandDatabase) {
						if _, err := db.admin.Exec(`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ('default', 'env_retired_shape', 'env_retired_shape', '{"retired_networking":true}', now(), now())`); err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() {
							if _, err := db.admin.Exec(`DELETE FROM environments WHERE id = 'env_retired_shape'`); err != nil {
								t.Error(err)
							}
						})
					},
					want: map[string]string{"startup.cause": "schema"}, messageHas: []string{"retired networking rows require operator remediation"}},
			}},
		{name: "auth", pkg: "./services/auth/cmd/tetral-auth", workload: "auth", ready: 0,
			env: func(_ *testing.T, addresses []string) map[string]string {
				return map[string]string{
					"TETRAL_AUTH_HTTP_ADDR":                          addresses[0],
					"TETRAL_AUTH_METRICS_ADDR":                       addresses[1],
					"TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64": privateKey,
					"ENGINE_API_KEY":                                 strings.Repeat("o", auth.MinBootstrapKeyBytes),
					"ENGINE_BOOTSTRAP_WORKSPACE_ID":                  string(workspace.DefaultID),
				}
			},
			exits: []commandExit{
				{name: "no bootstrap Workspace", set: map[string]string{"ENGINE_BOOTSTRAP_WORKSPACE_ID": ""},
					want: map[string]string{"error.class": "config_error"}, messageHas: []string{"ENGINE_BOOTSTRAP_WORKSPACE_ID is required"}},
				{name: "TLS material that does not exist", set: map[string]string{
					"TETRAL_HTTP_TRANSPORT":           "native-mtls",
					"TETRAL_HTTP_TLS_CA_PATH":         "/nonexistent/ca.pem",
					"TETRAL_HTTP_TLS_CERT_PATH":       "/nonexistent/tls.crt",
					"TETRAL_HTTP_TLS_KEY_PATH":        "/nonexistent/tls.key",
					"TETRAL_HTTP_TLS_EDGE_CLIENT_URI": "spiffe://tetral.test/ns/envoy-gateway-system/sa/tetral-public-edge",
				}, want: map[string]string{"startup.cause": "configuration"}, messageHas: []string{"TLS mounted generation is unavailable"}},
			},
			// Metrics are served on their own listener only.
			serving: func(t *testing.T, _ *commandDatabase, addresses []string) func() {
				if public, metrics := get("http://"+addresses[0]+"/metrics", nil), get("http://"+addresses[1]+"/metrics", nil); public != http.StatusNotFound || metrics != http.StatusOK {
					t.Errorf("/metrics = %d on the public listener and %d on the metrics listener, want 404 and 200", public, metrics)
				}
				return func() {}
			}},
		{name: "event-stream", pkg: "./services/event-stream/cmd/event-stream", workload: "event_stream", databaseVariable: "TETRAL_EVENT_STREAM_DATABASE_URL", ready: 0,
			env: func(_ *testing.T, addresses []string) map[string]string {
				return map[string]string{
					"TETRAL_EVENT_STREAM_HTTP_ADDR":                 addresses[0],
					"TETRAL_EVENT_STREAM_METRICS_ADDR":              addresses[1],
					"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": signer.PublicKeyBase64(),
				}
			},
			exits: []commandExit{
				{name: "no key for the signed principal", set: map[string]string{"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": ""},
					want: map[string]string{"error.class": "config_error"}, messageHas: []string{"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64 is required"}},
			},
			// It serves the signed principal with no bootstrap key of its own.
			serving: func(t *testing.T, _ *commandDatabase, addresses []string) func() {
				const path = "/v1/sessions/sesn_missing/events"
				token, err := signer.Mint(auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.DefaultID}, "key_command_process"), http.MethodGet, path, "req_command_process", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if status := get("http://"+addresses[0]+path, http.Header{"X-Tetral-Internal-Principal": {token}}); status != http.StatusNotFound {
					t.Errorf("events of a Session that does not exist = %d, want 404", status)
				}
				return func() {}
			}},
		{name: "computer-hub", pkg: "./services/computer-hub/cmd/tetral-computer-hub", workload: "computer_hub", ready: 0,
			env: func(_ *testing.T, addresses []string) map[string]string {
				return hubCommandEnvironment(addresses, signer.PublicKeyBase64(), "computer-hub-0")
			}},
		{name: "bridge-api", component: "bridge", pkg: "./services/bridge/cmd/bridge-api", workload: "bridge", ready: 0,
			env: func(t *testing.T, addresses []string) map[string]string {
				token := filepath.Join(t.TempDir(), "token")
				if err := os.WriteFile(token, []byte("unused"), 0o600); err != nil {
					t.Fatal(err)
				}
				env := map[string]string{
					"TETRAL_BRIDGE_API_HTTP_ADDR":              addresses[0],
					"TETRAL_BRIDGE_API_GRPC_ADDR":              addresses[1],
					"TETRAL_BRIDGE_MCP_CONNECTOR_GRPC_ADDR":    "127.0.0.1:9",
					"TETRAL_BRIDGE_GATEWAY_TOKEN_PATH":         token,
					"TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY":    strings.Repeat("x", 32),
					"TETRAL_INTERNAL_GRPC_AUDIENCE":            "tetral-internal-grpc",
					"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral-agent-runtime/agent-runtime",
					// The TokenReview endpoint is asked only for a caller, and
					// these processes have none.
					"KUBERNETES_API_SERVER_URL":                   "https://127.0.0.1:9",
					"KUBERNETES_API_CA_CERT_PATH":                 filepath.Join(os.Getenv(envIsolatedTLSPGCerts), "ca.pem"),
					"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH": token,
				}
				maps.Copy(env, commandBlobEnvironment())
				return env
			},
			// While it serves, the command holds one connection listening for
			// execution results; its exit releases it.
			serving: func(t *testing.T, db *commandDatabase, _ []string) func() {
				listeners := func() int {
					t.Helper()
					var count int
					if err := db.admin.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND query = $1`,
						"LISTEN "+resultnotify.ExecutionResultNotificationChannel).Scan(&count); err != nil {
						t.Fatal(err)
					}
					return count
				}
				for deadline := time.Now().Add(10 * time.Second); listeners() != 1; time.Sleep(20 * time.Millisecond) {
					if time.Now().After(deadline) {
						t.Fatalf("connections listening for execution results = %d, want 1", listeners())
					}
				}
				return func() {
					if count := listeners(); count != 0 {
						t.Errorf("connections listening for execution results after the exit = %d, want 0", count)
					}
				}
			}},
		{name: "cleanup", pkg: "./services/cleanup/cmd/tetral-cleanup", workload: "cleanup", neverServes: true,
			env: func(*testing.T, []string) map[string]string { return commandBlobEnvironment() },
			exits: []commandExit{
				{name: "claim limit of zero", set: map[string]string{"TETRAL_CLEANUP_CLAIM_LIMIT": "0"},
					want: map[string]string{"error.class": "config_error"}, messageHas: []string{"TETRAL_CLEANUP_CLAIM_LIMIT must be a positive integer"}},
				{name: "argument that is no mode, before the database opens", arguments: []string{"sessions"}, set: map[string]string{"TETRAL_DATABASE_URL": refusedDatabaseURL},
					want: map[string]string{"error.class": "config_error"}, messageHas: []string{"tetral-cleanup takes no argument, or the argument drives"}},
				{name: "pass with nothing due", succeeds: true, record: "cleanup.claim_due.completed", want: map[string]string{"outcome": "completed"}},
				{name: "drive run in which one drive fails", arguments: []string{"drives"}, record: "drive.cleanup_failed", want: map[string]string{"drive.id": "drive_exit_failing"},
					prepare: func(t *testing.T, db *commandDatabase) {
						for _, statement := range []string{
							`INSERT INTO drives(workspace_id,drive_id) VALUES ('default','drive_exit_failing')`,
							`CREATE FUNCTION fail_drive_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected drive cleanup failure'; END$$`,
							`CREATE TRIGGER fail_drive_cleanup BEFORE UPDATE ON drives FOR EACH ROW WHEN (OLD.drive_id = 'drive_exit_failing') EXECUTE FUNCTION fail_drive_cleanup()`,
						} {
							if _, err := db.admin.Exec(statement); err != nil {
								t.Fatalf("%s: %v", statement, err)
							}
						}
					}},
			}},
		{name: "job-runner", pkg: "./services/job-runner/cmd/job-runner", workload: "job_runner", pod: true,
			env: func(_ *testing.T, addresses []string) map[string]string {
				env := map[string]string{
					"TETRAL_BRIDGE_JOB_RUNNER_HTTP_ADDR":               addresses[0],
					"TETRAL_QUEUE_GRPC_ADDR":                           "127.0.0.1:9",
					"TETRAL_KUBERNETES_NAMESPACE":                      "tetral-agent-runtime",
					"TETRAL_AGENT_RUNTIME_LABEL_SELECTOR":              "app=runtime",
					"TETRAL_BRIDGE_RUNTIME_POD_TOKEN_PATH":             "/runtime-token",
					"TETRAL_BRIDGE_JOB_RUNNER_MCP_CONNECTOR_GRPC_ADDR": "127.0.0.1:9",
					"TETRAL_BRIDGE_JOB_RUNNER_GATEWAY_TOKEN_PATH":      "/gateway-token",
					"KUBERNETES_SERVICE_HOST":                          "",
				}
				maps.Copy(env, commandBlobEnvironment())
				return env
			},
			exits: []commandExit{
				{name: "no Queue address", set: map[string]string{"TETRAL_QUEUE_GRPC_ADDR": ""},
					want:       map[string]string{"service.name": "job-runner", "component": "job-runner", "error.class": "config_error", "startup.cause": "configuration"},
					messageHas: []string{"TETRAL_QUEUE_GRPC_ADDR is required"}},
				{name: "one database connection, before the database opens", set: map[string]string{"TETRAL_DB_MAX_OPEN_CONNS": "1", "TETRAL_DATABASE_URL": refusedDatabaseURL},
					want: configuration, messageHas: []string{"TETRAL_DB_MAX_OPEN_CONNS must be at least 2"}},
				// With its database verified, the command asks for its
				// cluster and has none.
				{name: "no cluster", want: map[string]string{"startup.cause": "dependency_readiness"}, messageHas: []string{"KUBERNETES_SERVICE_HOST"}},
			}},
		{name: "provider-gateway", bun: "services/gateway/packages/provider-gateway/src/command.ts", workload: "provider_gateway",
			component: "gateway", refusal: "startup_failed", refusalClass: "startup_error", ready: 0, readyPath: "/readyz",
			unreachable: map[string]string{"startup.cause_category": "dependency_readiness"},
			env: func(t *testing.T, addresses []string) map[string]string {
				env := gatewayCommandEnvironment(t)
				env["TETRAL_PROVIDER_GATEWAY_HTTP_ADDR"], env["TETRAL_PROVIDER_GATEWAY_GRPC_ADDR"] = addresses[0], addresses[1]
				env["TETRAL_PROVIDER_GATEWAY_BRIDGE_TOKEN_PATH"] = env["KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH"]
				return env
			}},
		{name: "mcp-connector", bun: "services/gateway/packages/mcp-connector/src/command.ts", workload: "mcp_connector",
			component: "workload", refusal: "workload.command_failed", refusalClass: "process_error", ready: 0, readyPath: "/readyz",
			unreachable: map[string]string{"phase": "dependency"},
			env: func(t *testing.T, addresses []string) map[string]string {
				env := gatewayCommandEnvironment(t)
				env["TETRAL_MCP_CONNECTOR_HTTP_ADDR"], env["TETRAL_MCP_CONNECTOR_GRPC_ADDR"] = addresses[0], addresses[1]
				env["TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH"] = env["KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH"]
				env["TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS"] = "tetral/bridge"
				return env
			}},
	}
}

// gatewayCommandEnvironment is what both Gateway commands read: a cluster
// they never call while no request arrives, with its token and its
// certificate authority as files.
func gatewayCommandEnvironment(t *testing.T) map[string]string {
	t.Helper()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("reviewer-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"TETRAL_INTERNAL_GRPC_AUDIENCE":               "tetral-internal-grpc",
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS":    "tetral/runtime",
		"TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY":       strings.Repeat("k", 32),
		"TETRAL_BRIDGE_API_GRPC_ADDR":                 "127.0.0.1:9",
		"ENGINE_VAULT_KEY":                            commandVaultKey,
		"KUBERNETES_API_SERVER_URL":                   "https://127.0.0.1:9",
		"KUBERNETES_API_CA_CERT_PATH":                 filepath.Join(os.Getenv(envIsolatedTLSPGCerts), "ca.pem"),
		"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH": token,
	}
}

// refusedDatabaseURL names a database address nothing listens at, with
// credentials no output may hold.
const refusedDatabaseURL = "postgres://runtime:url-secret@127.0.0.1:9/tetral?sslpassword=url-secret"

// Each Engine command, started as its built binary or as Bun runs it,
// refuses a bad environment: it exits with status 1 after a record that
// names the failure and holds no secret of its environment. Its database is
// checked before it binds a listener: during the database rows another
// process holds its addresses. In its good environment every command but
// cleanup, which serves nothing, becomes ready, is still running one second
// after it became ready, and exits 0 within 20 seconds of the SIGTERM sent
// then; job-runner, which serves only inside a cluster, does so in a
// container laid out as its pod. A Go command writes one record per listener and opens each only once ready; a Bun command
// writes workload.started once, after its listeners, and never in a refused
// start.
func TestServiceCommandsRefuseABadEnvironmentAndEndOnSIGTERM(t *testing.T) {
	isolatedTLSPostgreSQLRoot(t, "service_command_assertion=refused_and_ended", isolatedTLSRootBudget, func(t *testing.T) {
		current, behind, unprepared := newCommandDatabase(t), newCommandDatabase(t), storagetest.NewEmptyPostgreSQLAdminDB(t)
		if _, err := behind.admin.Exec(`DELETE FROM tetral_schema_migrations WHERE version = (SELECT max(version) FROM tetral_schema_migrations)`); err != nil {
			t.Fatal(err)
		}
		// schema is what a refused start must leave as it found it.
		schema := func(db *sql.DB) (migrations, tables int) {
			t.Helper()
			if err := db.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'`).Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if tables > 0 {
				if err := db.QueryRow(`SELECT count(*) FROM tetral_schema_migrations`).Scan(&migrations); err != nil {
					t.Fatal(err)
				}
			}
			return migrations, tables
		}
		behindMigrations, behindTables := schema(behind.admin)
		for _, command := range serviceCommands(t) {
			t.Run(command.name, func(t *testing.T) {
				var binary string
				var entry []string
				// A Go command names what failed in its database; a Bun
				// command names the class of the failure alone.
				refused, unsafeRole, noSchema := []string{"127.0.0.1:9"}, "is unsafe for workspace RLS", "phase=verify_schema"
				if command.bun == "" {
					binary = builtCommand(t, command.pkg)
				} else {
					binary, entry = "bun", []string{filepath.Join(repoRootFromBridgeTest(t), command.bun)}
					refused, unsafeRole, noSchema = []string{"postgresql database could not be reached"}, "postgresql runtime role is not permitted", "postgresql schema registry is missing"
				}
				if command.component == "" {
					command.component = command.name
				}
				if command.databaseVariable == "" {
					command.databaseVariable = "TETRAL_DATABASE_URL"
				}
				if command.refusal == "" {
					command.refusal, command.refusalClass = "startup.failed", "startup_error"
				}
				if command.readyPath == "" {
					command.readyPath = "/ready"
				}
				// The addresses stay held while the shared database rows
				// run: a command that binds before its database checks fails
				// on its listener.
				addresses, release := publicReserveAddresses(t, 3)
				good := command.env(t, addresses)
				maps.Copy(good, current.env(t, command.databaseVariable, command.workload))
				good["TETRAL_DEPLOYMENT_ENVIRONMENT"], good["TETRAL_SERVICE_VERSION"] = "test", "process-test"
				class := map[string]string{"error.class": command.refusalClass}
				unreachable := map[string]string{"error.class": command.refusalClass, "component": command.component}
				maps.Copy(unreachable, command.unreachable)
				// A Go command names the startup step that failed.
				role, schemaRefused := maps.Clone(class), maps.Clone(class)
				if command.bun == "" {
					unreachable["startup.cause"], role["startup.cause"], schemaRefused["startup.cause"] = "dependency_readiness", "dependency_readiness", "schema"
				}
				shared := []commandExit{
					{name: "database that refuses the connection", set: map[string]string{command.databaseVariable: refusedDatabaseURL},
						want:       unreachable,
						messageHas: refused, absent: []string{"url-secret", "runtime role is not permitted"}},
					{name: "database role that bypasses row security", set: map[string]string{command.databaseVariable: storagetest.AdminDatabaseURL(t, current.admin)},
						want: role, messageHas: []string{unsafeRole}},
					{name: "database that was never prepared", set: map[string]string{command.databaseVariable: storagetest.RuntimeDatabaseURL(t, unprepared)},
						want: schemaRefused, messageHas: []string{noSchema}},
					{name: "schema behind the binary", set: behind.env(t, command.databaseVariable, command.workload),
						want: schemaRefused, messageHas: []string{"postgresql schema is behind this binary"}},
				}
				exits := append(shared, command.exits...)
				for i, row := range exits {
					if i == len(shared) {
						release()
					}
					t.Run(row.name, func(t *testing.T) {
						env := maps.Clone(good)
						maps.Copy(env, row.set)
						if row.prepare != nil {
							row.prepare(t, current)
						}
						status, output := runToExit(t, binary, env, 30*time.Second, append(entry, row.arguments...)...)
						if want := map[bool]int{true: 0, false: 1}[row.succeeds]; status != want {
							t.Fatalf("exit status = %d, want %d: %s", status, want, output)
						}
						msg := row.record
						if msg == "" {
							msg = command.refusal
						}
						record := outputRecord(t, output, msg)
						for field, want := range row.want {
							if record[field] != want {
								t.Errorf("%s %s = %v, want %s: %s", msg, field, record[field], want, output)
							}
						}
						message, _ := record["error.message"].(string)
						for _, part := range row.messageHas {
							if !strings.Contains(message, part) {
								t.Errorf("error.message = %q, want it to hold %q", message, part)
							}
						}
						for _, part := range row.absent {
							if strings.Contains(output, part) {
								t.Errorf("the output holds %q: %s", part, output)
							}
						}
						if command.bun != "" && outputRecords(output, "workload.started") != 0 {
							t.Errorf("a refused start wrote workload.started: %s", output)
						}
					})
				}
				if migrations, tables := schema(behind.admin); migrations != behindMigrations || tables != behindTables {
					t.Errorf("the schema behind the binary has %d migrations and %d tables after the refused start, had %d and %d", migrations, tables, behindMigrations, behindTables)
				}
				if _, tables := schema(unprepared); tables != 0 {
					t.Errorf("the database never prepared has %d tables after the refused start", tables)
				}
				release()
				if command.neverServes {
					return
				}
				var process servingProcess
				if command.pod {
					process = startPodCommand(t, command.name, command.pkg, good["TETRAL_KUBERNETES_NAMESPACE"], good)
				} else {
					process = startCommand(t, command.name, binary, good, entry...)
				}
				process.ready(t, "http://"+addresses[command.ready]+command.readyPath)
				process.runs(t, time.Second)
				ended := func() {}
				if command.serving != nil {
					ended = command.serving(t, current, addresses)
				}
				process.terminate(t, 20*time.Second)
				ended()
				output := process.written()
				for _, line := range strings.Split(output, "\n") {
					var record map[string]any
					if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "workload.listener.starting" && record["readiness.state"] != "ready" {
						t.Errorf("a listener started before the command was ready: %s", line)
					}
				}
				if started := outputRecords(output, "workload.started"); command.bun != "" && started != 1 {
					t.Errorf("the command wrote workload.started %d times, want once: %s", started, output)
				}
			})
		}
		t.Log("service_command_assertion=refused_and_ended")
	})
}

// Every container the manifests give a database is a command of the table
// above, so each is started against the databases it must refuse; a
// service's own manifests and the composed ones name the same containers.
func TestEveryDatabaseContainerIsAStartedCommand(t *testing.T) {
	root := repoRootFromBridgeTest(t)
	// Its command reads its provider's configuration before it opens the
	// database; its own tests hold its start.
	started := []string{"sandbox"}
	for _, command := range serviceCommands(t) {
		started = append(started, command.name)
	}
	slices.Sort(started)
	manifests := map[bool][]string{}
	for _, pattern := range []string{"deploy/kubernetes/*.yaml", "services/*/k8s/*.yaml", "services/*/k8s/*/*.yaml"} {
		files, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(pattern)))
		if err != nil || len(files) == 0 {
			t.Fatalf("%s: %d files, %v", pattern, len(files), err)
		}
		manifests[strings.HasPrefix(pattern, "services/")] = append(manifests[strings.HasPrefix(pattern, "services/")], files...)
	}
	for own, files := range manifests {
		if containers := databaseContainers(t, files); !slices.Equal(containers, started) {
			t.Errorf("the manifests (the services' own: %t) give a database to %v; the started commands are %v", own, containers, started)
		}
	}
}

var databaseSource = regexp.MustCompile(`(?i)database|postgres`)

// databaseContainers are the sorted names of the containers in files that
// read a database URL from their environment or from a source named for a
// database.
func databaseContainers(t *testing.T, files []string) []string {
	t.Helper()
	found := map[string]bool{}
	var reads func(node any) bool
	reads = func(node any) bool {
		switch value := node.(type) {
		case []any:
			return slices.ContainsFunc(value, reads)
		case map[string]any:
			name, _ := value["name"].(string)
			if name == "TETRAL_DATABASE_URL" || name == "TETRAL_POSTGRES_DSN" || name == "TETRAL_EVENT_STREAM_DATABASE_URL" {
				return true
			}
			for key, child := range value {
				reference, _ := child.(map[string]any)
				source, _ := reference["name"].(string)
				if strings.HasSuffix(key, "Ref") && databaseSource.MatchString(source) || reads(child) {
					return true
				}
			}
		}
		return false
	}
	var walk func(node any)
	walk = func(node any) {
		switch value := node.(type) {
		case []any:
			for _, child := range value {
				walk(child)
			}
		case map[string]any:
			name, named := value["name"].(string)
			if _, container := value["image"]; container && named && reads(value) {
				found[name] = true
			}
			for _, child := range value {
				walk(child)
			}
		}
	}
	for _, file := range files {
		content, err := os.Open(file) //nolint:gosec // A manifest of this repository.
		if err != nil {
			t.Fatal(err)
		}
		for decoder := yaml.NewDecoder(content); ; {
			var document any
			if err := decoder.Decode(&document); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("%s: %v", file, err)
			}
			walk(document)
		}
		_ = content.Close()
	}
	return slices.Sorted(maps.Keys(found))
}
