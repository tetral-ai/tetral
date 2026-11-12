package integration

import (
	"database/sql"
	"encoding/json"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	// component is the name the command's records carry, when it is not
	// name; databaseVariable names its database URL, when it is not
	// TETRAL_DATABASE_URL.
	component, databaseVariable string
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
	// scheduled run, or a command that serves only inside a cluster.
	neverServes bool
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
					record: "workload.listener.failed", want: map[string]string{"listener.transport": "tcp"}, messageHas: []string{"listen tcp: lookup tcp/bad: unknown port"}, absent: []string{`"listener.address"`}},
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
		{name: "job-runner", pkg: "./services/job-runner/cmd/job-runner", workload: "job_runner", neverServes: true,
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
	}
}

// refusedDatabaseURL names a database address nothing listens at, with
// credentials no output may hold.
const refusedDatabaseURL = "postgres://runtime:url-secret@127.0.0.1:9/tetral?sslpassword=url-secret"

// Each Engine command, started as its built binary, refuses a bad
// environment: it exits with status 1 after a record that names the failure
// and holds no secret of its environment. In its good environment it becomes
// ready, opens its listeners only once ready, does not exit on its own, and
// exits 0 within 20 seconds of SIGTERM.
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
				binary := builtCommand(t, command.pkg)
				if command.component == "" {
					command.component = command.name
				}
				if command.databaseVariable == "" {
					command.databaseVariable = "TETRAL_DATABASE_URL"
				}
				addresses, release := publicReserveAddresses(t, 3)
				release()
				good := command.env(t, addresses)
				maps.Copy(good, current.env(t, command.databaseVariable, command.workload))
				good["TETRAL_DEPLOYMENT_ENVIRONMENT"], good["TETRAL_SERVICE_VERSION"] = "test", "process-test"
				exits := append([]commandExit{
					{name: "database that refuses the connection", set: map[string]string{command.databaseVariable: refusedDatabaseURL},
						want:       map[string]string{"error.class": "startup_error", "error.message_safe": "startup failed", "component": command.component},
						messageHas: []string{"127.0.0.1:9"}, absent: []string{"url-secret"}},
					{name: "database role that bypasses row security", set: map[string]string{command.databaseVariable: storagetest.AdminDatabaseURL(t, current.admin)},
						want: map[string]string{"error.class": "startup_error"}, messageHas: []string{"is unsafe for workspace RLS"}},
					{name: "database that was never prepared", set: map[string]string{command.databaseVariable: storagetest.AdminDatabaseURL(t, unprepared)},
						want: map[string]string{"error.class": "startup_error"}, messageHas: []string{"phase=verify_schema"}},
					{name: "schema behind the binary", set: behind.env(t, command.databaseVariable, command.workload),
						want: map[string]string{"error.class": "startup_error"}, messageHas: []string{"postgresql schema is behind this binary"}},
				}, command.exits...)
				for _, row := range exits {
					t.Run(row.name, func(t *testing.T) {
						env := maps.Clone(good)
						maps.Copy(env, row.set)
						if row.prepare != nil {
							row.prepare(t, current)
						}
						status, output := runToExit(t, binary, env, 30*time.Second, row.arguments...)
						if want := map[bool]int{true: 0, false: 1}[row.succeeds]; status != want {
							t.Fatalf("exit status = %d, want %d: %s", status, want, output)
						}
						msg := row.record
						if msg == "" {
							msg = "startup.failed"
						}
						record := outputRecord(t, output, msg)
						for field, want := range row.want {
							if record[field] != want {
								t.Errorf("%s %s = %v, want %s: %s", msg, field, record[field], want, output)
							}
						}
						if msg == "startup.failed" && (record["listener.state"] != "not started" || record["readiness.state"] != "not ready") {
							t.Errorf("startup.failed = %v; want no listener started and not ready", record)
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
					})
				}
				if migrations, tables := schema(behind.admin); migrations != behindMigrations || tables != behindTables {
					t.Errorf("the schema behind the binary has %d migrations and %d tables after the refused start, had %d and %d", migrations, tables, behindMigrations, behindTables)
				}
				if _, tables := schema(unprepared); tables != 0 {
					t.Errorf("the database never prepared has %d tables after the refused start", tables)
				}
				if command.neverServes {
					return
				}
				process := startCommand(t, command.name, binary, good)
				process.ready(t, "http://"+addresses[command.ready]+"/ready")
				ended := func() {}
				if command.serving != nil {
					ended = command.serving(t, current, addresses)
				}
				process.terminate(t, 20*time.Second)
				ended()
				for _, line := range strings.Split(process.output.String(), "\n") {
					var record map[string]any
					if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "workload.listener.starting" && record["readiness.state"] != "ready" {
						t.Errorf("a listener started before the command was ready: %s", line)
					}
				}
			})
		}
		t.Log("service_command_assertion=refused_and_ended")
	})
}
