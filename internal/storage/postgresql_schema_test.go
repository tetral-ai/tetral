package storage_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

const schemaTestSensitiveSentinel = "do-not-leak-this-schema-helper"

func newIsolatedPostgreSQLSchemaDB(t testing.TB) (*sql.DB, string) {
	t.Helper()
	db := storagetest.NewPostgreSQLDB(t)
	var schemaName string
	if err := db.QueryRowContext(context.Background(), `SELECT current_schema()`).Scan(&schemaName); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	if schemaName == "" {
		t.Fatal("current_schema returned empty schema")
	}
	return db, schemaName
}

func TestPostgreSQLSchemaTestSetupRedactsMalformedQuerySecretDSN(t *testing.T) {
	malformedDSN := "postgres://%zz@host/tetral?sslpassword=" + schemaTestSensitiveSentinel
	output, err := runStorageSchemaTestWithTestDSN(t, malformedDSN)
	if err == nil {
		t.Fatal("expected schema test to fail for malformed PostgreSQL test DSN")
	}
	msg := string(output)
	if strings.Contains(msg, schemaTestSensitiveSentinel) {
		t.Errorf("schema test output leaked query secret sentinel: %q", msg)
	}
	if strings.Contains(msg, malformedDSN) {
		t.Errorf("schema test output leaked raw DSN: %q", msg)
	}
	if strings.Contains(msg, "postgres://") || strings.Contains(msg, "postgresql://") {
		t.Errorf("schema test output leaked PostgreSQL connection string fragment: %q", msg)
	}
	if !strings.Contains(msg, "TETRAL_TEST_DATABASE_URL") {
		t.Errorf("schema test output should still name the env var for recovery, got %q", msg)
	}
}

func runStorageSchemaTestWithTestDSN(t *testing.T, dsn string) ([]byte, error) {
	t.Helper()
	engineDir := engineDirForSchemaTest(t)
	cmd := exec.Command(
		"go",
		"test",
		"./internal/storage",
		"-run",
		"^TestInitializePostgreSQLSchemaCreatesVersionOneTables$",
		"-count=1",
	)
	cmd.Dir = engineDir
	cmd.Env = append(os.Environ(), storagetest.EnvTestDatabaseURL+"="+dsn)
	return cmd.CombinedOutput()
}

func engineDirForSchemaTest(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

func TestInitializePostgreSQLSchemaHasNoRemovedRuntimeDropSteps(t *testing.T) {
	// The schema-step tuples are named-literal pairs in source. Pinning the
	// absence by step-name prefix keeps this guard non-evasive: any reintroduced
	// removed-table drop step would be named with this prefix and fail here.
	source := readPostgreSQLSchemaSource(t)
	if strings.Contains(source, `"drop_removed`) {
		t.Fatal("postgresql_schema.go declares a removed-table drop schema step; removed worker tables have no pre-existing databases and must not be tombstoned")
	}
}

func TestInitializePostgreSQLSchemaDoesNotSeedHiddenDefaultWorkspace(t *testing.T) {
	source := readPostgreSQLSchemaSource(t)
	for _, forbidden := range []string{
		"seedDefaultWorkspace",
		`"seed_default_workspace"`,
		"VALUES ('default', 'workspace'",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("postgresql_schema.go contains hidden default workspace seed %q", forbidden)
		}
	}
}

func readPostgreSQLSchemaSource(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Join(filepath.Dir(thisFile), "postgresql_schema.go")
	// #nosec G304 -- path is the sibling schema source file under test.
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read postgresql_schema.go: %v", err)
	}
	return string(body)
}

func TestInitializePostgreSQLSchemaCreatesVersionOneTables(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	if err := storage.InitializePostgreSQLSchema(context.Background(), db); err != nil {
		t.Fatalf("InitializePostgreSQLSchema: %v", err)
	}
	var schema string
	if err := db.QueryRowContext(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	expected := expectedVersionOneControlPlaneTables()

	got := readBaseTableNames(t, db, schema)
	if !equalStringSlices(got, expected) {
		t.Errorf("tables in isolated schema = %v; want exactly %v", got, expected)
	}
	assertPrimaryKeyColumns(t, db, schema, "session_bridge_operations", []string{
		"workspace_id",
		"session_id",
		"session_thread_id",
		"operation",
		"source_kind",
		"idempotency_key",
	})
}

func TestWorkspaceIDColumnsHaveNoDefault(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	if defaults := readWorkspaceIDColumnDefaults(t, db, schema); len(defaults) != 0 {
		t.Fatalf("workspace_id columns with defaults = %v; ownership selectors must not silently default", defaults)
	}
}

func TestSessionThreadsSubAgentTaskNameShape(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_subagent_task", "sesn_subagent_task")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status,
			created_at, last_active_at, updated_at
		) VALUES (
			'workspace_subagent_task', 'thr_parent', 'sesn_subagent_task', 'main',
			'public', 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
			'2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("seed parent thread: %v", err)
	}
	insertSubagent := func(threadID string, taskName sql.NullString) error {
		t.Helper()
		var taskNameValue any
		if taskName.Valid {
			taskNameValue = taskName.String
		}
		_, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_threads (
				workspace_id, id, session_id, parent_thread_id, role, visibility, status,
				task_name, created_at, last_active_at, updated_at
			) VALUES (
				'workspace_subagent_task', $1, 'sesn_subagent_task', 'thr_parent',
				'subagent', 'public', 'idle', $2,
				'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
			)`,
			threadID,
			taskNameValue,
		)
		return err
	}
	if err := insertSubagent("thr_null_task", sql.NullString{}); err == nil {
		t.Fatal("subagent with NULL task_name inserted successfully; want invariant violation")
	}
	if err := insertSubagent("thr_empty_task", sql.NullString{String: "", Valid: true}); err == nil {
		t.Fatal("subagent with empty task_name inserted successfully; want invariant violation")
	}
	if err := insertSubagent("thr_task_a", sql.NullString{String: "investigate", Valid: true}); err != nil {
		t.Fatalf("insert first subagent task: %v", err)
	}
	if err := insertSubagent("thr_task_b", sql.NullString{String: "investigate", Valid: true}); err == nil {
		t.Fatal("duplicate subagent task_name inserted successfully; want unique violation")
	}
}

func TestTransientAttachmentThreadMustBelongToSession(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_attachment_fk", "sesn_attachment_a")
	seedStorageSchemaSession(t, admin, "workspace_attachment_fk", "sesn_attachment_b")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status,
			created_at, last_active_at, updated_at
		) VALUES (
			'workspace_attachment_fk', 'thr_attachment_a', 'sesn_attachment_a',
			'main', 'public', 'idle',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("seed attachment thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_transient_attachments (
			workspace_id, attachment_ref, session_id, session_thread_id,
			source_tool_use_event_id, blob_pointer, mime, status,
			expires_at, created_at, updated_at
		) VALUES (
			'workspace_attachment_fk', 'att_valid', 'sesn_attachment_a', 'thr_attachment_a',
			'sevt_attachment_valid', 'attachments/valid', 'image/png', 'active',
			'2026-01-01T00:15:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("valid transient attachment insert: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_transient_attachments (
			workspace_id, attachment_ref, session_id, session_thread_id,
			source_tool_use_event_id, blob_pointer, mime, status,
			expires_at, created_at, updated_at
		) VALUES (
			'workspace_attachment_fk', 'att_cross_session', 'sesn_attachment_b', 'thr_attachment_a',
			'sevt_attachment_cross', 'attachments/cross', 'image/png', 'active',
			'2026-01-01T00:15:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err == nil {
		t.Fatal("cross-session transient attachment insert succeeded; want FK failure")
	}
}

func TestSessionMessagesSourceEventProjectionIsDurablyUnique(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_message_source", "sesn_message_source")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status,
			created_at, last_active_at, updated_at
		) VALUES (
			'workspace_message_source', 'thr_message_source', 'sesn_message_source',
			'main', 'public', 'idle',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`,
	); err != nil {
		t.Fatalf("seed main thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status, task_name,
			created_at, last_active_at, updated_at
		) VALUES (
			'workspace_message_source', 'thr_message_source_other', 'sesn_message_source',
			'thr_message_source', 'subagent', 'public', 'idle', 'child',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`,
	); err != nil {
		t.Fatalf("seed child thread: %v", err)
	}
	insertMessage := func(messageID string, threadID string, sequence int, sourceEventID any) error {
		_, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_messages (
				workspace_id, session_id, session_thread_id, message_id, sequence, kind,
				data_json, source_event_id, created_at, updated_at
			) VALUES (
				'workspace_message_source', 'sesn_message_source', $1, $2, $3, 'assistant',
				'{}', $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
			)`,
			threadID,
			messageID,
			sequence,
			sourceEventID,
		)
		return err
	}
	if err := insertMessage("msg_source_first", "thr_message_source", 1, "evt_source_once"); err != nil {
		t.Fatalf("insert first source projection: %v", err)
	}
	if err := insertMessage("msg_source_duplicate", "thr_message_source", 2, "evt_source_once"); err == nil {
		t.Fatal("duplicate source_event_id projection inserted in same thread; want unique violation")
	}
	if err := insertMessage("msg_source_other_thread", "thr_message_source_other", 1, "evt_source_once"); err != nil {
		t.Fatalf("same source_event_id in different thread should be scoped independently: %v", err)
	}
	if err := insertMessage("msg_source_null_a", "thr_message_source", 3, nil); err != nil {
		t.Fatalf("insert first null source projection: %v", err)
	}
	if err := insertMessage("msg_source_null_b", "thr_message_source", 4, nil); err != nil {
		t.Fatalf("insert second null source projection: %v", err)
	}
}

func TestSessionMessagesModelRequestAssociationIsAssistantOnlyAndScopedUnique(t *testing.T) {
	_, admin, schema := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_message_model", "sesn_message_model")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status,
			created_at, last_active_at, updated_at
		) VALUES (
			'workspace_message_model', 'thr_message_model', 'sesn_message_model',
			'main', 'public', 'idle',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`,
	); err != nil {
		t.Fatalf("seed model association thread: %v", err)
	}
	insert := func(messageID string, sequence int, kind string, modelRequestID any) error {
		_, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_messages (
				workspace_id, session_id, session_thread_id, message_id, sequence, kind,
				data_json, model_request_id, created_at, updated_at
			) VALUES (
				'workspace_message_model', 'sesn_message_model', 'thr_message_model', $1, $2, $3,
				'{}', $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
			)`,
			messageID, sequence, kind, modelRequestID,
		)
		return err
	}
	if err := insert("msg_model_user", 1, "user", "mreq_user_forbidden"); err == nil {
		t.Fatal("user message accepted model_request_id; want assistant-only check failure")
	}
	if err := insert("msg_model_empty", 2, "assistant", ""); err == nil {
		t.Fatal("assistant message accepted empty model_request_id; want shape check failure")
	}
	if err := insert("msg_model_first", 3, "assistant", "mreq_unique"); err != nil {
		t.Fatalf("insert first assistant model association: %v", err)
	}
	if err := insert("msg_model_duplicate", 4, "assistant", "mreq_unique"); err == nil {
		t.Fatal("duplicate scoped assistant model association inserted; want unique violation")
	}
	if columns := readIndexColumns(t, admin, schema, "idx_session_messages_model_request_unique"); !equalStringSlices(columns, []string{"workspace_id", "session_id", "session_thread_id", "model_request_id"}) {
		t.Fatalf("model request unique index columns = %v; want scoped association", columns)
	}
	if predicate := readIndexPredicate(t, admin, schema, "idx_session_messages_model_request_unique"); !strings.Contains(predicate, "model_request_id IS NOT NULL") {
		t.Fatalf("model request unique index predicate = %q; want partial non-null association", predicate)
	}
}

func TestSessionRuntimeInboxKindShapeOnlyAllowsRuntimeInputKinds(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	definition := readCheckConstraintDefinition(t, db, schema, "session_runtime_inbox", "session_runtime_inbox_kind_shape")
	for _, required := range []string{
		"messages",
		"interrupt_control",
		"tool_confirmation",
		"task_notification",
		"agent_mail",
		"approval_review",
		"rejection",
	} {
		if !strings.Contains(definition, required) {
			t.Fatalf("session_runtime_inbox_kind_shape missing runtime input kind %q: %s", required, definition)
		}
	}
	for _, forbidden := range []string{
		"runtime_config_patch",
		"cleanup_session",
	} {
		if strings.Contains(definition, forbidden) {
			t.Fatalf("session_runtime_inbox_kind_shape still admits command-only kind %q: %s", forbidden, definition)
		}
	}
}

func TestSessionRuntimeInboxStatusShapeIncludesParkedDelivery(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	definition := readCheckConstraintDefinition(t, db, schema, "session_runtime_inbox", "session_runtime_inbox_status_shape")
	for _, required := range []string{
		"queued",
		"delivering",
		"accepted",
		"parked",
		"dead_lettered",
		"committed",
		"cancelled",
	} {
		if !strings.Contains(definition, required) {
			t.Fatalf("session_runtime_inbox_status_shape missing status %q: %s", required, definition)
		}
	}
	predicate := readIndexPredicate(t, db, schema, "idx_session_runtime_inbox_repair")
	for _, required := range []string{"queued", "delivering", "accepted", "parked", "dead_lettered"} {
		if !strings.Contains(predicate, required) {
			t.Fatalf("runtime inbox repair predicate missing status %q: %s", required, predicate)
		}
	}
}

func TestSessionRuntimeInboxBindingShapeRequiresCompleteDeliveryIdentity(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_inbox_binding", "sesn_inbox_binding")
	if _, err := admin.Exec(`INSERT INTO session_threads (
		workspace_id, id, session_id, role, visibility, status,
		created_at, last_active_at, updated_at
	) VALUES (
		'workspace_inbox_binding', 'thr_inbox_binding', 'sesn_inbox_binding',
		'main', 'public', 'idle', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("seed inbox binding thread: %v", err)
	}
	insert := func(id string, statusValue string, bindingID any, generation any, podUID any) error {
		_, err := admin.Exec(`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id,
			input_kind, event_ids_json, status, binding_id, binding_generation,
			target_pod_uid, created_at, updated_at
		) VALUES (
			'workspace_inbox_binding', 'sesn_inbox_binding', 'thr_inbox_binding', $1,
			'messages', '[]', $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
		)`, id, statusValue, bindingID, generation, podUID)
		return err
	}
	if err := insert("rin_inbox_queued_unbound", "queued", nil, nil, nil); err != nil {
		t.Fatalf("insert unbound queued inbox row: %v", err)
	}
	if err := insert("rin_inbox_delivering_bound", "delivering", "bind_inbox", int64(1), "pod_inbox"); err != nil {
		t.Fatalf("insert bound delivering inbox row: %v", err)
	}
	if err := insert("rin_inbox_cancelled_unbound", "cancelled", nil, nil, nil); err != nil {
		t.Fatalf("insert unbound cancelled inbox row: %v", err)
	}
	if err := insert("rin_inbox_cancelled_bound", "cancelled", "bind_inbox", int64(1), "pod_inbox"); err != nil {
		t.Fatalf("insert bound cancelled inbox row: %v", err)
	}
	if err := insert("rin_inbox_dead_lettered_unbound", "dead_lettered", nil, nil, nil); err != nil {
		t.Fatalf("insert unbound dead-lettered inbox row: %v", err)
	}
	if err := insert("rin_inbox_dead_lettered_bound", "dead_lettered", "bind_inbox", int64(1), "pod_inbox"); err != nil {
		t.Fatalf("insert bound dead-lettered inbox row: %v", err)
	}
	invalidShapes := []struct {
		name       string
		status     string
		bindingID  any
		generation any
		podUID     any
	}{
		{name: "queued cannot carry binding", status: "queued", bindingID: "bind_inbox", generation: int64(1), podUID: "pod_inbox"},
		{name: "delivering requires binding", status: "delivering"},
	}
	for _, statusValue := range []string{"parked", "cancelled", "dead_lettered"} {
		invalidShapes = append(invalidShapes,
			struct {
				name       string
				status     string
				bindingID  any
				generation any
				podUID     any
			}{name: statusValue + " identity without pod", status: statusValue, bindingID: "bind_inbox", generation: int64(1)},
			struct {
				name       string
				status     string
				bindingID  any
				generation any
				podUID     any
			}{name: statusValue + " identity without generation", status: statusValue, bindingID: "bind_inbox", podUID: "pod_inbox"},
			struct {
				name       string
				status     string
				bindingID  any
				generation any
				podUID     any
			}{name: statusValue + " identity without binding", status: statusValue, generation: int64(1), podUID: "pod_inbox"},
		)
	}
	for _, test := range invalidShapes {
		t.Run(test.name, func(t *testing.T) {
			if err := insert("rin_inbox_invalid_"+strings.ReplaceAll(test.name, " ", "_"), test.status, test.bindingID, test.generation, test.podUID); err == nil {
				t.Fatal("invalid inbox binding shape was accepted")
			}
		})
	}
}

func TestSandboxToolTerminalResultRequiresNonemptyDigest(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_tool_digest", "sesn_tool_digest")
	if _, err := admin.Exec(`INSERT INTO session_threads (
		workspace_id, id, session_id, role, visibility, status,
		created_at, last_active_at, updated_at
	) VALUES (
		'workspace_tool_digest', 'thr_tool_digest', 'sesn_tool_digest',
		'main', 'public', 'idle', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("seed tool digest thread: %v", err)
	}
	_, err := admin.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, model_tool_call_id,
		execution_state, execution_attempt_generation, result_json, result_digest,
		created_at, updated_at
	) VALUES (
		'workspace_tool_digest', 'sesn_tool_digest', 'thr_tool_digest', 'sevt_tool_digest', 'sandbox_tool',
		'input_hash', 'bash', '{}', 'committed', 'call_tool_digest',
		'terminal_unconsumed', 1, '{"status":"success"}', '',
		CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
	)`)
	if err == nil {
		t.Fatal("terminal sandbox result accepted an empty digest")
	}
}

func TestSessionBackgroundTaskStatusShapeIncludesTerminalFacts(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	definition := readCheckConstraintDefinition(t, db, schema, "session_background_tasks", "session_background_tasks_status_shape")
	for _, required := range []string{
		"running",
		"completed",
		"failed",
		"cancelled",
		"expired",
	} {
		if !strings.Contains(definition, "'"+required+"'::text") {
			t.Fatalf("session_background_tasks_status_shape = %q; missing %q", definition, required)
		}
	}
	for _, retired := range []string{"cancelled_by_cleanup", "stale"} {
		if strings.Contains(definition, "'"+retired+"'::text") {
			t.Fatalf("session_background_tasks_status_shape = %q; still admits retired %q", definition, retired)
		}
	}
}

func TestSessionBackgroundTaskStdinWriteSequenceIsDurableAndNonNegative(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	definition := readCheckConstraintDefinition(t, db, schema, "session_background_tasks", "session_background_tasks_stdin_write_sequence_shape")
	if !strings.Contains(definition, "stdin_write_sequence >= 0") {
		t.Fatalf("session_background_tasks_stdin_write_sequence_shape = %q; want nonnegative sequence", definition)
	}
	var columnDefault sql.NullString
	if err := db.QueryRow(
		`SELECT column_default
		   FROM information_schema.columns
		  WHERE table_schema = $1
		    AND table_name = 'session_background_tasks'
		    AND column_name = 'stdin_write_sequence'`,
		schema,
	).Scan(&columnDefault); err != nil {
		t.Fatalf("read stdin_write_sequence default: %v", err)
	}
	if !columnDefault.Valid || columnDefault.String != "0" {
		t.Fatalf("stdin_write_sequence default = %q; want 0", columnDefault.String)
	}
}

func TestSessionBackgroundTaskProviderMetadataIsObjectBoundedToFourKiB(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	definition := readCheckConstraintDefinition(t, db, schema, "session_background_tasks", "session_background_tasks_provider_metadata_shape")
	for _, required := range []string{"jsonb_typeof", "object", "octet_length", "4096"} {
		if !strings.Contains(definition, required) {
			t.Fatalf("session_background_tasks_provider_metadata_shape = %q; missing %q", definition, required)
		}
	}
}

func TestRequestUsageDetailsKindShapeOnlyAllowsRuntimeRequestKinds(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	definition := readCheckConstraintDefinition(t, db, schema, "request_usage_details", "request_usage_details_kind_shape")
	for _, required := range []string{
		"agent_provider_request",
		"compaction_summary",
		"approval_reviewer",
	} {
		if !strings.Contains(definition, required) {
			t.Fatalf("request_usage_details_kind_shape missing request kind %q: %s", required, definition)
		}
	}
	for _, forbidden := range []string{
		"model",
		"request_too_large",
	} {
		if strings.Contains(definition, forbidden) {
			t.Fatalf("request_usage_details_kind_shape still admits old/invalid value %q: %s", forbidden, definition)
		}
	}
}

func TestSessionsSchemaPinsRuntimeApprovalMode(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	columns := readColumnNames(t, db, schema, "sessions")
	if !slices.Contains(columns, "approval_mode") {
		t.Fatalf("sessions columns = %v; want approval_mode", columns)
	}
	definition := readCheckConstraintDefinition(t, db, schema, "sessions", "sessions_approval_mode_shape")
	for _, required := range []string{"full_access", "ask_for_approval", "approve_for_me"} {
		if !strings.Contains(definition, required) {
			t.Fatalf("sessions_approval_mode_shape missing %q: %s", required, definition)
		}
	}
}

func TestSessionsSchemaPinsAgentVersionID(t *testing.T) {
	_, admin, schema := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	columns := readColumnNames(t, admin, schema, "sessions")
	if !slices.Contains(columns, "agent_version_id") {
		t.Fatalf("sessions columns = %v; want agent_version_id", columns)
	}
	assertUniqueConstraintColumns(t, admin, schema, "agent_versions", []string{"workspace_id", "agent_id", "version", "id"})
	assertForeignKeyColumns(t, admin, schema, "sessions", "agent_versions", []string{"workspace_id", "agent_id", "agent_version", "agent_version_id"})

	seedStorageSchemaSession(t, admin, "workspace_agent_version_id", "sesn_agent_version_id")
	var got string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT agent_version_id
		   FROM sessions
		  WHERE workspace_id = 'workspace_agent_version_id'
		    AND id = 'sesn_agent_version_id'`,
	).Scan(&got); err != nil {
		t.Fatalf("read pinned agent_version_id: %v", err)
	}
	if got != "agv_sesn_agent_version_id" {
		t.Fatalf("agent_version_id = %q; want trigger-filled version row id", got)
	}

	var nullable string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_nullable
		   FROM information_schema.columns
		  WHERE table_schema = $1
		    AND table_name = 'sessions'
		    AND column_name = 'agent_version_id'`,
		schema,
	).Scan(&nullable); err != nil {
		t.Fatalf("read agent_version_id nullability: %v", err)
	}
	if nullable != "NO" {
		t.Fatalf("sessions.agent_version_id is_nullable = %q; want NO", nullable)
	}
}

func TestSessionEventsSchemaMatchesDraftLedger(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	columns := readColumnNames(t, db, schema, "session_events")
	wantColumns := []string{
		"workspace_id",
		"session_id",
		"session_thread_id",
		"event_id",
		"sequence",
		"revision",
		"type",
		"payload_json",
		"visibility",
		"session_visible",
		"latest_stream_position",
		"insert_stream_position",
		"runtime_write_id",
		"model_request_id",
		"stable_reasoning_json",
		"projection_json",
		"created_at",
		"updated_at",
		"processed_at",
	}
	if !equalStringSlices(columns, wantColumns) {
		t.Fatalf("session_events columns = %v; want %v", columns, wantColumns)
	}
	constraints := readCheckConstraintNames(t, db, schema, "session_events")
	for _, constraint := range constraints {
		if strings.Contains(constraint, "source") {
			t.Fatalf("session_events has source-shaped check constraint %q", constraint)
		}
	}
	predicate := readIndexPredicate(t, db, schema, "idx_session_events_pending_client")
	if predicate == "" || !strings.Contains(predicate, "processed_at IS NULL") || strings.Contains(predicate, "source") {
		t.Fatalf("pending index predicate = %q; want processed_at IS NULL without source filter", predicate)
	}
	if columns := readIndexColumns(t, db, schema, "idx_session_events_insert_stream_position"); !equalStringSlices(columns, []string{"workspace_id", "session_id", "insert_stream_position"}) {
		t.Fatalf("idx_session_events_insert_stream_position columns = %v; want workspace/session/insert_stream_position", columns)
	}
	assertTableRLSForced(t, db, schema, "session_events")
	assertPrimaryKeyColumns(t, db, schema, "session_events", []string{"event_id"})
	assertUniqueConstraintColumns(t, db, schema, "session_events", []string{"workspace_id", "event_id"})
	assertUniqueConstraintColumns(t, db, schema, "session_events", []string{"workspace_id", "session_id", "session_thread_id", "sequence"})
	definition := readConstraintDefinition(t, db, schema, "session_events", "session_events_thread_sequence_key", "u")
	if !strings.Contains(definition, "UNIQUE NULLS NOT DISTINCT") {
		t.Fatalf("session_events_thread_sequence_key = %q; want NULLS NOT DISTINCT", definition)
	}
}

func TestDraftDurableRuntimeTablesExist(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	required := map[string][]string{
		"session_event_stream_changes": {
			"workspace_id", "session_id", "stream_position", "event_id",
			"session_thread_id", "revision", "visibility", "session_visible", "changed_at",
		},
		"session_messages": {
			"workspace_id", "session_id", "session_thread_id", "message_id",
			"sequence", "kind", "data_json", "source_event_id", "last_event_id",
			"repair_key", "model_request_id", "created_at", "updated_at",
		},
		"session_pending_tool_uses": {
			"workspace_id", "session_id", "session_thread_id", "tool_use_event_id",
			"model_tool_call_id", "tool_name", "input_json", "decision", "deny_message", "status",
			"result_event_id", "created_at", "updated_at", "resolved_at",
		},
		"session_background_tasks": {
			"workspace_id", "session_id", "session_thread_id", "task_id",
			"source_tool_use_event_id", "binding_id", "sandbox_id", "provider", "binding_revision",
			"provider_session_id", "provider_command_id", "provider_command_metadata_json", "resource_roots_json",
			"stdin_write_sequence", "status", "terminal_result_json", "terminal_result_digest", "terminal_event_id",
			"reconcile_generation", "next_poll_at", "release_operation_id", "created_at", "updated_at", "terminal_at",
		},
		"session_runtime_inbox": {
			"workspace_id", "session_id", "session_thread_id", "runtime_input_id",
			"input_kind", "rejection_reason_code", "event_ids_json", "sequence_from", "sequence_to", "status",
			"binding_id", "binding_generation", "target_pod_uid", "created_at",
			"updated_at", "committed_at",
		},
		"session_runtime_status": {
			"workspace_id", "session_id", "status", "status_event_id", "idle_since",
			"running_since", "active_seconds_total",
			"cleanup_after", "cleanup_enqueued_at", "cleanup_claimed_at",
			"cleanup_job_id", "binding_id", "binding_generation", "created_at",
			"updated_at",
		},
		"session_turn_retries": {
			"workspace_id", "session_id", "session_thread_id", "provider_attempts",
			"compaction_attempts", "updated_at",
		},
		"session_bridge_operations": {
			"workspace_id", "session_id", "session_thread_id", "operation",
			"idempotency_key", "source_kind", "request_hash", "declaration_digest",
			"receipt_json", "ack_status", "runtime_input_id", "runtime_write_id",
			"error_code", "result_json", "stdin_write_seq", "created_at", "updated_at",
		},
		"session_thread_context_prefixes": {
			"workspace_id", "session_id", "child_thread_id", "parent_thread_id",
			"parent_boundary_event_id", "entries_json", "created_at",
			"consumed_by_checkpoint_message_id",
		},
		"session_runtime_tool_results": {
			"workspace_id", "session_id", "session_thread_id", "tool_use_event_id",
			"tool_kind", "normalized_input_hash", "tool_name", "input_json",
			"ack_status", "result_json", "model_tool_call_id", "execution_state",
			"execution_attempt_generation", "waiting_activation_operation_id",
			"waiting_materialization_operation_id", "authorized_binding_revision",
			"authorized_provider_resource_id", "preparation_deadline", "result_digest",
			"provider_command_reference_json", "cancel_requested_at", "cancel_state",
			"cancel_submitted_at", "consumed_by_terminal_event_id", "consumption_reason",
			"helper_recovery_count", "background_task_started", "task_id",
			"background_operation_kind", "background_operation_state", "background_request_id",
			"background_task_id", "background_max_output_tokens", "background_write_sequence",
			"memory_projection_state", "mcp_claim_status", "mcp_claim_owner_request_id",
			"mcp_claim_lease_expires_at", "created_at", "updated_at",
		},
		"session_git_tickets": {
			"workspace_id", "session_id", "ticket_id", "token_hash", "status",
			"created_at", "rotated_at",
		},
		"session_resource_prefix_gc": {
			"workspace_id", "session_id", "prefix", "status", "attempt_count",
			"last_attempt_at", "next_attempt_at", "completed_at", "last_error_kind",
			"created_at", "updated_at",
		},
		"session_output_captures": {
			"workspace_id", "session_id", "source_path", "last_file_id",
			"last_size_bytes", "last_sha256", "last_captured_at",
			"created_at", "updated_at",
		},
		"sandbox_output_capture_operations": {
			"workspace_id", "session_id", "session_thread_id", "finish_idle_write_id", "capture_generation",
			"state", "binding_id", "binding_generation", "logical_sandbox_id", "provider", "provider_resource_id", "sandbox_binding_revision", "manifest_json", "skipped_json",
			"scan_records_json", "failure_kind", "failure_detail", "outcome_state", "outcome_digest", "retain_until",
			"cleanup_generation", "created_at", "updated_at", "staged_at", "adopted_at", "cleaned_at",
		},
		"sandbox_output_capture_blobs": {
			"workspace_id", "session_id", "finish_idle_write_id", "capture_generation",
			"source_path", "blob_pointer", "size_bytes", "sha256", "state", "file_id",
			"created_at", "updated_at", "uploaded_at", "adopted_at",
		},
		"queue_jobs": {
			"id", "workspace_id", "kind", "partition_key", "queue_partition_sequence", "dedupe_key",
			"payload_version", "status", "payload_json", "priority",
			"lease_token", "leased_by", "leased_at", "leased_until",
			"attempt_count", "defer_count", "max_attempts", "available_at", "created_at",
			"updated_at", "acknowledged_at", "cancelled_at",
			"dead_lettered_at", "last_error_kind", "last_error_message",
		},
		"queue_partition_counters": {
			"workspace_id", "partition_key", "last_sequence", "created_at", "updated_at",
		},
	}
	for table, wantColumns := range required {
		t.Run(table, func(t *testing.T) {
			columns := readColumnNames(t, db, schema, table)
			if !equalStringSlices(columns, wantColumns) {
				t.Fatalf("%s columns = %v; want %v", table, columns, wantColumns)
			}
			assertTableRLSForced(t, db, schema, table)
		})
	}
}

func TestQueuePartitionSequenceSchema(t *testing.T) {
	_, admin, schema := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	assertQueuePartitionSequenceSchema(t, admin, schema)
}

func TestSandboxQueueSchemaCarriesClosedKindsMaintenanceAndCleanupIndexes(t *testing.T) {
	_, admin, schema := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	ctx := context.Background()

	sandboxKinds := []string{
		"sandbox_tool_execute",
		"sandbox_activate",
		"sandbox_materialize",
		"sandbox_release",
		"sandbox_tool_cancel",
		"sandbox_output_capture",
		"sandbox_output_capture_cleanup",
		"sandbox_memory_projection",
		"sandbox_background_command",
		"sandbox_background_reconcile",
	}
	kindConstraint := readCheckConstraintDefinition(t, admin, schema, "queue_jobs", "queue_jobs_kind_shape")
	for _, kind := range sandboxKinds {
		if !strings.Contains(kindConstraint, "'"+kind+"'") {
			t.Fatalf("queue_jobs kind constraint = %q; missing %s", kindConstraint, kind)
		}
	}

	for _, index := range []struct {
		name      string
		fragments []string
	}{
		{
			name: "idx_queue_jobs_sandbox_terminal_retention",
			fragments: []string{
				"COALESCE(acknowledged_at, cancelled_at, dead_lettered_at)",
				"status = ANY",
				"sandbox_tool_execute",
				"sandbox_background_reconcile",
			},
		},
		{
			name: "idx_queue_jobs_sandbox_session_cleanup",
			fragments: []string{
				"workspace_id",
				"payload_json",
				"session_id",
				"status",
				"sandbox_tool_execute",
				"sandbox_background_reconcile",
			},
		},
	} {
		var definition string
		if err := admin.QueryRowContext(ctx,
			`SELECT indexdef
			   FROM pg_indexes
			  WHERE schemaname = $1 AND tablename = 'queue_jobs' AND indexname = $2`,
			schema,
			index.name,
		).Scan(&definition); err != nil {
			t.Fatalf("read %s: %v", index.name, err)
		}
		for _, fragment := range index.fragments {
			if !strings.Contains(definition, fragment) {
				t.Fatalf("%s definition = %q; missing %q", index.name, definition, fragment)
			}
		}
		predicate := readIndexPredicate(t, admin, schema, index.name)
		if got := strings.Count(predicate, "sandbox_"); got != len(sandboxKinds) {
			t.Fatalf("%s Sandbox kind count = %d; want exact set of %d: %q", index.name, got, len(sandboxKinds), predicate)
		}
		if index.name == "idx_queue_jobs_sandbox_terminal_retention" {
			for _, status := range []string{"acknowledged", "cancelled", "dead_lettered"} {
				if !strings.Contains(definition, status) {
					t.Fatalf("%s definition = %q; missing terminal status %s", index.name, definition, status)
				}
			}
			for _, status := range []string{"pending", "leased"} {
				if strings.Contains(definition, "'"+status+"'") {
					t.Fatalf("%s definition = %q; unexpectedly indexes %s", index.name, definition, status)
				}
			}
		}
	}

	for _, table := range []string{"queue_jobs", "queue_partition_counters"} {
		var maintenancePolicyCount int
		if err := admin.QueryRowContext(ctx,
			`SELECT COUNT(*)
			   FROM pg_policies
			  WHERE schemaname = $1
			    AND tablename = $2
			    AND policyname = 'queue_maintenance'
			    AND qual LIKE '%tetral.queue_maintenance%'
			    AND with_check LIKE '%tetral.queue_maintenance%'`,
			schema, table,
		).Scan(&maintenancePolicyCount); err != nil {
			t.Fatalf("read %s maintenance policy: %v", table, err)
		}
		if maintenancePolicyCount != 1 {
			t.Fatalf("%s maintenance policy count = %d; want 1", table, maintenancePolicyCount)
		}
	}
}

func assertQueuePartitionSequenceSchema(t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	assertTableRLSForced(t, db, schema, "queue_partition_counters")
	assertTableRLSForced(t, db, schema, "queue_jobs")
	assertPrimaryKeyColumns(t, db, schema, "queue_partition_counters", []string{"workspace_id", "partition_key"})

	if columns := readIndexColumns(t, db, schema, "idx_queue_jobs_partition_sequence"); !equalStringSlices(columns, []string{"workspace_id", "partition_key", "queue_partition_sequence"}) {
		t.Fatalf("queue partition sequence index columns = %v; want workspace_id, partition_key, queue_partition_sequence", columns)
	}
	if !readIndexIsUnique(t, db, schema, "idx_queue_jobs_partition_sequence") {
		t.Fatal("queue partition sequence index is not unique")
	}
	if columns := readIndexColumns(t, db, schema, "idx_queue_jobs_available"); !equalStringSlices(columns, []string{
		"workspace_id", "kind", "status", "partition_key", "priority", "available_at", "queue_partition_sequence",
	}) {
		t.Fatalf("queue available index columns = %v; want queue scan order", columns)
	}
	if predicate := readIndexPredicate(t, db, schema, "idx_queue_jobs_available"); !strings.Contains(predicate, "status = 'pending'") {
		t.Fatalf("queue available index predicate = %q; want pending jobs only", predicate)
	}

	for _, constraint := range []struct {
		table    string
		name     string
		fragment string
	}{
		{table: "queue_partition_counters", name: "queue_partition_counters_partition_shape", fragment: "partition_key <> ''"},
		{table: "queue_partition_counters", name: "queue_partition_counters_sequence_shape", fragment: "last_sequence >= 0"},
		{table: "queue_jobs", name: "queue_jobs_partition_sequence_shape", fragment: "queue_partition_sequence > 0"},
		{table: "queue_jobs", name: "queue_jobs_defer_count_shape", fragment: "defer_count >= 0"},
	} {
		definition := readCheckConstraintDefinition(t, db, schema, constraint.table, constraint.name)
		if !strings.Contains(definition, constraint.fragment) {
			t.Fatalf("%s definition = %q; want %q", constraint.name, definition, constraint.fragment)
		}
	}

	var nullable string
	var defaultValue sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT is_nullable, column_default
		   FROM information_schema.columns
		  WHERE table_schema = $1
		    AND table_name = 'queue_jobs'
		    AND column_name = 'queue_partition_sequence'`,
		schema,
	).Scan(&nullable, &defaultValue); err != nil {
		t.Fatalf("read queue partition sequence column shape: %v", err)
	}
	if nullable != "NO" || defaultValue.Valid {
		t.Fatalf("queue partition sequence column nullable=%q default=%q; want required with no default", nullable, defaultValue.String)
	}
	if err := db.QueryRowContext(context.Background(),
		`SELECT is_nullable, column_default
		   FROM information_schema.columns
		  WHERE table_schema = $1
		    AND table_name = 'queue_jobs'
		    AND column_name = 'defer_count'`,
		schema,
	).Scan(&nullable, &defaultValue); err != nil {
		t.Fatalf("read queue defer count column shape: %v", err)
	}
	if nullable != "NO" || !defaultValue.Valid || defaultValue.String != "0" {
		t.Fatalf("queue defer count column nullable=%q default=%q; want required default zero", nullable, defaultValue.String)
	}

	const (
		workspaceID = "workspace_queue_sequence"
		partition   = "session:workspace_queue_sequence:sesn_queue_sequence"
		now         = "2026-07-29T00:00:00Z"
	)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO queue_partition_counters (
			workspace_id, partition_key, last_sequence, created_at, updated_at
		) VALUES ($1, $2, 1, $3, $3)`,
		workspaceID,
		partition,
		now,
	); err != nil {
		t.Fatalf("insert queue partition counter: %v", err)
	}
	insertJob := func(id string, sequence int64) error {
		t.Helper()
		_, err := db.ExecContext(context.Background(),
			`INSERT INTO queue_jobs (
				id, workspace_id, kind, partition_key, queue_partition_sequence,
				payload_version, status, payload_json, priority, attempt_count,
				defer_count, max_attempts, available_at, created_at, updated_at
			) VALUES ($1, $2, 'cleanup_session', $3, $4, 1, 'pending', '{}', 0, 0, 0, 10, $5, $5, $5)`,
			id,
			workspaceID,
			partition,
			sequence,
			now,
		)
		return err
	}
	if err := insertJob("qjob_queue_sequence_one", 1); err != nil {
		t.Fatalf("insert first queue sequence: %v", err)
	}
	if err := insertJob("qjob_queue_sequence_duplicate", 1); err == nil {
		t.Fatal("duplicate queue partition sequence inserted successfully")
	}
	if err := insertJob("qjob_queue_sequence_zero", 0); err == nil {
		t.Fatal("nonpositive queue partition sequence inserted successfully")
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO queue_jobs (
			id, workspace_id, kind, partition_key, queue_partition_sequence,
			payload_version, status, payload_json, priority, attempt_count,
			defer_count, max_attempts, available_at, created_at, updated_at
		) VALUES (
			'qjob_queue_negative_defer', $1, 'cleanup_session', $2, 2,
			1, 'pending', '{}', 0, 0, -1, 10, $3, $3, $3
		)`,
		workspaceID,
		partition,
		now,
	); err == nil {
		t.Fatal("negative queue defer count inserted successfully")
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO queue_partition_counters (
			workspace_id, partition_key, last_sequence, created_at, updated_at
		) VALUES ($1, 'session:invalid', -1, $2, $2)`,
		workspaceID,
		now,
	); err == nil {
		t.Fatal("negative queue partition counter inserted successfully")
	}
}

func TestPlatformProviderKeysSchemaMatchesGatewayPoolContract(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	columns := readColumnNames(t, db, schema, "platform_provider_keys")
	wantColumns := []string{
		"key_id",
		"provider_id",
		"encrypted_key",
		"weight",
		"priority",
		"cache_scope",
		"status",
		"disabled_reason",
		"updated_at",
	}
	if !equalStringSlices(columns, wantColumns) {
		t.Fatalf("platform_provider_keys columns = %v; want %v", columns, wantColumns)
	}
	for _, forbidden := range []string{"workspace_id", "session_id", "credential_id", "vault_id"} {
		if slices.Contains(columns, forbidden) {
			t.Fatalf("platform_provider_keys contains selector column %q; want platform-global key pool only", forbidden)
		}
	}
	assertPrimaryKeyColumns(t, db, schema, "platform_provider_keys", []string{"key_id"})
	if predicate := readIndexPredicate(t, db, schema, "idx_platform_provider_keys_provider_status"); predicate != "" {
		t.Fatalf("platform key provider/status index predicate = %q; want full reader index", predicate)
	}

	validRows := []struct {
		keyID      string
		providerID string
		status     string
	}{
		{keyID: "pfk_anthropic_1", providerID: "anthropic", status: "active"},
		{keyID: "pfk_openai_1", providerID: "openai", status: "disabled"},
		{keyID: "pfk_deepseek_1", providerID: "deepseek", status: "active"},
	}
	for _, row := range validRows {
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO platform_provider_keys (
				key_id, provider_id, encrypted_key, weight, priority, cache_scope, status, disabled_reason, updated_at
			) VALUES ($1, $2, decode('010203', 'hex'), 0, 1, 'cache_scope_a', $3, NULL, '2026-07-01T00:00:00Z')`,
			row.keyID,
			row.providerID,
			row.status,
		); err != nil {
			t.Fatalf("insert valid platform key row %#v: %v", row, err)
		}
	}

	invalidRows := []struct {
		name string
		sql  string
	}{
		{
			name: "wrong key prefix",
			sql: `INSERT INTO platform_provider_keys (key_id, provider_id, encrypted_key, cache_scope, updated_at)
			      VALUES ('key_anthropic', 'anthropic', decode('01', 'hex'), 'cache_scope_a', '2026-07-01T00:00:00Z')`,
		},
		{
			name: "unsupported provider",
			sql: `INSERT INTO platform_provider_keys (key_id, provider_id, encrypted_key, cache_scope, updated_at)
			      VALUES ('pfk_moonshotai', 'moonshotai', decode('01', 'hex'), 'cache_scope_a', '2026-07-01T00:00:00Z')`,
		},
		{
			name: "empty encrypted key",
			sql: `INSERT INTO platform_provider_keys (key_id, provider_id, encrypted_key, cache_scope, updated_at)
			      VALUES ('pfk_empty_key', 'anthropic', decode('', 'hex'), 'cache_scope_a', '2026-07-01T00:00:00Z')`,
		},
		{
			name: "negative weight",
			sql: `INSERT INTO platform_provider_keys (key_id, provider_id, encrypted_key, weight, cache_scope, updated_at)
			      VALUES ('pfk_negative_weight', 'anthropic', decode('01', 'hex'), -1, 'cache_scope_a', '2026-07-01T00:00:00Z')`,
		},
		{
			name: "negative priority",
			sql: `INSERT INTO platform_provider_keys (key_id, provider_id, encrypted_key, priority, cache_scope, updated_at)
			      VALUES ('pfk_negative_priority', 'anthropic', decode('01', 'hex'), -1, 'cache_scope_a', '2026-07-01T00:00:00Z')`,
		},
		{
			name: "empty cache scope",
			sql: `INSERT INTO platform_provider_keys (key_id, provider_id, encrypted_key, cache_scope, updated_at)
			      VALUES ('pfk_empty_cache_scope', 'anthropic', decode('01', 'hex'), '', '2026-07-01T00:00:00Z')`,
		},
		{
			name: "unsupported status",
			sql: `INSERT INTO platform_provider_keys (key_id, provider_id, encrypted_key, cache_scope, status, updated_at)
			      VALUES ('pfk_bad_status', 'anthropic', decode('01', 'hex'), 'cache_scope_a', 'quarantined', '2026-07-01T00:00:00Z')`,
		},
	}
	for _, row := range invalidRows {
		t.Run(row.name, func(t *testing.T) {
			if _, err := db.ExecContext(context.Background(), row.sql); err == nil {
				t.Fatalf("invalid platform provider key row %q inserted successfully", row.name)
			}
		})
	}
}

func TestCredentialsSchemaSupportsArchivedAndRevokedLifecycle(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	columns := readColumnNames(t, db, schema, "credentials")
	for _, column := range []string{"archived_at", "revoked_at"} {
		if !slices.Contains(columns, column) {
			t.Fatalf("credentials columns = %v; want lifecycle column %q", columns, column)
		}
	}
}

func TestCredentialsSchemaScopesIdentityByVault(t *testing.T) {
	_, admin, schema := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	assertPrimaryKeyColumns(t, admin, schema, "credentials", []string{"workspace_id", "vault_id", "id"})
	assertForeignKeyColumns(t, admin, schema, "session_provider_auth", "credentials", []string{"workspace_id", "vault_id", "credential_id"})
	ctx := context.Background()
	for _, vaultID := range []string{"vlt_schema_scope_a", "vlt_schema_scope_b"} {
		if _, err := admin.ExecContext(ctx,
			`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at)
			 VALUES ('workspace_schema_scope', $1, $1, '2026-07-06T00:00:00Z', '2026-07-06T00:00:00Z')`,
			vaultID,
		); err != nil {
			t.Fatalf("seed vault %s: %v", vaultID, err)
		}
		if _, err := admin.ExecContext(ctx,
			`INSERT INTO credentials (
				workspace_id, vault_id, id, display_name, auth_type, auth_public_json, provider_id, access_mode, created_at, updated_at
			) VALUES (
				'workspace_schema_scope', $1, 'cred_same_id', $1, 'provider_api_key', '{}', 'anthropic', 'user_api_key',
				'2026-07-06T00:00:00Z', '2026-07-06T00:00:00Z'
			)`,
			vaultID,
		); err != nil {
			t.Fatalf("seed credential in %s: %v", vaultID, err)
		}
	}
}

func TestSessionRuntimeToolResultsMemoryProjectionStateShape(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_tool_projection", "sesn_tool_projection")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
		) VALUES ('workspace_tool_projection', 'thr_tool_projection', 'sesn_tool_projection', 'main', 'public', 'idle',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed session thread: %v", err)
	}
	for _, state := range []any{nil, "pending", "refreshed", "skipped_cold", "failed"} {
		toolUseEventID := fmt.Sprintf("tool_%v", state)
		if state == nil {
			toolUseEventID = "tool_null"
		}
		if _, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_runtime_tool_results (
				workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
				normalized_input_hash, tool_name, input_json, ack_status, result_json,
				memory_projection_state, created_at, updated_at
			) VALUES (
				'workspace_tool_projection', 'sesn_tool_projection', 'thr_tool_projection', $1, 'memory',
				'hash_' || $1, 'memory', '{}', 'committed', '{"status":"completed"}',
				$2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
			)`,
			toolUseEventID,
			state,
		); err != nil {
			t.Fatalf("insert memory projection state %v: %v", state, err)
		}
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			memory_projection_state, created_at, updated_at
		) VALUES (
			'workspace_tool_projection', 'sesn_tool_projection', 'thr_tool_projection', 'tool_invalid', 'memory',
			'hash_invalid', 'memory', '{}', 'committed', '{"status":"completed"}',
			'invalid', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err == nil {
		t.Fatal("invalid memory_projection_state inserted successfully; want check constraint failure")
	}
}

func TestSessionRuntimeToolResultsKeepsSandboxExecutionFieldsKindScoped(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_tool_kind_scope", "sesn_tool_kind_scope")
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_threads (
		workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
	) VALUES ('workspace_tool_kind_scope', 'thr_tool_kind_scope', 'sesn_tool_kind_scope', 'main', 'public', 'idle',
		'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed session thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		created_at, updated_at
	) VALUES (
		'workspace_tool_kind_scope', 'sesn_tool_kind_scope', 'thr_tool_kind_scope', 'evt_mcp_null_result', 'mcp',
		'hash_mcp_null_result', 'mcp:server/tool', '{}', 'committed', NULL,
		'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
	)`); err == nil {
		t.Fatal("MCP row accepted a Sandbox-only nullable result body")
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		execution_state, execution_attempt_generation, created_at, updated_at
	) VALUES (
		'workspace_tool_kind_scope', 'sesn_tool_kind_scope', 'thr_tool_kind_scope', 'evt_memory_execution', 'memory',
		'hash_memory_execution', 'memory', '{}', 'committed', '{}', 'pending', 1,
		'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
	)`); err == nil {
		t.Fatal("memory row accepted Sandbox execution state")
	}
}

func TestSessionRuntimeToolResultsMCPClaimStateShape(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_mcp_claim", "sesn_mcp_claim")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
		) VALUES ('workspace_mcp_claim', 'thr_mcp_claim', 'sesn_mcp_claim', 'main', 'public', 'idle',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed session thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			created_at, updated_at
		) VALUES (
			'workspace_mcp_claim', 'sesn_mcp_claim', 'thr_mcp_claim', 'tool_memory_null_claim', 'memory',
			'hash_memory_null_claim', 'memory', '{}', 'committed', '{}',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("insert non-MCP row with null claim state: %v", err)
	}
	var claimStatus sql.NullString
	var claimOwner sql.NullString
	var claimLease sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'workspace_mcp_claim'
		    AND session_id = 'sesn_mcp_claim'
		    AND tool_use_event_id = 'tool_memory_null_claim'`).Scan(&claimStatus, &claimOwner, &claimLease); err != nil {
		t.Fatalf("read non-MCP claim state: %v", err)
	}
	if claimStatus.Valid || claimOwner.Valid || claimLease.Valid {
		t.Fatalf("non-MCP claim state = status=%+v owner=%+v lease=%+v; want all NULL", claimStatus, claimOwner, claimLease)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at,
			created_at, updated_at
		) VALUES (
			'workspace_mcp_claim', 'sesn_mcp_claim', 'thr_mcp_claim', 'tool_mcp_all_null', 'mcp',
			'hash_mcp_all_null', 'github/create_issue', '{}', 'committed', '{}',
			NULL, NULL, NULL,
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err == nil {
		t.Fatal("MCP all-NULL claim state inserted successfully; want check constraint failure")
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at,
			created_at, updated_at
		) VALUES (
			'workspace_mcp_claim', 'sesn_mcp_claim', 'thr_mcp_claim', 'tool_mcp_claim', 'mcp',
			'hash_mcp_claim', 'github/create_issue', '{}', 'committed', '{}',
			'in_flight', 'req_claim', '2026-01-01T00:03:00Z',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
	)`); err != nil {
		t.Fatalf("insert in-flight MCP claim: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			mcp_claim_status, created_at, updated_at
		) VALUES (
			'workspace_mcp_claim', 'sesn_mcp_claim', 'thr_mcp_claim', 'tool_mcp_consumed', 'mcp',
			'hash_mcp_consumed', 'github/create_issue', '{}', 'committed', '{}',
			'consumed', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("insert consumed MCP materialization: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at,
			created_at, updated_at
		) VALUES (
			'workspace_mcp_claim', 'sesn_mcp_claim', 'thr_mcp_claim', 'tool_memory_claim', 'memory',
			'hash_memory_claim', 'memory', '{}', 'committed', '{}',
			'stored', NULL, NULL,
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err == nil {
		t.Fatal("non-MCP stored MCP claim state inserted successfully; want check constraint failure")
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			mcp_claim_status, created_at, updated_at
		) VALUES (
			'workspace_mcp_claim', 'sesn_mcp_claim', 'thr_mcp_claim', 'tool_mcp_invalid', 'mcp',
			'hash_mcp_invalid', 'github/create_issue', '{}', 'committed', '{}',
			'in_flight', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err == nil {
		t.Fatal("MCP in-flight claim without lease owner inserted successfully; want check constraint failure")
	}
}

func TestSessionGitHubRepositoryResourceCheckoutShape(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_github_checkout", "sesn_github_checkout")
	insertResource := func(resourceID string) {
		t.Helper()
		if _, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_resources (
				workspace_id, session_id, resource_id, type, created_at, updated_at
			) VALUES (
				'workspace_github_checkout', 'sesn_github_checkout', $1, 'github_repository',
				'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
			)`,
			resourceID,
		); err != nil {
			t.Fatalf("seed github session resource %s: %v", resourceID, err)
		}
	}
	insertGitHub := func(resourceID string, mountPath any, checkoutType any, checkoutRef any) error {
		t.Helper()
		_, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_github_repository_resources (
				workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref,
				authorization_token_encrypted
			) VALUES (
				'workspace_github_checkout', 'sesn_github_checkout', $1,
				'https://github.com/tetral-ai/tetral.git', $2, $3, $4, decode('00', 'hex')
			)`,
			resourceID,
			mountPath,
			checkoutType,
			checkoutRef,
		)
		return err
	}
	for _, row := range []struct {
		resourceID   string
		mountPath    any
		checkoutType any
		checkoutRef  any
	}{
		{resourceID: "res_github_checkout_default", mountPath: "/workspace/tetral", checkoutType: nil, checkoutRef: nil},
		{resourceID: "res_github_checkout_branch", mountPath: "/workspace/tetral", checkoutType: "branch", checkoutRef: "main"},
		{resourceID: "res_github_checkout_commit", mountPath: "/workspace/tetral", checkoutType: "commit", checkoutRef: "0123456789abcdef0123456789abcdef01234567"},
	} {
		insertResource(row.resourceID)
		if err := insertGitHub(row.resourceID, row.mountPath, row.checkoutType, row.checkoutRef); err != nil {
			t.Fatalf("insert valid github checkout %s: %v", row.resourceID, err)
		}
	}
	insertResource("res_github_checkout_null_mount")
	if err := insertGitHub("res_github_checkout_null_mount", nil, nil, nil); err != nil {
		t.Fatalf("insert github checkout with null mount_path: %v", err)
	}
	for _, row := range []struct {
		resourceID   string
		checkoutType any
		checkoutRef  any
	}{
		{resourceID: "res_github_checkout_tag", checkoutType: "tag", checkoutRef: "v1.0.0"},
		{resourceID: "res_github_checkout_missing_ref", checkoutType: "branch", checkoutRef: nil},
		{resourceID: "res_github_checkout_ref_without_type", checkoutType: nil, checkoutRef: "main"},
		{resourceID: "res_github_checkout_empty_ref", checkoutType: "commit", checkoutRef: ""},
	} {
		insertResource(row.resourceID)
		if err := insertGitHub(row.resourceID, "/workspace/tetral", row.checkoutType, row.checkoutRef); err == nil {
			t.Fatalf("insert invalid github checkout %s succeeded; want checkout shape constraint failure", row.resourceID)
		}
	}
}

func TestSessionResourcePrimaryKeysMatchDraftLedger(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	assertPrimaryKeyColumns(t, db, schema, "session_resources", []string{"workspace_id", "session_id", "resource_id"})
	assertPrimaryKeyColumns(t, db, schema, "session_file_resources", []string{"workspace_id", "session_id", "resource_id"})
	assertPrimaryKeyColumns(t, db, schema, "session_memory_store_resources", []string{"workspace_id", "session_id", "resource_id"})
	assertPrimaryKeyColumns(t, db, schema, "session_github_repository_resources", []string{"workspace_id", "session_id", "resource_id"})
}

func TestSessionResourceIdentityIsSessionScoped(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_resource_identity", "sesn_resource_identity_a")
	seedStorageSchemaSession(t, admin, "workspace_resource_identity", "sesn_resource_identity_b")
	for _, sessionID := range []string{"sesn_resource_identity_a", "sesn_resource_identity_b"} {
		if _, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_resources (
				workspace_id, session_id, resource_id, type, created_at, updated_at
			) VALUES (
				'workspace_resource_identity', $1, 'sesrsc_same_id', 'file',
				'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
			)`,
			sessionID,
		); err != nil {
			t.Fatalf("insert session-scoped resource for %s: %v", sessionID, err)
		}
	}
}

func TestSessionEventsWorkspaceIsolation(t *testing.T) {
	runtimeDB, adminDB, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, adminDB, "workspace_events_a", "sesn_events_a")
	seedStorageSchemaSession(t, adminDB, "workspace_events_b", "sesn_events_b")
	mustInsertSessionEventLedgerRow(t, adminDB, "workspace_events_a", "sesn_events_a", "sevt_events_a", 1)
	mustInsertSessionEventLedgerRow(t, adminDB, "workspace_events_b", "sesn_events_b", "sevt_events_b", 1)

	errCrossWorkspaceRejected := errors.New("cross-workspace event write rejected")
	err := storage.WithWorkspaceTx(context.Background(), runtimeDB, "workspace_events_a", func(tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events`).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("workspace_events_a sees %d events; want 1", count)
		}
		result, err := tx.ExecContext(context.Background(),
			`INSERT INTO session_events (workspace_id, session_id, event_id, sequence, type, payload_json, created_at, updated_at)
			 VALUES ('workspace_events_b', 'sesn_events_b', 'sevt_events_b_cross', 2, 'user.message', '{}', '2026-06-09T10:01:00Z', '2026-06-09T10:01:00Z')`)
		if err == nil {
			affected, _ := result.RowsAffected()
			return fmt.Errorf("cross-workspace event write succeeded rows=%d; want RLS failure", affected)
		}
		return errCrossWorkspaceRejected
	})
	if !errors.Is(err, errCrossWorkspaceRejected) {
		t.Fatalf("workspace isolation: %v", err)
	}
}

func TestSessionRuntimeBindingsSchemaShapeAndRLS(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	columns := readColumnNames(t, db, schema, "session_runtime_bindings")
	wantColumns := []string{
		"workspace_id",
		"session_id",
		"binding_id",
		"binding_generation",
		"agent_runtime_namespace",
		"agent_runtime_pod_name",
		"agent_runtime_pod_uid",
		"agent_runtime_pod_ip",
		"bound_at",
		"updated_at",
	}
	if !equalStringSlices(columns, wantColumns) {
		t.Fatalf("session_runtime_bindings columns = %v; want %v", columns, wantColumns)
	}
	assertTableRLSForced(t, db, schema, "session_runtime_bindings")
	assertPrimaryKeyColumns(t, db, schema, "session_runtime_bindings", []string{"workspace_id", "session_id"})
	assertForeignKeyCascade(t, db, schema, "session_runtime_bindings", "sessions")
	for _, forbidden := range []string{"worker", "fallback", "snapshot", "status", "idle", "expires"} {
		for _, column := range columns {
			if strings.Contains(column, forbidden) {
				t.Fatalf("session_runtime_bindings column %q contains forbidden binding concept %q", column, forbidden)
			}
		}
	}
}

func TestSandboxBindingAndLifecycleOperationSchemaShape(t *testing.T) {
	_, db, schema := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	for _, table := range []string{"session_sandbox_bindings", "sandbox_lifecycle_operations"} {
		assertTableRLSForced(t, db, schema, table)
	}
	assertPrimaryKeyColumns(t, db, schema, "session_sandbox_bindings", []string{"workspace_id", "session_id"})
	assertUniqueConstraintColumns(t, db, schema, "session_sandbox_bindings", []string{"workspace_id", "logical_sandbox_id"})
	if columns := readIndexColumns(t, db, schema, "idx_session_sandbox_bindings_provider_resource_unique"); !equalStringSlices(columns, []string{"provider", "provider_resource_id"}) {
		t.Fatalf("provider-resource index columns = %v", columns)
	}
	if !readIndexIsUnique(t, db, schema, "idx_session_sandbox_bindings_provider_resource_unique") {
		t.Fatal("provider-resource index is not unique")
	}
	if predicate := readIndexPredicate(t, db, schema, "idx_session_sandbox_bindings_provider_resource_unique"); !strings.Contains(predicate, "provider_resource_id IS NOT NULL") {
		t.Fatalf("provider-resource index predicate = %q", predicate)
	}
	assertPrimaryKeyColumns(t, db, schema, "sandbox_lifecycle_operations", []string{"workspace_id", "operation_id"})
	if !slices.Contains(readColumnNames(t, db, schema, "sandbox_lifecycle_operations"), "materialization_resources_json") {
		t.Fatal("sandbox lifecycle operations is missing immutable materialization resources")
	}
	if !slices.Contains(readColumnNames(t, db, schema, "sessions"), "sandbox_resource_revision") {
		t.Fatal("sessions is missing sandbox_resource_revision")
	}

	seedStorageSchemaSession(t, db, "workspace_sandbox_binding", "sesn_sandbox_binding")
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, binding_revision,
		materialized_resource_revision, resource_roots_json,
		provider_metadata_json, created_at, updated_at
	) VALUES (
		'workspace_sandbox_binding', 'sesn_sandbox_binding', 'sbox_binding',
		'env_sesn_sandbox_binding', 1, 'daytona', 1, 0, '[]', '{}',
		'2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z'
	)`); err != nil {
		t.Fatalf("insert daytona binding: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_sandbox_bindings SET provider = 'tetral' WHERE workspace_id = 'workspace_sandbox_binding' AND session_id = 'sesn_sandbox_binding'`); err == nil {
		t.Fatal("retired tetral provider label was accepted")
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_sandbox_bindings SET provider_resource_id = 'sandbox_shared_provider_id' WHERE workspace_id = 'workspace_sandbox_binding' AND session_id = 'sesn_sandbox_binding'`); err != nil {
		t.Fatalf("set first provider resource id: %v", err)
	}
	seedStorageSchemaSession(t, db, "workspace_sandbox_binding_other", "sesn_sandbox_binding_other")
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		materialized_resource_revision, resource_roots_json,
		provider_metadata_json, created_at, updated_at
	) VALUES (
		'workspace_sandbox_binding_other', 'sesn_sandbox_binding_other', 'sbox_binding_other',
		'env_sesn_sandbox_binding_other', 1, 'daytona', 'sandbox_shared_provider_id', 1,
		0, '[]', '{}', '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z'
	)`); err == nil {
		t.Fatal("one provider resource was accepted by two workspaces")
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		observed_binding_revision, target_environment_generation,
		provider_create_name, provider_request_labels_json,
		queue_job_id, queue_kind, queue_partition_key, queue_dedupe_key,
		created_at, updated_at
	) VALUES (
		'workspace_sandbox_binding', 'sop_create', 'sesn_sandbox_binding', 'sbox_binding',
		'create', 'pending', 1, 1, 'tetral-sbox-binding', '{"workspace_id":"workspace_sandbox_binding"}',
		'qjob_create', 'sandbox_activate', 'sandbox-lifecycle:workspace_sandbox_binding:sbox_binding',
		'sandbox_activate:workspace_sandbox_binding:sbox_binding:sop_create',
		'2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z'
	)`); err != nil {
		t.Fatalf("insert lifecycle operation: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE sandbox_lifecycle_operations SET state = 'mystery' WHERE workspace_id = 'workspace_sandbox_binding' AND operation_id = 'sop_create'`); err == nil {
		t.Fatal("unknown lifecycle state was accepted")
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE sandbox_lifecycle_operations SET state = 'waiting_activation' WHERE workspace_id = 'workspace_sandbox_binding' AND operation_id = 'sop_create'`); err == nil {
		t.Fatal("materialization-only state was accepted for an activation operation")
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE sandbox_lifecycle_operations
		SET state = 'completed', completed_at = '2026-07-31T00:01:00Z'
		WHERE workspace_id = 'workspace_sandbox_binding' AND operation_id = 'sop_create'`); err != nil {
		t.Fatalf("complete first lifecycle operation: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		observed_binding_revision, target_environment_generation,
		provider_create_name, provider_request_labels_json,
		created_at, updated_at
	) VALUES (
		'workspace_sandbox_binding', 'sop_waiting_artifact', 'sesn_sandbox_binding', 'sbox_binding',
		'create', 'waiting_artifact', 1, 1, 'sbox_binding', '{"workspace_id":"workspace_sandbox_binding"}',
		'2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z'
	)`); err != nil {
		t.Fatalf("insert lifecycle operation before queue notification: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE sandbox_lifecycle_operations
		SET queue_job_id = 'qjob_partial'
		WHERE workspace_id = 'workspace_sandbox_binding' AND operation_id = 'sop_waiting_artifact'`); err == nil {
		t.Fatal("partial lifecycle queue identity was accepted")
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		target_provider_resource_id, release_reason,
		queue_job_id, queue_kind, queue_partition_key, queue_dedupe_key,
		created_at, updated_at
	) VALUES (
		'workspace_sandbox_binding', 'sop_release', 'sesn_sandbox_binding', 'sbox_binding',
		'release', 'pending', 'provider_old', 'replaced_handle',
		'qjob_release', 'sandbox_release', 'sandbox-lifecycle:workspace_sandbox_binding:sbox_binding',
		'sandbox_release:workspace_sandbox_binding:sbox_binding:sop_release',
		'2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z'
	)`); err != nil {
		t.Fatalf("insert replaced-handle release: %v", err)
	}
}

func TestSandboxExecutionHandoffSchemaShape(t *testing.T) {
	_, db, schema := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, db, "workspace_sandbox_execution", "sesn_sandbox_execution")
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_threads (
		workspace_id, session_id, id, role, status, visibility, created_at, last_active_at, updated_at
	) VALUES (
		'workspace_sandbox_execution', 'sesn_sandbox_execution', 'thr_sandbox_execution',
		'main', 'idle', 'internal', '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z'
	)`); err != nil {
		t.Fatalf("insert execution thread: %v", err)
	}
	columns := readColumnNames(t, db, schema, "session_runtime_tool_results")
	for _, column := range []string{
		"model_tool_call_id", "execution_state", "execution_attempt_generation",
		"waiting_activation_operation_id", "waiting_materialization_operation_id",
		"authorized_binding_revision", "authorized_provider_resource_id",
		"preparation_deadline", "result_digest", "provider_command_reference_json",
		"cancel_requested_at", "cancel_state", "cancel_submitted_at",
		"consumed_by_terminal_event_id", "consumption_reason", "helper_recovery_count",
	} {
		if !slices.Contains(columns, column) {
			t.Fatalf("session_runtime_tool_results is missing %s", column)
		}
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		model_tool_call_id, execution_state, execution_attempt_generation,
		created_at, updated_at
	) VALUES (
		'workspace_sandbox_execution', 'sesn_sandbox_execution', 'thr_sandbox_execution',
		'evt_sandbox_execution', 'sandbox_tool', 'input_hash', 'bash', '{}', 'committed', NULL,
		'call_sandbox_execution', 'pending', 1,
		'2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z'
	)`); err != nil {
		t.Fatalf("insert pending Sandbox execution handoff: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_tool_results
		SET cancel_state = 'submitted'
		WHERE workspace_id = 'workspace_sandbox_execution'
		  AND session_id = 'sesn_sandbox_execution'
		  AND session_thread_id = 'thr_sandbox_execution'
		  AND tool_use_event_id = 'evt_sandbox_execution'`); err == nil {
		t.Fatal("submitted cancellation without request/submission timestamps was accepted")
	}
	assertSandboxExecutionShapeRejected(t, db, `UPDATE session_runtime_tool_results
		SET execution_state = 'consumed'
		WHERE workspace_id = 'workspace_sandbox_execution'
		  AND session_id = 'sesn_sandbox_execution'
		  AND session_thread_id = 'thr_sandbox_execution'
		  AND tool_use_event_id = 'evt_sandbox_execution'`, "consumed execution without terminal event and reason")
	assertSandboxExecutionShapeRejected(t, db, `UPDATE session_runtime_tool_results
		SET consumed_by_terminal_event_id = 'evt_missing', consumption_reason = 'runtime_terminated'
		WHERE workspace_id = 'workspace_sandbox_execution'
		  AND session_id = 'sesn_sandbox_execution'
		  AND session_thread_id = 'thr_sandbox_execution'
		  AND tool_use_event_id = 'evt_sandbox_execution'`, "non-consumed execution with consumption fields")
	mustInsertSessionEventLedgerRow(t, db, "workspace_sandbox_execution", "sesn_sandbox_execution", "evt_sandbox_consumed", 1)
	assertSandboxExecutionShapeRejected(t, db, `UPDATE session_runtime_tool_results
		SET execution_state = 'consumed', result_digest = '',
		    consumed_by_terminal_event_id = 'evt_sandbox_consumed', consumption_reason = 'runtime_terminated'
		WHERE workspace_id = 'workspace_sandbox_execution'
		  AND session_id = 'sesn_sandbox_execution'
		  AND session_thread_id = 'thr_sandbox_execution'
		  AND tool_use_event_id = 'evt_sandbox_execution'`, "consumed execution with empty digest")
}

func assertSandboxExecutionShapeRejected(t *testing.T, db *sql.DB, statement string, description string) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin %s transaction: %v", description, err)
	}
	_, executionErr := tx.ExecContext(context.Background(), statement)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback %s transaction: %v", description, err)
	}
	if executionErr == nil {
		t.Fatalf("schema accepted %s", description)
	}
}

func assertSessionMCPManifestsSchemaShapeAndRLS(t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	columns := readColumnNames(t, db, schema, "session_mcp_manifests")
	wantColumns := []string{
		"workspace_id",
		"session_id",
		"mcp_server_name",
		"tools_json",
		"manifest_etag",
		"manifest_generation",
		"readiness",
		"diagnostic",
		"created_at",
		"updated_at",
	}
	if !equalStringSlices(columns, wantColumns) {
		t.Fatalf("session_mcp_manifests columns = %v; want %v", columns, wantColumns)
	}
	assertTableRLSForced(t, db, schema, "session_mcp_manifests")
	assertPrimaryKeyColumns(t, db, schema, "session_mcp_manifests", []string{"workspace_id", "session_id", "mcp_server_name"})
	assertForeignKeyCascade(t, db, schema, "session_mcp_manifests", "sessions")
	if definition := readCheckConstraintDefinition(t, db, schema, "session_mcp_manifests", "session_mcp_manifests_tools_json_shape"); !strings.Contains(definition, "octet_length(tools_json) <= 262144") || !strings.Contains(definition, "jsonb_typeof((tools_json)::jsonb) = 'array'") {
		t.Fatalf("session_mcp_manifests_tools_json_shape = %q; want JSON array and 256 KiB byte bound", definition)
	}
	if definition := readCheckConstraintDefinition(t, db, schema, "session_mcp_manifests", "session_mcp_manifests_generation_shape"); !strings.Contains(definition, "manifest_generation > 0") {
		t.Fatalf("session_mcp_manifests_generation_shape = %q; want positive generation", definition)
	}
	if definition := readCheckConstraintDefinition(t, db, schema, "session_mcp_manifests", "session_mcp_manifests_readiness_shape"); !strings.Contains(definition, "readiness = 'ready'") || !strings.Contains(definition, "readiness = 'unready'") || !strings.Contains(definition, "diagnostic IS NULL") || !strings.Contains(definition, "diagnostic IS NOT NULL") {
		t.Fatalf("session_mcp_manifests_readiness_shape = %q; want ready/unready diagnostic invariant", definition)
	}
	for _, column := range []string{"tools_json", "manifest_etag", "diagnostic"} {
		var nullable string
		if err := db.QueryRow(`SELECT is_nullable FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'session_mcp_manifests' AND column_name = $2`, schema, column).Scan(&nullable); err != nil {
			t.Fatalf("read session_mcp_manifests.%s nullability: %v", column, err)
		}
		if nullable != "YES" {
			t.Fatalf("session_mcp_manifests.%s nullable = %q, want YES", column, nullable)
		}
	}
	if columns := readIndexColumns(t, db, schema, "idx_session_mcp_manifests_session_generation"); !equalStringSlices(columns, []string{"workspace_id", "session_id", "manifest_generation"}) {
		t.Fatalf("idx_session_mcp_manifests_session_generation columns = %v; want workspace/session/generation", columns)
	}
}

func TestSessionEventIdempotencySchemaShapeAndRLS(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)
	columns := readColumnNames(t, db, schema, "session_event_idempotency_keys")
	wantColumns := []string{
		"workspace_id",
		"session_id",
		"idempotency_key_digest",
		"canonical_request_hash",
		"response_events_json",
		"created_at",
		"updated_at",
	}
	if !equalStringSlices(columns, wantColumns) {
		t.Fatalf("session_event_idempotency_keys columns = %v; want %v", columns, wantColumns)
	}
	for _, forbidden := range []string{"raw", "key_value", "api_key", "authorization", "bearer", "credential", "request_body"} {
		for _, column := range columns {
			if strings.Contains(column, forbidden) {
				t.Fatalf("session_event_idempotency_keys column %q contains forbidden raw/secret concept %q", column, forbidden)
			}
		}
	}
	assertTableRLSForced(t, db, schema, "session_event_idempotency_keys")
	assertUniqueConstraintColumns(t, db, schema, "session_event_idempotency_keys", []string{"workspace_id", "session_id", "idempotency_key_digest"})
	assertForeignKeyCascade(t, db, schema, "session_event_idempotency_keys", "sessions")
}

func TestSessionRuntimeBindingsStateInvariants(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_binding", "sesn_binding_active")
	mustInsertSessionRuntimeBinding(t, admin, bindingRow{
		workspaceID:       "workspace_binding",
		sessionID:         "sesn_binding_active",
		bindingID:         "bind_active",
		bindingGeneration: 1,
		namespace:         "runtime-ns",
		podName:           "agent-runtime-0",
		podUID:            "uid-active",
		podIP:             "10.0.0.4",
		boundAt:           "2026-06-09T10:00:00Z",
		updatedAt:         "2026-06-09T10:00:01Z",
	})

	seedStorageSchemaSession(t, admin, "workspace_binding", "sesn_binding_max_generation")
	mustInsertSessionRuntimeBinding(t, admin, bindingRow{
		workspaceID:       "workspace_binding",
		sessionID:         "sesn_binding_max_generation",
		bindingID:         "bind_max_generation",
		bindingGeneration: 4294967295,
		namespace:         "runtime-ns",
		podName:           "agent-runtime-max",
		podUID:            "uid-max",
		podIP:             "10.0.0.9",
		boundAt:           "2026-06-09T10:00:00Z",
		updatedAt:         "2026-06-09T10:00:01Z",
	})
	assertBindingGenerationRoundTripsThroughCAS(t, admin, "workspace_binding", "sesn_binding_max_generation", "bind_max_generation", 4294967295)

	invalidRows := map[string]bindingRow{
		"missing binding id": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_missing_binding_id",
			bindingGeneration: 1,
			namespace:         "runtime-ns",
			podName:           "agent-runtime-0",
			podUID:            "uid-active",
			podIP:             "10.0.0.4",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"empty binding id": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_empty_binding_id",
			bindingIDPresent:  true,
			bindingID:         "",
			bindingGeneration: 1,
			namespace:         "runtime-ns",
			podName:           "agent-runtime-0",
			podUID:            "uid-active",
			podIP:             "10.0.0.4",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"zero binding generation": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_zero_generation",
			bindingID:         "bind_zero_generation",
			bindingGeneration: 0,
			namespace:         "runtime-ns",
			podName:           "agent-runtime-0",
			podUID:            "uid-active",
			podIP:             "10.0.0.4",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"out of range binding generation": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_high_generation",
			bindingID:         "bind_high_generation",
			bindingGeneration: 4294967296,
			namespace:         "runtime-ns",
			podName:           "agent-runtime-0",
			podUID:            "uid-active",
			podIP:             "10.0.0.4",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"partial active identity": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_partial_identity",
			bindingID:         "bind_partial_identity",
			bindingGeneration: 1,
			namespace:         "runtime-ns",
			podName:           "agent-runtime-0",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"empty active identity component": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_empty_identity",
			bindingID:         "bind_empty_identity",
			bindingGeneration: 1,
			namespacePresent:  true,
			namespace:         "",
			podName:           "agent-runtime-0",
			podUID:            "uid-active",
			podIP:             "10.0.0.4",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"empty pod name": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_empty_pod_name",
			bindingID:         "bind_empty_pod_name",
			bindingGeneration: 1,
			namespace:         "runtime-ns",
			podNamePresent:    true,
			podName:           "",
			podUID:            "uid-active",
			podIP:             "10.0.0.4",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"empty pod uid": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_empty_pod_uid",
			bindingID:         "bind_empty_pod_uid",
			bindingGeneration: 1,
			namespace:         "runtime-ns",
			podName:           "agent-runtime-0",
			podUIDPresent:     true,
			podUID:            "",
			podIP:             "10.0.0.4",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"active missing bound_at": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_missing_bound",
			bindingID:         "bind_missing_bound",
			bindingGeneration: 1,
			namespace:         "runtime-ns",
			podName:           "agent-runtime-0",
			podUID:            "uid-active",
			podIP:             "10.0.0.4",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
		"empty optional pod ip": {
			workspaceID:       "workspace_binding",
			sessionID:         "sesn_binding_empty_ip",
			bindingID:         "bind_empty_ip",
			bindingGeneration: 1,
			namespace:         "runtime-ns",
			podName:           "agent-runtime-0",
			podUID:            "uid-active",
			podIPPresent:      true,
			podIP:             "",
			boundAt:           "2026-06-09T10:00:00Z",
			updatedAt:         "2026-06-09T10:00:01Z",
		},
	}
	for name, row := range invalidRows {
		t.Run(name, func(t *testing.T) {
			seedStorageSchemaSession(t, admin, row.workspaceID, row.sessionID)
			if err := insertSessionRuntimeBinding(t, admin, row); err == nil {
				t.Fatalf("insertSessionRuntimeBinding(%s) succeeded; want invariant violation", name)
			}
		})
	}
}

func TestSessionRuntimeBindingsWorkspaceIsolation(t *testing.T) {
	runtimeDB, adminDB, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, adminDB, "workspace_binding_a", "sesn_binding_a")
	seedStorageSchemaSession(t, adminDB, "workspace_binding_b", "sesn_binding_b")
	mustInsertSessionRuntimeBinding(t, adminDB, bindingRow{
		workspaceID:       "workspace_binding_a",
		sessionID:         "sesn_binding_a",
		bindingID:         "bind_a",
		bindingGeneration: 1,
		namespace:         "runtime-ns",
		podName:           "agent-runtime-a",
		podUID:            "uid-a",
		podIP:             "10.0.0.4",
		boundAt:           "2026-06-09T10:00:00Z",
		updatedAt:         "2026-06-09T10:00:00Z",
	})
	mustInsertSessionRuntimeBinding(t, adminDB, bindingRow{
		workspaceID:       "workspace_binding_b",
		sessionID:         "sesn_binding_b",
		bindingID:         "bind_b",
		bindingGeneration: 2,
		namespace:         "runtime-ns",
		podName:           "agent-runtime-b",
		podUID:            "uid-b",
		podIP:             "10.0.0.5",
		boundAt:           "2026-06-09T10:00:00Z",
		updatedAt:         "2026-06-09T10:00:00Z",
	})

	errCrossWorkspaceRejected := errors.New("cross-workspace binding write rejected")
	err := storage.WithWorkspaceTx(context.Background(), runtimeDB, "workspace_binding_a", func(tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings`).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("workspace_binding_a sees %d bindings; want 1", count)
		}
		result, err := tx.ExecContext(context.Background(),
			`INSERT INTO session_runtime_bindings (
				workspace_id, session_id, binding_id, binding_generation,
				agent_runtime_namespace, agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip,
				bound_at, updated_at
			 )
			 VALUES ('workspace_binding_b', 'sesn_binding_b', 'bind_b_cross', 3,
				'runtime-ns', 'agent-runtime-b', 'uid-b', '10.0.0.6',
				'2026-06-09T10:01:00Z', '2026-06-09T10:01:00Z')
			 ON CONFLICT (workspace_id, session_id) DO UPDATE SET updated_at = EXCLUDED.updated_at`)
		if err == nil {
			affected, _ := result.RowsAffected()
			return fmt.Errorf("cross-workspace binding write succeeded rows=%d; want RLS failure", affected)
		}
		return errCrossWorkspaceRejected
	})
	if !errors.Is(err, errCrossWorkspaceRejected) {
		t.Fatalf("workspace isolation: %v", err)
	}
}

func TestSessionRuntimeBindingGenerationAllocatorIsMonotonic(t *testing.T) {
	_, admin, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, admin, "workspace_generation", "sesn_generation")

	first := nextSessionRuntimeBindingGeneration(t, admin)
	if first <= 0 {
		t.Fatalf("first generation = %d; want positive", first)
	}
	mustInsertSessionRuntimeBinding(t, admin, bindingRow{
		workspaceID:       "workspace_generation",
		sessionID:         "sesn_generation",
		bindingID:         "bind_first",
		bindingGeneration: first,
		namespace:         "runtime-ns",
		podName:           "agent-runtime-0",
		podUID:            "uid-first",
		podIP:             "10.0.0.4",
		boundAt:           "2026-06-09T10:00:00Z",
		updatedAt:         "2026-06-09T10:00:00Z",
	})

	second := nextSessionRuntimeBindingGeneration(t, admin)
	if second <= first {
		t.Fatalf("replacement generation = %d; want greater than %d", second, first)
	}
	mustReplaceSessionRuntimeBinding(t, admin, bindingRow{
		workspaceID:       "workspace_generation",
		sessionID:         "sesn_generation",
		bindingID:         "bind_second",
		bindingGeneration: second,
		namespace:         "runtime-ns",
		podName:           "agent-runtime-1",
		podUID:            "uid-second",
		podIP:             "10.0.0.5",
		boundAt:           "2026-06-09T10:01:00Z",
		updatedAt:         "2026-06-09T10:01:00Z",
	})

	if _, err := admin.ExecContext(context.Background(),
		`DELETE FROM session_runtime_bindings
		  WHERE workspace_id = 'workspace_generation'
		    AND session_id = 'sesn_generation'
		    AND binding_id = 'bind_second'
		    AND binding_generation = $1`, second); err != nil {
		t.Fatalf("delete attempted binding: %v", err)
	}
	third := nextSessionRuntimeBindingGeneration(t, admin)
	if third <= second {
		t.Fatalf("retry generation = %d; want greater than %d after delete", third, second)
	}
	mustInsertSessionRuntimeBinding(t, admin, bindingRow{
		workspaceID:       "workspace_generation",
		sessionID:         "sesn_generation",
		bindingID:         "bind_third",
		bindingGeneration: third,
		namespace:         "runtime-ns",
		podName:           "agent-runtime-2",
		podUID:            "uid-third",
		podIP:             "10.0.0.6",
		boundAt:           "2026-06-09T10:02:00Z",
		updatedAt:         "2026-06-09T10:02:00Z",
	})

	result, err := admin.ExecContext(context.Background(),
		`DELETE FROM session_runtime_bindings
		  WHERE workspace_id = 'workspace_generation'
		    AND session_id = 'sesn_generation'
		    AND binding_id = 'bind_second'
		    AND binding_generation = $1`, second)
	if err != nil {
		t.Fatalf("stale unbind delete: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("stale unbind rows affected: %v", err)
	}
	if affected != 0 {
		t.Fatalf("stale unbind affected %d rows; want 0", affected)
	}

	if _, err := admin.ExecContext(context.Background(),
		`DELETE FROM session_runtime_bindings
		  WHERE workspace_id = 'workspace_generation'
		    AND session_id = 'sesn_generation'
		    AND binding_id = 'bind_third'
		    AND binding_generation = $1`, third); err != nil {
		t.Fatalf("matching unbind delete: %v", err)
	}
	fourth := nextSessionRuntimeBindingGeneration(t, admin)
	if fourth <= third {
		t.Fatalf("post-unbind generation = %d; want greater than %d", fourth, third)
	}
}

func TestSessionEventIdempotencyWorkspaceIsolation(t *testing.T) {
	runtimeDB, adminDB, _ := newIsolatedPostgreSQLSchemaDBWithAdmin(t)
	seedStorageSchemaSession(t, adminDB, "workspace_idem_a", "sesn_idem_a")
	seedStorageSchemaSession(t, adminDB, "workspace_idem_b", "sesn_idem_b")
	mustInsertSessionEventIdempotencyRow(t, adminDB, "workspace_idem_a", "sesn_idem_a", []byte("digest-a"))
	mustInsertSessionEventIdempotencyRow(t, adminDB, "workspace_idem_b", "sesn_idem_b", []byte("digest-b"))

	errCrossWorkspaceRejected := errors.New("cross-workspace idempotency write rejected")
	err := storage.WithWorkspaceTx(context.Background(), runtimeDB, "workspace_idem_a", func(tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(context.Background(), `SELECT count(*) FROM session_event_idempotency_keys`).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("workspace_idem_a sees %d idempotency rows; want 1", count)
		}
		result, err := tx.ExecContext(context.Background(),
			`INSERT INTO session_event_idempotency_keys (
				workspace_id, session_id, idempotency_key_digest,
				canonical_request_hash, response_events_json, created_at, updated_at
			 )
			 VALUES ('workspace_idem_b', 'sesn_idem_b', '\x0304', '\x0506', '[]',
				'2026-06-09T10:01:00Z', '2026-06-09T10:01:00Z')`)
		if err == nil {
			affected, _ := result.RowsAffected()
			return fmt.Errorf("cross-workspace idempotency write succeeded rows=%d; want RLS failure", affected)
		}
		return errCrossWorkspaceRejected
	})
	if !errors.Is(err, errCrossWorkspaceRejected) {
		t.Fatalf("idempotency workspace isolation: %v", err)
	}
}

func newIsolatedPostgreSQLSchemaDBWithAdmin(t testing.TB) (*sql.DB, *sql.DB, string) {
	t.Helper()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	var schemaName string
	if err := runtimeDB.QueryRowContext(context.Background(), `SELECT current_schema()`).Scan(&schemaName); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	if schemaName == "" {
		t.Fatal("current_schema returned empty schema")
	}
	return runtimeDB, adminDB, schemaName
}

type bindingRow struct {
	workspaceID       string
	sessionID         string
	bindingID         string
	bindingIDPresent  bool
	bindingGeneration int64
	namespace         string
	namespacePresent  bool
	podName           string
	podNamePresent    bool
	podUID            string
	podUIDPresent     bool
	podIP             string
	podIPPresent      bool
	boundAt           string
	boundAtPresent    bool
	updatedAt         string
}

func mustInsertSessionRuntimeBinding(t *testing.T, db *sql.DB, row bindingRow) {
	t.Helper()
	if err := insertSessionRuntimeBinding(t, db, row); err != nil {
		t.Fatalf("insert session runtime binding: %v", err)
	}
}

func insertSessionRuntimeBinding(t *testing.T, db *sql.DB, row bindingRow) error {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_bindings (
			workspace_id,
			session_id,
			binding_id,
			binding_generation,
			agent_runtime_namespace,
			agent_runtime_pod_name,
			agent_runtime_pod_uid,
			agent_runtime_pod_ip,
			bound_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		row.workspaceID,
		row.sessionID,
		nullableString(row.bindingID, row.bindingIDPresent),
		row.bindingGeneration,
		nullableString(row.namespace, row.namespacePresent),
		nullableString(row.podName, row.podNamePresent),
		nullableString(row.podUID, row.podUIDPresent),
		nullableString(row.podIP, row.podIPPresent),
		nullableString(row.boundAt, row.boundAtPresent),
		row.updatedAt,
	)
	return err
}

func mustReplaceSessionRuntimeBinding(t *testing.T, db *sql.DB, row bindingRow) {
	t.Helper()
	result, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_bindings (
			workspace_id,
			session_id,
			binding_id,
			binding_generation,
			agent_runtime_namespace,
			agent_runtime_pod_name,
			agent_runtime_pod_uid,
			agent_runtime_pod_ip,
			bound_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (workspace_id, session_id) DO UPDATE SET
			binding_id = EXCLUDED.binding_id,
			binding_generation = EXCLUDED.binding_generation,
			agent_runtime_namespace = EXCLUDED.agent_runtime_namespace,
			agent_runtime_pod_name = EXCLUDED.agent_runtime_pod_name,
			agent_runtime_pod_uid = EXCLUDED.agent_runtime_pod_uid,
			agent_runtime_pod_ip = EXCLUDED.agent_runtime_pod_ip,
			bound_at = EXCLUDED.bound_at,
			updated_at = EXCLUDED.updated_at`,
		row.workspaceID,
		row.sessionID,
		row.bindingID,
		row.bindingGeneration,
		row.namespace,
		row.podName,
		row.podUID,
		row.podIP,
		row.boundAt,
		row.updatedAt,
	)
	if err != nil {
		t.Fatalf("replace session runtime binding: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("replace session runtime binding rows=%d err=%v; want 1 nil", affected, err)
	}
}

func nextSessionRuntimeBindingGeneration(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var generation int64
	if err := db.QueryRowContext(context.Background(), `SELECT nextval('session_runtime_binding_generation_seq')`).Scan(&generation); err != nil {
		t.Fatalf("next binding generation: %v", err)
	}
	return generation
}

func assertBindingGenerationRoundTripsThroughCAS(t *testing.T, db *sql.DB, workspaceID string, sessionID string, bindingID string, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT binding_generation
		   FROM session_runtime_bindings
		  WHERE workspace_id = $1 AND session_id = $2 AND binding_id = $3`,
		workspaceID,
		sessionID,
		bindingID,
	).Scan(&got); err != nil {
		t.Fatalf("read binding generation: %v", err)
	}
	if got != want {
		t.Fatalf("binding generation readback = %d; want %d", got, want)
	}
	result, err := db.ExecContext(context.Background(),
		`DELETE FROM session_runtime_bindings
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND binding_id = $3
		    AND binding_generation = $4`,
		workspaceID,
		sessionID,
		bindingID,
		want,
	)
	if err != nil {
		t.Fatalf("max binding generation CAS delete: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("max binding generation CAS rows affected: %v", err)
	}
	if affected != 1 {
		t.Fatalf("max binding generation CAS affected %d rows; want 1", affected)
	}
}

func mustInsertSessionEventLedgerRow(t *testing.T, db *sql.DB, workspaceID string, sessionID string, eventID string, sequence int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (workspace_id, session_id, event_id, sequence, type, payload_json, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, 'user.message', '{}', '2026-06-09T10:00:00Z', '2026-06-09T10:00:00Z')`,
		workspaceID,
		sessionID,
		eventID,
		sequence,
	); err != nil {
		t.Fatalf("insert session event ledger row: %v", err)
	}
}

func mustInsertSessionEventIdempotencyRow(t *testing.T, db *sql.DB, workspaceID string, sessionID string, digest []byte) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_event_idempotency_keys (
			workspace_id, session_id, idempotency_key_digest,
			canonical_request_hash, response_events_json, created_at, updated_at
		 ) VALUES ($1, $2, $3, $4, '[]', '2026-06-09T10:00:00Z', '2026-06-09T10:00:00Z')`,
		workspaceID,
		sessionID,
		digest,
		[]byte("request-hash-"+sessionID),
	); err != nil {
		t.Fatalf("insert session event idempotency row: %v", err)
	}
}

func nullableString(value string, present bool) sql.NullString {
	return sql.NullString{String: value, Valid: present || value != ""}
}

func expectedVersionOneControlPlaneTables() []string {
	return []string{
		"agent_versions",
		"agents",
		"api_keys",
		"credentials",
		"environment_artifacts",
		"environments",
		"file_objects",
		"files",
		"memories",
		"memory_stores",
		"memory_versions",
		"platform_provider_keys",
		"queue_jobs",
		"queue_partition_counters",
		"request_usage_details",
		"sandbox_lifecycle_operations",
		"sandbox_output_capture_blobs",
		"sandbox_output_capture_operations",
		"session_background_tasks",
		"session_bridge_operations",
		"session_event_idempotency_keys",
		"session_event_stream_changes",
		"session_events",
		"session_file_attachment_consumptions",
		"session_file_resources",
		"session_git_tickets",
		"session_github_repository_resources",
		"session_mcp_manifests",
		"session_memory_store_resources",
		"session_messages",
		"session_output_captures",
		"session_pending_tool_uses",
		"session_provider_auth",
		"session_resource_prefix_gc",
		"session_resources",
		"session_runtime_bindings",
		"session_runtime_inbox",
		"session_runtime_status",
		"session_runtime_tool_results",
		"session_sandbox_bindings",
		"session_thread_context_prefixes",
		"session_threads",
		"session_transient_attachments",
		"session_turn_retries",
		"sessions",
		"skill_versions",
		"skills",
		"vaults",
		"workspaces",
	}
}

func seedStorageSchemaSession(t *testing.T, db *sql.DB, workspaceID string, sessionID string) {
	t.Helper()
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		workspaceID, "workspace-"+workspaceID); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, $3, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, agentID, "agent-"+sessionID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, 1, '{}', $4, '2026-01-01T00:00:00Z')`,
		workspaceID, agentVersionID, agentID, "hash-"+sessionID); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, $3, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, environmentID, "environment-"+sessionID); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sessions (workspace_id, id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at)
		 VALUES ($1, $2, 'session', 'idle', 'active', $3, 1, $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, agentID, environmentID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func assertTableRLSForced(t *testing.T, db *sql.DB, schema string, table string) {
	t.Helper()
	var enabled, forced bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT class.relrowsecurity, class.relforcerowsecurity
		   FROM pg_class class
		   JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
		  WHERE namespace.nspname = $1 AND class.relname = $2`,
		schema,
		table,
	).Scan(&enabled, &forced); err != nil {
		t.Fatalf("query RLS flags for %s: %v", table, err)
	}
	if !enabled || !forced {
		t.Fatalf("%s RLS enabled=%t forced=%t; want both true", table, enabled, forced)
	}
}

func assertPrimaryKeyColumns(t *testing.T, db *sql.DB, schema string, table string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT column_name
		   FROM information_schema.key_column_usage
		  WHERE table_schema = $1
		    AND table_name = $2
		    AND constraint_name = (
				SELECT constraint_name
				  FROM information_schema.table_constraints
				 WHERE table_schema = $1
				   AND table_name = $2
				   AND constraint_type = 'PRIMARY KEY'
		    )
		  ORDER BY ordinal_position`,
		schema,
		table,
	)
	if err != nil {
		t.Fatalf("query primary key columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan primary key column: %v", err)
		}
		got = append(got, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("primary key rows: %v", err)
	}
	if !equalStringSlices(got, want) {
		t.Fatalf("%s primary key columns = %v; want %v", table, got, want)
	}
}

func assertUniqueConstraintColumns(t *testing.T, db *sql.DB, schema string, table string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT string_agg(key_column_usage.column_name, ',' ORDER BY key_column_usage.ordinal_position)
		   FROM information_schema.table_constraints table_constraints
		   JOIN information_schema.key_column_usage key_column_usage
			 ON key_column_usage.constraint_schema = table_constraints.constraint_schema
			AND key_column_usage.constraint_name = table_constraints.constraint_name
			AND key_column_usage.table_schema = table_constraints.table_schema
			AND key_column_usage.table_name = table_constraints.table_name
		  WHERE table_constraints.table_schema = $1
		    AND table_constraints.table_name = $2
		    AND table_constraints.constraint_type = 'UNIQUE'
		  GROUP BY table_constraints.constraint_name
		  ORDER BY table_constraints.constraint_name`,
		schema,
		table,
	)
	if err != nil {
		t.Fatalf("query unique constraints: %v", err)
	}
	defer func() { _ = rows.Close() }()
	wantJoined := strings.Join(want, ",")
	var got []string
	for rows.Next() {
		var columns string
		if err := rows.Scan(&columns); err != nil {
			t.Fatalf("scan unique columns: %v", err)
		}
		got = append(got, columns)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("unique constraint rows: %v", err)
	}
	if !slices.Contains(got, wantJoined) {
		t.Fatalf("%s unique constraints = %v; want one on %v", table, got, want)
	}
}

func assertForeignKeyColumns(t *testing.T, db *sql.DB, schema string, table string, referencedTable string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT string_agg(key_column_usage.column_name, ',' ORDER BY key_column_usage.ordinal_position)
		   FROM information_schema.table_constraints table_constraints
		   JOIN information_schema.key_column_usage key_column_usage
			 ON key_column_usage.constraint_schema = table_constraints.constraint_schema
			AND key_column_usage.constraint_name = table_constraints.constraint_name
			AND key_column_usage.table_schema = table_constraints.table_schema
			AND key_column_usage.table_name = table_constraints.table_name
		   JOIN information_schema.referential_constraints referential_constraints
			 ON referential_constraints.constraint_schema = table_constraints.constraint_schema
			AND referential_constraints.constraint_name = table_constraints.constraint_name
		   JOIN information_schema.table_constraints referenced_constraints
			 ON referenced_constraints.constraint_schema = referential_constraints.unique_constraint_schema
			AND referenced_constraints.constraint_name = referential_constraints.unique_constraint_name
		  WHERE table_constraints.table_schema = $1
		    AND table_constraints.table_name = $2
		    AND referenced_constraints.table_name = $3
		    AND table_constraints.constraint_type = 'FOREIGN KEY'
		  GROUP BY table_constraints.constraint_name
		  ORDER BY table_constraints.constraint_name`,
		schema,
		table,
		referencedTable,
	)
	if err != nil {
		t.Fatalf("query foreign key columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	wantJoined := strings.Join(want, ",")
	var got []string
	for rows.Next() {
		var columns string
		if err := rows.Scan(&columns); err != nil {
			t.Fatalf("scan foreign key columns: %v", err)
		}
		got = append(got, columns)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("foreign key rows: %v", err)
	}
	if !slices.Contains(got, wantJoined) {
		t.Fatalf("%s -> %s foreign keys = %v; want one on %v", table, referencedTable, got, want)
	}
}

func assertForeignKeyCascade(t *testing.T, db *sql.DB, schema string, table string, referencedTable string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM information_schema.referential_constraints constraints
		   JOIN information_schema.table_constraints table_constraints
			 ON table_constraints.constraint_schema = constraints.constraint_schema
			AND table_constraints.constraint_name = constraints.constraint_name
		   JOIN information_schema.table_constraints referenced_constraints
			 ON referenced_constraints.constraint_schema = constraints.unique_constraint_schema
			AND referenced_constraints.constraint_name = constraints.unique_constraint_name
		  WHERE table_constraints.table_schema = $1
		    AND table_constraints.table_name = $2
		    AND referenced_constraints.table_name = $3
		    AND constraints.delete_rule = 'CASCADE'`,
		schema,
		table,
		referencedTable,
	).Scan(&count); err != nil {
		t.Fatalf("query cascade foreign key: %v", err)
	}
	if count != 1 {
		t.Fatalf("%s has %d cascade foreign keys to %s; want 1", table, count, referencedTable)
	}
}

func readColumnNames(t *testing.T, db *sql.DB, schema string, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT column_name
		   FROM information_schema.columns
		  WHERE table_schema = $1 AND table_name = $2
		  ORDER BY ordinal_position`,
		schema,
		table,
	)
	if err != nil {
		t.Fatalf("query columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("column rows: %v", err)
	}
	return columns
}

func readWorkspaceIDColumnDefaults(t *testing.T, db *sql.DB, schema string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT table_name, column_default
		   FROM information_schema.columns
		  WHERE table_schema = $1
		    AND column_name = 'workspace_id'
		    AND column_default IS NOT NULL
		  ORDER BY table_name`,
		schema,
	)
	if err != nil {
		t.Fatalf("query workspace_id defaults: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var defaults []string
	for rows.Next() {
		var tableName, columnDefault string
		if err := rows.Scan(&tableName, &columnDefault); err != nil {
			t.Fatalf("scan workspace_id default: %v", err)
		}
		defaults = append(defaults, tableName+"="+columnDefault)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("workspace_id default rows: %v", err)
	}
	return defaults
}

func readCheckConstraintNames(t *testing.T, db *sql.DB, schema string, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT constraint_name
		   FROM information_schema.table_constraints
		  WHERE table_schema = $1
		    AND table_name = $2
		    AND constraint_type = 'CHECK'
		  ORDER BY constraint_name`,
		schema,
		table,
	)
	if err != nil {
		t.Fatalf("query constraints: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var constraints []string
	for rows.Next() {
		var constraint string
		if err := rows.Scan(&constraint); err != nil {
			t.Fatalf("scan constraint: %v", err)
		}
		constraints = append(constraints, constraint)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("constraint rows: %v", err)
	}
	return constraints
}

func readCheckConstraintDefinition(t *testing.T, db *sql.DB, schema string, table string, constraint string) string {
	t.Helper()
	return readConstraintDefinition(t, db, schema, table, constraint, "c")
}

func readConstraintDefinition(t *testing.T, db *sql.DB, schema string, table string, constraint string, constraintType string) string {
	t.Helper()
	var definition string
	if err := db.QueryRowContext(context.Background(),
		`SELECT pg_get_constraintdef(pg_constraint.oid)
		   FROM pg_constraint
		   JOIN pg_class
			 ON pg_class.oid = pg_constraint.conrelid
		   JOIN pg_namespace
			 ON pg_namespace.oid = pg_class.relnamespace
		  WHERE pg_namespace.nspname = $1
		    AND pg_class.relname = $2
		    AND pg_constraint.conname = $3
		    AND pg_constraint.contype = $4`,
		schema,
		table,
		constraint,
		constraintType,
	).Scan(&definition); err != nil {
		t.Fatalf("read constraint %s.%s %s: %v", schema, table, constraint, err)
	}
	return definition
}

func readIndexPredicate(t *testing.T, db *sql.DB, schema string, indexName string) string {
	t.Helper()
	var predicate sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT pg_get_expr(indexes.indpred, indexes.indrelid)
		   FROM pg_index indexes
		   JOIN pg_class index_class ON index_class.oid = indexes.indexrelid
		   JOIN pg_namespace namespace ON namespace.oid = index_class.relnamespace
		  WHERE namespace.nspname = $1 AND index_class.relname = $2`,
		schema,
		indexName,
	).Scan(&predicate); err != nil {
		t.Fatalf("query index predicate: %v", err)
	}
	return predicate.String
}

func readIndexColumns(t *testing.T, db *sql.DB, schema string, indexName string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT attributes.attname
		   FROM pg_index indexes
		   JOIN pg_class index_class ON index_class.oid = indexes.indexrelid
		   JOIN pg_namespace namespace ON namespace.oid = index_class.relnamespace
		   JOIN LATERAL unnest(indexes.indkey) WITH ORDINALITY AS keys(attnum, ord) ON TRUE
		   JOIN pg_attribute attributes
		     ON attributes.attrelid = indexes.indrelid
		    AND attributes.attnum = keys.attnum
		  WHERE namespace.nspname = $1
		    AND index_class.relname = $2
		  ORDER BY keys.ord`,
		schema,
		indexName,
	)
	if err != nil {
		t.Fatalf("query index columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan index column: %v", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("index column rows: %v", err)
	}
	return columns
}

func readIndexIsUnique(t *testing.T, db *sql.DB, schema string, indexName string) bool {
	t.Helper()
	var unique bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT indexes.indisunique
		   FROM pg_index indexes
		   JOIN pg_class index_class ON index_class.oid = indexes.indexrelid
		   JOIN pg_namespace namespace ON namespace.oid = index_class.relnamespace
		  WHERE namespace.nspname = $1
		    AND index_class.relname = $2`,
		schema,
		indexName,
	).Scan(&unique); err != nil {
		t.Fatalf("query index uniqueness: %v", err)
	}
	return unique
}

func readBaseTableNames(t *testing.T, db *sql.DB, schema string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		 ORDER BY table_name`, schema)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return got
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
