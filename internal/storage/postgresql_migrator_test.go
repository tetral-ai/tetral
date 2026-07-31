package storage_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

const migrationTestSecret = "postgres://schema-user:do-not-leak@db.internal/tetral"

func TestMigrateSchemaCreatesAndStampsBaselineAtomically(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)

	if err := storage.MigrateSchema(context.Background(), db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT count(*) FROM tetral_schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("read migration stamp count: %v", err)
	}
	if count != 1 {
		t.Fatalf("migration stamp count = %d, want 1", count)
	}
	assertTableExists(t, db, "sessions", true)
	assertTableExists(t, db, "session_turn_retries", true)
	assertTableExists(t, db, "session_mcp_manifests", true)
}

func TestMigrateSchemaCreatesQueuePartitionSequenceSchema(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	var schema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	assertQueuePartitionSequenceSchema(t, db, schema)
	var version int64
	var checksum string
	if err := db.QueryRowContext(ctx,
		`SELECT version, checksum FROM tetral_schema_migrations`,
	).Scan(&version, &checksum); err != nil {
		t.Fatalf("read schema migration stamp: %v", err)
	}
	if version != 1 || checksum != storage.PostgreSQLSchemaVersionOneChecksum {
		t.Fatalf("schema migration stamp = version %d checksum %q; want baseline checksum", version, checksum)
	}
}

func TestMigrateSchemaVersionSixCreatesSessionMCPManifestsShapeAndRLS(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	var schema string
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	assertSessionMCPManifestsSchemaShapeAndRLS(t, db, schema)
}

func TestMigrateSchemaVersionSevenCreatesStableReasoningMessageAssociation(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	var stampCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tetral_schema_migrations`).Scan(&stampCount); err != nil {
		t.Fatalf("count migration stamps: %v", err)
	}
	if stampCount != 1 {
		t.Fatalf("migration stamp count = %d, want 1", stampCount)
	}

	var nullable string
	if err := db.QueryRowContext(ctx, `SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'session_messages'
		  AND column_name = 'model_request_id'`).Scan(&nullable); err != nil {
		t.Fatalf("read session_messages.model_request_id: %v", err)
	}
	if nullable != "YES" {
		t.Fatalf("session_messages.model_request_id nullable = %q, want YES", nullable)
	}

	var constraintDefinition string
	if err := db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = current_schema()
		  AND t.relname = 'session_messages'
		  AND c.conname = 'session_messages_model_request_id_shape'`).Scan(&constraintDefinition); err != nil {
		t.Fatalf("read model request association constraint: %v", err)
	}
	if !strings.Contains(constraintDefinition, "model_request_id IS NULL") || !strings.Contains(constraintDefinition, "kind = 'assistant'") {
		t.Fatalf("model request association constraint = %q; want nullable assistant-only shape", constraintDefinition)
	}

	var indexDefinition string
	if err := db.QueryRowContext(ctx, `SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'session_messages'
		  AND indexname = 'idx_session_messages_model_request_unique'`).Scan(&indexDefinition); err != nil {
		t.Fatalf("read model request association index: %v", err)
	}
	for _, fragment := range []string{"UNIQUE INDEX", "workspace_id", "session_id", "session_thread_id", "model_request_id", "WHERE (model_request_id IS NOT NULL)"} {
		if !strings.Contains(indexDefinition, fragment) {
			t.Fatalf("model request association index = %q; missing %q", indexDefinition, fragment)
		}
	}

	var operationConstraintDefinition string
	if err := db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = current_schema()
		  AND t.relname = 'session_bridge_operations'
		  AND c.conname = 'session_bridge_operations_operation_shape'`).Scan(&operationConstraintDefinition); err != nil {
		t.Fatalf("read Bridge operation constraint: %v", err)
	}
	if strings.Contains(operationConstraintDefinition, "commit_stable_reasoning_part") {
		t.Fatalf("Bridge operation constraint retains retired standalone reasoning operation: %q", operationConstraintDefinition)
	}
}

func TestPostgreSQLSchemaVersionOneChecksumIsGolden(t *testing.T) {
	const want = "7bab2eca56d2198033f4be4453bdc0205f784ad397b9136073f2394f0f9a6b04"
	if storage.PostgreSQLSchemaVersionOneChecksum != want {
		t.Fatalf("PostgreSQLSchemaVersionOneChecksum = %q, want %q", storage.PostgreSQLSchemaVersionOneChecksum, want)
	}
}

func TestMigrateSchemaBaselinesLegacyInitializerWithoutChangingSentinel(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.InitializePostgreSQLSchema(ctx, db); err != nil {
		t.Fatalf("InitializePostgreSQLSchema: %v", err)
	}
	const sentinelID = "ws_schema_migration_sentinel"
	const sentinelName = "durable sentinel"
	if _, err := db.ExecContext(ctx, `INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $2, '2026-07-13T00:00:00Z')`, sentinelID, sentinelName); err != nil {
		t.Fatalf("insert sentinel: %v", err)
	}
	assertTableExists(t, db, "tetral_schema_migrations", false)

	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	var got string
	if err := db.QueryRowContext(ctx, `SELECT name FROM workspaces WHERE id = $1`, sentinelID).Scan(&got); err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	if got != sentinelName {
		t.Fatalf("sentinel name = %q, want %q", got, sentinelName)
	}
}

func TestMigrateSchemaRerunKeepsAllStampsUnchanged(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("first MigrateSchema: %v", err)
	}
	var firstChecksum string
	var firstAppliedAt time.Time
	if err := db.QueryRowContext(ctx, `SELECT checksum, applied_at FROM tetral_schema_migrations WHERE version = 1`).Scan(&firstChecksum, &firstAppliedAt); err != nil {
		t.Fatalf("read first stamp: %v", err)
	}
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("second MigrateSchema: %v", err)
	}
	var count int
	var checksum string
	var appliedAt time.Time
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tetral_schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("read rerun stamp: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT checksum, applied_at FROM tetral_schema_migrations WHERE version = 1`).Scan(&checksum, &appliedAt); err != nil {
		t.Fatalf("read rerun version-one stamp: %v", err)
	}
	if count != 1 || checksum != firstChecksum || !appliedAt.Equal(firstAppliedAt) {
		t.Fatalf("rerun stamp = count %d version-one checksum %q applied_at %v; want unchanged count 1 checksum %q applied_at %v", count, checksum, appliedAt, firstChecksum, firstAppliedAt)
	}
}

func TestVerifySchemaUsesReadOnlyBoundaryAndNeverRepairs(t *testing.T) {
	t.Run("current", func(t *testing.T) {
		db := storagetest.NewEmptyPostgreSQLAdminDB(t)
		if err := storage.MigrateSchema(context.Background(), db); err != nil {
			t.Fatalf("MigrateSchema: %v", err)
		}
		db.SetMaxOpenConns(1)
		if _, err := db.Exec(`SET default_transaction_read_only = on`); err != nil {
			t.Fatalf("set read only: %v", err)
		}
		if err := storage.VerifySchema(context.Background(), db); err != nil {
			t.Fatalf("VerifySchema through read-only boundary: %v", err)
		}
	})

	t.Run("missing", func(t *testing.T) {
		db := storagetest.NewEmptyPostgreSQLAdminDB(t)
		err := storage.VerifySchema(context.Background(), db)
		assertSchemaErrorKind(t, err, storage.SchemaErrorMissing)
		assertTableExists(t, db, "tetral_schema_migrations", false)
	})
}

func TestSchemaHistoryValidationRejectsInvalidStateBeforeMutation(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *sql.DB)
		kind  storage.SchemaErrorKind
	}{
		{
			name: "behind",
			setup: func(t *testing.T, db *sql.DB) {
				createMigrationRegistry(t, db)
			},
			kind: storage.SchemaErrorBehind,
		},
		{
			name: "ahead",
			setup: func(t *testing.T, db *sql.DB) {
				migrateForHistoryTest(t, db)
				if _, err := db.Exec(`INSERT INTO tetral_schema_migrations (version, checksum) VALUES (2, $1)`, strings.Repeat("a", 64)); err != nil {
					t.Fatalf("insert ahead row: %v", err)
				}
			},
			kind: storage.SchemaErrorAhead,
		},
		{
			name: "gap",
			setup: func(t *testing.T, db *sql.DB) {
				createMigrationRegistry(t, db)
				if _, err := db.Exec(`INSERT INTO tetral_schema_migrations (version, checksum) VALUES (2, $1)`, strings.Repeat("a", 64)); err != nil {
					t.Fatalf("insert gap row: %v", err)
				}
			},
			kind: storage.SchemaErrorGap,
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, db *sql.DB) {
				createMigrationRegistryWithoutKey(t, db)
				if _, err := db.Exec(`INSERT INTO tetral_schema_migrations (version, checksum) VALUES (1, $1), (1, $1)`, storage.PostgreSQLSchemaVersionOneChecksum); err != nil {
					t.Fatalf("insert duplicate rows: %v", err)
				}
			},
			kind: storage.SchemaErrorDuplicate,
		},
		{
			name: "drift",
			setup: func(t *testing.T, db *sql.DB) {
				migrateForHistoryTest(t, db)
				if _, err := db.Exec(`UPDATE tetral_schema_migrations SET checksum = $1 WHERE version = 1`, strings.Repeat("b", 64)); err != nil {
					t.Fatalf("update drift checksum: %v", err)
				}
			},
			kind: storage.SchemaErrorChecksumDrift,
		},
		{
			name: "malformed",
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`CREATE TABLE tetral_schema_migrations (wrong_column TEXT)`); err != nil {
					t.Fatalf("create malformed registry: %v", err)
				}
			},
			kind: storage.SchemaErrorMalformed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := storagetest.NewEmptyPostgreSQLAdminDB(t)
			test.setup(t, db)
			before := baseTableNames(t, db)
			operations := []struct {
				name string
				call func(context.Context, *sql.DB) error
			}{
				{name: "verify", call: storage.VerifySchema},
				{name: "migrate", call: storage.MigrateSchema},
			}
			// A behind registry is the sole valid mutation case for MigrateSchema
			// and is covered separately by TestMigrateSchemaAppliesValidBehindRegistry.
			if test.kind == storage.SchemaErrorBehind {
				operations = operations[:1]
			}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					err := operation.call(context.Background(), db)
					assertSchemaErrorKind(t, err, test.kind)
					if after := baseTableNames(t, db); strings.Join(after, "\x00") != strings.Join(before, "\x00") {
						t.Fatalf("tables mutated before=%v after=%v", before, after)
					}
				})
			}
		})
	}
}

func TestMigrateSchemaAppliesValidBehindRegistry(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	createMigrationRegistry(t, db)
	if err := storage.MigrateSchema(context.Background(), db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	if err := storage.VerifySchema(context.Background(), db); err != nil {
		t.Fatalf("VerifySchema: %v", err)
	}
}

func TestMigrateSchemaLateFailureRollsBackSchemaAndStampAndReleasesLock(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE workspaces (wrong_column TEXT)`); err != nil {
		t.Fatalf("create incompatible legacy table: %v", err)
	}
	err := storage.MigrateSchema(ctx, db)
	assertSchemaErrorKind(t, err, storage.SchemaErrorApply)
	assertTableExists(t, db, "tetral_schema_migrations", false)
	assertTableExists(t, db, "environments", false)
	assertSchemaErrorIsPublicSafe(t, err)

	if _, err := db.ExecContext(ctx, `DROP TABLE workspaces`); err != nil {
		t.Fatalf("remove incompatible table: %v", err)
	}
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema after forced failure (lock must be released): %v", err)
	}
}

func TestMigrateSchemaCancellationRollsBackStampAndReleasesLock(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.InitializePostgreSQLSchema(ctx, db); err != nil {
		t.Fatalf("InitializePostgreSQLSchema: %v", err)
	}
	blocker, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	if _, err := blocker.ExecContext(ctx, `LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock sessions: %v", err)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		result <- storage.MigrateSchema(cancelCtx, db)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case err = <-result:
	case <-time.After(2 * time.Second):
		_ = blocker.Rollback()
		t.Fatal("MigrateSchema did not honor cancellation while blocked in baseline DDL")
	}
	if rollbackErr := blocker.Rollback(); rollbackErr != nil {
		t.Fatalf("rollback blocker: %v", rollbackErr)
	}
	assertSchemaErrorKind(t, err, storage.SchemaErrorCanceled)
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("database after cancellation: %v", err)
	}
	assertTableExists(t, db, "tetral_schema_migrations", false)
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema after cancellation (lock must be released): %v", err)
	}
}

func TestMigrateSchemaConcurrentReplicasSerialize(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	lockConnection, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("lock connection: %v", err)
	}
	defer func() { _ = lockConnection.Close() }()
	if _, err := lockConnection.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, storage.PostgreSQLSchemaAdvisoryLockID); err != nil {
		t.Fatalf("hold schema lock: %v", err)
	}

	result := make(chan error, 2)
	var start sync.WaitGroup
	start.Add(2)
	for range 2 {
		go func() {
			start.Done()
			start.Wait()
			result <- storage.MigrateSchema(ctx, db)
		}()
	}
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-result:
		t.Fatalf("migration returned before advisory lock release: %v", err)
	default:
	}
	assertTableExists(t, db, "tetral_schema_migrations", false)
	if _, err := lockConnection.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, storage.PostgreSQLSchemaAdvisoryLockID); err != nil {
		t.Fatalf("release schema lock: %v", err)
	}
	for range 2 {
		if err := <-result; err != nil {
			t.Fatalf("serialized MigrateSchema: %v", err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM tetral_schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count stamps: %v", err)
	}
	if count != 1 {
		t.Fatalf("stamp count = %d, want 1", count)
	}
}

func TestMigrateSchemaVersionNineAddsNullableGitHubResourceCredential(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	seedStorageSchemaSession(t, db, "wksp_v9_upgrade", "sesn_v9_upgrade")

	var nullable string
	if err := db.QueryRowContext(ctx, `SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'session_github_repository_resources'
		  AND column_name = 'authorization_token_encrypted'`).Scan(&nullable); err != nil {
		t.Fatalf("read token column nullability: %v", err)
	}
	if nullable != "YES" {
		t.Fatalf("authorization_token_encrypted nullable = %q; want YES", nullable)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO session_resources (
			workspace_id, session_id, resource_id, type, created_at, updated_at
		) VALUES (
			'wksp_v9_upgrade', 'sesn_v9_upgrade', 'sesrsc_token', 'github_repository',
			'2026-07-17T00:00:00Z', '2026-07-17T00:00:00Z'
		)`); err != nil {
		t.Fatalf("insert token resource: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_github_repository_resources (
			workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref,
			authorization_token_encrypted
		) VALUES (
			'wksp_v9_upgrade', 'sesn_v9_upgrade', 'sesrsc_token',
			'https://github.com/tetral-ai/tetral', '/workspace/tetral', NULL, NULL,
			decode('0102', 'hex')
		)`); err != nil {
		t.Fatalf("insert github resource with token: %v", err)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO session_resources (
			workspace_id, session_id, resource_id, type, created_at, updated_at
		) VALUES (
			'wksp_v9_upgrade', 'sesn_v9_upgrade', 'sesrsc_nulltoken', 'github_repository',
			'2026-07-17T00:01:00Z', '2026-07-17T00:01:00Z'
		)`); err != nil {
		t.Fatalf("insert null-token resource: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_github_repository_resources (
			workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref,
			authorization_token_encrypted
		) VALUES (
			'wksp_v9_upgrade', 'sesn_v9_upgrade', 'sesrsc_nulltoken',
			'https://github.com/tetral-ai/nulltoken', '/workspace/nulltoken', NULL, NULL, NULL
		)`); err == nil {
		t.Fatal("GitHub resource accepted a NULL authorization token")
	}
}

func TestMigrateSchemaVersionSixteenAddsCompletionMailReconciliationIndex(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	var definition string
	if err := db.QueryRowContext(ctx,
		`SELECT indexdef
		   FROM pg_indexes
		  WHERE schemaname = current_schema()
		    AND indexname = 'idx_session_events_completion_mail_reconciliation'`,
	).Scan(&definition); err != nil {
		t.Fatalf("read completion mail reconciliation index: %v", err)
	}
	for _, want := range []string{
		"workspace_id",
		"session_id",
		"payload_json",
		"delivery_id",
		"agent.thread_message_sent",
		"agent.thread_message_received",
	} {
		if !strings.Contains(definition, want) {
			t.Fatalf("completion mail reconciliation index = %q; want %q", definition, want)
		}
	}
}

func TestMigrateSchemaVersionFifteenAuthorizesSilentAgentMailSettlement(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	var definition string
	if err := db.QueryRowContext(ctx,
		`SELECT pg_get_constraintdef(oid)
		   FROM pg_constraint
		  WHERE conrelid='session_bridge_operations'::regclass
		    AND conname='session_bridge_operations_operation_shape'`,
	).Scan(&definition); err != nil {
		t.Fatalf("read bridge operation constraint: %v", err)
	}
	if !strings.Contains(definition, "settle_sealed_agent_mail") {
		t.Fatalf("bridge operation constraint = %q; want sealed agent mail settlement", definition)
	}
}

func TestMigrateSchemaVersionFourteenAddsMultimodalFileAttachmentState(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	var nullable string
	if err := db.QueryRowContext(ctx, `SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'file_objects'
		  AND column_name = 'pdf_page_count'`).Scan(&nullable); err != nil {
		t.Fatalf("read file_objects.pdf_page_count: %v", err)
	}
	if nullable != "YES" {
		t.Fatalf("file_objects.pdf_page_count nullable = %q, want YES", nullable)
	}
	assertTableExists(t, db, "session_file_attachment_consumptions", true)
	var rowSecurity bool
	var forceRowSecurity bool
	if err := db.QueryRowContext(ctx, `SELECT relrowsecurity, relforcerowsecurity
		FROM pg_class
		WHERE oid = 'session_file_attachment_consumptions'::regclass`).Scan(&rowSecurity, &forceRowSecurity); err != nil {
		t.Fatalf("read consumption RLS state: %v", err)
	}
	if !rowSecurity || !forceRowSecurity {
		t.Fatalf("consumption RLS enabled/forced = %v/%v, want true/true", rowSecurity, forceRowSecurity)
	}
	var mediaIndexDefinition string
	if err := db.QueryRowContext(ctx, `SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND indexname = 'session_events_pending_media_lookup'`).Scan(&mediaIndexDefinition); err != nil {
		t.Fatalf("read media lookup index: %v", err)
	}
	if !strings.Contains(mediaIndexDefinition, "payload_json") ||
		!strings.Contains(mediaIndexDefinition, "user.message") {
		t.Fatalf("media lookup index = %q; want user-message media predicate", mediaIndexDefinition)
	}

	seedStorageSchemaSession(t, db, "wksp_media", "sesn_media")
	if _, err := db.ExecContext(ctx, `INSERT INTO session_threads (
		workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
	) VALUES (
		'wksp_media', 'thr_media', 'sesn_media', 'main', 'public', 'idle',
		'2026-07-19T00:00:00Z', '2026-07-19T00:00:00Z', '2026-07-19T00:00:00Z'
	)`); err != nil {
		t.Fatalf("seed media thread: %v", err)
	}
	for _, eventID := range []string{"sevt_source_media", "sevt_request_end_media"} {
		if _, err := db.ExecContext(ctx, `INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, visibility, session_visible, created_at, updated_at
		) VALUES (
			'wksp_media', 'sesn_media', 'thr_media', $1,
			CASE WHEN $1 = 'sevt_source_media' THEN 1 ELSE 2 END,
			CASE WHEN $1 = 'sevt_source_media' THEN 'user.message' ELSE 'span.model_request_end' END,
			'{}', 'public', true, '2026-07-19T00:00:00Z', '2026-07-19T00:00:00Z'
		)`, eventID); err != nil {
			t.Fatalf("seed media event %q: %v", eventID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO file_objects (
		object_id, workspace_id, blob_key, size_bytes, sha256, created_at
	) VALUES (
		'fobj_media', 'wksp_media', 'files/wksp_media/fobj_media', 1, 'sha',
		'2026-07-19T00:00:00Z'
	)`); err != nil {
		t.Fatalf("seed media object: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO files (
		file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at
	) VALUES (
		'file_media', 'wksp_media', 'fobj_media', 'media.png', 'image/png', false,
		'2026-07-19T00:00:00Z'
	)`); err != nil {
		t.Fatalf("seed media file: %v", err)
	}
	insertConsumption := `INSERT INTO session_file_attachment_consumptions (
		workspace_id, session_id, session_thread_id, request_end_event_id, source_event_id, file_id
	) VALUES (
		'wksp_media', 'sesn_media', 'thr_media', 'sevt_request_end_media', 'sevt_source_media', 'file_media'
	)`
	if _, err := db.ExecContext(ctx, insertConsumption); err != nil {
		t.Fatalf("insert media consumption: %v", err)
	}
	if _, err := db.ExecContext(ctx, insertConsumption); err == nil {
		t.Fatal("duplicate six-key media consumption was accepted")
	}
	seedStorageSchemaSession(t, db, "wksp_media", "sesn_media_other")
	if _, err := db.ExecContext(ctx, `INSERT INTO session_threads (
		workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
	) VALUES (
		'wksp_media', 'thr_media_other', 'sesn_media_other', 'main', 'public', 'idle',
		'2026-07-19T00:00:00Z', '2026-07-19T00:00:00Z', '2026-07-19T00:00:00Z'
	)`); err != nil {
		t.Fatalf("seed other media thread: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_file_attachment_consumptions (
		workspace_id, session_id, session_thread_id, request_end_event_id, source_event_id, file_id
	) VALUES (
		'wksp_media', 'sesn_media_other', 'thr_media_other',
		'sevt_request_end_media', 'sevt_source_media', 'file_media'
	)`); err == nil {
		t.Fatal("cross-session media consumption was accepted")
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM session_events WHERE event_id='sevt_source_media'`,
	); err == nil {
		t.Fatal("source event deletion removed or orphaned a media consumption")
	}
	if _, err := db.ExecContext(ctx,
		`DELETE FROM session_threads WHERE workspace_id='wksp_media' AND session_id='sesn_media' AND id='thr_media'`,
	); err == nil {
		t.Fatal("thread deletion removed or orphaned a media consumption")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM sessions WHERE workspace_id='wksp_media' AND id='sesn_media'`); err != nil {
		t.Fatalf("hard-delete media session: %v", err)
	}
	var remaining int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM session_file_attachment_consumptions
		WHERE workspace_id='wksp_media'`).Scan(&remaining); err != nil {
		t.Fatalf("count cascaded consumption rows: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("consumption rows after session hard delete = %d, want 0", remaining)
	}
}

func TestMultimodalConsumptionPoliciesPermitInsertAndSessionCascadeOnly(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx := context.Background()
	seedStorageSchemaSession(t, admin, "wksp_media_policy", "sesn_media_policy")
	if _, err := admin.ExecContext(ctx, `INSERT INTO session_threads (
		workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
	) VALUES (
		'wksp_media_policy', 'thr_media_policy', 'sesn_media_policy',
		'main', 'public', 'idle', '2026-07-19T00:00:00Z', '2026-07-19T00:00:00Z', '2026-07-19T00:00:00Z'
	)`); err != nil {
		t.Fatalf("seed policy thread: %v", err)
	}
	for _, eventID := range []string{"sevt_source_policy", "sevt_end_policy"} {
		if _, err := admin.ExecContext(ctx, `INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, visibility, session_visible, created_at, updated_at
		) VALUES (
			'wksp_media_policy', 'sesn_media_policy', 'thr_media_policy', $1,
			CASE WHEN $1='sevt_source_policy' THEN 1 ELSE 2 END,
			CASE WHEN $1='sevt_source_policy' THEN 'user.message' ELSE 'span.model_request_end' END,
			'{}', 'public', true, '2026-07-19T00:00:00Z', '2026-07-19T00:00:00Z'
		)`, eventID); err != nil {
			t.Fatalf("seed policy event %q: %v", eventID, err)
		}
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO file_objects (
		object_id, workspace_id, blob_key, size_bytes, sha256, created_at
	) VALUES (
		'fobj_media_policy', 'wksp_media_policy',
		'files/wksp_media_policy/fobj_media_policy', 1, 'sha',
		'2026-07-19T00:00:00Z'
	)`); err != nil {
		t.Fatalf("seed policy file object: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO files (
		file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at
	) VALUES (
		'file_media_policy', 'wksp_media_policy', 'fobj_media_policy',
		'media.png', 'image/png', false, '2026-07-19T00:00:00Z'
	)`); err != nil {
		t.Fatalf("seed policy file: %v", err)
	}

	tx, err := runtime.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin runtime policy transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('tetral.workspace_id', 'wksp_media_policy', true)`,
	); err != nil {
		t.Fatalf("set runtime workspace: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO session_file_attachment_consumptions (
		workspace_id, session_id, session_thread_id, request_end_event_id, source_event_id, file_id
	) VALUES (
		'wksp_media_policy', 'sesn_media_policy', 'thr_media_policy',
		'sevt_end_policy', 'sevt_source_policy', 'file_media_policy'
	)`); err != nil {
		t.Fatalf("runtime insert consumption: %v", err)
	}
	updateResult, err := tx.ExecContext(ctx, `UPDATE session_file_attachment_consumptions
		SET source_event_id='sevt_end_policy'
		WHERE workspace_id='wksp_media_policy' AND session_id='sesn_media_policy'`)
	if err != nil {
		t.Fatalf("runtime direct update: %v", err)
	}
	if updated, err := updateResult.RowsAffected(); err != nil || updated != 0 {
		t.Fatalf("runtime direct update rows = %d, err %v; want 0", updated, err)
	}
	deleteResult, err := tx.ExecContext(ctx, `DELETE FROM session_file_attachment_consumptions
		WHERE workspace_id='wksp_media_policy' AND session_id='sesn_media_policy'`)
	if err != nil {
		t.Fatalf("runtime direct delete: %v", err)
	}
	if deleted, err := deleteResult.RowsAffected(); err != nil || deleted != 0 {
		t.Fatalf("runtime direct delete rows = %d, err %v; want 0", deleted, err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM sessions WHERE workspace_id='wksp_media_policy' AND id='sesn_media_policy'`,
	); err != nil {
		t.Fatalf("runtime session hard delete: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit runtime policy transaction: %v", err)
	}

	var remaining int
	if err := admin.QueryRowContext(ctx, `SELECT count(*)
		FROM session_file_attachment_consumptions
		WHERE workspace_id='wksp_media_policy'`).Scan(&remaining); err != nil {
		t.Fatalf("count policy consumption rows: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("policy consumption rows after session hard delete = %d; want 0", remaining)
	}
}

func TestMigrateSchemaVersionThirteenAddsExclusiveStartupCleanupLeaseShape(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	seedStorageSchemaSession(t, db, "wksp_cleanup_lease", "sesn_cleanup_lease")
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_metadata_json,
			created_at, updated_at, startup_failure_reason, cleanup_status,
			cleanup_lease_token, cleanup_lease_expires_at
		) VALUES (
			'wksp_cleanup_lease', 'sandbox_cleanup_lease', 'sesn_cleanup_lease',
			'failed', 'tetral', '{}', '2026-07-18T00:00:00Z', '2026-07-18T00:00:00Z',
			'startup_interrupted', 'in_progress', 'lease_test', '2026-07-18T00:02:00Z'
		)`); err != nil {
		t.Fatalf("insert leased cleanup row: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE sandboxes
		    SET cleanup_status = 'pending'
		  WHERE workspace_id = 'wksp_cleanup_lease'
		    AND id = 'sandbox_cleanup_lease'`); err == nil {
		t.Fatal("cleanup lease columns survived a non-in-progress status transition")
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE sandboxes
		    SET cleanup_status = 'in_progress',
		        cleanup_lease_token = NULL,
		        cleanup_lease_expires_at = NULL
		  WHERE workspace_id = 'wksp_cleanup_lease'
		    AND id = 'sandbox_cleanup_lease'`); err == nil {
		t.Fatal("in_progress cleanup accepted without its lease pair")
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE sandboxes
		    SET cleanup_status='released',
		        cleanup_lease_token=NULL,
		        cleanup_lease_expires_at=NULL
		  WHERE workspace_id='wksp_cleanup_lease'
		    AND id='sandbox_cleanup_lease'`); err != nil {
		t.Fatalf("settle cleanup lease row before retired-status checks: %v", err)
	}
	for _, retired := range []string{"not_found", "no_handle"} {
		if _, err := db.ExecContext(ctx,
			`UPDATE sandboxes SET cleanup_status=$1
			  WHERE workspace_id='wksp_cleanup_lease' AND id='sandbox_cleanup_lease'`,
			retired,
		); err == nil {
			t.Fatalf("retired cleanup status %q was accepted", retired)
		}
	}
}

func TestMigrateSchemaVersionTenAddsNullableSandboxMachineUsabilityFact(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	var nullable string
	if err := db.QueryRowContext(ctx,
		`SELECT is_nullable
		   FROM information_schema.columns
		  WHERE table_schema = current_schema()
		    AND table_name = 'sandboxes'
		    AND column_name = 'machine_was_usable'`,
	).Scan(&nullable); err != nil {
		t.Fatalf("read sandboxes.machine_was_usable nullability: %v", err)
	}
	if nullable != "YES" {
		t.Fatalf("sandboxes.machine_was_usable is_nullable = %q; want YES", nullable)
	}
}

func TestMigrateSchemaVersionElevenEnforcesActiveSandboxUsabilityOnNewWrites(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	var validated bool
	if err := db.QueryRowContext(ctx,
		`SELECT convalidated
		   FROM pg_constraint
		  WHERE conrelid = 'sandboxes'::regclass
		    AND conname = 'sandboxes_active_machine_was_usable'`,
	).Scan(&validated); err != nil {
		t.Fatalf("read active usability constraint: %v", err)
	}
	if validated {
		t.Fatal("active usability constraint is validated; want NOT VALID rollout")
	}
	seedStorageSchemaSession(t, db, "wksp_active_fact", "sesn_active_fact")
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_metadata_json,
			created_at, updated_at, machine_was_usable
		) VALUES (
			'wksp_active_fact', 'sandbox_active_fact', 'sesn_active_fact', 'active', 'tetral', '{}',
			'2026-07-18T00:00:00Z', '2026-07-18T00:00:00Z', FALSE
	)`); err == nil {
		t.Fatal("active sandbox write with false machine_was_usable succeeded")
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_metadata_json,
			created_at, updated_at, machine_was_usable
		) VALUES (
			'wksp_active_fact', 'sandbox_active_fact_null', 'sesn_active_fact', 'active', 'tetral', '{}',
			'2026-07-18T00:00:00Z', '2026-07-18T00:00:00Z', NULL
		)`); err == nil {
		t.Fatal("active sandbox write with NULL machine_was_usable succeeded")
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_metadata_json,
			created_at, updated_at
		) VALUES (
			'wksp_active_fact', 'sandbox_active_fact_omitted', 'sesn_active_fact', 'active', 'tetral', '{}',
			'2026-07-18T00:00:00Z', '2026-07-18T00:00:00Z'
		)`); err == nil {
		t.Fatal("active sandbox write omitting machine_was_usable succeeded")
	}
}

func assertSchemaErrorKind(t *testing.T, err error, want storage.SchemaErrorKind) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want schema kind %q", want)
	}
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr == nil {
		t.Fatalf("error type = %T, want *storage.SchemaMigrationError: %v", err, err)
	}
	if schemaErr.Kind != want {
		t.Fatalf("schema error kind = %q, want %q", schemaErr.Kind, want)
	}
	assertSchemaErrorIsPublicSafe(t, err)
}

func assertSchemaErrorIsPublicSafe(t *testing.T, err error) {
	t.Helper()
	text := err.Error()
	for _, forbidden := range []string{migrationTestSecret, "postgres://", "CREATE TABLE", "SELECT ", "db.internal", "do-not-leak"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("schema error leaked %q: %q", forbidden, text)
		}
	}
}

func createMigrationRegistry(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE tetral_schema_migrations (
		version BIGINT PRIMARY KEY,
		checksum TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("create migration registry: %v", err)
	}
}

func createMigrationRegistryWithoutKey(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE tetral_schema_migrations (
		version BIGINT NOT NULL,
		checksum TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("create migration registry without key: %v", err)
	}
}

func migrateForHistoryTest(t *testing.T, db *sql.DB) {
	t.Helper()
	if err := storage.MigrateSchema(context.Background(), db); err != nil {
		t.Fatalf("MigrateSchema setup: %v", err)
	}
}

func assertTableExists(t *testing.T, db *sql.DB, table string, want bool) {
	t.Helper()
	var got bool
	if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&got); err != nil {
		t.Fatalf("lookup table %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("table %s exists = %t, want %t", table, got, want)
	}
}

func baseTableNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' ORDER BY table_name`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list tables rows: %v", err)
	}
	return names
}
