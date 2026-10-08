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
		`SELECT version, checksum FROM tetral_schema_migrations WHERE version = 1`,
	).Scan(&version, &checksum); err != nil {
		t.Fatalf("read schema migration stamp: %v", err)
	}
	if version != 1 || checksum != storage.PostgreSQLSchemaVersionOneChecksum {
		t.Fatalf("schema migration stamp = version %d checksum %q; want baseline checksum", version, checksum)
	}
}

func TestMigrateSchemaCreatesSessionMCPManifestsShapeAndRLS(t *testing.T) {
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

func TestMigrateSchemaOwnsAttachmentAuthorityInboxIndex(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	assertAttachmentAuthorityInboxIndex(t, db)

}

func assertAttachmentAuthorityInboxIndex(t testing.TB, db *sql.DB) {
	t.Helper()
	var definition string
	if err := db.QueryRow(`SELECT indexdef FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'session_runtime_inbox'
		  AND indexname = 'session_runtime_inbox_attachment_authority_lookup'`).Scan(&definition); err != nil {
		t.Fatalf("read attachment authority Inbox index: %v", err)
	}
	for _, fragment := range []string{"workspace_id", "session_id", "session_thread_id", "runtime_input_id", "INCLUDE (input_kind, status)"} {
		if !strings.Contains(definition, fragment) {
			t.Fatalf("attachment authority Inbox index = %q; missing %q", definition, fragment)
		}
	}
	if strings.Contains(definition, "event_ids_json") {
		t.Fatalf("attachment authority index stores unbounded event_ids_json: %q", definition)
	}
}

func TestMigrateSchemaCreatesStableReasoningMessageAssociation(t *testing.T) {
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

}

func TestPostgreSQLSchemaVersionOneChecksumIsGolden(t *testing.T) {
	const want = "4912e39c1607f44f7a4028e1b664cc39e7a2ba88982e522d25b6083a985b0365"
	if storage.PostgreSQLSchemaVersionOneChecksum != want {
		t.Fatalf("PostgreSQLSchemaVersionOneChecksum = %q, want %q", storage.PostgreSQLSchemaVersionOneChecksum, want)
	}
}

func TestMigrateSchemaRerunKeepsAllStampsUnchanged(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("first MigrateSchema: %v", err)
	}
	stamp := func() string {
		t.Helper()
		var value string
		if err := db.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(m) ORDER BY version)::text FROM tetral_schema_migrations m`).Scan(&value); err != nil {
			t.Fatalf("read migration stamps: %v", err)
		}
		return value
	}
	before := stamp()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("second MigrateSchema: %v", err)
	}
	if after := stamp(); after != before {
		t.Fatal("rerun changed migration history")
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
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					assertSchemaErrorKind(t, operation.call(context.Background(), db), test.kind)
					if after := baseTableNames(t, db); strings.Join(after, "\x00") != strings.Join(before, "\x00") {
						t.Fatalf("tables mutated before=%v after=%v", before, after)
					}
				})
			}
		})
	}
}

func TestMigrateSchemaLateFailureRollsBackSchemaAndStampAndReleasesLock(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	if _, err := db.Exec(`CREATE SCHEMA schema_fault;
 CREATE FUNCTION schema_fault.reject_initialization() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private-ddl-rejection'; END $$;
 CREATE EVENT TRIGGER reject_initialization ON ddl_command_start WHEN TAG IN ('CREATE INDEX') EXECUTE FUNCTION schema_fault.reject_initialization()`); err != nil {
		t.Fatal(err)
	}
	err := storage.MigrateSchema(context.Background(), db)
	assertSchemaErrorKind(t, err, storage.SchemaErrorApply)
	assertTableExists(t, db, "tetral_schema_migrations", false)
	assertTableExists(t, db, "environments", false)
	if _, err := db.Exec(`DROP EVENT TRIGGER reject_initialization`); err != nil {
		t.Fatal(err)
	}
	if err := storage.MigrateSchema(context.Background(), db); err != nil {
		t.Fatalf("retry after actual rollback/lock release: %v", err)
	}
}

func TestMigrateSchemaCancellationRollsBackStampAndReleasesLock(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx, cancelAll := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelAll()
	if _, err := db.Exec(`CREATE SCHEMA schema_fault;
 CREATE FUNCTION schema_fault.block_initialization() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(709000); END $$;
 CREATE EVENT TRIGGER block_initialization ON ddl_command_start WHEN TAG IN ('CREATE INDEX') EXECUTE FUNCTION schema_fault.block_initialization()`); err != nil {
		t.Fatal(err)
	}
	blocker, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := blocker.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := blocker.ExecContext(ctx, `SELECT pg_advisory_lock(709000)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := blocker.ExecContext(context.Background(), `SELECT pg_advisory_unlock(709000)`); err != nil {
			t.Error(err)
		}
	}()
	cancelCtx, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { result <- storage.MigrateSchema(cancelCtx, db) }()
	for {
		var waiting bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'CREATE%INDEX%')`).Scan(&waiting); err != nil {
			cancel()
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-result:
			cancel()
			t.Fatalf("initialization missed DDL barrier: %v", err)
		case <-ctx.Done():
			cancel()
			t.Fatal("initialization DDL barrier not reached")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err = <-result:
	case <-ctx.Done():
		t.Fatal("initialization did not cancel")
	}
	assertSchemaErrorKind(t, err, storage.SchemaErrorCanceled)
	assertTableExists(t, db, "tetral_schema_migrations", false)
	assertTableExists(t, db, "runtime_processes", false)
	if _, err := db.Exec(`DROP EVENT TRIGGER block_initialization`); err != nil {
		t.Fatal(err)
	}
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("retry after cancellation/lock release: %v", err)
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

func TestMigrateSchemaRequiresGitHubResourceCredential(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	seedStorageSchemaSession(t, db, "wksp_github_credential", "sesn_github_credential")

	var nullable string
	if err := db.QueryRowContext(ctx, `SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'session_github_repository_resources'
		  AND column_name = 'authorization_token_encrypted'`).Scan(&nullable); err != nil {
		t.Fatalf("read token column nullability: %v", err)
	}
	if nullable != "NO" {
		t.Fatalf("authorization_token_encrypted nullable = %q; want NO", nullable)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO session_resources (
			workspace_id, session_id, resource_id, type, created_at, updated_at
		) VALUES (
			'wksp_github_credential', 'sesn_github_credential', 'sesrsc_token', 'github_repository',
			'2026-07-17T00:00:00Z', '2026-07-17T00:00:00Z'
		)`); err != nil {
		t.Fatalf("insert token resource: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_github_repository_resources (
			workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref,
			authorization_token_encrypted
		) VALUES (
			'wksp_github_credential', 'sesn_github_credential', 'sesrsc_token',
			'https://github.com/tetral-ai/tetral', '/workspace/tetral', NULL, NULL,
			decode('0102', 'hex')
		)`); err != nil {
		t.Fatalf("insert github resource with token: %v", err)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO session_resources (
			workspace_id, session_id, resource_id, type, created_at, updated_at
		) VALUES (
			'wksp_github_credential', 'sesn_github_credential', 'sesrsc_nulltoken', 'github_repository',
			'2026-07-17T00:01:00Z', '2026-07-17T00:01:00Z'
		)`); err != nil {
		t.Fatalf("insert null-token resource: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_github_repository_resources (
			workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref,
			authorization_token_encrypted
		) VALUES (
			'wksp_github_credential', 'sesn_github_credential', 'sesrsc_nulltoken',
			'https://github.com/tetral-ai/nulltoken', '/workspace/nulltoken', NULL, NULL, NULL
		)`); err == nil {
		t.Fatal("GitHub resource accepted a NULL authorization token")
	}
}

func TestMigrateSchemaCreatesAgentMailDeliveryIndex(t *testing.T) {
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
		    AND indexname = 'idx_session_events_agent_mail_delivery'`,
	).Scan(&definition); err != nil {
		t.Fatalf("read agent mail delivery index: %v", err)
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
			t.Fatalf("agent mail delivery index = %q; want %q", definition, want)
		}
	}
}

func TestMigrateSchemaCreatesMultimodalFileAttachmentState(t *testing.T) {
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
	var consumptionPairConstraint string
	if err := db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid)
		FROM pg_constraint
		WHERE conrelid = 'session_file_attachment_consumptions'::regclass
		  AND conname = 'session_file_attachment_consumptions_source_file_key'`).Scan(&consumptionPairConstraint); err != nil {
		t.Fatalf("read consumption source/file constraint: %v", err)
	}
	if consumptionPairConstraint != "UNIQUE (workspace_id, source_event_id, file_id)" {
		t.Fatalf("consumption source/file constraint = %q", consumptionPairConstraint)
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
	for _, eventID := range []string{"sevt_source_media", "sevt_request_start_media", "sevt_request_start_media_second"} {
		if _, err := db.ExecContext(ctx, `INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, visibility, session_visible, created_at, updated_at
		) VALUES (
			'wksp_media', 'sesn_media', 'thr_media', $1,
			CASE $1 WHEN 'sevt_source_media' THEN 1 WHEN 'sevt_request_start_media' THEN 2 ELSE 3 END,
			CASE WHEN $1 = 'sevt_source_media' THEN 'user.message' ELSE 'span.model_request_start' END,
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
		workspace_id, session_id, session_thread_id, request_start_event_id, source_event_id, file_id
	) VALUES (
		'wksp_media', 'sesn_media', 'thr_media', 'sevt_request_start_media', 'sevt_source_media', 'file_media'
	)`
	if _, err := db.ExecContext(ctx, insertConsumption); err != nil {
		t.Fatalf("insert media consumption: %v", err)
	}
	if _, err := db.ExecContext(ctx, insertConsumption); err == nil {
		t.Fatal("duplicate source/file media consumption was accepted")
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_file_attachment_consumptions (
		workspace_id, session_id, session_thread_id, request_start_event_id, source_event_id, file_id
	) VALUES (
		'wksp_media', 'sesn_media', 'thr_media',
		'sevt_request_start_media_second', 'sevt_source_media', 'file_media'
	)`); err == nil {
		t.Fatal("second Request Start consumed an existing source/file pair")
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
		workspace_id, session_id, session_thread_id, request_start_event_id, source_event_id, file_id
	) VALUES (
		'wksp_media', 'sesn_media_other', 'thr_media_other',
		'sevt_request_start_media', 'sevt_source_media', 'file_media'
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
	for _, eventID := range []string{"sevt_source_policy", "sevt_start_policy"} {
		if _, err := admin.ExecContext(ctx, `INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, visibility, session_visible, created_at, updated_at
		) VALUES (
			'wksp_media_policy', 'sesn_media_policy', 'thr_media_policy', $1,
			CASE WHEN $1='sevt_source_policy' THEN 1 ELSE 2 END,
			CASE WHEN $1='sevt_source_policy' THEN 'user.message' ELSE 'span.model_request_start' END,
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
		workspace_id, session_id, session_thread_id, request_start_event_id, source_event_id, file_id
	) VALUES (
		'wksp_media_policy', 'sesn_media_policy', 'thr_media_policy',
		'sevt_start_policy', 'sevt_source_policy', 'file_media_policy'
	)`); err != nil {
		t.Fatalf("runtime insert consumption: %v", err)
	}
	updateResult, err := tx.ExecContext(ctx, `UPDATE session_file_attachment_consumptions
		SET source_event_id='sevt_start_policy'
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

func TestMigrateSchemaRejectsUnregisteredObjectsAndPredecessorIdentity(t *testing.T) {
	for _, test := range []struct {
		name, setup string
		kind        storage.SchemaErrorKind
	}{
		{"table", `CREATE TABLE unregistered_table(id integer)`, storage.SchemaErrorUnexpectedState},
		{"sequence", `CREATE SEQUENCE unregistered_sequence`, storage.SchemaErrorUnexpectedState},
		{"function", `CREATE FUNCTION unregistered_function() RETURNS integer LANGUAGE sql AS 'SELECT 1'`, storage.SchemaErrorUnexpectedState},
		{"enum", `CREATE TYPE unregistered_enum AS ENUM ('first','second')`, storage.SchemaErrorUnexpectedState},
		{"domain", `CREATE DOMAIN unregistered_domain AS text DEFAULT 'private-domain-default' NOT NULL CHECK (VALUE <> '')`, storage.SchemaErrorUnexpectedState},
		{"collation", `CREATE COLLATION unregistered_collation (provider=libc, locale='C')`, storage.SchemaErrorUnexpectedState},
		{"predecessor", `CREATE TABLE tetral_schema_migrations(version bigint PRIMARY KEY,checksum text NOT NULL); INSERT INTO tetral_schema_migrations VALUES(1,'d42f4f8936525f02525b621e943d9ad98a91c6d8a76ca11a309c62dee496ade6')`, storage.SchemaErrorChecksumDrift},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := storagetest.NewEmptyPostgreSQLAdminDB(t)
			if _, err := db.Exec(test.setup); err != nil {
				t.Fatal(err)
			}
			before := baseTableNames(t, db)
			catalogBefore := unregisteredNamespaceCatalog(t, db)
			assertSchemaErrorKind(t, storage.MigrateSchema(context.Background(), db), test.kind)
			if after := baseTableNames(t, db); strings.Join(before, "|") != strings.Join(after, "|") {
				t.Fatal("rejected initialization changed predecessor objects")
			}
			if after := unregisteredNamespaceCatalog(t, db); after != catalogBefore {
				t.Fatal("rejected initialization changed namespace or object catalog metadata")
			}
			if test.name != "predecessor" {
				assertTableExists(t, db, "tetral_schema_migrations", false)
			}
			assertTableExists(t, db, "sessions", false)
		})
	}
}

// Snapshot object identities and definitions before a registry exists. Unlike
// the canonical catalog helper, this never reads or fabricates migration rows.
func unregisteredNamespaceCatalog(t *testing.T, db *sql.DB) string {
	t.Helper()
	var snapshot string
	if err := db.QueryRow(`SELECT jsonb_build_object(
		'namespace',to_jsonb(n),
		'dependencies',(SELECT jsonb_agg(to_jsonb(d) ORDER BY d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype)
			FROM pg_catalog.pg_depend d WHERE d.refclassid='pg_catalog.pg_namespace'::pg_catalog.regclass AND d.refobjid=n.oid),
		'types',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_catalog.pg_type t WHERE t.typnamespace=n.oid),
		'enum_values',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.enumtypid,e.enumsortorder)
			FROM pg_catalog.pg_enum e JOIN pg_catalog.pg_type t ON t.oid=e.enumtypid WHERE t.typnamespace=n.oid),
		'constraints',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.oid) FROM pg_catalog.pg_constraint c WHERE c.connamespace=n.oid),
		'collations',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.oid) FROM pg_catalog.pg_collation c WHERE c.collnamespace=n.oid),
		'relations',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.oid) FROM pg_catalog.pg_class c WHERE c.relnamespace=n.oid),
		'routines',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.oid) FROM pg_catalog.pg_proc p WHERE p.pronamespace=n.oid)
	)::text FROM pg_catalog.pg_namespace n WHERE n.nspname=pg_catalog.current_schema()`).Scan(&snapshot); err != nil {
		t.Fatalf("snapshot unregistered namespace catalog: %v", err)
	}
	return snapshot
}
