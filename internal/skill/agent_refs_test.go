package skill_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func runCheck(t *testing.T, runtimeDB *sql.DB, service *skill.Service, ws workspace.ID, refs []agent.SkillReference) error {
	t.Helper()
	return dbconnect.NewClientForTesting(runtimeDB).WithWorkspaceTx(context.Background(), string(ws), "skill.validate_agent_refs", func(tx *dbconnect.Tx) error {
		return service.ValidateAgentSkillReferences(context.Background(), tx, string(ws), refs)
	})
}

func TestServiceValidateAgentSkillReferencesHappyPathLatestAndConcrete(t *testing.T) {
	runtime, _, _, stageDir, store := newSkillStoreEnv(t)
	service := skill.NewService(store, skill.WithPackageStageDir(stageDir))
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "ref/SKILL.md", body: skillMD("ref", "v1")}})
	defer func() { _ = pkg.Cleanup() }()
	created, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}

	if err := runCheck(t, runtime, service, workspace.DefaultID, []agent.SkillReference{{SkillID: created.ID, Version: "latest"}}); err != nil {
		t.Fatalf("latest ref rejected: %v", err)
	}
	if err := runCheck(t, runtime, service, workspace.DefaultID, []agent.SkillReference{{SkillID: created.ID, Version: *created.LatestVersion}}); err != nil {
		t.Fatalf("concrete ref rejected: %v", err)
	}
}

func TestPostgreSQLStoreValidateAgentSkillReferencesUsesCallerTransaction(t *testing.T) {
	store := skill.NewPostgreSQLStore(nil, nil)
	tx := &recordingAgentTransaction{
		rows: []recordingRow{
			{values: []any{sql.NullString{String: "version_live", Valid: true}, sql.NullInt64{Int64: 1, Valid: true}}},
			{values: []any{sql.NullString{String: "version_live", Valid: true}, sql.NullInt64{Int64: 1, Valid: true}}},
			{values: []any{1}},
		},
	}
	refs := []agent.SkillReference{
		{SkillID: "skill_live", Version: "latest"},
		{SkillID: "skill_live", Version: "version_live"},
	}

	if err := store.ValidateAgentSkillReferences(context.Background(), tx, string(workspace.DefaultID), refs); err != nil {
		t.Fatalf("ValidateAgentSkillReferences with caller tx: %v", err)
	}
	if tx.execContextCalls != 1 {
		t.Fatalf("ExecContext calls = %d; want advisory lock through caller tx", tx.execContextCalls)
	}
	if tx.execCalls != 0 {
		t.Fatalf("Exec calls = %d; want lock through ExecContext compatibility path only", tx.execCalls)
	}
	if len(tx.queryRowScannerCalls) != 3 {
		t.Fatalf("QueryRowScanner calls = %d; want latest parent, concrete parent, concrete version reads", len(tx.queryRowScannerCalls))
	}
	if !strings.Contains(tx.execContextQueries[0], "pg_advisory_xact_lock") {
		t.Fatalf("advisory lock query = %q; want pg_advisory_xact_lock", tx.execContextQueries[0])
	}
}

func TestServiceValidateAgentSkillReferencesRejectsMissingAndCrossWorkspace(t *testing.T) {
	runtime, admin, _, stageDir, store := newSkillStoreEnv(t)
	service := skill.NewService(store, skill.WithPackageStageDir(stageDir))
	err := runCheck(t, runtime, service, workspace.DefaultID, []agent.SkillReference{{SkillID: "skill_missing", Version: "latest"}})
	if !errors.As(err, new(*agent.ValidationError)) {
		t.Fatalf("missing ref error = %T %v; want agent.ValidationError", err, err)
	}
	seedWorkspace(t, admin, "other_ws")
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "other/SKILL.md", body: skillMD("other", "v1")}})
	defer func() { _ = pkg.Cleanup() }()
	other, err := store.CreateSkill(context.Background(), "other_ws", skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill other: %v", err)
	}
	err = runCheck(t, runtime, service, workspace.DefaultID, []agent.SkillReference{{SkillID: other.ID, Version: "latest"}})
	if !errors.As(err, new(*agent.ValidationError)) {
		t.Fatalf("cross-workspace ref error = %T %v; want agent.ValidationError", err, err)
	}
}

type recordingAgentTransaction struct {
	execCalls            int
	execContextCalls     int
	execContextQueries   []string
	queryRowScannerCalls []string
	rows                 []recordingRow
}

func (tx *recordingAgentTransaction) Exec(context.Context, string, ...any) (sql.Result, error) {
	tx.execCalls++
	return recordingResult(0), nil
}

func (tx *recordingAgentTransaction) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	tx.execContextCalls++
	tx.execContextQueries = append(tx.execContextQueries, query)
	return recordingResult(0), nil
}

func (tx *recordingAgentTransaction) QueryRows(context.Context, string, ...any) (interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}, error) {
	return nil, errors.New("QueryRows must not be used for Agent Skill reference validation")
}

func (tx *recordingAgentTransaction) QueryRowScanner(_ context.Context, query string, _ ...any) interface{ Scan(dest ...any) error } {
	tx.queryRowScannerCalls = append(tx.queryRowScannerCalls, query)
	if len(tx.rows) == 0 {
		return recordingRow{err: errors.New("unexpected QueryRowScanner call")}
	}
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}

type recordingResult int64

func (r recordingResult) LastInsertId() (int64, error) { return 0, errors.New("LastInsertId unused") }
func (r recordingResult) RowsAffected() (int64, error) { return int64(r), nil }

type recordingRow struct {
	values []any
	err    error
}

func (row recordingRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(dest) != len(row.values) {
		return fmt.Errorf("scan destination count %d does not match values %d", len(dest), len(row.values))
	}
	for index, value := range row.values {
		switch target := dest[index].(type) {
		case *sql.NullString:
			*target = value.(sql.NullString)
		case *sql.NullInt64:
			*target = value.(sql.NullInt64)
		case *int:
			*target = value.(int)
		default:
			return fmt.Errorf("unsupported scan destination %T", dest[index])
		}
	}
	return nil
}

func TestServiceValidateAgentSkillReferencesRejectsLatestWhenNoActiveVersion(t *testing.T) {
	runtime, _, _, stageDir, store := newSkillStoreEnv(t)
	service := skill.NewService(store, skill.WithPackageStageDir(stageDir))
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "empty/SKILL.md", body: skillMD("empty", "v1")}})
	defer func() { _ = pkg.Cleanup() }()
	created, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	if err := store.DeleteVersion(context.Background(), workspace.DefaultID, created.ID, *created.LatestVersion); err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}
	err = runCheck(t, runtime, service, workspace.DefaultID, []agent.SkillReference{{SkillID: created.ID, Version: "latest"}})
	if !errors.As(err, new(*agent.ValidationError)) {
		t.Fatalf("latest with no active version error = %T %v; want agent.ValidationError", err, err)
	}
}

func TestServiceValidateAgentSkillReferencesRejectsConcreteDeletedVersion(t *testing.T) {
	runtime, _, _, stageDir, store := newSkillStoreEnv(t)
	service := skill.NewService(store, skill.WithPackageStageDir(stageDir))
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "deleted/SKILL.md", body: skillMD("deleted", "v1")}})
	defer func() { _ = pkg.Cleanup() }()
	created, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	deletedVersion := *created.LatestVersion
	if err := store.DeleteVersion(context.Background(), workspace.DefaultID, created.ID, deletedVersion); err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}
	err = runCheck(t, runtime, service, workspace.DefaultID, []agent.SkillReference{{SkillID: created.ID, Version: deletedVersion}})
	if !errors.As(err, new(*agent.ValidationError)) {
		t.Fatalf("deleted concrete version error = %T %v; want agent.ValidationError", err, err)
	}
}
