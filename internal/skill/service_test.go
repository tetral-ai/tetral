package skill_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type contextKey struct{}

func TestSkillDTOJSONShapeMatchesAnthropicParent(t *testing.T) {
	title := "Financial Analysis"
	latest := "1759178010641129"
	created := time.Date(2026, 4, 7, 14, 0, 0, 0, time.UTC)
	parent := &skill.Skill{
		ID:            "skill_parent",
		Type:          "skill",
		Source:        "custom",
		DisplayTitle:  &title,
		LatestVersion: &latest,
		CreatedAt:     created,
		UpdatedAt:     created,
	}

	raw, err := json.Marshal(parent)
	if err != nil {
		t.Fatalf("Marshal Skill: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal Skill JSON: %v", err)
	}
	assertJSONKeys(t, got, []string{"id", "type", "source", "display_title", "latest_version", "created_at", "updated_at"})
	assertJSONValue(t, got, "id", `"skill_parent"`)
	assertJSONValue(t, got, "type", `"skill"`)
	assertJSONValue(t, got, "source", `"custom"`)
	assertJSONValue(t, got, "display_title", `"Financial Analysis"`)
	assertJSONValue(t, got, "latest_version", `"1759178010641129"`)
	assertJSONValue(t, got, "created_at", `"2026-04-07T14:00:00Z"`)
	assertJSONValue(t, got, "updated_at", `"2026-04-07T14:00:00Z"`)
}

func TestSkillDTOJSONShapeAllowsNullDisplayTitleAndLatestVersion(t *testing.T) {
	parent := &skill.Skill{
		ID:            "skill_parent",
		Type:          "skill",
		Source:        "custom",
		DisplayTitle:  nil,
		LatestVersion: nil,
	}

	raw, err := json.Marshal(parent)
	if err != nil {
		t.Fatalf("Marshal Skill: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal Skill JSON: %v", err)
	}
	assertJSONKeys(t, got, []string{"id", "type", "source", "display_title", "latest_version", "created_at", "updated_at"})
	assertJSONValue(t, got, "display_title", "null")
	assertJSONValue(t, got, "latest_version", "null")
}

func TestSkillVersionDTOJSONShapeMatchesAnthropicVersion(t *testing.T) {
	created := time.Date(2026, 4, 7, 14, 0, 0, 0, time.UTC)
	version := &skill.SkillVersion{
		ID:          "skill_version_object",
		Type:        "skill_version",
		SkillID:     "skill_parent",
		Name:        "financial-analysis",
		Description: "Analyze financial data.",
		Directory:   "finance",
		Version:     "1759178010641129",
		CreatedAt:   created,
	}

	raw, err := json.Marshal(version)
	if err != nil {
		t.Fatalf("Marshal SkillVersion: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal SkillVersion JSON: %v", err)
	}
	assertJSONKeys(t, got, []string{"id", "type", "skill_id", "name", "description", "directory", "version", "created_at"})
	assertJSONValue(t, got, "id", `"skill_version_object"`)
	assertJSONValue(t, got, "type", `"skill_version"`)
	assertJSONValue(t, got, "skill_id", `"skill_parent"`)
	assertJSONValue(t, got, "name", `"financial-analysis"`)
	assertJSONValue(t, got, "description", `"Analyze financial data."`)
	assertJSONValue(t, got, "directory", `"finance"`)
	assertJSONValue(t, got, "version", `"1759178010641129"`)
	assertJSONValue(t, got, "created_at", `"2026-04-07T14:00:00Z"`)
}

func TestSkillListDTOJSONShapeIncludesNextPage(t *testing.T) {
	nextPage := "page_2"
	result := skill.SkillListResult{
		Data:     []*skill.Skill{{ID: "skill_parent", Type: "skill", Source: "custom"}},
		HasMore:  true,
		NextPage: &nextPage,
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Marshal SkillListResult: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal SkillListResult JSON: %v", err)
	}
	assertJSONKeys(t, got, []string{"data", "has_more", "next_page"})
	assertJSONValue(t, got, "has_more", "true")
	assertJSONValue(t, got, "next_page", `"page_2"`)
}

func TestSkillServiceCreateSkillValidatesDisplayTitleAndDelegates(t *testing.T) {
	stageDir := t.TempDir()
	files := stageUploadParts(t, stageDir, []uploadFile{{filename: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")}})
	defer cleanupParts(t, files)
	backend := &recordingSkillBackend{
		createSkillResult: &skill.Skill{ID: "skill_created", Type: "skill", Source: "custom"},
	}
	service := skill.NewService(backend, skill.WithPackageStageDir(stageDir))
	ctx := context.WithValue(context.Background(), contextKey{}, "create")
	title := "Financial Analysis\nQ1"
	input := skill.CreateSkillInput{DisplayTitle: &title, Files: files}

	got, err := service.CreateSkill(ctx, workspace.DefaultID, input)
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	if got.ID != "skill_created" {
		t.Fatalf("CreateSkill result ID = %q; want backend result", got.ID)
	}
	if backend.createSkillCalls != 1 {
		t.Fatalf("backend CreateSkill calls = %d; want 1", backend.createSkillCalls)
	}
	if backend.createSkillContext != ctx {
		t.Fatalf("backend context was not delegated")
	}
	if backend.createSkillWorkspace != workspace.DefaultID {
		t.Fatalf("backend workspace = %q; want %q", backend.createSkillWorkspace, workspace.DefaultID)
	}
	if backend.createSkillInput.DisplayTitle == nil || *backend.createSkillInput.DisplayTitle != title {
		t.Fatalf("backend display_title = %v; want %q", backend.createSkillInput.DisplayTitle, title)
	}
	if len(backend.createSkillInput.Files) != 0 {
		t.Fatalf("backend received raw Files: %+v", backend.createSkillInput.Files)
	}
	assertNormalizedPackageInput(t, backend.createSkillInput.Package, "finance", "financial-analysis", "Analyze financial data.")
	if _, err := backend.createSkillInput.Package.Open(); err == nil {
		t.Fatal("service must cleanup normalized package after backend returns")
	}
}

func TestSkillServiceCreateSkillAcceptsValidDisplayTitleBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		title string
	}{
		{name: "exactly 1024 ascii runes", title: strings.Repeat("a", 1024)},
		{name: "exactly 1024 multibyte runes", title: strings.Repeat("界", 1024)},
		{name: "ordinary whitespace", title: "Finance\tQ1\nReports\r"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stageDir := t.TempDir()
			files := stageUploadParts(t, stageDir, []uploadFile{{filename: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")}})
			defer cleanupParts(t, files)
			backend := &recordingSkillBackend{createSkillResult: &skill.Skill{ID: "skill_created"}}
			service := skill.NewService(backend, skill.WithPackageStageDir(stageDir))

			if _, err := service.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{DisplayTitle: &tc.title, Files: files}); err != nil {
				t.Fatalf("CreateSkill: %v", err)
			}
			if backend.createSkillCalls != 1 {
				t.Fatalf("backend CreateSkill calls = %d; want 1", backend.createSkillCalls)
			}
		})
	}
}

func TestSkillServiceCreateSkillRejectsInvalidDisplayTitleBeforeBackend(t *testing.T) {
	tests := []struct {
		name  string
		title string
	}{
		{name: "invalid utf8", title: string([]byte{0xff, 0xfe})},
		{name: "nul", title: "bad\x00title"},
		{name: "control", title: "bad\x01title"},
		{name: "oversized ascii", title: strings.Repeat("a", 1025)},
		{name: "oversized multibyte", title: strings.Repeat("界", 1025)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &recordingSkillBackend{}
			service := skill.NewService(backend)

			_, err := service.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{DisplayTitle: &tc.title})
			var validation *skill.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("CreateSkill error = %T %v; want ValidationError", err, err)
			}
			if backend.createSkillCalls != 0 {
				t.Fatalf("backend CreateSkill calls = %d; want 0", backend.createSkillCalls)
			}
			if strings.Contains(err.Error(), tc.title) {
				t.Fatalf("validation error echoed submitted display_title: %q", err.Error())
			}
		})
	}
}

func TestSkillServiceRejectsInvalidPackageBeforeBackend(t *testing.T) {
	stageDir := t.TempDir()
	parts := stageUploadParts(t, stageDir, []uploadFile{{filename: "upload.zip", body: []byte("not a zip")}})
	defer cleanupParts(t, parts)
	backend := &recordingSkillBackend{}
	service := skill.NewService(backend, skill.WithPackageStageDir(stageDir))

	_, err := service.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Files: parts})
	var validation *skill.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("CreateSkill error = %T %v; want ValidationError", err, err)
	}
	if backend.createSkillCalls != 0 {
		t.Fatalf("backend CreateSkill calls = %d; want 0", backend.createSkillCalls)
	}
}

func TestSkillServiceCleansNormalizedPackageOnBackendError(t *testing.T) {
	stageDir := t.TempDir()
	files := stageUploadParts(t, stageDir, []uploadFile{{filename: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")}})
	defer cleanupParts(t, files)
	backend := &recordingSkillBackend{createSkillError: errors.New("database password=secret exploded")}
	service := skill.NewService(backend, skill.WithPackageStageDir(stageDir))

	_, err := service.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Files: files})
	var internal *skill.InternalError
	if !errors.As(err, &internal) {
		t.Fatalf("CreateSkill error = %T %v; want InternalError", err, err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "database") {
		t.Fatalf("InternalError leaked backend detail: %q", err.Error())
	}
	if backend.createSkillCalls != 1 {
		t.Fatalf("backend CreateSkill calls = %d; want 1", backend.createSkillCalls)
	}
	if backend.createSkillInput.Package == nil {
		t.Fatal("backend did not receive normalized package before error")
	}
	if _, err := backend.createSkillInput.Package.Open(); err == nil {
		t.Fatal("service must cleanup normalized package after backend error")
	}
}

func TestSkillServiceDelegatesReadListDeleteMethods(t *testing.T) {
	backend := &recordingSkillBackend{
		getSkillResult:      &skill.Skill{ID: "skill_get", Type: "skill", Source: "custom"},
		listSkillsResult:    skill.SkillListResult{Data: []*skill.Skill{{ID: "skill_list", Type: "skill", Source: "custom"}}},
		createVersionResult: &skill.SkillVersion{ID: "skill_version_created", Type: "skill_version"},
		getVersionResult:    &skill.SkillVersion{ID: "skill_version_get", Type: "skill_version"},
		listVersionsResult:  skill.SkillVersionListResult{Data: []*skill.SkillVersion{{ID: "skill_version_list", Type: "skill_version"}}},
	}
	stageDir := t.TempDir()
	service := skill.NewService(backend, skill.WithPackageStageDir(stageDir))
	ctx := context.WithValue(context.Background(), contextKey{}, "delegation")
	createVersionFiles := stageUploadParts(t, stageDir, []uploadFile{{filename: "finance/SKILL.md", body: skillMD("financial-analysis-v2", "Analyze financial data v2.")}})
	defer cleanupParts(t, createVersionFiles)
	createVersionInput := skill.CreateVersionInput{Files: createVersionFiles}
	listSkillsOptions := skill.ListSkillsOptions{Limit: 10, Source: "custom"}
	listVersionsOptions := skill.ListVersionsOptions{Limit: 11}

	if _, err := service.GetSkill(ctx, workspace.DefaultID, "skill_get"); err != nil {
		t.Fatalf("GetSkill: %v", err)
	}
	if _, err := service.ListSkills(ctx, workspace.DefaultID, listSkillsOptions); err != nil {
		t.Fatalf("ListSkills: %v", err)
	}
	if err := service.DeleteSkill(ctx, workspace.DefaultID, "skill_delete"); err != nil {
		t.Fatalf("DeleteSkill: %v", err)
	}
	if _, err := service.CreateVersion(ctx, workspace.DefaultID, "skill_version_parent", createVersionInput); err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	if _, err := service.GetVersion(ctx, workspace.DefaultID, "skill_version_parent", "1759178010641129"); err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if _, err := service.ListVersions(ctx, workspace.DefaultID, "skill_version_parent", listVersionsOptions); err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if err := service.DeleteVersion(ctx, workspace.DefaultID, "skill_version_parent", "1759178010641129"); err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}

	if backend.getSkillCalls != 1 || backend.listSkillsCalls != 1 || backend.deleteSkillCalls != 1 ||
		backend.createVersionCalls != 1 || backend.getVersionCalls != 1 || backend.listVersionsCalls != 1 || backend.deleteVersionCalls != 1 {
		t.Fatalf("unexpected backend call counts: %+v", backend.callCounts())
	}
	if backend.getSkillContext != ctx || backend.getSkillWorkspace != workspace.DefaultID || backend.getSkillID != "skill_get" {
		t.Fatalf("GetSkill args = ctx:%v ws:%q id:%q", backend.getSkillContext == ctx, backend.getSkillWorkspace, backend.getSkillID)
	}
	if backend.listSkillsContext != ctx || backend.listSkillsWorkspace != workspace.DefaultID || !reflect.DeepEqual(backend.listSkillsOptions, listSkillsOptions) {
		t.Fatalf("ListSkills args = ctx:%v ws:%q options:%+v", backend.listSkillsContext == ctx, backend.listSkillsWorkspace, backend.listSkillsOptions)
	}
	if backend.deleteSkillContext != ctx || backend.deleteSkillWorkspace != workspace.DefaultID || backend.deleteSkillID != "skill_delete" {
		t.Fatalf("DeleteSkill args = ctx:%v ws:%q id:%q", backend.deleteSkillContext == ctx, backend.deleteSkillWorkspace, backend.deleteSkillID)
	}
	if backend.createVersionContext != ctx || backend.createVersionWorkspace != workspace.DefaultID ||
		backend.createVersionSkillID != "skill_version_parent" {
		t.Fatalf("CreateVersion args = ctx:%v ws:%q id:%q input:%+v", backend.createVersionContext == ctx, backend.createVersionWorkspace, backend.createVersionSkillID, backend.createVersionInput)
	}
	if len(backend.createVersionInput.Files) != 0 {
		t.Fatalf("backend received raw version Files: %+v", backend.createVersionInput.Files)
	}
	assertNormalizedPackageInput(t, backend.createVersionInput.Package, "finance", "financial-analysis-v2", "Analyze financial data v2.")
	if _, err := backend.createVersionInput.Package.Open(); err == nil {
		t.Fatal("service must cleanup normalized version package after backend returns")
	}
	if backend.getVersionContext != ctx || backend.getVersionWorkspace != workspace.DefaultID ||
		backend.getVersionSkillID != "skill_version_parent" || backend.getVersionVersion != "1759178010641129" {
		t.Fatalf("GetVersion args = ctx:%v ws:%q id:%q version:%q", backend.getVersionContext == ctx, backend.getVersionWorkspace, backend.getVersionSkillID, backend.getVersionVersion)
	}
	if backend.listVersionsContext != ctx || backend.listVersionsWorkspace != workspace.DefaultID ||
		backend.listVersionsSkillID != "skill_version_parent" || !reflect.DeepEqual(backend.listVersionsOptions, listVersionsOptions) {
		t.Fatalf("ListVersions args = ctx:%v ws:%q id:%q options:%+v", backend.listVersionsContext == ctx, backend.listVersionsWorkspace, backend.listVersionsSkillID, backend.listVersionsOptions)
	}
	if backend.deleteVersionContext != ctx || backend.deleteVersionWorkspace != workspace.DefaultID ||
		backend.deleteVersionSkillID != "skill_version_parent" || backend.deleteVersionVersion != "1759178010641129" {
		t.Fatalf("DeleteVersion args = ctx:%v ws:%q id:%q version:%q", backend.deleteVersionContext == ctx, backend.deleteVersionWorkspace, backend.deleteVersionSkillID, backend.deleteVersionVersion)
	}
}

func TestSkillServiceValidateAgentSkillReferencesDelegatesWithTransaction(t *testing.T) {
	ctx := context.Background()
	backend := &recordingSkillBackend{}
	service := skill.NewService(backend)
	refs := []agent.SkillReference{{SkillID: "skill_ref", Version: "latest"}}

	if err := service.ValidateAgentSkillReferences(ctx, nil, string(workspace.DefaultID), refs); err != nil {
		t.Fatalf("ValidateAgentSkillReferences: %v", err)
	}
	if backend.validateAgentCalls != 1 {
		t.Fatalf("ValidateAgentSkillReferences calls = %d; want 1", backend.validateAgentCalls)
	}
	if backend.validateAgentContext != ctx || backend.validateAgentWorkspaceID != string(workspace.DefaultID) {
		t.Fatalf("ValidateAgentSkillReferences args = ctx:%v workspace:%q", backend.validateAgentContext == ctx, backend.validateAgentWorkspaceID)
	}
	if len(backend.validateAgentRefs) != 1 || backend.validateAgentRefs[0].SkillID != "skill_ref" || backend.validateAgentRefs[0].Version != "latest" {
		t.Fatalf("ValidateAgentSkillReferences refs = %+v", backend.validateAgentRefs)
	}
}

func TestSkillServiceCleansNormalizedVersionPackageOnBackendError(t *testing.T) {
	stageDir := t.TempDir()
	files := stageUploadParts(t, stageDir, []uploadFile{{filename: "finance/SKILL.md", body: skillMD("financial-analysis-v2", "Analyze financial data v2.")}})
	defer cleanupParts(t, files)
	backend := &recordingSkillBackend{createVersionError: errors.New("database password=secret exploded")}
	service := skill.NewService(backend, skill.WithPackageStageDir(stageDir))

	_, err := service.CreateVersion(context.Background(), workspace.DefaultID, "skill_parent", skill.CreateVersionInput{Files: files})
	var internal *skill.InternalError
	if !errors.As(err, &internal) {
		t.Fatalf("CreateVersion error = %T %v; want InternalError", err, err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "database") {
		t.Fatalf("InternalError leaked backend detail: %q", err.Error())
	}
	if backend.createVersionCalls != 1 {
		t.Fatalf("backend CreateVersion calls = %d; want 1", backend.createVersionCalls)
	}
	if backend.createVersionInput.Package == nil {
		t.Fatal("backend did not receive normalized version package before error")
	}
	if _, err := backend.createVersionInput.Package.Open(); err == nil {
		t.Fatal("service must cleanup normalized version package after backend error")
	}
}

func TestSkillServiceNormalizesBackendErrors(t *testing.T) {
	typed := &skill.NotFoundError{Message: "skill not found"}
	backend := &recordingSkillBackend{getSkillError: typed}
	service := skill.NewService(backend)

	_, err := service.GetSkill(context.Background(), workspace.DefaultID, "skill_missing")
	if !errors.Is(err, typed) {
		t.Fatalf("GetSkill error = %T %v; want original typed error", err, err)
	}

	backend.getSkillError = errors.New("database password=secret exploded")
	_, err = service.GetSkill(context.Background(), workspace.DefaultID, "skill_missing")
	var internal *skill.InternalError
	if !errors.As(err, &internal) {
		t.Fatalf("GetSkill error = %T %v; want InternalError", err, err)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "database") {
		t.Fatalf("InternalError leaked backend detail: %q", err.Error())
	}
}

func TestSkillServiceInternalErrorPreservesDiagnosticCause(t *testing.T) {
	diagnostic := &dbconnect.DiagnosticError{
		Provider:  dbconnect.ProviderPlainDSN,
		Phase:     dbconnect.PhaseRuntimeQuery,
		Kind:      dbconnect.KindInternalError,
		Operation: "skill.registry.get",
		Message:   "private database diagnostic",
		Cause:     errors.New("private driver detail"),
	}
	backend := &recordingSkillBackend{getSkillError: diagnostic}
	service := skill.NewService(backend)

	_, err := service.GetSkill(context.Background(), workspace.DefaultID, "skill_missing")
	var internal *skill.InternalError
	if !errors.As(err, &internal) {
		t.Fatalf("GetSkill error = %T %v; want InternalError", err, err)
	}
	var gotDiagnostic *dbconnect.DiagnosticError
	if !errors.As(err, &gotDiagnostic) {
		t.Fatalf("GetSkill error = %T %v; want DiagnosticError cause", err, err)
	}
	if gotDiagnostic != diagnostic {
		t.Fatalf("diagnostic cause = %p; want %p", gotDiagnostic, diagnostic)
	}
	if err.Error() != "skill operation failed" {
		t.Fatalf("public error = %q; want safe skill message", err.Error())
	}
}

func assertJSONKeys(t *testing.T, got map[string]json.RawMessage, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("JSON keys = %v; want %v", keysOf(got), want)
	}
	for _, key := range want {
		if _, ok := got[key]; !ok {
			t.Fatalf("JSON missing key %q from %v", key, keysOf(got))
		}
	}
}

func assertJSONValue(t *testing.T, got map[string]json.RawMessage, key string, want string) {
	t.Helper()
	if string(got[key]) != want {
		t.Fatalf("%s = %s; want %s", key, got[key], want)
	}
}

func assertNormalizedPackageInput(t *testing.T, pkg *skill.StagedPackage, directory, name, description string) {
	t.Helper()
	if pkg == nil {
		t.Fatal("backend did not receive normalized package")
		return
	}
	if pkg.Directory != directory || pkg.Name != name || pkg.Description != description {
		t.Fatalf("package metadata = directory:%q name:%q description:%q", pkg.Directory, pkg.Name, pkg.Description)
	}
	if pkg.SizeBytes <= 0 || pkg.SHA256 == "" {
		t.Fatalf("package size/sha missing: %+v", pkg)
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

type recordingSkillBackend struct {
	createSkillCalls     int
	createSkillContext   context.Context
	createSkillWorkspace workspace.ID
	createSkillInput     skill.CreateSkillInput
	createSkillResult    *skill.Skill
	createSkillError     error

	getSkillCalls     int
	getSkillContext   context.Context
	getSkillWorkspace workspace.ID
	getSkillID        string
	getSkillResult    *skill.Skill
	getSkillError     error

	listSkillsCalls     int
	listSkillsContext   context.Context
	listSkillsWorkspace workspace.ID
	listSkillsOptions   skill.ListSkillsOptions
	listSkillsResult    skill.SkillListResult
	listSkillsError     error

	deleteSkillCalls     int
	deleteSkillContext   context.Context
	deleteSkillWorkspace workspace.ID
	deleteSkillID        string
	deleteSkillError     error

	createVersionCalls     int
	createVersionContext   context.Context
	createVersionWorkspace workspace.ID
	createVersionSkillID   string
	createVersionInput     skill.CreateVersionInput
	createVersionResult    *skill.SkillVersion
	createVersionError     error

	getVersionCalls     int
	getVersionContext   context.Context
	getVersionWorkspace workspace.ID
	getVersionSkillID   string
	getVersionVersion   string
	getVersionResult    *skill.SkillVersion
	getVersionError     error

	listVersionsCalls     int
	listVersionsContext   context.Context
	listVersionsWorkspace workspace.ID
	listVersionsSkillID   string
	listVersionsOptions   skill.ListVersionsOptions
	listVersionsResult    skill.SkillVersionListResult
	listVersionsError     error

	deleteVersionCalls     int
	deleteVersionContext   context.Context
	deleteVersionWorkspace workspace.ID
	deleteVersionSkillID   string
	deleteVersionVersion   string
	deleteVersionError     error

	validateAgentCalls       int
	validateAgentContext     context.Context
	validateAgentTx          agent.Transaction
	validateAgentWorkspaceID string
	validateAgentRefs        []agent.SkillReference
	validateAgentError       error
}

func (b *recordingSkillBackend) CreateSkill(ctx context.Context, ws workspace.ID, input skill.CreateSkillInput) (*skill.Skill, error) {
	b.createSkillCalls++
	b.createSkillContext = ctx
	b.createSkillWorkspace = ws
	b.createSkillInput = input
	return b.createSkillResult, b.createSkillError
}

func (b *recordingSkillBackend) GetSkill(ctx context.Context, ws workspace.ID, skillID string) (*skill.Skill, error) {
	b.getSkillCalls++
	b.getSkillContext = ctx
	b.getSkillWorkspace = ws
	b.getSkillID = skillID
	return b.getSkillResult, b.getSkillError
}

func (b *recordingSkillBackend) ListSkills(ctx context.Context, ws workspace.ID, options skill.ListSkillsOptions) (skill.SkillListResult, error) {
	b.listSkillsCalls++
	b.listSkillsContext = ctx
	b.listSkillsWorkspace = ws
	b.listSkillsOptions = options
	return b.listSkillsResult, b.listSkillsError
}

func (b *recordingSkillBackend) DeleteSkill(ctx context.Context, ws workspace.ID, skillID string) error {
	b.deleteSkillCalls++
	b.deleteSkillContext = ctx
	b.deleteSkillWorkspace = ws
	b.deleteSkillID = skillID
	return b.deleteSkillError
}

func (b *recordingSkillBackend) CreateVersion(ctx context.Context, ws workspace.ID, skillID string, input skill.CreateVersionInput) (*skill.SkillVersion, error) {
	b.createVersionCalls++
	b.createVersionContext = ctx
	b.createVersionWorkspace = ws
	b.createVersionSkillID = skillID
	b.createVersionInput = input
	return b.createVersionResult, b.createVersionError
}

func (b *recordingSkillBackend) GetVersion(ctx context.Context, ws workspace.ID, skillID, version string) (*skill.SkillVersion, error) {
	b.getVersionCalls++
	b.getVersionContext = ctx
	b.getVersionWorkspace = ws
	b.getVersionSkillID = skillID
	b.getVersionVersion = version
	return b.getVersionResult, b.getVersionError
}

func (b *recordingSkillBackend) OpenVersionContent(context.Context, workspace.ID, string, string) (io.ReadCloser, error) {
	return nil, errors.New("version content is not configured")
}

func (b *recordingSkillBackend) ListVersions(ctx context.Context, ws workspace.ID, skillID string, options skill.ListVersionsOptions) (skill.SkillVersionListResult, error) {
	b.listVersionsCalls++
	b.listVersionsContext = ctx
	b.listVersionsWorkspace = ws
	b.listVersionsSkillID = skillID
	b.listVersionsOptions = options
	return b.listVersionsResult, b.listVersionsError
}

func (b *recordingSkillBackend) DeleteVersion(ctx context.Context, ws workspace.ID, skillID, version string) error {
	b.deleteVersionCalls++
	b.deleteVersionContext = ctx
	b.deleteVersionWorkspace = ws
	b.deleteVersionSkillID = skillID
	b.deleteVersionVersion = version
	return b.deleteVersionError
}

func (b *recordingSkillBackend) ValidateAgentSkillReferences(ctx context.Context, tx agent.Transaction, workspaceID string, refs []agent.SkillReference) error {
	b.validateAgentCalls++
	b.validateAgentContext = ctx
	b.validateAgentTx = tx
	b.validateAgentWorkspaceID = workspaceID
	b.validateAgentRefs = append([]agent.SkillReference(nil), refs...)
	return b.validateAgentError
}

func (b *recordingSkillBackend) callCounts() map[string]int {
	return map[string]int{
		"CreateSkill":                  b.createSkillCalls,
		"GetSkill":                     b.getSkillCalls,
		"ListSkills":                   b.listSkillsCalls,
		"DeleteSkill":                  b.deleteSkillCalls,
		"CreateVersion":                b.createVersionCalls,
		"GetVersion":                   b.getVersionCalls,
		"ListVersions":                 b.listVersionsCalls,
		"DeleteVersion":                b.deleteVersionCalls,
		"ValidateAgentSkillReferences": b.validateAgentCalls,
	}
}
