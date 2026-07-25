package skill_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func newSkillStoreEnv(t *testing.T) (runtime, admin *sql.DB, blobStore *blob.FakeBlobStore, stageDir string, store *skill.PostgreSQLSkillStore) {
	t.Helper()
	runtime, admin = storagetest.NewPostgreSQLDBWithAdmin(t)
	blobStore = blob.NewFakeBlobStore()
	stageDir = t.TempDir()
	store = skill.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime), blobStore)
	return
}

func newSkillStoreBackendEnv(t *testing.T) (*blob.FakeBlobStore, string, *skill.PostgreSQLSkillStore, *sql.DB) {
	t.Helper()
	runtime, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
	blobStore := blob.NewFakeBlobStore()
	stageDir := t.TempDir()
	store := skill.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime), blobStore)
	return blobStore, stageDir, store, runtime
}

func seedWorkspace(t *testing.T, admin *sql.DB, id workspace.ID) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`, string(id)); err != nil {
		t.Fatalf("seed workspace %s: %v", id, err)
	}
}

func TestCreateSkillStoresParentVersionAndNormalizedZipBlob(t *testing.T) {
	blobStore, stageDir, store, db := newSkillStoreBackendEnv(t)
	title := "Financial Analysis"
	store.SetClock(fixedClock("2026-04-07T14:00:00Z"))
	store.SetVersionStrategy(func(time.Time) string { return "1759178010641129" })
	pkg := normalizedPackage(t, stageDir, []uploadFile{
		{filename: "finance/SKILL.md", body: skillMD("financial-analysis", "Analyze financial data.")},
		{filename: "finance/data.csv", body: []byte("a,b\n1,2\n")},
	})
	defer func() { _ = pkg.Cleanup() }()

	created, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{DisplayTitle: &title, Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	if !strings.HasPrefix(created.ID, skill.IDPrefix) {
		t.Fatalf("skill id = %q; want %q prefix", created.ID, skill.IDPrefix)
	}
	if created.Type != "skill" || created.Source != "custom" {
		t.Fatalf("parent type/source = %q/%q", created.Type, created.Source)
	}
	if created.DisplayTitle == nil || *created.DisplayTitle != title {
		t.Fatalf("display_title = %v; want %q", created.DisplayTitle, title)
	}
	if created.LatestVersion == nil || *created.LatestVersion != "1759178010641129" {
		t.Fatalf("latest_version = %v", created.LatestVersion)
	}

	version, err := store.GetVersion(context.Background(), workspace.DefaultID, created.ID, "1759178010641129")
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if !strings.HasPrefix(version.ID, skill.VersionIDPrefix) {
		t.Fatalf("version id = %q; want %q prefix", version.ID, skill.VersionIDPrefix)
	}
	if version.SkillID != created.ID || version.Name != "financial-analysis" || version.Description != "Analyze financial data." || version.Directory != "finance" {
		t.Fatalf("version metadata = %+v", version)
	}

	expectedKey := fmt.Sprintf("skills/%s/%s/versions/%s/package.zip", workspace.DefaultID, created.ID, version.Version)
	raw, ok := blobStore.Bytes(expectedKey)
	if !ok {
		t.Fatalf("normalized package blob missing at %q", expectedKey)
	}
	assertNormalizedZipBlob(t, raw, []string{"finance/", "finance/SKILL.md", "finance/data.csv"})
	sum := sha256.Sum256(raw)
	assertStoredBlobMetadata(t, db, created.ID, version.Version, expectedKey, int64(len(raw)), hex.EncodeToString(sum[:]))
}

func TestCreateSkillWithoutDisplayTitleStoresNullAndDuplicateNamesAreAllowed(t *testing.T) {
	_, stageDir, store, _ := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(sequenceVersions("1001", "1002"))
	firstPackage := normalizedPackage(t, stageDir, []uploadFile{{filename: "one/SKILL.md", body: skillMD("shared-name", "one")}})
	defer func() { _ = firstPackage.Cleanup() }()
	secondPackage := normalizedPackage(t, stageDir, []uploadFile{{filename: "two/SKILL.md", body: skillMD("shared-name", "two")}})
	defer func() { _ = secondPackage.Cleanup() }()

	first, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: firstPackage})
	if err != nil {
		t.Fatalf("CreateSkill first: %v", err)
	}
	second, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: secondPackage})
	if err != nil {
		t.Fatalf("CreateSkill second with same frontmatter name: %v", err)
	}
	if first.DisplayTitle != nil {
		t.Fatalf("first display_title = %v; want nil", first.DisplayTitle)
	}
	if first.ID == second.ID {
		t.Fatalf("duplicate name created same parent id %q", first.ID)
	}
}

func TestCreateVersionAllowsDifferentMetadataAndAdvancesLatestVersion(t *testing.T) {
	_, stageDir, store, _ := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(sequenceVersions("2001", "2002"))
	firstPackage := normalizedPackage(t, stageDir, []uploadFile{{filename: "alpha/SKILL.md", body: skillMD("alpha-name", "first")}})
	defer func() { _ = firstPackage.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: firstPackage})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	secondPackage := normalizedPackage(t, stageDir, []uploadFile{{filename: "beta/SKILL.md", body: skillMD("beta-name", "second")}})
	defer func() { _ = secondPackage.Cleanup() }()

	version, err := store.CreateVersion(context.Background(), workspace.DefaultID, parent.ID, skill.CreateVersionInput{Package: secondPackage})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	if version.Name != "beta-name" || version.Description != "second" || version.Directory != "beta" || version.Version != "2002" {
		t.Fatalf("CreateVersion metadata = %+v", version)
	}
	got, err := store.GetSkill(context.Background(), workspace.DefaultID, parent.ID)
	if err != nil {
		t.Fatalf("GetSkill: %v", err)
	}
	if got.LatestVersion == nil || *got.LatestVersion != "2002" {
		t.Fatalf("latest_version = %v; want 2002", got.LatestVersion)
	}
}

func TestCreateVersionRetriesDuplicateVersionThroughRuntimeClientSavepoint(t *testing.T) {
	blobStore, stageDir, store, db := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(sequenceVersions("initial", "initial", "retry_ok"))
	initialPackage := normalizedPackage(t, stageDir, []uploadFile{{filename: "retry/SKILL.md", body: skillMD("retry", "initial")}})
	defer func() { _ = initialPackage.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: initialPackage})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	nextPackage := normalizedPackage(t, stageDir, []uploadFile{{filename: "retry/SKILL.md", body: skillMD("retry", "next")}})
	defer func() { _ = nextPackage.Cleanup() }()

	version, err := store.CreateVersion(context.Background(), workspace.DefaultID, parent.ID, skill.CreateVersionInput{Package: nextPackage})
	if err != nil {
		t.Fatalf("CreateVersion retry after duplicate: %v", err)
	}
	if version.Version != "retry_ok" {
		t.Fatalf("retried version = %q; want retry_ok", version.Version)
	}
	got, err := store.GetSkill(context.Background(), workspace.DefaultID, parent.ID)
	if err != nil {
		t.Fatalf("GetSkill after retry: %v", err)
	}
	if got.LatestVersion == nil || *got.LatestVersion != "retry_ok" {
		t.Fatalf("latest_version after retry = %v; want retry_ok", got.LatestVersion)
	}
	var versionCount int
	if err := storage.WithWorkspaceTx(context.Background(), db, string(workspace.DefaultID), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(),
			`SELECT count(*) FROM skill_versions WHERE workspace_id = $1 AND skill_id = $2`,
			string(workspace.DefaultID), parent.ID).Scan(&versionCount)
	}); err != nil {
		t.Fatalf("count skill_versions after retry: %v", err)
	}
	if versionCount != 2 {
		t.Fatalf("skill_versions after savepoint retry = %d; want initial plus retry_ok", versionCount)
	}
	if _, ok := blobStore.Bytes(fmt.Sprintf("skills/%s/%s/versions/retry_ok/package.zip", workspace.DefaultID, parent.ID)); !ok {
		t.Fatalf("retried package blob missing")
	}
}

func TestDeleteSemanticsKeepParentActiveUntilVersionsAreGone(t *testing.T) {
	_, stageDir, store, _ := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(sequenceVersions("3001", "3002", "3003"))
	parentPackage := normalizedPackage(t, stageDir, []uploadFile{{filename: "pkg/SKILL.md", body: skillMD("pkg", "v1")}})
	defer func() { _ = parentPackage.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: parentPackage})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	v2Package := normalizedPackage(t, stageDir, []uploadFile{{filename: "pkg/SKILL.md", body: skillMD("pkg", "v2")}})
	defer func() { _ = v2Package.Cleanup() }()
	v2, err := store.CreateVersion(context.Background(), workspace.DefaultID, parent.ID, skill.CreateVersionInput{Package: v2Package})
	if err != nil {
		t.Fatalf("CreateVersion v2: %v", err)
	}
	v3Package := normalizedPackage(t, stageDir, []uploadFile{{filename: "pkg/SKILL.md", body: skillMD("pkg", "v3")}})
	defer func() { _ = v3Package.Cleanup() }()
	v3, err := store.CreateVersion(context.Background(), workspace.DefaultID, parent.ID, skill.CreateVersionInput{Package: v3Package})
	if err != nil {
		t.Fatalf("CreateVersion v3: %v", err)
	}

	err = store.DeleteSkill(context.Background(), workspace.DefaultID, parent.ID)
	var validation *skill.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("DeleteSkill with active versions error = %T %v; want ValidationError", err, err)
	}
	if err := store.DeleteVersion(context.Background(), workspace.DefaultID, parent.ID, v2.Version); err != nil {
		t.Fatalf("DeleteVersion non-latest: %v", err)
	}
	got, err := store.GetSkill(context.Background(), workspace.DefaultID, parent.ID)
	if err != nil {
		t.Fatalf("GetSkill after non-latest delete: %v", err)
	}
	if got.LatestVersion == nil || *got.LatestVersion != v3.Version {
		t.Fatalf("latest after non-latest delete = %v; want %s", got.LatestVersion, v3.Version)
	}
	if err := store.DeleteVersion(context.Background(), workspace.DefaultID, parent.ID, v3.Version); err != nil {
		t.Fatalf("DeleteVersion latest: %v", err)
	}
	got, err = store.GetSkill(context.Background(), workspace.DefaultID, parent.ID)
	if err != nil {
		t.Fatalf("GetSkill after latest delete: %v", err)
	}
	initialVersion := *parent.LatestVersion
	if got.LatestVersion == nil || *got.LatestVersion != initialVersion {
		t.Fatalf("latest after latest delete = %v; want initial %s", got.LatestVersion, initialVersion)
	}
	if err := store.DeleteVersion(context.Background(), workspace.DefaultID, parent.ID, initialVersion); err != nil {
		t.Fatalf("DeleteVersion last: %v", err)
	}
	got, err = store.GetSkill(context.Background(), workspace.DefaultID, parent.ID)
	if err != nil {
		t.Fatalf("parent should remain active with no versions: %v", err)
	}
	if got.LatestVersion != nil {
		t.Fatalf("latest_version after deleting last version = %v; want nil", got.LatestVersion)
	}
	if err := store.DeleteSkill(context.Background(), workspace.DefaultID, parent.ID); err != nil {
		t.Fatalf("DeleteSkill after zero active versions: %v", err)
	}
}

func TestListSkillsReturnsParentWithNullLatestVersion(t *testing.T) {
	_, stageDir, store, _ := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(func(time.Time) string { return "null_latest" })
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "nil-latest/SKILL.md", body: skillMD("nil-latest", "latest")}})
	defer func() { _ = pkg.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	if err := store.DeleteVersion(context.Background(), workspace.DefaultID, parent.ID, *parent.LatestVersion); err != nil {
		t.Fatalf("DeleteVersion last: %v", err)
	}
	result, err := store.ListSkills(context.Background(), workspace.DefaultID, skill.ListSkillsOptions{Limit: 20})
	if err != nil {
		t.Fatalf("ListSkills: %v", err)
	}
	if len(result.Data) != 1 {
		t.Fatalf("ListSkills length = %d; want 1", len(result.Data))
	}
	if result.Data[0].ID != parent.ID || result.Data[0].LatestVersion != nil {
		t.Fatalf("listed parent = %+v; want same parent with nil latest_version", result.Data[0])
	}
}

func TestListVersionsReturnsPublicMetadataWithoutBlobFields(t *testing.T) {
	_, stageDir, store, _ := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(func(time.Time) string { return "public_meta" })
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "public/SKILL.md", body: skillMD("public", "metadata")}})
	defer func() { _ = pkg.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	result, err := store.ListVersions(context.Background(), workspace.DefaultID, parent.ID, skill.ListVersionsOptions{Limit: 20})
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(result.Data) != 1 {
		t.Fatalf("ListVersions length = %d; want 1", len(result.Data))
	}
	version := result.Data[0]
	if version.Name != "public" || version.Description != "metadata" || version.Directory != "public" || version.Version != "public_meta" {
		t.Fatalf("listed version metadata = %+v", version)
	}
	versionType := reflect.TypeOf(*version)
	for _, forbidden := range []string{"BlobKey", "SizeBytes", "SHA256"} {
		if _, ok := versionType.FieldByName(forbidden); ok {
			t.Fatalf("public SkillVersion DTO exposes forbidden field %s", forbidden)
		}
	}
}

func TestListSkillsAndVersionsUseScopedSignedPageTokens(t *testing.T) {
	secret := bytes.Repeat([]byte{7}, 32)
	_, stageDir, backend, _ := newSkillStoreBackendEnv(t)
	backend.SetVersionStrategy(sequenceVersions("4001", "4002", "4003", "5002", "5003"))
	service := skill.NewService(backend, skill.WithPackageStageDir(stageDir), skill.WithPageTokenSecret(secret))
	assertSkillsInvalidPageToken := func(ws workspace.ID, options skill.ListSkillsOptions, labels ...string) {
		t.Helper()
		_, err := service.ListSkills(context.Background(), ws, options)
		assertInvalidPageToken(t, err, labels...)
	}
	assertVersionsInvalidPageToken := func(ws workspace.ID, skillID string, options skill.ListVersionsOptions, labels ...string) {
		t.Helper()
		_, err := service.ListVersions(context.Background(), ws, skillID, options)
		assertInvalidPageToken(t, err, labels...)
	}
	var versionedSkillID string
	for _, name := range []string{"one", "two", "three"} {
		files := stageUploadParts(t, stageDir, []uploadFile{{filename: name + "/SKILL.md", body: skillMD(name, name)}})
		defer cleanupParts(t, files)
		created, err := service.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Files: files})
		if err != nil {
			t.Fatalf("CreateSkill %s: %v", name, err)
		}
		if versionedSkillID == "" {
			versionedSkillID = created.ID
		}
	}

	first, err := service.ListSkills(context.Background(), workspace.DefaultID, skill.ListSkillsOptions{Limit: 2, Source: "custom"})
	if err != nil {
		t.Fatalf("ListSkills first: %v", err)
	}
	if len(first.Data) != 2 || !first.HasMore || first.NextPage == nil {
		t.Fatalf("first page = %+v", first)
	}
	second, err := service.ListSkills(context.Background(), workspace.DefaultID, skill.ListSkillsOptions{Limit: 2, Source: "custom", Page: *first.NextPage})
	if err != nil {
		t.Fatalf("ListSkills second: %v", err)
	}
	if len(second.Data) != 1 || second.HasMore || second.NextPage != nil {
		t.Fatalf("second page = %+v", second)
	}
	skillCursor := first.Data[1]
	if skillCursor.LatestVersion == nil {
		t.Fatalf("cursor skill latest_version is nil")
	}
	if err := backend.DeleteVersion(context.Background(), workspace.DefaultID, skillCursor.ID, *skillCursor.LatestVersion); err != nil {
		t.Fatalf("DeleteVersion cursor skill: %v", err)
	}
	if err := backend.DeleteSkill(context.Background(), workspace.DefaultID, skillCursor.ID); err != nil {
		t.Fatalf("DeleteSkill cursor skill: %v", err)
	}
	afterDeletedCursor, err := service.ListSkills(context.Background(), workspace.DefaultID, skill.ListSkillsOptions{Limit: 2, Source: "custom", Page: *first.NextPage})
	if err != nil {
		t.Fatalf("ListSkills after deleted cursor: %v", err)
	}
	if len(afterDeletedCursor.Data) != 1 || afterDeletedCursor.Data[0].ID != second.Data[0].ID || afterDeletedCursor.HasMore || afterDeletedCursor.NextPage != nil {
		t.Fatalf("page after deleted cursor = %+v; want remaining skill %s only", afterDeletedCursor, second.Data[0].ID)
	}
	assertSkillsInvalidPageToken("workspace_b", skill.ListSkillsOptions{Limit: 2, Source: "custom", Page: *first.NextPage})
	assertSkillsInvalidPageToken(workspace.DefaultID, skill.ListSkillsOptions{Limit: 2, Source: "anthropic", Page: *first.NextPage})
	tampered := *first.NextPage
	if strings.HasSuffix(tampered, "A") {
		tampered = strings.TrimSuffix(tampered, "A") + "B"
	} else {
		tampered += "A"
	}
	assertSkillsInvalidPageToken(workspace.DefaultID, skill.ListSkillsOptions{Limit: 2, Source: "custom", Page: tampered})

	for _, name := range []string{"one-v2", "one-v3"} {
		files := stageUploadParts(t, stageDir, []uploadFile{{filename: name + "/SKILL.md", body: skillMD(name, name)}})
		defer cleanupParts(t, files)
		if _, err := service.CreateVersion(context.Background(), workspace.DefaultID, versionedSkillID, skill.CreateVersionInput{Files: files}); err != nil {
			t.Fatalf("CreateVersion %s: %v", name, err)
		}
	}
	versionFirst, err := service.ListVersions(context.Background(), workspace.DefaultID, versionedSkillID, skill.ListVersionsOptions{Limit: 2})
	if err != nil {
		t.Fatalf("ListVersions first: %v", err)
	}
	if len(versionFirst.Data) != 2 || !versionFirst.HasMore || versionFirst.NextPage == nil {
		t.Fatalf("version first page = %+v", versionFirst)
	}
	versionSecond, err := service.ListVersions(context.Background(), workspace.DefaultID, versionedSkillID, skill.ListVersionsOptions{Limit: 2, Page: *versionFirst.NextPage})
	if err != nil {
		t.Fatalf("ListVersions second: %v", err)
	}
	if len(versionSecond.Data) != 1 || versionSecond.HasMore || versionSecond.NextPage != nil {
		t.Fatalf("version second page = %+v", versionSecond)
	}
	versionCursor := versionFirst.Data[1]
	if err := backend.DeleteVersion(context.Background(), workspace.DefaultID, versionedSkillID, versionCursor.Version); err != nil {
		t.Fatalf("DeleteVersion cursor version: %v", err)
	}
	versionsAfterDeletedCursor, err := service.ListVersions(context.Background(), workspace.DefaultID, versionedSkillID, skill.ListVersionsOptions{Limit: 2, Page: *versionFirst.NextPage})
	if err != nil {
		t.Fatalf("ListVersions after deleted cursor: %v", err)
	}
	if len(versionsAfterDeletedCursor.Data) != 1 || versionsAfterDeletedCursor.Data[0].Version != versionSecond.Data[0].Version || versionsAfterDeletedCursor.HasMore || versionsAfterDeletedCursor.NextPage != nil {
		t.Fatalf("version page after deleted cursor = %+v; want remaining version %s only", versionsAfterDeletedCursor, versionSecond.Data[0].Version)
	}
	assertVersionsInvalidPageToken("workspace_b", versionedSkillID, skill.ListVersionsOptions{Limit: 2, Page: *versionFirst.NextPage})
	assertVersionsInvalidPageToken(workspace.DefaultID, "skill_other", skill.ListVersionsOptions{Limit: 2, Page: *versionFirst.NextPage})
	tamperedVersion := *versionFirst.NextPage
	if strings.HasSuffix(tamperedVersion, "A") {
		tamperedVersion = strings.TrimSuffix(tamperedVersion, "A") + "B"
	} else {
		tamperedVersion += "A"
	}
	assertVersionsInvalidPageToken(workspace.DefaultID, versionedSkillID, skill.ListVersionsOptions{Limit: 2, Page: tamperedVersion})
	assertVersionsInvalidPageToken(workspace.DefaultID, versionedSkillID, skill.ListVersionsOptions{Limit: 2, Page: *first.NextPage})
	assertSkillsInvalidPageToken(workspace.DefaultID, skill.ListSkillsOptions{Limit: 2, Source: "custom", Page: *versionFirst.NextPage})
	for name, token := range map[string]string{
		"malformed_base64": "%%%",
		"malformed_json":   base64.RawURLEncoding.EncodeToString([]byte("not-json")),
		"bad_signature":    signedSkillPageTokenForTest("custom", 10, "skill_cursor", "wrong-signature"),
		"payload_source":   signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 1, Kind: "skills", Workspace: string(workspace.DefaultID), Source: "anthropic", SkillID: "skill_cursor", Sequence: 10}),
		"wrong_version":    signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 2, Kind: "skills", Workspace: string(workspace.DefaultID), Source: "custom", SkillID: "skill_cursor", Sequence: 10}),
		"wrong_kind":       signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 1, Kind: "skill_versions", Workspace: string(workspace.DefaultID), Source: "custom", SkillID: "skill_cursor", Sequence: 10, VersionValue: "1"}),
		"empty_cursor":     signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 1, Kind: "skills", Workspace: string(workspace.DefaultID), Source: "custom", Sequence: 10}),
		"zero_sequence":    signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 1, Kind: "skills", Workspace: string(workspace.DefaultID), Source: "custom", SkillID: "skill_cursor"}),
	} {
		assertSkillsInvalidPageToken(workspace.DefaultID, skill.ListSkillsOptions{Limit: 2, Source: "custom", Page: token}, name)
	}
	for name, token := range map[string]string{
		"malformed_base64": "%%%",
		"malformed_json":   base64.RawURLEncoding.EncodeToString([]byte("not-json")),
		"bad_signature":    signedVersionPageTokenForTest(versionedSkillID, 10, "4001", "wrong-signature"),
		"wrong_version":    signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 2, Kind: "skill_versions", Workspace: string(workspace.DefaultID), SkillID: versionedSkillID, Sequence: 10, VersionValue: "4001"}),
		"wrong_kind":       signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 1, Kind: "skills", Workspace: string(workspace.DefaultID), Source: "custom", SkillID: versionedSkillID, Sequence: 10}),
		"empty_version":    signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 1, Kind: "skill_versions", Workspace: string(workspace.DefaultID), SkillID: versionedSkillID, Sequence: 10}),
		"zero_sequence":    signedPageTokenForTest(secret, pageTokenPayloadForTest{Version: 1, Kind: "skill_versions", Workspace: string(workspace.DefaultID), SkillID: versionedSkillID, VersionValue: "4001"}),
	} {
		assertVersionsInvalidPageToken(workspace.DefaultID, versionedSkillID, skill.ListVersionsOptions{Limit: 2, Page: token}, name)
	}
}

func TestPrivateGuardrailsRejectBeforeBlobPut(t *testing.T) {
	blobStore, stageDir, store, db := newSkillStoreBackendEnv(t)
	seedSkillRows(t, db, workspace.DefaultID, skill.MaxSkillsPerWorkspace)
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "quota/SKILL.md", body: skillMD("quota", "quota")}})
	defer func() { _ = pkg.Cleanup() }()

	_, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	var quota *skill.QuotaError
	if !errors.As(err, &quota) || quota.Kind != skill.QuotaKindCount {
		t.Fatalf("CreateSkill quota error = %T %v", err, err)
	}
	if blobStore.Len() != 0 {
		t.Fatalf("blob count changed before guardrail reject: %d", blobStore.Len())
	}
}

func TestRetainedBytesGuardrailCountsSoftDeletedVersionsBeforeBlobPut(t *testing.T) {
	blobStore, stageDir, store, db := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(func(time.Time) string { return "retained_seed" })
	seedPackage := normalizedPackage(t, stageDir, []uploadFile{{filename: "seed/SKILL.md", body: skillMD("seed", "seed")}})
	defer func() { _ = seedPackage.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: seedPackage})
	if err != nil {
		t.Fatalf("CreateSkill seed: %v", err)
	}
	if err := updateVersionForRetainedBytes(t, db, workspace.DefaultID, parent.ID, *parent.LatestVersion, skill.MaxRetainedCompressedBytesPerWorkspace, true); err != nil {
		t.Fatalf("update retained bytes fixture: %v", err)
	}
	putCalls := int32(0)
	blobStore.SetPutHook(func(context.Context, string) error {
		atomic.AddInt32(&putCalls, 1)
		return nil
	})
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "next/SKILL.md", body: skillMD("next", "next")}})
	defer func() { _ = pkg.Cleanup() }()

	_, err = store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	var quota *skill.QuotaError
	if !errors.As(err, &quota) || quota.Kind != skill.QuotaKindRetainedBytes {
		t.Fatalf("CreateSkill retained-byte error = %T %v", err, err)
	}
	if got := atomic.LoadInt32(&putCalls); got != 0 {
		t.Fatalf("BlobStore.Put calls before retained-byte reject = %d; want 0", got)
	}
}

func TestVersionCountGuardrailRejectsBeforeBlobPut(t *testing.T) {
	blobStore, stageDir, store, db := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(func(time.Time) string { return "version_seed" })
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "versions/SKILL.md", body: skillMD("versions", "seed")}})
	defer func() { _ = pkg.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill seed: %v", err)
	}
	seedVersionRows(t, db, workspace.DefaultID, parent.ID, skill.MaxVersionsPerSkill-1)
	putCalls := int32(0)
	blobStore.SetPutHook(func(context.Context, string) error {
		atomic.AddInt32(&putCalls, 1)
		return nil
	})
	next := normalizedPackage(t, stageDir, []uploadFile{{filename: "versions/SKILL.md", body: skillMD("versions", "next")}})
	defer func() { _ = next.Cleanup() }()

	_, err = store.CreateVersion(context.Background(), workspace.DefaultID, parent.ID, skill.CreateVersionInput{Package: next})
	var quota *skill.QuotaError
	if !errors.As(err, &quota) || quota.Kind != skill.QuotaKindCount {
		t.Fatalf("CreateVersion count error = %T %v", err, err)
	}
	if got := atomic.LoadInt32(&putCalls); got != 0 {
		t.Fatalf("BlobStore.Put calls before version-count reject = %d; want 0", got)
	}
}

func TestConcurrentCreateVersionGuardrailSerializesUnderAdvisoryLock(t *testing.T) {
	_, stageDir, store, db := newSkillStoreBackendEnv(t)
	var versionCounter atomic.Int64
	store.SetVersionStrategy(func(time.Time) string {
		return fmt.Sprintf("race_version_%d", versionCounter.Add(1))
	})
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "parent/SKILL.md", body: skillMD("parent", "seed")}})
	defer func() { _ = pkg.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill seed: %v", err)
	}
	seedVersionRows(t, db, workspace.DefaultID, parent.ID, skill.MaxVersionsPerSkill-2)
	const concurrent = 3
	errs := make([]error, concurrent)
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: fmt.Sprintf("v-%d/SKILL.md", index), body: skillMD(fmt.Sprintf("v-%d", index), "x")}})
			defer func() { _ = pkg.Cleanup() }()
			_, errs[index] = store.CreateVersion(context.Background(), workspace.DefaultID, parent.ID, skill.CreateVersionInput{Package: pkg})
		}(i)
	}
	wg.Wait()
	successes, quotaErrors := 0, 0
	for _, err := range errs {
		if err == nil {
			successes++
			continue
		}
		var quota *skill.QuotaError
		if errors.As(err, &quota) && quota.Kind == skill.QuotaKindCount {
			quotaErrors++
			continue
		}
		t.Fatalf("unexpected concurrent CreateVersion error: %T %v", err, err)
	}
	if successes != 1 || quotaErrors != concurrent-1 {
		t.Fatalf("concurrent CreateVersion outcomes successes=%d quota=%d", successes, quotaErrors)
	}
}

func TestPackagePutFailureRollsBackSQLAndCleansMaybeCreatedBlob(t *testing.T) {
	blobStore, stageDir, store, db := newSkillStoreBackendEnv(t)
	putError := errors.New("provider write failed")
	blobStore.SetPutHook(func(context.Context, string) error { return putError })
	store.SetVersionStrategy(func(time.Time) string { return "put_failed" })
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "fail/SKILL.md", body: skillMD("fail", "put")}})
	defer func() { _ = pkg.Cleanup() }()

	_, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if !errors.Is(err, putError) {
		t.Fatalf("CreateSkill error = %v; want putError", err)
	}
	assertSkillRowCount(t, db, workspace.DefaultID, 0)
	if blobStore.Len() != 0 {
		t.Fatalf("blob count after failed Put = %d; want 0", blobStore.Len())
	}
	deletes := blobStore.Deletes()
	if len(deletes) != 1 || !strings.HasSuffix(deletes[0], "/versions/put_failed/package.zip") {
		t.Fatalf("cleanup deletes = %v; want failed package key", deletes)
	}
}

func TestCreateVersionDuplicateBlobKeyRollsBackSQLAndKeepsLatestVersion(t *testing.T) {
	blobStore, stageDir, store, db := newSkillStoreBackendEnv(t)
	store.SetVersionStrategy(sequenceVersions("initial", "duplicate_blob"))
	initial := normalizedPackage(t, stageDir, []uploadFile{{filename: "dup/SKILL.md", body: skillMD("dup", "initial")}})
	defer func() { _ = initial.Cleanup() }()
	parent, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: initial})
	if err != nil {
		t.Fatalf("CreateSkill: %v", err)
	}
	duplicateKey := fmt.Sprintf("skills/%s/%s/versions/%s/package.zip", workspace.DefaultID, parent.ID, "duplicate_blob")
	if err := blobStore.Put(context.Background(), duplicateKey, bytes.NewReader([]byte("existing")), int64(len("existing"))); err != nil {
		t.Fatalf("seed duplicate blob key: %v", err)
	}
	next := normalizedPackage(t, stageDir, []uploadFile{{filename: "dup/SKILL.md", body: skillMD("dup", "next")}})
	defer func() { _ = next.Cleanup() }()

	_, err = store.CreateVersion(context.Background(), workspace.DefaultID, parent.ID, skill.CreateVersionInput{Package: next})
	var conflict *skill.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("CreateVersion duplicate blob error = %T %v; want ConflictError", err, err)
	}
	got, err := store.GetSkill(context.Background(), workspace.DefaultID, parent.ID)
	if err != nil {
		t.Fatalf("GetSkill after duplicate blob: %v", err)
	}
	if got.LatestVersion == nil || *got.LatestVersion != "initial" {
		t.Fatalf("latest_version after duplicate blob = %v; want initial", got.LatestVersion)
	}
	assertVersionMissing(t, db, workspace.DefaultID, parent.ID, "duplicate_blob")
	if deletes := blobStore.Deletes(); len(deletes) != 0 {
		t.Fatalf("duplicate blob key should not trigger cleanup delete; got %v", deletes)
	}
}

func TestCommitFailureCleansJustCreatedBlob(t *testing.T) {
	blobStore, stageDir, store, db := newSkillStoreBackendEnv(t)
	commitError := errors.New("synthetic commit failure")
	store.SetTxRunner(func(ctx context.Context, workspaceID string, fn func(skill.Transaction) error, cleanup func()) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "SELECT set_config('tetral.workspace_id', $1, true)", workspaceID); err != nil {
			_ = tx.Rollback()
			t.Fatalf("set workspace: %v", err)
		}
		skillTx := testSkillTransaction{tx: dbconnect.NewTxForTesting(tx, dbconnect.NewClientForTesting(db), "skill.transaction")}
		if err := fn(skillTx); err != nil {
			_ = tx.Rollback()
			return err
		}
		_ = tx.Rollback()
		cleanup()
		return commitError
	})
	store.SetVersionStrategy(func(time.Time) string { return "commit_failed" })
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "commit/SKILL.md", body: skillMD("commit", "x")}})
	defer func() { _ = pkg.Cleanup() }()

	_, err := store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
	if !errors.Is(err, commitError) {
		t.Fatalf("CreateSkill error = %v; want commitError", err)
	}
	if blobStore.Len() != 0 {
		t.Fatalf("blob store retained commit-failed object: len=%d", blobStore.Len())
	}
	deletes := blobStore.Deletes()
	if len(deletes) != 1 {
		t.Fatalf("cleanup deletes = %v; want one delete", deletes)
	}
	if !strings.HasPrefix(deletes[0], fmt.Sprintf("skills/%s/", workspace.DefaultID)) || !strings.HasSuffix(deletes[0], "/versions/commit_failed/package.zip") {
		t.Fatalf("cleanup delete key = %v; want normalized package key", deletes)
	}
}

type testSkillTransaction struct {
	tx *dbconnect.Tx
}

func (t testSkillTransaction) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.Exec(ctx, query, args...)
}

func (t testSkillTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t testSkillTransaction) Query(ctx context.Context, query string, args ...any) (skill.Rows, error) {
	return t.tx.Query(ctx, query, args...)
}

func (t testSkillTransaction) QueryRow(ctx context.Context, query string, args ...any) skill.Row {
	return t.tx.QueryRow(ctx, query, args...)
}

func TestConcurrentCreateSkillGuardrailSerializesUnderAdvisoryLock(t *testing.T) {
	_, stageDir, store, db := newSkillStoreBackendEnv(t)
	seedSkillRows(t, db, workspace.DefaultID, skill.MaxSkillsPerWorkspace-1)
	const concurrent = 3
	errs := make([]error, concurrent)
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: fmt.Sprintf("race-%d/SKILL.md", index), body: skillMD(fmt.Sprintf("race-%d", index), "x")}})
			defer func() { _ = pkg.Cleanup() }()
			_, errs[index] = store.CreateSkill(context.Background(), workspace.DefaultID, skill.CreateSkillInput{Package: pkg})
		}(i)
	}
	wg.Wait()
	successes, quotaErrors := 0, 0
	for _, err := range errs {
		if err == nil {
			successes++
			continue
		}
		var quota *skill.QuotaError
		if errors.As(err, &quota) && quota.Kind == skill.QuotaKindCount {
			quotaErrors++
			continue
		}
		t.Fatalf("unexpected concurrent error: %T %v", err, err)
	}
	if successes != 1 || quotaErrors != concurrent-1 {
		t.Fatalf("concurrent outcomes successes=%d quota=%d", successes, quotaErrors)
	}
}

func TestStoreOperationsIsolateCrossWorkspaceRows(t *testing.T) {
	_, admin, _, stageDir, store := newSkillStoreEnv(t)
	seedWorkspace(t, admin, "workspace_b")
	store.SetVersionStrategy(func(time.Time) string { return "workspace_b_version" })
	pkg := normalizedPackage(t, stageDir, []uploadFile{{filename: "isolated/SKILL.md", body: skillMD("isolated", "workspace")}})
	defer func() { _ = pkg.Cleanup() }()
	other, err := store.CreateSkill(context.Background(), "workspace_b", skill.CreateSkillInput{Package: pkg})
	if err != nil {
		t.Fatalf("CreateSkill workspace_b: %v", err)
	}
	if _, err := store.GetSkill(context.Background(), workspace.DefaultID, other.ID); !errors.As(err, new(*skill.NotFoundError)) {
		t.Fatalf("default GetSkill cross-workspace error = %T %v; want NotFoundError", err, err)
	}
	list, err := store.ListSkills(context.Background(), workspace.DefaultID, skill.ListSkillsOptions{Limit: 20})
	if err != nil {
		t.Fatalf("default ListSkills: %v", err)
	}
	if len(list.Data) != 0 {
		t.Fatalf("default ListSkills leaked workspace_b rows: %+v", list.Data)
	}
	if err := store.DeleteSkill(context.Background(), workspace.DefaultID, other.ID); !errors.As(err, new(*skill.NotFoundError)) {
		t.Fatalf("default DeleteSkill cross-workspace error = %T %v; want NotFoundError", err, err)
	}
	if _, err := store.GetSkill(context.Background(), "workspace_b", other.ID); err != nil {
		t.Fatalf("workspace_b GetSkill after default delete attempt: %v", err)
	}
}

func fixedClock(raw string) func() time.Time {
	return func() time.Time {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			panic(err)
		}
		return t
	}
}

func sequenceVersions(values ...string) func(time.Time) string {
	index := 0
	return func(time.Time) string {
		if index >= len(values) {
			return fmt.Sprintf("overflow_%d", index)
		}
		value := values[index]
		index++
		return value
	}
}

func assertStoredBlobMetadata(t *testing.T, db *sql.DB, skillID, version, key string, size int64, sha string) {
	t.Helper()
	var gotKey, gotSHA string
	var gotSize int64
	err := storage.WithWorkspaceTx(context.Background(), db, string(workspace.DefaultID), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(),
			`SELECT blob_key, size_bytes, sha256 FROM skill_versions WHERE workspace_id = $1 AND skill_id = $2 AND version = $3`,
			string(workspace.DefaultID), skillID, version,
		).Scan(&gotKey, &gotSize, &gotSHA)
	})
	if err != nil {
		t.Fatalf("query blob metadata: %v", err)
	}
	if gotKey != key || gotSize != size || gotSHA != sha {
		t.Fatalf("blob metadata = key:%q size:%d sha:%q; want key:%q size:%d sha:%q", gotKey, gotSize, gotSHA, key, size, sha)
	}
}

func seedSkillRows(t *testing.T, db *sql.DB, ws workspace.ID, count int) {
	t.Helper()
	if err := storage.WithWorkspaceTx(context.Background(), db, string(ws), func(tx *sql.Tx) error {
		for i := 0; i < count; i++ {
			skillID := fmt.Sprintf("skill_seed_%d", i)
			if _, err := tx.ExecContext(context.Background(),
				`INSERT INTO skills (workspace_id, skill_id, display_title, latest_version, created_at, updated_at)
				 VALUES ($1, $2, NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
				string(ws), skillID); err != nil {
				return fmt.Errorf("seed skill row %d: %w", i, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func seedVersionRows(t *testing.T, db *sql.DB, ws workspace.ID, skillID string, count int) {
	t.Helper()
	if err := storage.WithWorkspaceTx(context.Background(), db, string(ws), func(tx *sql.Tx) error {
		for i := 0; i < count; i++ {
			version := fmt.Sprintf("seed_version_%d", i)
			if _, err := tx.ExecContext(context.Background(),
				`INSERT INTO skill_versions (workspace_id, skill_id, skill_version_id, version, name, description, directory, blob_key, size_bytes, sha256, created_at)
				 VALUES ($1, $2, $3, $4, 'seed', 'seed', 'seed', $5, 1, 'seed-sha', '2026-01-01T00:00:00Z')`,
				string(ws), skillID, fmt.Sprintf("skill_version_seed_%d", i), version, fmt.Sprintf("skills/%s/%s/versions/%s/package.zip", ws, skillID, version)); err != nil {
				return fmt.Errorf("seed version row %d: %w", i, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func updateVersionForRetainedBytes(t *testing.T, db *sql.DB, ws workspace.ID, skillID, version string, size int64, deleted bool) error {
	t.Helper()
	var deletedAt any
	if deleted {
		deletedAt = "2026-01-02T00:00:00Z"
	}
	return storage.WithWorkspaceTx(context.Background(), db, string(ws), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(),
			`UPDATE skill_versions SET size_bytes = $1, deleted_at = $2 WHERE workspace_id = $3 AND skill_id = $4 AND version = $5`,
			size, deletedAt, string(ws), skillID, version)
		return err
	})
}

func assertSkillRowCount(t *testing.T, db *sql.DB, ws workspace.ID, want int) {
	t.Helper()
	var got int
	if err := storage.WithWorkspaceTx(context.Background(), db, string(ws), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT count(*) FROM skills WHERE workspace_id = $1`, string(ws)).Scan(&got)
	}); err != nil {
		t.Fatalf("count skills: %v", err)
	}
	if got != want {
		t.Fatalf("skill row count = %d; want %d", got, want)
	}
}

func assertVersionMissing(t *testing.T, db *sql.DB, ws workspace.ID, skillID, version string) {
	t.Helper()
	var got int
	if err := storage.WithWorkspaceTx(context.Background(), db, string(ws), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(),
			`SELECT count(*) FROM skill_versions WHERE workspace_id = $1 AND skill_id = $2 AND version = $3`,
			string(ws), skillID, version).Scan(&got)
	}); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if got != 0 {
		t.Fatalf("version %s row count = %d; want 0", version, got)
	}
}

func normalizedPackage(t *testing.T, stageDir string, files []uploadFile) *skill.StagedPackage {
	t.Helper()
	parts := stageUploadParts(t, stageDir, files)
	t.Cleanup(func() { _ = skill.CleanupStagedUploadParts(parts) })
	pkg, err := skill.BuildNormalizedPackage(context.Background(), parts, stageDir)
	if err != nil {
		t.Fatalf("BuildNormalizedPackage: %v", err)
	}
	return pkg
}

func assertInvalidPageToken(t *testing.T, err error, labels ...string) {
	t.Helper()
	name := "page token"
	if len(labels) > 0 {
		name = labels[0]
	}
	var validation *skill.ValidationError
	if !errors.As(err, &validation) || validation.Message != "invalid page token" {
		t.Fatalf("%s error = %T %v; want ValidationError invalid page token", name, err, err)
	}
}

func assertNormalizedZipBlob(t *testing.T, raw []byte, want []string) {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("normalized blob is not zip: %v", err)
	}
	got := make([]string, 0, len(zr.File))
	for _, file := range zr.File {
		got = append(got, file.Name)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("zip entries = %v; want %v", got, want)
	}
}

type pageTokenPayloadForTest struct {
	Version      int    `json:"version"`
	Kind         string `json:"kind"`
	Workspace    string `json:"workspace"`
	Source       string `json:"source,omitempty"`
	SkillID      string `json:"skill_id,omitempty"`
	Sequence     int64  `json:"sequence"`
	VersionValue string `json:"version_value,omitempty"`
}

type signedPageTokenForTestEnvelope struct {
	Payload   pageTokenPayloadForTest `json:"payload"`
	Signature string                  `json:"signature"`
}

func signedSkillPageTokenForTest(source string, sequence int64, skillID string, signature string) string {
	return signedPageTokenEnvelopeForTest(pageTokenPayloadForTest{
		Version:   1,
		Kind:      "skills",
		Workspace: string(workspace.DefaultID),
		Source:    source,
		Sequence:  sequence,
		SkillID:   skillID,
	}, signature)
}

func signedVersionPageTokenForTest(skillID string, sequence int64, version string, signature string) string {
	return signedPageTokenEnvelopeForTest(pageTokenPayloadForTest{
		Version:      1,
		Kind:         "skill_versions",
		Workspace:    string(workspace.DefaultID),
		Sequence:     sequence,
		SkillID:      skillID,
		VersionValue: version,
	}, signature)
}

func signedPageTokenForTest(secret []byte, payload pageTokenPayloadForTest) string {
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return signedPageTokenEnvelopeForTest(payload, base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
}

func signedPageTokenEnvelopeForTest(payload pageTokenPayloadForTest, signature string) string {
	raw, err := json.Marshal(signedPageTokenForTestEnvelope{Payload: payload, Signature: signature})
	if err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}
