package memory_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestMemoryVersionListFiltersOrderViewAndToken(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_version_a")
	seedMemoryAPIKey(t, admin, "ak_version_b")
	actorA := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_version_a"} //nolint:gosec // Public test API key id.
	actorB := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_version_b"} //nolint:gosec // Public test API key id.
	store := createMemoryStore(t, service, "versions")
	first := createTestMemory(t, service, store.ID, "/first.md", "first", actorA)
	second := createTestMemory(t, service, store.ID, "/second.md", "second", actorB)
	updated, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, first.ID, memory.UpdateMemoryRequest{Content: "first-updated", ContentSet: true}, actorA)
	if err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}
	setVersionCreatedAt(t, admin, store.ID, first.MemoryVersionID, "2026-01-01T00:00:00Z")
	setVersionCreatedAt(t, admin, store.ID, second.MemoryVersionID, "2026-01-03T00:00:00Z")
	setVersionCreatedAt(t, admin, store.ID, updated.MemoryVersionID, "2026-01-03T00:00:00Z")
	tied := []string{second.MemoryVersionID, updated.MemoryVersionID}
	sort.Sort(sort.Reverse(sort.StringSlice(tied)))

	all, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{})
	if err != nil {
		t.Fatalf("ListMemoryVersions all: %v", err)
	}
	assertVersionIDs(t, all.Data, []string{tied[0], tied[1], first.MemoryVersionID})
	if all.Data[0].Content != nil || all.Data[0].ContentSHA256 == nil || all.Data[0].ContentSizeBytes == nil {
		t.Fatalf("basic version projection = %+v", all.Data[0])
	}

	byMemory, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{MemoryID: first.ID, View: memory.ViewFull})
	if err != nil {
		t.Fatalf("ListMemoryVersions memory filter: %v", err)
	}
	assertVersionIDs(t, byMemory.Data, []string{updated.MemoryVersionID, first.MemoryVersionID})
	if byMemory.Data[0].Content == nil || *byMemory.Data[0].Content != "first-updated" {
		t.Fatalf("full version content = %v; want first-updated", byMemory.Data[0].Content)
	}

	byOperation, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Operation: memory.OperationModified})
	if err != nil {
		t.Fatalf("ListMemoryVersions operation filter: %v", err)
	}
	assertVersionIDs(t, byOperation.Data, []string{updated.MemoryVersionID})

	byAPIKey, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{APIKeyID: "ak_version_b"})
	if err != nil {
		t.Fatalf("ListMemoryVersions api key filter: %v", err)
	}
	assertVersionIDs(t, byAPIKey.Data, []string{second.MemoryVersionID})

	byCreatedAt, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{CreatedAtGTE: "2026-01-02T00:00:00Z", CreatedAtLTE: "2026-01-03T00:00:00Z"})
	if err != nil {
		t.Fatalf("ListMemoryVersions created_at filter: %v", err)
	}
	assertVersionIDs(t, byCreatedAt.Data, []string{tied[0], tied[1]})

	pageOne, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: first.ID})
	if err != nil {
		t.Fatalf("ListMemoryVersions page one: %v", err)
	}
	if pageOne.NextPage == nil {
		t.Fatal("version page one next_page is nil")
	}
	pageTwo, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: first.ID, Page: *pageOne.NextPage})
	if err != nil {
		t.Fatalf("ListMemoryVersions page two: %v", err)
	}
	assertVersionIDs(t, pageTwo.Data, []string{first.MemoryVersionID})
	if _, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, APIKeyID: "ak_version_a", Page: *pageOne.NextPage}); err == nil {
		t.Fatal("version token replay with changed filter succeeded; want error")
	}
	otherStore := createMemoryStore(t, service, "versions-token-other")
	otherWorkspaceStore, err := service.CreateStore(ctx, "workspace_b", memory.CreateStoreRequest{Name: "versions-other-workspace"})
	if err != nil {
		t.Fatalf("CreateStore workspace_b: %v", err)
	}
	for label, call := range map[string]func() error{
		"workspace": func() error {
			_, err := service.ListMemoryVersions(ctx, "workspace_b", otherWorkspaceStore.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: first.ID, Page: *pageOne.NextPage})
			return err
		},
		"store": func() error {
			_, err := service.ListMemoryVersions(ctx, workspace.DefaultID, otherStore.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: first.ID, Page: *pageOne.NextPage})
			return err
		},
		"memory_id": func() error {
			_, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: second.ID, Page: *pageOne.NextPage})
			return err
		},
		"operation": func() error {
			_, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: first.ID, Operation: memory.OperationModified, Page: *pageOne.NextPage})
			return err
		},
		"created_at_gte": func() error {
			_, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: first.ID, CreatedAtGTE: "2026-01-01T00:00:00Z", Page: *pageOne.NextPage})
			return err
		},
		"created_at_lte": func() error {
			_, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: first.ID, CreatedAtLTE: "2026-01-03T00:00:00Z", Page: *pageOne.NextPage})
			return err
		},
		"view": func() error {
			_, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, MemoryID: first.ID, View: memory.ViewFull, Page: *pageOne.NextPage})
			return err
		},
	} {
		if err := call(); err == nil {
			t.Fatalf("%s token replay succeeded; want validation error", label)
		}
	}
}

func TestMemoryVersionListSessionIDFilterAndToken(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_version_session")
	apiActor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_version_session"} //nolint:gosec // Public test API key id.
	sessionActorA := memory.Actor{Type: memory.ActorSession, SessionID: "sesn_version_a"}
	sessionActorB := memory.Actor{Type: memory.ActorSession, SessionID: "sesn_version_b"}
	store := createMemoryStore(t, service, "version-session-filter")
	first := createTestMemory(t, service, store.ID, "/session-a-first.md", "first", sessionActorA)
	second := createTestMemory(t, service, store.ID, "/session-a-second.md", "second", sessionActorA)
	otherSession := createTestMemory(t, service, store.ID, "/session-b.md", "other-session", sessionActorB)
	apiCreated := createTestMemory(t, service, store.ID, "/api.md", "api", apiActor)
	setVersionCreatedAt(t, admin, store.ID, first.MemoryVersionID, "2026-01-01T00:00:00Z")
	setVersionCreatedAt(t, admin, store.ID, second.MemoryVersionID, "2026-01-03T00:00:00Z")
	setVersionCreatedAt(t, admin, store.ID, otherSession.MemoryVersionID, "2026-01-04T00:00:00Z")
	setVersionCreatedAt(t, admin, store.ID, apiCreated.MemoryVersionID, "2026-01-05T00:00:00Z")

	bySession, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{SessionID: "sesn_version_a"})
	if err != nil {
		t.Fatalf("ListMemoryVersions session_id filter: %v", err)
	}
	assertVersionIDs(t, bySession.Data, []string{second.MemoryVersionID, first.MemoryVersionID})
	for _, version := range bySession.Data {
		if version.CreatedBy.Type != memory.ActorSession || version.CreatedBy.SessionID != "sesn_version_a" {
			t.Fatalf("session filter returned actor %+v", version.CreatedBy)
		}
	}

	pageOne, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, SessionID: "sesn_version_a"})
	if err != nil {
		t.Fatalf("ListMemoryVersions session page one: %v", err)
	}
	if pageOne.NextPage == nil {
		t.Fatal("session-filtered page one next_page is nil")
	}
	pageTwo, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, SessionID: "sesn_version_a", Page: *pageOne.NextPage})
	if err != nil {
		t.Fatalf("ListMemoryVersions session page two: %v", err)
	}
	assertVersionIDs(t, pageTwo.Data, []string{first.MemoryVersionID})
	if _, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 1, LimitSet: true, SessionID: "sesn_version_b", Page: *pageOne.NextPage}); err == nil {
		t.Fatal("session-filtered token replay with changed session_id succeeded; want error")
	}
}

func TestMemoryVersionListLimitsAndDecodeValidation(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_version_limit")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_version_limit"} //nolint:gosec // Public test API key id.
	store := createMemoryStore(t, service, "version-limits")
	for i := 0; i < 105; i++ {
		createTestMemory(t, service, store.ID, fmt.Sprintf("/%03d.md", i), "x", actor)
	}
	basicDefault, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{})
	if err != nil {
		t.Fatalf("ListMemoryVersions basic default: %v", err)
	}
	if len(basicDefault.Data) != 20 {
		t.Fatalf("basic default len = %d; want 20", len(basicDefault.Data))
	}
	basicCap, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 101, LimitSet: true})
	if err != nil {
		t.Fatalf("ListMemoryVersions basic cap: %v", err)
	}
	if len(basicCap.Data) != 100 {
		t.Fatalf("basic cap len = %d; want 100", len(basicCap.Data))
	}
	basicBoundary, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 100, LimitSet: true})
	if err != nil {
		t.Fatalf("ListMemoryVersions basic boundary: %v", err)
	}
	if len(basicBoundary.Data) != 100 {
		t.Fatalf("basic boundary len = %d; want 100", len(basicBoundary.Data))
	}
	fullDefault, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{View: memory.ViewFull})
	if err != nil {
		t.Fatalf("ListMemoryVersions full default: %v", err)
	}
	if len(fullDefault.Data) != 20 || fullDefault.Data[0].Content == nil {
		t.Fatalf("full default len/content = %d/%v; want 20/content", len(fullDefault.Data), fullDefault.Data[0].Content)
	}
	fullBoundary, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 20, LimitSet: true, View: memory.ViewFull})
	if err != nil {
		t.Fatalf("ListMemoryVersions full boundary: %v", err)
	}
	if len(fullBoundary.Data) != 20 || fullBoundary.Data[0].Content == nil {
		t.Fatalf("full boundary len/content = %d/%v; want 20/content", len(fullBoundary.Data), fullBoundary.Data[0].Content)
	}
	fullCap, err := service.ListMemoryVersions(ctx, workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{Limit: 100, LimitSet: true, View: memory.ViewFull})
	if err != nil {
		t.Fatalf("ListMemoryVersions full cap: %v", err)
	}
	if len(fullCap.Data) != 20 || fullCap.Data[0].Content == nil {
		t.Fatalf("full cap len/content = %d/%v; want 20/content", len(fullCap.Data), fullCap.Data[0].Content)
	}
	for _, raw := range []string{"limit=abc", "limit=", "limit=0", "limit=-1"} {
		values, err := url.ParseQuery(raw)
		if err != nil {
			t.Fatalf("ParseQuery: %v", err)
		}
		if _, err := memory.DecodeListMemoryVersionsOptions(values); err == nil {
			t.Fatalf("DecodeListMemoryVersionsOptions(%s) succeeded; want error", raw)
		}
	}
	values, err := url.ParseQuery("session_id=sesn_filter&api_key_id=ak_filter")
	if err != nil {
		t.Fatalf("ParseQuery session filter: %v", err)
	}
	options, err := memory.DecodeListMemoryVersionsOptions(values)
	if err != nil {
		t.Fatalf("DecodeListMemoryVersionsOptions session filter: %v", err)
	}
	if options.SessionID != "sesn_filter" || options.APIKeyID != "ak_filter" {
		t.Fatalf("options = %+v; want session_id and api_key_id filters", options)
	}
}

func TestMemoryVersionRetrieveDeletedAndRedact(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_version_redact")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_version_redact"} //nolint:gosec // Public test API key id.
	store := createMemoryStore(t, service, "redact")
	created := createTestMemory(t, service, store.ID, "/redact.md", "secret", actor)
	updated, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{Content: "public", ContentSet: true}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}

	defaultFull, err := service.GetMemoryVersion(ctx, workspace.DefaultID, store.ID, updated.MemoryVersionID, "")
	if err != nil {
		t.Fatalf("GetMemoryVersion default full: %v", err)
	}
	if defaultFull.Content == nil || *defaultFull.Content != "public" || defaultFull.ContentSHA256 == nil || defaultFull.ContentSizeBytes == nil {
		t.Fatalf("default full version payload = %+v", defaultFull)
	}
	basic, err := service.GetMemoryVersion(ctx, workspace.DefaultID, store.ID, updated.MemoryVersionID, memory.ViewBasic)
	if err != nil {
		t.Fatalf("GetMemoryVersion basic: %v", err)
	}
	if basic.Content != nil || basic.ContentSHA256 == nil || basic.ContentSizeBytes == nil {
		t.Fatalf("basic version payload = %+v", basic)
	}

	if _, err := service.RedactMemoryVersion(ctx, workspace.DefaultID, store.ID, updated.MemoryVersionID, actor); !isMemoryValidation(err) {
		t.Fatalf("redact live head err = %T %v; want validation", err, err)
	}
	preRedaction, err := service.GetMemoryVersion(ctx, workspace.DefaultID, store.ID, created.MemoryVersionID, memory.ViewFull)
	if err != nil {
		t.Fatalf("GetMemoryVersion before redact: %v", err)
	}
	redacted, err := service.RedactMemoryVersion(ctx, workspace.DefaultID, store.ID, created.MemoryVersionID, actor)
	if err != nil {
		t.Fatalf("RedactMemoryVersion old version: %v", err)
	}
	assertVersionRedacted(t, redacted, preRedaction, actor)
	again, err := service.RedactMemoryVersion(ctx, workspace.DefaultID, store.ID, created.MemoryVersionID, actor)
	if err != nil {
		t.Fatalf("RedactMemoryVersion idempotent: %v", err)
	}
	if again.RedactedAt == nil || *again.RedactedAt != *redacted.RedactedAt {
		t.Fatalf("idempotent redacted_at = %v; want %v", again.RedactedAt, redacted.RedactedAt)
	}

	deletedMemory := createTestMemory(t, service, store.ID, "/deleted.md", "delete-me", actor)
	if _, err := service.DeleteMemory(ctx, workspace.DefaultID, store.ID, deletedMemory.ID, nil, actor); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}
	deletedVersions := listMemoryVersionsByStorageSequence(t, admin, store.ID, deletedMemory.ID)
	deletedVersion := deletedVersions[len(deletedVersions)-1]
	retrievedDeleted, err := service.GetMemoryVersion(ctx, workspace.DefaultID, store.ID, deletedVersion.ID, "")
	if err != nil {
		t.Fatalf("GetMemoryVersion deleted default: %v", err)
	}
	if retrievedDeleted.Path == nil || *retrievedDeleted.Path != "/deleted.md" || retrievedDeleted.Content != nil || retrievedDeleted.ContentSHA256 != nil || retrievedDeleted.ContentSizeBytes != nil {
		t.Fatalf("deleted version payload = %+v", retrievedDeleted)
	}
	redactedDeleted, err := service.RedactMemoryVersion(ctx, workspace.DefaultID, store.ID, deletedVersion.ID, actor)
	if err != nil {
		t.Fatalf("RedactMemoryVersion deleted current head: %v", err)
	}
	if redactedDeleted.Path != nil {
		t.Fatalf("redacted deleted path = %v; want nil", redactedDeleted.Path)
	}
}

func TestRedactMemoryVersionReturnsMutationStateBeforePostLockWriter(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_response_first")
	seedMemoryAPIKey(t, admin, "ak_response_second")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_response_first"} //nolint:gosec // Public test API key id.
	store := createMemoryStore(t, service, "response-race-redact")
	created := createTestMemory(t, service, store.ID, "/redact-response.md", "secret", actor)
	if _, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{
		Content:    "public",
		ContentSet: true,
	}, actor); err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}
	releaseGate := installPostCommitResponseRaceGate(t, admin, "memory_versions", "redact_version_response")

	type redactResult struct {
		version *memory.MemoryVersion
		err     error
	}
	done := make(chan redactResult, 1)
	go func() {
		redacted, err := service.RedactMemoryVersion(ctx, workspace.DefaultID, store.ID, created.MemoryVersionID, actor)
		done <- redactResult{version: redacted, err: err}
	}()

	waitForPostCommitResponseRaceGate(t, admin, "redact_version_response")
	var result redactResult
	var returned bool
	select {
	case result = <-done:
		returned = true
	default:
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE memory_versions
		    SET redacted_at = '2030-01-01T00:00:00Z',
		        redacted_actor_type = 'api_actor',
		        redacted_api_key_id = 'ak_response_second',
		        redacted_session_id = NULL,
		        redacted_user_id = NULL
		  WHERE workspace_id = $1 AND memory_store_id = $2 AND memory_version_id = $3`,
		string(workspace.DefaultID), store.ID, created.MemoryVersionID); err != nil {
		t.Fatalf("second writer redaction audit update: %v", err)
	}
	releaseGate()
	if !returned {
		select {
		case result = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for RedactMemoryVersion")
		}
	}
	if result.err != nil {
		t.Fatalf("RedactMemoryVersion: %v", result.err)
	}
	if result.version.RedactedBy == nil || *result.version.RedactedBy != actor {
		t.Fatalf("RedactMemoryVersion returned redacted_by %+v; want first mutation actor %+v", result.version.RedactedBy, actor)
	}
	if result.version.RedactedAt == nil || *result.version.RedactedAt == "2030-01-01T00:00:00Z" {
		t.Fatalf("RedactMemoryVersion returned redacted_at %v; want first mutation timestamp", result.version.RedactedAt)
	}
}

func TestVersionRedactLowersQuotaAndArchivedStoreRejects(t *testing.T) {
	service, admin := newMemoryStoreTestEnv(t)
	ctx := context.Background()
	seedMemoryAPIKey(t, admin, "ak_version_quota")
	actor := memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_version_quota"} //nolint:gosec // Public test API key id.
	storeID := seedMemoryStoreBySQL(t, admin, "version-quota")
	seedVersionQuotaRedactableHistory(t, admin, storeID)
	_, err := service.CreateMemory(ctx, workspace.DefaultID, storeID, memory.CreateMemoryRequest{Path: "/blocked.md", Content: "x", ContentSet: true}, actor)
	var tooLarge *memory.RequestTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("quota before redact err = %T %v; want RequestTooLargeError", err, err)
	}
	if _, err := service.RedactMemoryVersion(ctx, workspace.DefaultID, storeID, "memver_quota_old", actor); err != nil {
		t.Fatalf("RedactMemoryVersion quota old: %v", err)
	}
	if _, err := service.CreateMemory(ctx, workspace.DefaultID, storeID, memory.CreateMemoryRequest{Path: "/allowed.md", Content: "x", ContentSet: true}, actor); err != nil {
		t.Fatalf("CreateMemory after redact lowered quota: %v", err)
	}

	store := createMemoryStore(t, service, "redact-archived")
	created := createTestMemory(t, service, store.ID, "/archived.md", "archived", actor)
	updated, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{Content: "archived-next", ContentSet: true}, actor)
	if err != nil {
		t.Fatalf("UpdateMemory archived fixture: %v", err)
	}
	if _, err := service.ArchiveStore(ctx, workspace.DefaultID, store.ID); err != nil {
		t.Fatalf("ArchiveStore: %v", err)
	}
	_, err = service.RedactMemoryVersion(ctx, workspace.DefaultID, store.ID, created.MemoryVersionID, actor)
	assertMemoryValidationError(t, err)
	unredacted, err := service.GetMemoryVersion(ctx, workspace.DefaultID, store.ID, created.MemoryVersionID, memory.ViewFull)
	if err != nil {
		t.Fatalf("GetMemoryVersion after archived reject: %v", err)
	}
	if unredacted.RedactedAt != nil || unredacted.Content == nil || *unredacted.Content != "archived" {
		t.Fatalf("archived redact side effect = %+v (current head %s)", unredacted, updated.MemoryVersionID)
	}
}

func assertVersionIDs(t *testing.T, versions []*memory.MemoryVersion, want []string) {
	t.Helper()
	if len(versions) != len(want) {
		t.Fatalf("version len = %d; want %d (%+v)", len(versions), len(want), versions)
	}
	for i, expected := range want {
		if versions[i].ID != expected {
			t.Fatalf("version %d id = %s; want %s", i, versions[i].ID, expected)
		}
	}
}

func TestMemoryVersionActorJSONShape(t *testing.T) {
	redactedAt := "2026-01-01T00:00:00Z"
	version := memory.MemoryVersion{
		ID:            "memver_json",
		Type:          "memory_version",
		MemoryStoreID: "memstore_json",
		MemoryID:      "mem_json",
		Operation:     memory.OperationModified,
		CreatedAt:     "2026-01-01T00:00:00Z",
		CreatedBy:     memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_json"},
		RedactedAt:    &redactedAt,
		RedactedBy:    &memory.Actor{Type: memory.ActorAPI, APIKeyID: "ak_redact"},
	}
	payload, err := json.Marshal(version)
	if err != nil {
		t.Fatalf("Marshal MemoryVersion: %v", err)
	}
	body := string(payload)
	for _, want := range []string{`"created_by":{"type":"api_actor","api_key_id":"ak_json"}`, `"redacted_by":{"type":"api_actor","api_key_id":"ak_redact"}`} {
		if !strings.Contains(body, want) {
			t.Fatalf("MemoryVersion JSON %s missing %s", body, want)
		}
	}
	for _, forbidden := range []string{"APIKeyID", "SessionID", "UserID", "session_id", "user_id"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("MemoryVersion JSON exposed forbidden actor field %q: %s", forbidden, body)
		}
	}
}

func assertVersionRedacted(t *testing.T, version *memory.MemoryVersion, before *memory.MemoryVersion, actor memory.Actor) {
	t.Helper()
	if version.Path != nil || version.Content != nil || version.ContentSHA256 != nil || version.ContentSizeBytes != nil {
		t.Fatalf("redacted payload = %+v; want all nil", version)
	}
	if version.RedactedAt == nil || version.RedactedBy == nil || *version.RedactedBy != actor {
		t.Fatalf("redaction audit = at %v by %v; want actor %+v", version.RedactedAt, version.RedactedBy, actor)
	}
	if version.ID != before.ID ||
		version.MemoryStoreID != before.MemoryStoreID ||
		version.MemoryID != before.MemoryID ||
		version.Operation != before.Operation ||
		version.CreatedAt != before.CreatedAt ||
		version.CreatedBy != before.CreatedBy {
		t.Fatalf("redaction did not preserve exact immutable fields: before=%+v after=%+v", before, version)
	}
}

func isMemoryValidation(err error) bool {
	var validation *memory.ValidationError
	return errors.As(err, &validation)
}

func setVersionCreatedAt(t *testing.T, admin anySQL, storeID string, versionID string, createdAt string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE memory_versions SET created_at = $1 WHERE workspace_id = 'default' AND memory_store_id = $2 AND memory_version_id = $3`,
		createdAt, storeID, versionID); err != nil {
		t.Fatalf("set version created_at: %v", err)
	}
}

func seedVersionQuotaRedactableHistory(t *testing.T, admin anySQL, storeID string) {
	t.Helper()
	beginner, ok := admin.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	})
	if !ok {
		t.Fatal("admin does not support transactions")
	}
	tx, err := beginner.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin quota redact seed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO memories (workspace_id, memory_store_id, memory_id, current_version_id, path, content_sha256, content_size_bytes, created_at, updated_at)
		 VALUES ('default', $1, 'mem_quota_redact', 'memver_quota_head', '/quota-redact.md', 'sha', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		storeID); err != nil {
		t.Fatalf("seed quota redact memory: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO memory_versions (workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content, content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id)
		 VALUES
		 ('default', $1, 'mem_quota_redact', 'memver_quota_old', 'modified', '/quota-redact.md', repeat('x', $2), 'sha', $2, '2026-01-01T00:00:00Z', 'session_actor', 'sesn_quota'),
		 ('default', $1, 'mem_quota_redact', 'memver_quota_head', 'modified', '/quota-redact.md', 'x', 'sha', 1, '2026-01-02T00:00:00Z', 'session_actor', 'sesn_quota')`,
		storeID, memory.MaxRetainedMemoryPayloadBytesPerStore); err != nil {
		t.Fatalf("seed quota redact versions: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit quota redact seed: %v", err)
	}
}
