package session

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestSessionListOrderDefaultsToDescendingAndHonorsAscending(t *testing.T) {
	descendingQuery, _ := buildListSessionsQuery(workspace.DefaultID, ListOptions{}, 20)
	if !strings.Contains(descendingQuery, "ORDER BY s.created_at DESC, s.id DESC") {
		t.Fatalf("default order query = %s; want descending created_at/id", descendingQuery)
	}

	ascendingQuery, _ := buildListSessionsQuery(workspace.DefaultID, ListOptions{Order: ListOrderAscending}, 20)
	if !strings.Contains(ascendingQuery, "ORDER BY s.created_at ASC, s.id ASC") {
		t.Fatalf("ascending order query = %s; want ascending created_at/id", ascendingQuery)
	}
}

func TestSessionListQueryPinsSDKStatusAndUnsupportedDeploymentFilters(t *testing.T) {
	query, args := buildListSessionsQuery(workspace.DefaultID, ListOptions{
		DeploymentID: "deployment_unsupported",
		Statuses:     []Status{StatusRunning, StatusIdle},
	}, 20)
	if !strings.Contains(query, "FALSE") {
		t.Fatalf("deployment-filter query = %s; want contract-empty predicate", query)
	}
	if !strings.Contains(query, "END IN ($2, $3)") {
		t.Fatalf("status-filter query = %s; want effective-status predicate", query)
	}
	if !reflect.DeepEqual(args[1:3], []any{"running", "idle"}) {
		t.Fatalf("status args = %#v; want running,idle", args)
	}
}

func TestPostgreSQLTransactionListSessionsRejectsInvalidOrderBeforeQuery(t *testing.T) {
	executor := &recordingListOrderExecutor{}
	tx := &postgresqlTransaction{
		store:       &PostgreSQLSessionStore{pageTokenSecret: []byte("0123456789abcdef0123456789abcdef")},
		workspaceID: workspace.DefaultID,
		tx:          executor,
	}

	_, _, err := tx.ListSessions(context.Background(), ListOptions{Order: ListOrder("sideways")})
	if err == nil {
		t.Fatal("ListSessions succeeded; want validation error")
	}
	var validation *ValidationError
	if !strings.Contains(err.Error(), "order") {
		t.Fatalf("err = %T %v; want order validation", err, err)
	}
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if executor.queryCount != 0 {
		t.Fatalf("query count = %d; want invalid order rejected before query", executor.queryCount)
	}
}

func TestResourcePageTokenPayloadUsesOnlyPublicCursor(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	associatedData := resourceListAssociatedData(workspace.DefaultID, "sesn_resource_token")
	payload := resourcePageTokenPayload(&Resource{
		ID:              "sesrsc_cursor",
		StorageSequence: 42,
	})
	token, err := encodePageToken(secret, payload, associatedData)
	if err != nil {
		t.Fatalf("encodePageToken: %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("decode page token: %v", err)
	}
	if strings.Contains(string(raw), "storage_sequence") {
		t.Fatalf("decoded resource page token = %s; must not expose storage_sequence", raw)
	}
	var envelope struct {
		Payload map[string]any `json:"payload"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("unmarshal page token envelope: %v", err)
	}
	if envelope.Payload["resource_id"] != "sesrsc_cursor" {
		t.Fatalf("resource_id = %#v; want cursor resource id", envelope.Payload["resource_id"])
	}
	if _, ok := envelope.Payload["storage_sequence"]; ok {
		t.Fatalf("payload = %#v; must not include storage_sequence", envelope.Payload)
	}
	decoded, err := decodeResourcePageTokenForTest(secret, token, associatedData)
	if err != nil {
		t.Fatalf("decode resource page token: %v", err)
	}
	if decoded.ResourceID != "sesrsc_cursor" {
		t.Fatalf("ResourceID = %q; want cursor resource id", decoded.ResourceID)
	}
}

func TestSessionPageTokenRejectsInvalidEnvelopeOrContext(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	base := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	options := ListOptions{
		IncludeArchived: true,
		AgentID:         "agent_token",
		AgentVersion:    2,
		MemoryStoreID:   "memstore_token",
		CreatedAtGT:     timePointer(base.Add(-4 * time.Hour)),
		CreatedAtGTE:    timePointer(base.Add(-3 * time.Hour)),
		CreatedAtLT:     timePointer(base.Add(3 * time.Hour)),
		CreatedAtLTE:    timePointer(base.Add(4 * time.Hour)),
		Order:           ListOrderAscending,
	}
	associatedData := sessionListAssociatedData(workspace.DefaultID, options)
	validPayload := pageTokenPayload{
		Version:   pageTokenVersion,
		Kind:      pageKindSessions,
		CreatedAt: base.Format(pageTokenTimeFormat),
		SessionID: "sesn_cursor",
	}
	valid := signedPageTokenForTest(t, secret, validPayload, associatedData)

	cases := []struct {
		name           string
		token          string
		associatedData pageTokenAssociatedData
	}{
		{
			name:           "malformed",
			token:          "not-a-token",
			associatedData: associatedData,
		},
		{
			name: "wrong_version",
			token: signedPageTokenForTest(t, secret, pageTokenPayload{
				Version:   pageTokenVersion + 1,
				Kind:      pageKindSessions,
				CreatedAt: base.Format(pageTokenTimeFormat),
				SessionID: "sesn_cursor",
			}, associatedData),
			associatedData: associatedData,
		},
		{
			name: "wrong_kind",
			token: signedPageTokenForTest(t, secret, pageTokenPayload{
				Version:   pageTokenVersion,
				Kind:      pageKindResources,
				CreatedAt: base.Format(pageTokenTimeFormat),
				SessionID: "sesn_cursor",
			}, associatedData),
			associatedData: associatedData,
		},
		{
			name: "missing_session_cursor",
			token: signedPageTokenForTest(t, secret, pageTokenPayload{
				Version:   pageTokenVersion,
				Kind:      pageKindSessions,
				CreatedAt: base.Format(pageTokenTimeFormat),
			}, associatedData),
			associatedData: associatedData,
		},
		{
			name: "missing_created_at_cursor",
			token: signedPageTokenForTest(t, secret, pageTokenPayload{
				Version:   pageTokenVersion,
				Kind:      pageKindSessions,
				SessionID: "sesn_cursor",
			}, associatedData),
			associatedData: associatedData,
		},
		{
			name: "bad_signature",
			token: pageTokenEnvelopeForTest(t, validPayload,
				base64.RawURLEncoding.EncodeToString([]byte("not the expected signature"))),
			associatedData: associatedData,
		},
		{
			name:           "byte_tamper",
			token:          tamperPageTokenCursorForTest(t, valid, "sesn_cursor"),
			associatedData: associatedData,
		},
		{
			name:           "changed_workspace",
			token:          valid,
			associatedData: sessionListAssociatedData("workspace_b", options),
		},
		{
			name:  "changed_include_archived",
			token: valid,
			associatedData: sessionListAssociatedData(workspace.DefaultID, ListOptions{
				IncludeArchived: false,
				AgentID:         options.AgentID,
				AgentVersion:    options.AgentVersion,
				MemoryStoreID:   options.MemoryStoreID,
				CreatedAtGT:     options.CreatedAtGT,
				CreatedAtGTE:    options.CreatedAtGTE,
				CreatedAtLT:     options.CreatedAtLT,
				CreatedAtLTE:    options.CreatedAtLTE,
				Order:           options.Order,
			}),
		},
		{
			name:  "changed_created_at_gt",
			token: valid,
			associatedData: sessionListAssociatedData(workspace.DefaultID, ListOptions{
				IncludeArchived: options.IncludeArchived,
				AgentID:         options.AgentID,
				AgentVersion:    options.AgentVersion,
				MemoryStoreID:   options.MemoryStoreID,
				CreatedAtGT:     timePointer(base.Add(-5 * time.Hour)),
				CreatedAtGTE:    options.CreatedAtGTE,
				CreatedAtLT:     options.CreatedAtLT,
				CreatedAtLTE:    options.CreatedAtLTE,
				Order:           options.Order,
			}),
		},
		{
			name:  "changed_created_at_gte",
			token: valid,
			associatedData: sessionListAssociatedData(workspace.DefaultID, ListOptions{
				IncludeArchived: options.IncludeArchived,
				AgentID:         options.AgentID,
				AgentVersion:    options.AgentVersion,
				MemoryStoreID:   options.MemoryStoreID,
				CreatedAtGT:     options.CreatedAtGT,
				CreatedAtGTE:    timePointer(base.Add(-2 * time.Hour)),
				CreatedAtLT:     options.CreatedAtLT,
				CreatedAtLTE:    options.CreatedAtLTE,
				Order:           options.Order,
			}),
		},
		{
			name:  "changed_created_at_lt",
			token: valid,
			associatedData: sessionListAssociatedData(workspace.DefaultID, ListOptions{
				IncludeArchived: options.IncludeArchived,
				AgentID:         options.AgentID,
				AgentVersion:    options.AgentVersion,
				MemoryStoreID:   options.MemoryStoreID,
				CreatedAtGT:     options.CreatedAtGT,
				CreatedAtGTE:    options.CreatedAtGTE,
				CreatedAtLT:     timePointer(base.Add(2 * time.Hour)),
				CreatedAtLTE:    options.CreatedAtLTE,
				Order:           options.Order,
			}),
		},
		{
			name:  "changed_created_at_lte",
			token: valid,
			associatedData: sessionListAssociatedData(workspace.DefaultID, ListOptions{
				IncludeArchived: options.IncludeArchived,
				AgentID:         options.AgentID,
				AgentVersion:    options.AgentVersion,
				MemoryStoreID:   options.MemoryStoreID,
				CreatedAtGT:     options.CreatedAtGT,
				CreatedAtGTE:    options.CreatedAtGTE,
				CreatedAtLT:     options.CreatedAtLT,
				CreatedAtLTE:    timePointer(base.Add(5 * time.Hour)),
				Order:           options.Order,
			}),
		},
		{
			name:  "changed_agent_filter",
			token: valid,
			associatedData: sessionListAssociatedData(workspace.DefaultID, ListOptions{
				IncludeArchived: options.IncludeArchived,
				AgentID:         "agent_other",
				AgentVersion:    options.AgentVersion,
				MemoryStoreID:   options.MemoryStoreID,
				CreatedAtGT:     options.CreatedAtGT,
				CreatedAtGTE:    options.CreatedAtGTE,
				CreatedAtLT:     options.CreatedAtLT,
				CreatedAtLTE:    options.CreatedAtLTE,
				Order:           options.Order,
			}),
		},
		{
			name:  "changed_order",
			token: valid,
			associatedData: sessionListAssociatedData(workspace.DefaultID, ListOptions{
				IncludeArchived: options.IncludeArchived,
				AgentID:         options.AgentID,
				AgentVersion:    options.AgentVersion,
				MemoryStoreID:   options.MemoryStoreID,
				CreatedAtGT:     options.CreatedAtGT,
				CreatedAtGTE:    options.CreatedAtGTE,
				CreatedAtLT:     options.CreatedAtLT,
				CreatedAtLTE:    options.CreatedAtLTE,
				Order:           ListOrderDescending,
			}),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := decodeSessionPageTokenForTest(secret, testCase.token, testCase.associatedData); err == nil {
				t.Fatal("decode session page token succeeded; want invalid page token")
			}
		})
	}

	payload, err := decodeSessionPageTokenForTest(secret, valid, associatedData)
	if err != nil {
		t.Fatalf("valid session page token rejected: %v", err)
	}
	if payload.SessionID != "sesn_cursor" || payload.CreatedAt != base.Format(pageTokenTimeFormat) {
		t.Fatalf("payload = %+v; want cursor session and created_at", payload)
	}
}

func TestResourcePageTokenRejectsInvalidEnvelopeOrContext(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	associatedData := resourceListAssociatedData(workspace.DefaultID, "sesn_resource_token")
	validPayload := pageTokenPayload{
		Version:    pageTokenVersion,
		Kind:       pageKindResources,
		ResourceID: "sesrsc_cursor",
	}
	valid := signedPageTokenForTest(t, secret, validPayload, associatedData)

	cases := []struct {
		name           string
		token          string
		associatedData pageTokenAssociatedData
	}{
		{
			name:           "malformed",
			token:          "not-a-token",
			associatedData: associatedData,
		},
		{
			name: "wrong_version",
			token: signedPageTokenForTest(t, secret, pageTokenPayload{
				Version:    pageTokenVersion + 1,
				Kind:       pageKindResources,
				ResourceID: "sesrsc_cursor",
			}, associatedData),
			associatedData: associatedData,
		},
		{
			name: "wrong_kind",
			token: signedPageTokenForTest(t, secret, pageTokenPayload{
				Version:    pageTokenVersion,
				Kind:       pageKindSessions,
				ResourceID: "sesrsc_cursor",
			}, associatedData),
			associatedData: associatedData,
		},
		{
			name: "missing_cursor",
			token: signedPageTokenForTest(t, secret, pageTokenPayload{
				Version: pageTokenVersion,
				Kind:    pageKindResources,
			}, associatedData),
			associatedData: associatedData,
		},
		{
			name: "bad_signature",
			token: pageTokenEnvelopeForTest(t, validPayload,
				base64.RawURLEncoding.EncodeToString([]byte("not the expected signature"))),
			associatedData: associatedData,
		},
		{
			name:           "byte_tamper",
			token:          tamperPageTokenCursorForTest(t, valid, "sesrsc_cursor"),
			associatedData: associatedData,
		},
		{
			name:           "changed_session",
			token:          valid,
			associatedData: resourceListAssociatedData(workspace.DefaultID, "sesn_other"),
		},
		{
			name:           "changed_workspace",
			token:          valid,
			associatedData: resourceListAssociatedData("workspace_b", "sesn_resource_token"),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := decodeResourcePageTokenForTest(secret, testCase.token, testCase.associatedData); err == nil {
				t.Fatal("decode resource page token succeeded; want invalid page token")
			}
		})
	}

	payload, err := decodeResourcePageTokenForTest(secret, valid, associatedData)
	if err != nil {
		t.Fatalf("valid resource page token rejected: %v", err)
	}
	if payload.ResourceID != "sesrsc_cursor" {
		t.Fatalf("ResourceID = %q; want cursor resource id", payload.ResourceID)
	}
}

func decodeSessionPageTokenForTest(secret []byte, token string, associatedData pageTokenAssociatedData) (pageTokenPayload, error) {
	payload, err := decodePageToken(secret, token, associatedData)
	if err != nil {
		return pageTokenPayload{}, err
	}
	if payload.Kind != pageKindSessions || payload.SessionID == "" || payload.CreatedAt == "" {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if _, err := time.Parse(pageTokenTimeFormat, payload.CreatedAt); err != nil {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return payload, nil
}

func decodeResourcePageTokenForTest(secret []byte, token string, associatedData pageTokenAssociatedData) (pageTokenPayload, error) {
	payload, err := decodePageToken(secret, token, associatedData)
	if err != nil {
		return pageTokenPayload{}, err
	}
	if payload.Kind != pageKindResources || payload.ResourceID == "" {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return payload, nil
}

func signedPageTokenForTest(t *testing.T, secret []byte, payload pageTokenPayload, associatedData pageTokenAssociatedData) string {
	t.Helper()
	body, err := json.Marshal(pageTokenSignedBody{Payload: payload, AssociatedData: associatedData})
	if err != nil {
		t.Fatalf("marshal signed page token body: %v", err)
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return pageTokenEnvelopeForTest(t, payload, base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
}

func pageTokenEnvelopeForTest(t *testing.T, payload pageTokenPayload, signature string) string {
	t.Helper()
	raw, err := json.Marshal(signedPageToken{Payload: payload, Signature: signature})
	if err != nil {
		t.Fatalf("marshal page token envelope: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func tamperPageTokenCursorForTest(t *testing.T, token string, cursor string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("decode page token envelope: %v", err)
	}
	index := bytes.Index(raw, []byte(cursor))
	if index < 0 {
		t.Fatalf("decoded page token = %s; cursor not found", raw)
	}
	raw[index] = 't'
	return base64.RawURLEncoding.EncodeToString(raw)
}

func timePointer(value time.Time) *time.Time { return &value }

type recordingListOrderExecutor struct {
	queryCount int
}

func (e *recordingListOrderExecutor) Exec(context.Context, string, ...any) (ExecResult, error) {
	return nil, nil
}

func (e *recordingListOrderExecutor) QueryRows(context.Context, string, ...any) (QueryRows, error) {
	e.queryCount++
	return emptySessionRows{}, nil
}

func (e *recordingListOrderExecutor) QueryRowScanner(context.Context, string, ...any) RowScanner {
	return emptySessionRow{}
}

type emptySessionRows struct{}

func (emptySessionRows) Next() bool        { return false }
func (emptySessionRows) Scan(...any) error { return nil }
func (emptySessionRows) Err() error        { return nil }
func (emptySessionRows) Close() error      { return nil }

type emptySessionRow struct{}

func (emptySessionRow) Scan(...any) error { return nil }
