package httpapi_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const sessionIntegrationPageTokenSecret = "session-integration-secret-12345"

func TestSessionIntegrationRejectsInvalidSessionPageTokens(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	env.clock = time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	first := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[],"title":"first"}`)
	env.clock = env.clock.Add(time.Minute)
	second := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[],"title":"second"}`)

	firstPagePath := "/v1/sessions?beta=true&limit=1&order=asc"
	firstPageRecorder := env.request(http.MethodGet, firstPagePath, "")
	assertHTTPStatus(t, firstPageRecorder, http.StatusOK)
	var firstPage sessionPageTokenSessionListResponse
	decodeSessionIntegrationJSON(t, firstPageRecorder, &firstPage)
	if len(firstPage.Data) != 1 || firstPage.Data[0].ID != first.ID || firstPage.NextPage == nil {
		t.Fatalf("first page = %#v next=%v; want %s and next page", firstPage.Data, firstPage.NextPage, first.ID)
	}

	secondPageRecorder := env.request(http.MethodGet, firstPagePath+"&page="+url.QueryEscape(*firstPage.NextPage), "")
	assertHTTPStatus(t, secondPageRecorder, http.StatusOK)
	var secondPage sessionPageTokenSessionListResponse
	decodeSessionIntegrationJSON(t, secondPageRecorder, &secondPage)
	if len(secondPage.Data) != 1 || secondPage.Data[0].ID != second.ID {
		t.Fatalf("second page = %#v; want valid token to reach %s", secondPage.Data, second.ID)
	}

	validPayload := sessionPageTokenPayload{
		Version:   1,
		Kind:      "sessions",
		CreatedAt: firstPage.Data[0].CreatedAt.UTC().Format(time.RFC3339Nano),
		SessionID: first.ID,
	}
	validAssociatedData := sessionPageTokenAssociatedData{
		Version:     1,
		Kind:        "sessions",
		WorkspaceID: string(workspace.DefaultID),
		Order:       "asc",
	}

	cases := []struct {
		name string
		page string
		path string
	}{
		{name: "malformed", page: "not-a-token", path: firstPagePath},
		{
			name: "wrong_version",
			page: signSessionIntegrationPageToken(t, sessionPageTokenPayload{
				Version:   2,
				Kind:      "sessions",
				CreatedAt: validPayload.CreatedAt,
				SessionID: first.ID,
			}, validAssociatedData),
			path: firstPagePath,
		},
		{
			name: "wrong_kind",
			page: signSessionIntegrationPageToken(t, sessionPageTokenPayload{
				Version:   1,
				Kind:      "session_resources",
				CreatedAt: validPayload.CreatedAt,
				SessionID: first.ID,
			}, validAssociatedData),
			path: firstPagePath,
		},
		{
			name: "missing_cursor",
			page: signSessionIntegrationPageToken(t, sessionPageTokenPayload{
				Version: 1,
				Kind:    "sessions",
			}, validAssociatedData),
			path: firstPagePath,
		},
		{name: "bad_signature", page: sessionIntegrationPageTokenEnvelope(t, validPayload, base64.RawURLEncoding.EncodeToString([]byte("bad signature"))), path: firstPagePath},
		{name: "byte_tampered", page: tamperSessionIntegrationPageToken(t, *firstPage.NextPage, first.ID), path: firstPagePath},
		{
			name: "workspace_mismatch",
			page: signSessionIntegrationPageToken(t, validPayload, sessionPageTokenAssociatedData{
				Version:     1,
				Kind:        "sessions",
				WorkspaceID: "workspace_b",
				Order:       "asc",
			}),
			path: firstPagePath,
		},
		{name: "include_archived_mismatch", page: *firstPage.NextPage, path: "/v1/sessions?beta=true&limit=1&order=asc&include_archived=true"},
		{name: "filter_mismatch", page: *firstPage.NextPage, path: "/v1/sessions?beta=true&limit=1&order=asc&agent_id=agent_http_session"},
		{name: "order_mismatch", page: *firstPage.NextPage, path: "/v1/sessions?beta=true&limit=1&order=desc"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := env.request(http.MethodGet, testCase.path+"&page="+url.QueryEscape(testCase.page), "")
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
		})
	}
}

func TestSessionIntegrationRejectsSessionPageTokenReplayWhenHTTPFiltersChange(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	base := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	env.clock = base
	first := env.createSession(t, `{
		"agent":"agent_http_session",
		"environment_id":"env_http_session",
		"title":"first filtered",
		"vault_ids":[],
		"resources":[{"type":"memory_store","memory_store_id":"memstore_http_session","access":"read_only"}]
	}`)
	env.clock = base.Add(time.Minute)
	second := env.createSession(t, `{
		"agent":"agent_http_session",
		"environment_id":"env_http_session",
		"title":"second filtered",
		"vault_ids":[],
		"resources":[{"type":"memory_store","memory_store_id":"memstore_http_session","access":"read_only"}]
	}`)

	filters := url.Values{}
	filters.Set("limit", "1")
	filters.Set("order", "asc")
	filters.Set("memory_store_id", sessionIntegrationMemoryStore)
	filters.Set("created_at[gt]", base.Add(-time.Hour).Format(time.RFC3339))
	filters.Set("created_at[gte]", base.Add(-30*time.Minute).Format(time.RFC3339))
	filters.Set("created_at[lt]", base.Add(time.Hour).Format(time.RFC3339))
	filters.Set("created_at[lte]", base.Add(90*time.Minute).Format(time.RFC3339))

	firstPageRecorder := env.request(http.MethodGet, sessionIntegrationSessionsPath(filters), "")
	assertHTTPStatus(t, firstPageRecorder, http.StatusOK)
	var firstPage sessionPageTokenSessionListResponse
	decodeSessionIntegrationJSON(t, firstPageRecorder, &firstPage)
	if len(firstPage.Data) != 1 || firstPage.Data[0].ID != first.ID || firstPage.NextPage == nil {
		t.Fatalf("first page = %#v next=%v; want %s and next page", firstPage.Data, firstPage.NextPage, first.ID)
	}

	validReplay := cloneSessionIntegrationQuery(filters)
	validReplay.Set("page", *firstPage.NextPage)
	secondPageRecorder := env.request(http.MethodGet, sessionIntegrationSessionsPath(validReplay), "")
	assertHTTPStatus(t, secondPageRecorder, http.StatusOK)
	var secondPage sessionPageTokenSessionListResponse
	decodeSessionIntegrationJSON(t, secondPageRecorder, &secondPage)
	if len(secondPage.Data) != 1 || secondPage.Data[0].ID != second.ID {
		t.Fatalf("second page = %#v; want valid token to reach %s", secondPage.Data, second.ID)
	}

	cases := []struct {
		name  string
		key   string
		value string
	}{
		{name: "memory_store_id", key: "memory_store_id", value: "memstore_http_session_other"},
		{name: "created_at_gt", key: "created_at[gt]", value: base.Add(-2 * time.Hour).Format(time.RFC3339)},
		{name: "created_at_gte", key: "created_at[gte]", value: base.Add(-15 * time.Minute).Format(time.RFC3339)},
		{name: "created_at_lt", key: "created_at[lt]", value: base.Add(2 * time.Hour).Format(time.RFC3339)},
		{name: "created_at_lte", key: "created_at[lte]", value: base.Add(2 * time.Hour).Format(time.RFC3339)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			replay := cloneSessionIntegrationQuery(filters)
			replay.Set(testCase.key, testCase.value)
			replay.Set("page", *firstPage.NextPage)

			recorder := env.request(http.MethodGet, sessionIntegrationSessionsPath(replay), "")

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
		})
	}
}

func TestSessionIntegrationRejectsInvalidResourcePageTokens(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	created := env.createSession(t, `{
		"agent":"agent_http_session",
		"environment_id":"env_http_session",
		"vault_ids":[],
		"resources":[
			{"type":"file","file_id":"file_http_source_a","mount_path":"/workspace/input.txt"},
			{"type":"memory_store","memory_store_id":"memstore_http_session","access":"read_only","instructions":"use the stable snapshot"}
		]
	}`)

	firstPagePath := "/v1/sessions/" + created.ID + "/resources?limit=1"
	firstPageRecorder := env.request(http.MethodGet, firstPagePath, "")
	assertHTTPStatus(t, firstPageRecorder, http.StatusOK)
	var firstPage sessionPageTokenResourceListResponse
	decodeSessionIntegrationJSON(t, firstPageRecorder, &firstPage)
	if len(firstPage.Data) != 1 || firstPage.NextPage == nil {
		t.Fatalf("first resource page = %#v next=%v; want next page", firstPage.Data, firstPage.NextPage)
	}

	secondPageRecorder := env.request(http.MethodGet, firstPagePath+"&page="+url.QueryEscape(*firstPage.NextPage), "")
	assertHTTPStatus(t, secondPageRecorder, http.StatusOK)
	var secondPage sessionPageTokenResourceListResponse
	decodeSessionIntegrationJSON(t, secondPageRecorder, &secondPage)
	if len(secondPage.Data) != 1 || secondPage.Data[0].ID == firstPage.Data[0].ID {
		t.Fatalf("second resource page = %#v; want valid token to advance", secondPage.Data)
	}

	validPayload := sessionPageTokenPayload{
		Version:    1,
		Kind:       "session_resources",
		ResourceID: firstPage.Data[0].ID,
	}
	validAssociatedData := sessionPageTokenAssociatedData{
		Version:     1,
		Kind:        "session_resources",
		WorkspaceID: string(workspace.DefaultID),
		SessionID:   created.ID,
	}
	otherSession := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[]}`)

	cases := []struct {
		name string
		page string
		path string
	}{
		{name: "malformed", page: "not-a-token", path: firstPagePath},
		{
			name: "wrong_version",
			page: signSessionIntegrationPageToken(t, sessionPageTokenPayload{
				Version:    2,
				Kind:       "session_resources",
				ResourceID: firstPage.Data[0].ID,
			}, validAssociatedData),
			path: firstPagePath,
		},
		{
			name: "wrong_kind",
			page: signSessionIntegrationPageToken(t, sessionPageTokenPayload{
				Version:    1,
				Kind:       "sessions",
				ResourceID: firstPage.Data[0].ID,
			}, validAssociatedData),
			path: firstPagePath,
		},
		{
			name: "missing_cursor",
			page: signSessionIntegrationPageToken(t, sessionPageTokenPayload{
				Version: 1,
				Kind:    "session_resources",
			}, validAssociatedData),
			path: firstPagePath,
		},
		{name: "bad_signature", page: sessionIntegrationPageTokenEnvelope(t, validPayload, base64.RawURLEncoding.EncodeToString([]byte("bad signature"))), path: firstPagePath},
		{name: "byte_tampered", page: tamperSessionIntegrationPageToken(t, *firstPage.NextPage, firstPage.Data[0].ID), path: firstPagePath},
		{
			name: "workspace_mismatch",
			page: signSessionIntegrationPageToken(t, validPayload, sessionPageTokenAssociatedData{
				Version:     1,
				Kind:        "session_resources",
				WorkspaceID: "workspace_b",
				SessionID:   created.ID,
			}),
			path: firstPagePath,
		},
		{name: "filter_mismatch", page: *firstPage.NextPage, path: "/v1/sessions/" + otherSession.ID + "/resources?limit=1"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := env.request(http.MethodGet, testCase.path+"&page="+url.QueryEscape(testCase.page), "")
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
		})
	}
}

type sessionPageTokenPayload struct {
	Version    int    `json:"version"`
	Kind       string `json:"kind"`
	CreatedAt  string `json:"created_at,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
	ResourceID string `json:"resource_id,omitempty"`
}

type sessionPageTokenAssociatedData struct {
	Version         int    `json:"version"`
	Kind            string `json:"kind"`
	WorkspaceID     string `json:"workspace_id"`
	SessionID       string `json:"session_id,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	AgentVersion    int    `json:"agent_version,omitempty"`
	MemoryStoreID   string `json:"memory_store_id,omitempty"`
	CreatedAtGT     string `json:"created_at_gt,omitempty"`
	CreatedAtGTE    string `json:"created_at_gte,omitempty"`
	CreatedAtLT     string `json:"created_at_lt,omitempty"`
	CreatedAtLTE    string `json:"created_at_lte,omitempty"`
	Order           string `json:"order,omitempty"`
}

type sessionPageTokenSignedBody struct {
	Payload        sessionPageTokenPayload        `json:"payload"`
	AssociatedData sessionPageTokenAssociatedData `json:"associated_data"`
}

type sessionPageTokenEnvelope struct {
	Payload   sessionPageTokenPayload `json:"payload"`
	Signature string                  `json:"signature"`
}

type sessionPageTokenSessionResponse struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

type sessionPageTokenResourceResponse struct {
	ID string `json:"id"`
}

type sessionPageTokenSessionListResponse struct {
	Data     []sessionPageTokenSessionResponse `json:"data"`
	NextPage *string                           `json:"next_page"`
}

type sessionPageTokenResourceListResponse struct {
	Data     []sessionPageTokenResourceResponse `json:"data"`
	NextPage *string                            `json:"next_page"`
}

func signSessionIntegrationPageToken(t *testing.T, payload sessionPageTokenPayload, associatedData sessionPageTokenAssociatedData) string {
	t.Helper()
	body, err := json.Marshal(sessionPageTokenSignedBody{Payload: payload, AssociatedData: associatedData})
	if err != nil {
		t.Fatalf("marshal signed page token body: %v", err)
	}
	mac := hmac.New(sha256.New, []byte(sessionIntegrationPageTokenSecret))
	_, _ = mac.Write(body)
	return sessionIntegrationPageTokenEnvelope(t, payload, base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
}

func sessionIntegrationPageTokenEnvelope(t *testing.T, payload sessionPageTokenPayload, signature string) string {
	t.Helper()
	raw, err := json.Marshal(sessionPageTokenEnvelope{Payload: payload, Signature: signature})
	if err != nil {
		t.Fatalf("marshal page token envelope: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func tamperSessionIntegrationPageToken(t *testing.T, token string, cursor string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("decode page token: %v", err)
	}
	index := bytes.Index(raw, []byte(cursor))
	if index < 0 {
		t.Fatalf("decoded page token = %s; cursor %q not found", raw, cursor)
	}
	raw[index] = 't'
	return base64.RawURLEncoding.EncodeToString(raw)
}

func sessionIntegrationSessionsPath(values url.Values) string {
	return "/v1/sessions?beta=true&" + values.Encode()
}

func cloneSessionIntegrationQuery(values url.Values) url.Values {
	clone := make(url.Values, len(values))
	for key, rawValues := range values {
		clone[key] = append([]string(nil), rawValues...)
	}
	return clone
}
