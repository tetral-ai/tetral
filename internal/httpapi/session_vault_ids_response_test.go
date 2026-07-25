package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSessionVaultIDsResponseEncodesEmptyArray(t *testing.T) {
	env := newSessionIntegrationEnv(t)

	createRecorder := env.request(http.MethodPost, "/v1/sessions?beta=true", `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[]}`)
	assertHTTPStatus(t, createRecorder, http.StatusOK)
	assertSessionVaultIDsRawJSONArray(t, createRecorder)
	var created sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, createRecorder, &created)

	getRecorder := env.request(http.MethodGet, "/v1/sessions/"+created.ID+"?beta=true", "")
	assertHTTPStatus(t, getRecorder, http.StatusOK)
	assertSessionVaultIDsRawJSONArray(t, getRecorder)

	listRecorder := env.request(http.MethodGet, "/v1/sessions?beta=true", "")
	assertHTTPStatus(t, listRecorder, http.StatusOK)
	assertSessionListVaultIDsRawJSONArray(t, listRecorder, created.ID)

	updateRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"?beta=true", `{"title":"empty vault ids update"}`)
	assertHTTPStatus(t, updateRecorder, http.StatusOK)
	assertSessionVaultIDsRawJSONArray(t, updateRecorder)

	archiveRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"/archive?beta=true", "")
	assertHTTPStatus(t, archiveRecorder, http.StatusOK)
	assertSessionVaultIDsRawJSONArray(t, archiveRecorder)
}

func assertSessionVaultIDsRawJSONArray(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	var response map[string]json.RawMessage
	decodeSessionIntegrationJSON(t, recorder, &response)
	assertRawJSONArray(t, response["vault_ids"])
}

func assertSessionListVaultIDsRawJSONArray(t *testing.T, recorder *httptest.ResponseRecorder, sessionID string) {
	t.Helper()
	var response struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	decodeSessionIntegrationJSON(t, recorder, &response)
	for _, item := range response.Data {
		var id string
		if err := json.Unmarshal(item["id"], &id); err != nil {
			t.Fatalf("decode session id: %v", err)
		}
		if id == sessionID {
			assertRawJSONArray(t, item["vault_ids"])
			return
		}
	}
	t.Fatalf("session %s not found in list response", sessionID)
}

func assertRawJSONArray(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, raw); err != nil {
		t.Fatalf("compact vault_ids JSON: %v raw=%s", err, string(raw))
	}
	if compacted.String() != "[]" {
		t.Fatalf("vault_ids JSON = %s; want []", compacted.String())
	}
}
