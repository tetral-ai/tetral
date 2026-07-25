package httpapi_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/memory"
)

func TestTCompatMEM19HTTPCreateNullRejectsAndEmptyContentSucceeds(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/memstore_missing/memories", env.envKey, `{"path":"/null.md","content":null}`)
	if code != http.StatusBadRequest || !strings.Contains(body, `"type":"invalid_request_error"`) || !strings.Contains(body, `"message":"content must be a string"`) {
		t.Fatalf("content:null status/body = %d %s; want named 400", code, body)
	}

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores", env.envKey, `{"name":"mem19"}`)
	if code != http.StatusOK {
		t.Fatalf("create store status/body = %d %s", code, body)
	}
	var store memory.Store
	decodeMemoryJSON(t, body, &store)
	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories?view=full", env.envKey, `{"path":"/empty.md","content":""}`)
	if code != http.StatusOK {
		t.Fatalf("empty content status/body = %d %s; want 200", code, body)
	}
	var created memory.Memory
	decodeMemoryJSON(t, body, &created)
	if created.Content == nil || *created.Content != "" || created.ContentSizeBytes != 0 {
		t.Fatalf("created empty memory = %+v", created)
	}
}

func TestTCompatMEM20HTTPUpdateNullsRejectAndOmissionLeavesValuesUnchanged(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores", env.envKey, `{"name":"mem20"}`)
	if code != http.StatusOK {
		t.Fatalf("create store status/body = %d %s", code, body)
	}
	var store memory.Store
	decodeMemoryJSON(t, body, &store)
	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories?view=full", env.envKey, `{"path":"/unchanged.md","content":"unchanged-content"}`)
	if code != http.StatusOK {
		t.Fatalf("create memory status/body = %d %s", code, body)
	}
	var created memory.Memory
	decodeMemoryJSON(t, body, &created)

	for field, requestBody := range map[string]string{
		"content": `{"content":null}`,
		"path":    `{"path":null}`,
	} {
		code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories/"+created.ID, env.envKey, requestBody)
		wantMessage := `"message":"` + field + ` must be a string"`
		if code != http.StatusBadRequest || !strings.Contains(body, `"type":"invalid_request_error"`) || !strings.Contains(body, wantMessage) {
			t.Fatalf("%s:null status/body = %d %s; want named 400", field, code, body)
		}
	}

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories/"+created.ID+"?view=full", env.envKey, `{"content":"updated-content"}`)
	if code != http.StatusOK {
		t.Fatalf("omitted path status/body = %d %s; want 200", code, body)
	}
	var contentUpdated memory.Memory
	decodeMemoryJSON(t, body, &contentUpdated)
	if contentUpdated.Path != created.Path || contentUpdated.Content == nil || *contentUpdated.Content != "updated-content" {
		t.Fatalf("content update = %+v; want omitted path unchanged from %+v", contentUpdated, created)
	}

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories/"+created.ID+"?view=full", env.envKey, `{"path":"/renamed.md"}`)
	if code != http.StatusOK {
		t.Fatalf("omitted content status/body = %d %s; want 200", code, body)
	}
	var pathUpdated memory.Memory
	decodeMemoryJSON(t, body, &pathUpdated)
	if pathUpdated.Path != "/renamed.md" || pathUpdated.Content == nil || *pathUpdated.Content != "updated-content" {
		t.Fatalf("path update = %+v; want omitted content unchanged from %+v", pathUpdated, contentUpdated)
	}
}

func TestHTTPMemoryUpdateWithNoMutableFieldsIsNoOp(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores", env.envKey, `{"name":"empty-update"}`)
	if code != http.StatusOK {
		t.Fatalf("create store status/body = %d %s", code, body)
	}
	var store memory.Store
	decodeMemoryJSON(t, body, &store)

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories?view=full", env.envKey, `{"path":"/unchanged.md","content":"unchanged-content"}`)
	if code != http.StatusOK {
		t.Fatalf("create memory status/body = %d %s", code, body)
	}
	var created memory.Memory
	decodeMemoryJSON(t, body, &created)

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories/"+created.ID+"?view=full", env.envKey, `{}`)
	if code != http.StatusOK {
		t.Fatalf("empty update status/body = %d %s; want 200", code, body)
	}
	var noOp memory.Memory
	decodeMemoryJSON(t, body, &noOp)
	if noOp.MemoryVersionID != created.MemoryVersionID ||
		noOp.Path != created.Path ||
		noOp.Content == nil ||
		created.Content == nil ||
		*noOp.Content != *created.Content ||
		noOp.ContentSHA256 != created.ContentSHA256 ||
		noOp.ContentSizeBytes != created.ContentSizeBytes ||
		noOp.UpdatedAt != created.UpdatedAt {
		t.Fatalf("empty update changed durable projection:\ncreated = %+v\nno-op   = %+v", created, noOp)
	}

	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores/"+store.ID+"/memories/"+created.ID+"?view=full", env.envKey, "")
	if code != http.StatusOK {
		t.Fatalf("get after empty update status/body = %d %s", code, body)
	}
	var stored memory.Memory
	decodeMemoryJSON(t, body, &stored)
	if stored.MemoryVersionID != created.MemoryVersionID || stored.UpdatedAt != created.UpdatedAt {
		t.Fatalf("stored memory changed after empty update:\ncreated = %+v\nstored  = %+v", created, stored)
	}
}

func TestSDKCompatibilityMemoryStoreNullClearsSurviveStore(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores", env.envKey, `{"name":"before","metadata":{"team":"runtime"}}`)
	if code != http.StatusOK {
		t.Fatalf("create store status/body = %d %s", code, body)
	}
	var store memory.Store
	decodeMemoryJSON(t, body, &store)
	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID, env.envKey, `{"name":null,"metadata":null}`)
	if code != http.StatusOK {
		t.Fatalf("clear store status/body = %d %s; want 200", code, body)
	}
	if !strings.Contains(body, `"name":null`) || !strings.Contains(body, `"metadata":{}`) {
		t.Fatalf("clear response = %s; want nullable name and empty metadata", body)
	}
	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores/"+store.ID, env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"name":null`) || !strings.Contains(body, `"metadata":{}`) {
		t.Fatalf("stored clear response = %d %s", code, body)
	}
}
