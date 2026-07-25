package httpapi_test

import (
	"database/sql"
	"net/http"
	"testing"
)

func TestSDKCompatibilitySessionNullClearsSurviveServiceAndStore(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	created := env.createSession(t, `{
		"agent":"agent_http_session",
		"environment_id":"env_http_session",
		"title":"before",
		"metadata":{"team":"runtime","region":"iad"},
		"vault_ids":[]
	}`)

	recorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"?beta=true", `{"title":null,"metadata":null}`)
	assertHTTPStatus(t, recorder, http.StatusOK)
	var updated sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, recorder, &updated)
	if updated.Title != nil || len(updated.Metadata) != 0 {
		t.Fatalf("updated title/metadata = %v/%#v; want cleared", updated.Title, updated.Metadata)
	}

	var title sql.NullString
	var metadataJSON string
	if err := env.admin.QueryRow(`SELECT title, metadata_json FROM sessions WHERE id = $1`, created.ID).Scan(&title, &metadataJSON); err != nil {
		t.Fatalf("read stored session: %v", err)
	}
	if title.Valid || metadataJSON != `{}` {
		t.Fatalf("stored title/metadata = %+v/%s; want NULL/{}", title, metadataJSON)
	}
}

func TestSDKCompatibilitySessionNestedCheckoutAndMemoryNullOverridesSurviveStore(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	created := env.createSession(t, `{
		"agent":"agent_http_session",
		"environment_id":"env_http_session",
		"vault_ids":[],
		"resources":[
			{"type":"memory_store","memory_store_id":"memstore_http_session","access":null,"instructions":null},
			{"type":"github_repository","url":"https://github.com/tetral-ai/tetral.git","authorization_token":"github_token_compat","checkout":{"type":"branch","name":"main"}}
		]
	}`)
	if len(created.Resources) != 2 {
		t.Fatalf("resources = %+v; want two", created.Resources)
	}
	if created.Resources[0].Access != "read_only" {
		t.Fatalf("memory access = %q; want read_only default", created.Resources[0].Access)
	}
	if created.Resources[1].CheckoutType != "" || created.Resources[1].CheckoutRef != "" {
		t.Fatalf("response leaked flat checkout: %+v", created.Resources[1])
	}
	if created.Resources[1].Checkout["type"] != "branch" || created.Resources[1].Checkout["name"] != "main" || len(created.Resources[1].Checkout) != 2 {
		t.Fatalf("nested checkout = %#v; want exact branch union", created.Resources[1].Checkout)
	}

	var access, instructions, checkoutType, checkoutRef sql.NullString
	if err := env.admin.QueryRow(`
		SELECT m.access, m.instructions, g.checkout_type, g.checkout_ref
		FROM session_resources r1
		JOIN session_memory_store_resources m ON m.resource_id = r1.resource_id
		JOIN session_resources r2 ON r2.workspace_id = r1.workspace_id AND r2.session_id = r1.session_id AND r2.type = 'github_repository'
		JOIN session_github_repository_resources g ON g.resource_id = r2.resource_id
		WHERE r1.session_id = $1 AND r1.type = 'memory_store'`, created.ID).
		Scan(&access, &instructions, &checkoutType, &checkoutRef); err != nil {
		t.Fatalf("read stored resources: %v", err)
	}
	if access.String != "read_only" || instructions.String != "" || checkoutType.String != "branch" || checkoutRef.String != "main" {
		t.Fatalf("stored resources = access=%+v instructions=%+v checkout=%+v/%+v", access, instructions, checkoutType, checkoutRef)
	}
}

func TestSDKCompatibilitySessionRejectsDuplicateCanonicalGitHubRepositoriesAtHTTPBoundary(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	recorder := env.request(http.MethodPost, "/v1/sessions?beta=true", `{
		"agent":"agent_http_session",
		"environment_id":"env_http_session",
		"vault_ids":[],
		"resources":[
			{"type":"github_repository","url":"https://github.com/tetral-ai/tetral.git","authorization_token":"github_token_first","mount_path":"/workspace/first"},
			{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","authorization_token":"github_token_second","mount_path":"/workspace/second"}
		]
	}`)
	assertHTTPStatus(t, recorder, http.StatusBadRequest)
	assertErrorType(t, recorder, "invalid_request_error")

	var count int
	if err := env.admin.QueryRow(`SELECT count(*) FROM sessions`).Scan(&count); err != nil {
		t.Fatalf("count sessions after duplicate repository rejection: %v", err)
	}
	if count != 0 {
		t.Fatalf("sessions persisted after duplicate repository rejection: %d", count)
	}
}
