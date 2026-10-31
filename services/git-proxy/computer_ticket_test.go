package gitproxy

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// A computer ticket, looked up as the git_proxy role, is accepted only while
// live and only from X-Tetral-Git-Ticket, and injects the token of the
// matching repository of its own computer, never that of another computer of
// the same Session with the same repository.
func TestComputerTicketsReachOnlyTheirComputersRepositories(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	encryptor, err := vault.NewEncryptor(gitProxyTestVaultKey)
	if err != nil {
		t.Fatal(err)
	}
	env := &repositoryTokenTestEnv{admin: admin, encryptor: encryptor}
	env.seedSession(t, "sesn_ticket_a")
	env.seedRepository(t, "sesn_ticket_a", "rsrc_a", "https://github.com/acme/app", tokenPointer("token-a"))
	// The same repository, mounted for another computer of the Session; a
	// Session-scoped lookup would find both and answer neither.
	env.seedRepository(t, "sesn_ticket_a", "rsrc_other", "https://github.com/acme/app", tokenPointer("token-other"))
	for _, statement := range []string{
		`INSERT INTO computers (workspace_id,session_id,id,kind,environment_id,working_directory,networking,drive_exclude,association_state,execution_state)
			VALUES ($1,'sesn_ticket_a','comp_other','cloud','env_other','/workspace','{"type":"unrestricted"}','[]','attached','waiting')`,
		`UPDATE session_github_repository_resources SET computer_id='comp_other' WHERE workspace_id=$1 AND resource_id='rsrc_other'`,
	} {
		if _, err := admin.ExecContext(context.Background(), statement, string(workspace.DefaultID)); err != nil {
			t.Fatal(err)
		}
	}
	owner := dbconnect.NewClientForTesting(admin)
	var first, second string
	if err := owner.WithWorkspaceTx(context.Background(), string(workspace.DefaultID), "test.computer_tickets", func(tx *dbconnect.Tx) error {
		var err error
		if first, err = gitticket.IssueComputerTicket(context.Background(), tx, workspace.DefaultID, "sesn_ticket_a", "comp_sesn_ticket_a"); err != nil {
			return err
		}
		second, err = gitticket.IssueComputerTicket(context.Background(), tx, workspace.DefaultID, "sesn_ticket_a", "comp_sesn_ticket_a")
		return err
	}); err != nil {
		t.Fatal(err)
	}

	proxyRole := dbconnect.NewClientForTesting(storagetest.OpenWorkloadDB(t, admin, "git_proxy").DB)
	var upstreamCalls atomic.Int32
	var authorization atomic.Value
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		authorization.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, _ := url.Parse(upstream.URL)
	publicBase, _ := url.Parse("https://git.tetral.test")
	proxy := NewHTTPHandler(TicketValidator{ComputerStore: gitticket.NewPostgreSQLComputerTicketStore(proxyRole)},
		NewRepositoryPolicyAuthorizer(NewPostgreSQLRepositoryTokenResolver(proxyRole, encryptor)),
		HandlerOptions{PublicBaseURL: publicBase, Transport: upstreamRewriteTransport{base: upstreamURL}})
	const infoRefs = "https://git.tetral.test/github.com/acme/app.git/info/refs?service=git-upload-pack"
	send := func(target string, set func(*http.Request)) int {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		set(request)
		recorder := httptest.NewRecorder()
		proxy.ServeHTTP(recorder, request)
		return recorder.Code
	}

	if status := send(infoRefs, func(r *http.Request) { r.Header.Set(gitTicketHeader, second) }); status != http.StatusOK ||
		authorization.Load() != GitHubBasicAuthorization("token-a") {
		t.Fatalf("live ticket = %d with %v; want 200 and its computer's token", status, authorization.Load())
	}
	for name, set := range map[string]func(*http.Request){
		"revoked ticket in the header": func(r *http.Request) { r.Header.Set(gitTicketHeader, first) },
		"live ticket as a Basic password": func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("x-access-token:"+second)))
		},
	} {
		if status := send(infoRefs, set); status != http.StatusUnauthorized {
			t.Errorf("%s = %d; want 401", name, status)
		}
	}
	if status := send(infoRefs+"&ticket="+second, func(*http.Request) {}); status == http.StatusOK {
		t.Errorf("live ticket as a URL query = %d; want a refusal", status)
	}
	if calls := upstreamCalls.Load(); calls != 1 {
		t.Fatalf("upstream calls = %d; want only the live header ticket's", calls)
	}
}
