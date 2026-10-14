package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tetral-ai/tetral/internal/memory"
)

// TestListQueriesAcceptOnlyAnEmptyPage covers the httpapi list parsers: the SDK
// sends a null page cursor as "page=".
func TestListQueriesAcceptOnlyAnEmptyPage(t *testing.T) {
	for _, test := range []struct {
		name  string
		parse func(*http.Request) (any, error)
	}{
		{"agents", func(r *http.Request) (any, error) { return parseAgentListOptions(r) }},
		{"agent versions", func(r *http.Request) (any, error) { return parseAgentVersionListOptions(r) }},
		{"sessions", func(r *http.Request) (any, error) { return parseSessionListOptions(r) }},
		{"session threads", func(r *http.Request) (any, error) { return parseSessionThreadListOptions(r) }},
		{"session resources", func(r *http.Request) (any, error) { return parseSessionResourceListOptions(r) }},
		{"environments", func(r *http.Request) (any, error) { return parseEnvironmentListOptions(r) }},
		{"vaults and credentials", func(r *http.Request) (any, error) { return parseVaultListOptions(r) }},
		{"skills", func(r *http.Request) (any, error) { return parseSkillListOptions(r) }},
		{"skill versions", func(r *http.Request) (any, error) { return parseSkillVersionListOptions(r) }},
		{"files", func(r *http.Request) (any, error) { return parseFileListOptions(r) }},
		{"memory lists", func(r *http.Request) (any, error) {
			if err := requireStrictQuery(r, []string{"page"}); err != nil {
				return nil, err
			}
			return memory.DecodeListStoresOptions(memoryQueryWithoutBeta(r))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.parse(httptest.NewRequest(http.MethodGet, "/v1/list?beta=true&page=", nil)); err != nil {
				t.Fatalf("page= rejected: %v; want the first page", err)
			}
		})
	}
	// Any other empty value stays malformed; an empty filter must not be dropped.
	if _, err := parseSessionListOptions(httptest.NewRequest(http.MethodGet, "/v1/list?beta=true&agent_id=", nil)); err == nil {
		t.Fatal("agent_id= accepted; want 400")
	}
}
