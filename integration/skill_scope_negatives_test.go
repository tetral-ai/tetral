package integration

import (
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/httpapi"
)

// TestPublicSkillContentRouteIsSolePackageDownloadSurface pins the one SDK download route.
func TestPublicSkillContentRouteIsSolePackageDownloadSurface(t *testing.T) {
	allowed := map[string]struct{}{
		"POST /v1/skills":                                      {},
		"GET /v1/skills":                                       {},
		"GET /v1/skills/{skill_id}":                            {},
		"DELETE /v1/skills/{skill_id}":                         {},
		"POST /v1/skills/{skill_id}/versions":                  {},
		"GET /v1/skills/{skill_id}/versions":                   {},
		"GET /v1/skills/{skill_id}/versions/{version}":         {},
		"GET /v1/skills/{skill_id}/versions/{version}/content": {},
		"DELETE /v1/skills/{skill_id}/versions/{version}":      {},
	}

	for name, router := range map[string]http.Handler{
		"real-skill-handler-branch": httpapi.NewRouter(nil, "", httpapi.WithSkillHandler(httpapi.NewSkillHandler(nil, ""))),
		"stub-skill-handler-branch": httpapi.NewRouter(nil, ""),
	} {
		t.Run(name, func(t *testing.T) {
			assertNoPublicBlobDownloadRoutes(t, router, allowed)
		})
	}
}

func assertNoPublicBlobDownloadRoutes(t *testing.T, router http.Handler, allowed map[string]struct{}) {
	t.Helper()
	routes, ok := router.(chi.Routes)
	if !ok {
		t.Fatalf("router %T does not expose chi routes", router)
	}
	seen := map[string]struct{}{}
	if err := chi.Walk(routes, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if !strings.HasPrefix(route, "/v1/skills") {
			return nil
		}
		key := method + " " + route
		if _, ok := allowed[key]; !ok {
			t.Errorf("registered unexpected Skill route %q", key)
		} else {
			seen[key] = struct{}{}
		}
		for _, segment := range []string{"archive", "download", "blob"} {
			if skillRouteHasSegment(route, segment) {
				t.Errorf("registered public Skill package-byte route %q (forbidden segment %q)", key, segment)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("walk router: %v", err)
	}
	for missing := range allowed {
		if _, ok := seen[missing]; !ok {
			t.Errorf("router did not expose expected Skill route %q; route-family invariant may be stale", missing)
		}
	}
}

func skillRouteHasSegment(path, segment string) bool {
	for _, part := range strings.Split(strings.Trim(path, "/"), "/") {
		if strings.Contains(part, segment) {
			return true
		}
	}
	return false
}
