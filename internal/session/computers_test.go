package session

import (
	"testing"
	"time"
)

func TestRepositoryDestinationsPreserveIntentionalLayouts(t *testing.T) {
	for _, paths := range [][2]string{{"/src/app", "/src/app/deps/lib"}, {"/", "///"}, {"/src/a/../app", "/src/app"}} {
		t.Run(paths[0]+" and "+paths[1], func(t *testing.T) {
			service := newTestService(newRecordingSessionStore(), &recordingFileIdentities{}, &recordingVaultValidator{}, time.Now())
			requests := []ResourceRequest{
				{Type: "github_repository", GitHubURL: "https://github.com/acme/app", AuthorizationToken: "test-token", MountPath: &paths[0]},
				{Type: "github_repository", GitHubURL: "https://github.com/acme/lib", AuthorizationToken: "test-token", MountPath: &paths[1]},
			}
			for range 2 { // Independent computers may use the same destinations.
				prepared, err := service.prepareRepositories("/workspace", requests)
				if err != nil {
					t.Fatalf("prepare repositories: %v", err)
				}
				for i, resource := range prepared {
					if resource.GitHubRepository.MountPath != paths[i] {
						t.Fatalf("stored destination changed: %q; want %q", resource.GitHubRepository.MountPath, paths[i])
					}
				}
			}
		})
	}
}
