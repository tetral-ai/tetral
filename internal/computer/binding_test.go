package computer

import (
	"reflect"
	"testing"
)

func TestPreparationOrdersRepositoryAncestorsBeforeDescendants(t *testing.T) {
	for _, test := range []struct {
		name        string
		input, want []string
	}{
		{"inner first", []string{"/src/app/lib", "/src/app"}, []string{"/src/app", "/src/app/lib"}},
		{"available entries retain creation order", []string{"/src/app/lib", "/other", "/src/app"}, []string{"/other", "/src/app", "/src/app/lib"}},
		{"newly available child precedes later sibling", []string{"/src/app", "/src/app/lib", "/other"}, []string{"/src/app", "/src/app/lib", "/other"}},
		{"path components", []string{"/src/application", "/src/app/child", "/src/app/"}, []string{"/src/application", "/src/app/", "/src/app/child"}},
		{"three levels", []string{"/a/b/c", "/a/b", "/a"}, []string{"/a", "/a/b", "/a/b/c"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repositories := make([]Repository, len(test.input))
			for i, path := range test.input {
				repositories[i] = Repository{URL: "https://github.com/acme/repo", Path: path}
			}
			payload := preparePayload("sesn_test", "bind_test", KindCloud, "/workspace", nil, repositories, nil)
			got := make([]string, len(payload.Repositories))
			for i, repository := range payload.Repositories {
				got[i] = repository.Path
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("preparation order = %v; want %v", got, test.want)
			}
			for i, repository := range repositories {
				if repository.Path != test.input[i] {
					t.Fatalf("preparation reordered stored entries")
				}
			}
		})
	}
}
