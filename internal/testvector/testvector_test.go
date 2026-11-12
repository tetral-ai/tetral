package testvector

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Each producer's manifest gives every vector in these folders exactly one
// pinning test, and names no vector outside them or missing from them.
func TestPinnedVectorFoldersMatchProducerManifests(t *testing.T) {
	root := RepositoryRoot(t)
	producers := []string{"services/bridge", "integration", "internal/runtimecontrol"}
	folders := []string{
		"services/agent-runtime/packages/protocol/testdata/bridge-context",
		"services/agent-runtime/packages/protocol/testdata/mcp-manifest-commands",
		"services/agent-runtime/packages/protocol/testdata/runtime-hot-context",
		"testdata/task-notifications",
	}
	declared := map[string]string{}
	for _, producer := range producers {
		rows, err := ReadManifest(filepath.Join(root, producer, ManifestPath))
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if other, ok := declared[row.Path]; ok {
				t.Errorf("%s is declared by %s and %s", row.Path, other, producer)
			}
			declared[row.Path] = producer
			if !slices.Contains(folders, filepath.Dir(row.Path)) {
				t.Errorf("%s declares %s outside the vector folders", producer, row.Path)
			}
		}
	}
	for _, folder := range folders {
		entries, err := os.ReadDir(filepath.Join(root, folder))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			path := folder + "/" + entry.Name()
			if !strings.HasSuffix(path, ".json") {
				continue
			}
			if _, ok := declared[path]; !ok {
				t.Errorf("no producer declares %s; delete it or pin it again", path)
			}
			delete(declared, path)
		}
	}
	for path, producer := range declared {
		t.Errorf("%s declares %s, which does not exist", producer, path)
	}
}

func TestSelectsWholeMatchesGoTestFilters(t *testing.T) {
	cases := []struct {
		run, skip, test string
		want            bool
	}{
		{"", "", "TestA", true},
		{"^(?:TestColdContext|TestRetention)$", "", "TestRetention", true},
		{"^(?:TestColdContext|TestRetention)$", "", "TestRetentionCold", false},
		{"Cold", "", "TestRetentionCold", true},
		{"TestA/case", "", "TestA", false},
		{"TestB|TestA", "", "TestA", true},
		{"TestB|TestA/case", "", "TestA", false},
		{"Test[/]A", "", "Test/A", true},
		{"a^", "", "TestA", false},
		{"", "TestA", "TestA", false},
		{"", "TestA/case", "TestA", false},
		{"", "TestB", "TestA", true},
	}
	for _, testCase := range cases {
		if got := SelectsWhole(testCase.run, testCase.skip, testCase.test); got != testCase.want {
			t.Errorf("SelectsWhole(%q, %q, %q) = %v; want %v", testCase.run, testCase.skip, testCase.test, got, testCase.want)
		}
	}
}
