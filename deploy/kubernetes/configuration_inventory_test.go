package kubernetesmanifest_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// The configuration inventory documents every environment key an operator can
// meet; each key a deployed container is given must be one of them.
func TestConfigurationInventoryListsEveryEnvironmentKeyAManifestSets(t *testing.T) {
	repository := filepath.Join("..", "..")
	documented := inventoryEnvironmentKeys(t, filepath.Join(repository, "deploy/managed/configuration-inventory.json"))
	var paths []string
	for _, pattern := range []string{"deploy/kubernetes/*.yaml", "deploy/kubernetes/*/*.yaml", "deploy/kubernetes/*/*/*.yaml", "services/*/k8s/*.yaml", "services/*/k8s/*/*.yaml"} {
		matches, err := filepath.Glob(filepath.Join(repository, pattern))
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, matches...)
	}
	set := map[string][]string{}
	for _, path := range paths {
		for _, document := range decodeDocuments(t, path) {
			for _, key := range containerEnvironmentKeys(document) {
				set[key] = append(set[key], path)
			}
		}
	}
	if len(set) == 0 {
		t.Fatal("no manifest sets an environment key; the file patterns missed the manifests")
	}
	var undocumented []string
	for key := range set {
		if !documented[key] {
			undocumented = append(undocumented, key)
		}
	}
	sort.Strings(undocumented)
	for _, key := range undocumented {
		t.Errorf("%s is set by %v and listed by no configuration inventory family", key, set[key])
	}
}

func inventoryEnvironmentKeys(t *testing.T, path string) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // The repository's inventory.
	if err != nil {
		t.Fatal(err)
	}
	var inventory struct {
		Families []struct {
			EnvironmentKeys        []string `json:"environment_keys"`
			DerivedEnvironmentKeys []string `json:"derived_environment_keys"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for _, family := range inventory.Families {
		for _, key := range append(family.EnvironmentKeys, family.DerivedEnvironmentKeys...) {
			keys[key] = true
		}
	}
	return keys
}

// containerEnvironmentKeys are the env names of every container and init
// container of every pod template in node.
func containerEnvironmentKeys(node any) []string {
	var keys []string
	switch node := node.(type) {
	case map[string]any:
		for _, field := range []string{"containers", "initContainers"} {
			containers, _ := node[field].([]any)
			for _, container := range containers {
				container, _ := container.(map[string]any)
				env, _ := container["env"].([]any)
				for _, variable := range env {
					if variable, ok := variable.(map[string]any); ok {
						if name, ok := variable["name"].(string); ok {
							keys = append(keys, name)
						}
					}
				}
			}
		}
		for _, child := range node {
			keys = append(keys, containerEnvironmentKeys(child)...)
		}
	case []any:
		for _, child := range node {
			keys = append(keys, containerEnvironmentKeys(child)...)
		}
	}
	return keys
}
