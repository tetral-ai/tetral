// Package testinfra owns evidence selection and execution for local and CI
// verification. It deliberately has no dependency on Engine service packages.
package testinfra

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed inventory.json
var inventoryJSON []byte

type Inventory struct {
	Version           int      `json:"version"`
	Groups            []Group  `json:"groups"`
	GoTests           []GoTest `json:"go_tests"`
	FullFallbackPaths []string `json:"full_fallback_paths"`
}

// GoTest declares the complete dependency contract for a Go test whose
// composition spans repositories. Its requirements replace source inference.
// Full and affected package selections run it; Fast compiles it without running.
type GoTest struct {
	Package      string   `json:"package"`
	Name         string   `json:"name"`
	Dependencies []string `json:"dependencies"`
}

type Group struct {
	ID           string   `json:"id"`
	Owner        string   `json:"owner"`
	Kind         string   `json:"kind"`
	Profiles     []string `json:"profiles"`
	Dependencies []string `json:"dependencies"`
	Paths        []string `json:"paths"`
}

func LoadInventory() (Inventory, error) {
	var inventory Inventory
	if err := json.Unmarshal(inventoryJSON, &inventory); err != nil {
		return Inventory{}, fmt.Errorf("decode evidence inventory: %w", err)
	}
	if inventory.Version != 1 || len(inventory.Groups) == 0 {
		return Inventory{}, fmt.Errorf("unsupported or empty evidence inventory")
	}
	seen := map[string]bool{}
	for _, group := range inventory.Groups {
		if group.ID == "" || group.Kind == "" || group.Owner == "" || seen[group.ID] {
			return Inventory{}, fmt.Errorf("invalid evidence group %q", group.ID)
		}
		seen[group.ID] = true
	}
	seenTests := map[string]bool{}
	for _, test := range inventory.GoTests {
		key := test.Package + "\x00" + test.Name
		if test.Package == "" || !isGoRunnable(test.Name) || len(test.Dependencies) == 0 || seenTests[key] {
			return Inventory{}, fmt.Errorf("invalid Go test declaration %q in %q", test.Name, test.Package)
		}
		seenTests[key] = true
		for _, dependency := range test.Dependencies {
			switch dependency {
			case "postgresql", "minio", "docker", "bun-workspaces", "sdk":
			default:
				return Inventory{}, fmt.Errorf("go test %q declares unknown dependency %q", test.Name, dependency)
			}
		}
	}
	return inventory, nil
}

func (i Inventory) Group(id string) (Group, bool) {
	for _, group := range i.Groups {
		if group.ID == id {
			return group, true
		}
	}
	return Group{}, false
}

func (i Inventory) GroupsForProfile(profile string) []Group {
	var selected []Group
	for _, group := range i.Groups {
		if contains(group.Profiles, profile) {
			selected = append(selected, group)
		}
	}
	sort.Slice(selected, func(a, b int) bool { return selected[a].ID < selected[b].ID })
	return selected
}

func (i Inventory) MatchPath(path string) []Group {
	path = filepath.ToSlash(path)
	var matched []Group
	for _, group := range i.Groups {
		for _, pattern := range group.Paths {
			if matchPath(pattern, path) {
				matched = append(matched, group)
				break
			}
		}
	}
	return matched
}

func (i Inventory) RequiresFull(path string) bool {
	path = filepath.ToSlash(path)
	for _, pattern := range i.FullFallbackPaths {
		if matchPath(pattern, path) {
			return true
		}
	}
	return false
}

func matchPath(pattern, path string) bool {
	pattern = filepath.ToSlash(pattern)
	return matchPathSegments(strings.Split(pattern, "/"), strings.Split(filepath.ToSlash(path), "/"))
}

func matchPathSegments(pattern, value []string) bool {
	if len(pattern) == 0 {
		return len(value) == 0
	}
	if pattern[0] == "**" {
		return matchPathSegments(pattern[1:], value) || (len(value) > 0 && matchPathSegments(pattern, value[1:]))
	}
	if len(value) == 0 {
		return false
	}
	matched, err := path.Match(pattern[0], value[0])
	return err == nil && matched && matchPathSegments(pattern[1:], value[1:])
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
