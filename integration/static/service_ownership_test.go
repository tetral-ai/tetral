package static

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const serviceModule = "github.com/tetral-ai/tetral/"

// Production dependencies must preserve the independently composed owners.
// Follow indirect imports too: routing an upward dependency through another
// internal package must not hide it. Generated protocol packages are permitted
// at service boundaries and do not confer ownership of a service's behavior.
func TestSeparatedServiceDependencyDirection(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	graph := map[string][]string{}
	for _, directory := range []string{"internal", "services"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				switch entry.Name() {
				case "node_modules", "vendor", "testdata", "dist":
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, filepath.Dir(path))
			if err != nil {
				return err
			}
			owner := serviceModule + filepath.ToSlash(relative)
			if _, exists := graph[owner]; !exists {
				graph[owner] = nil
			}
			for _, spec := range file.Imports {
				dependency, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return err
				}
				graph[owner] = append(graph[owner], dependency)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("inspect production imports under %s: %v", directory, err)
		}
	}
	for _, owner := range []string{"services/bridge", "services/job-runner", "internal/runtimeconfig", "internal/mcpmanifest", "internal/runtimecontrol"} {
		if _, exists := graph[serviceModule+owner]; !exists {
			t.Errorf("required separated owner %s has no production Go package", owner)
		}
	}
	for _, violation := range serviceDependencyViolations(graph) {
		t.Error(violation)
	}
}

func serviceDependencyViolations(graph map[string][]string) []string {
	within := func(path, owner string) bool { return path == owner || strings.HasPrefix(path, owner+"/") }
	isProtocol := func(path string) bool {
		parts := strings.Split(strings.TrimPrefix(path, serviceModule), "/")
		return strings.HasPrefix(path, serviceModule) && len(parts) >= 3 && parts[0] == "services" && parts[2] == "gen"
	}
	var violations []string
	var origins []string
	for origin := range graph {
		origins = append(origins, origin)
	}
	sort.Strings(origins)
	for _, origin := range origins {
		if isProtocol(origin) {
			continue
		}
		shared := within(origin, serviceModule+"internal/runtimeconfig") || within(origin, serviceModule+"internal/mcpmanifest") || within(origin, serviceModule+"internal/runtimecontrol")
		other := ""
		switch {
		case within(origin, serviceModule+"services/bridge"):
			other = serviceModule + "services/job-runner"
		case within(origin, serviceModule+"services/job-runner"):
			other = serviceModule + "services/bridge"
		}
		if !shared && other == "" {
			continue
		}
		seen := map[string]bool{origin: true}
		var visit func(string, []string)
		visit = func(current string, route []string) {
			for _, dependency := range graph[current] {
				if seen[dependency] {
					continue
				}
				seen[dependency] = true
				next := append(append([]string(nil), route...), dependency)
				if !isProtocol(dependency) && ((shared && strings.HasPrefix(dependency, serviceModule+"services/")) || (other != "" && within(dependency, other))) {
					violations = append(violations, fmt.Sprintf("forbidden service dependency: %s", strings.Join(next, " -> ")))
					continue
				}
				visit(dependency, next)
			}
		}
		visit(origin, []string{origin})
	}
	return violations
}

func TestSeparatedServiceDependencyDirectionRejectsForbiddenGraphs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		graph map[string][]string
		bad   bool
	}{
		{"bridge to runner", map[string][]string{"services/bridge": {"services/job-runner"}}, true},
		{"runner to bridge subpackage", map[string][]string{"services/job-runner/cmd/job-runner": {"services/bridge/store"}}, true},
		{"indirect service dependency", map[string][]string{"services/bridge": {"internal/adapter"}, "internal/adapter": {"services/job-runner"}}, true},
		{"shared upward dependency", map[string][]string{"internal/runtimeconfig": {"services/bridge"}}, true},
		{"shared indirect upward dependency", map[string][]string{"internal/mcpmanifest": {"internal/adapter"}, "internal/adapter": {"services/job-runner"}}, true},
		{"shared to other service", map[string][]string{"internal/runtimecontrol": {"services/sandbox"}}, true},
		{"generated protocol boundary", map[string][]string{"services/job-runner": {"services/bridge/gen/tetral/bridge/v1"}, "internal/mcpmanifest": {"services/gateway/gen/tetral/gateway/v1"}}, false},
		{"protocol cannot hide business dependency", map[string][]string{"services/job-runner": {"services/bridge/gen/tetral/bridge/v1"}, "services/bridge/gen/tetral/bridge/v1": {"services/bridge"}}, true},
		{"protocol cannot hide shared upward dependency", map[string][]string{"internal/mcpmanifest": {"services/gateway/gen/tetral/gateway/v1"}, "services/gateway/gen/tetral/gateway/v1": {"services/gateway"}}, true},
		{"lower level shared rules", map[string][]string{"services/bridge": {"internal/runtimecontrol"}, "services/job-runner": {"internal/runtimecontrol"}, "internal/runtimecontrol": {"internal/storage"}}, false},
		{"exact package segments", map[string][]string{"services/bridge": {"services/job-runner-metrics"}, "internal/runtimeconfig-extra": {"services/bridge"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			graph := map[string][]string{}
			for owner, dependencies := range tc.graph {
				for _, dependency := range dependencies {
					graph[serviceModule+owner] = append(graph[serviceModule+owner], serviceModule+dependency)
				}
			}
			if got := serviceDependencyViolations(graph); (len(got) > 0) != tc.bad {
				t.Fatalf("violations = %v, want forbidden=%v", got, tc.bad)
			}
		})
	}
}
