package testinfra

import (
	"bytes"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func decodeOnePackage(body []byte, destination *listedPackage) error {
	return json.NewDecoder(bytes.NewReader(body)).Decode(destination)
}

type listedPackage struct {
	ImportPath   string
	Dir          string
	GoFiles      []string
	TestGoFiles  []string
	XTestGoFiles []string
}

type testFunction struct {
	name           string
	calls          []string
	infrastructure bool
	capability     string
	reason         string
}

func fastGoSelections(root string) ([]Selection, []Exclusion, error) {
	packages, err := listGoPackages(root)
	if err != nil {
		return nil, nil, err
	}
	var selections []Selection
	var exclusions []Exclusion
	for _, pkg := range packages {
		tests, excluded, err := noInfrastructureTests(pkg)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range excluded {
			item.Group = "go"
			item.Package = pkg.ImportPath
			exclusions = append(exclusions, item)
		}
		selections = append(selections, Selection{
			Group:    "go",
			Reason:   "fast no-infrastructure tests",
			Packages: []string{pkg.ImportPath},
			Tests:    tests,
			Mode:     "ordinary",
		})
	}
	sort.Slice(selections, func(a, b int) bool { return selections[a].Packages[0] < selections[b].Packages[0] })
	sort.Slice(exclusions, func(a, b int) bool {
		if exclusions[a].Package != exclusions[b].Package {
			return exclusions[a].Package < exclusions[b].Package
		}
		return exclusions[a].Runnable < exclusions[b].Runnable
	})
	return selections, exclusions, nil
}

func fullGoSelections(root, reason string) ([]Selection, []Exclusion, error) {
	packages, err := listGoPackages(root)
	if err != nil {
		return nil, nil, err
	}
	selections := make([]Selection, 0, len(packages))
	var exclusions []Exclusion
	for _, pkg := range packages {
		runnables, err := allGoRunnables(pkg)
		if err != nil {
			return nil, nil, err
		}
		_, classified, err := noInfrastructureTests(pkg)
		if err != nil {
			return nil, nil, err
		}
		dependencies := dependenciesForCapabilities(classified)
		excluded := map[string]bool{}
		for _, item := range classified {
			switch item.Capability {
			case "live-external-service":
				item.Disposition = "not-applicable"
			case "root-linux":
				item.Disposition = "delegated"
				item.Reason = "executed by the sandbox-image root proof"
			default:
				continue
			}
			item.Group = "go"
			item.Package = pkg.ImportPath
			exclusions = append(exclusions, item)
			excluded[item.Runnable] = true
		}
		selected := runnables[:0]
		for _, runnable := range runnables {
			if !excluded[runnable] {
				selected = append(selected, runnable)
			}
		}
		selections = append(selections, Selection{
			Group: "go", Reason: reason, Packages: []string{pkg.ImportPath}, Tests: selected, Mode: "race",
			Dependencies: dependencies,
		})
	}
	sort.Slice(exclusions, func(a, b int) bool {
		if exclusions[a].Package != exclusions[b].Package {
			return exclusions[a].Package < exclusions[b].Package
		}
		return exclusions[a].Runnable < exclusions[b].Runnable
	})
	return selections, exclusions, nil
}

func goPackageDependencies(pkg listedPackage) ([]string, error) {
	_, exclusions, err := noInfrastructureTests(pkg)
	if err != nil {
		return nil, err
	}
	return dependenciesForCapabilities(exclusions), nil
}

func dependenciesForCapabilities(exclusions []Exclusion) []string {
	set := map[string]bool{}
	for _, exclusion := range exclusions {
		switch exclusion.Capability {
		case "postgresql", "minio", "docker", "bun-workspaces":
			set[exclusion.Capability] = true
		case "external-sdk-checkout":
			set["sdk"] = true
		}
	}
	dependencies := make([]string, 0, len(set))
	for dependency := range set {
		dependencies = append(dependencies, dependency)
	}
	sort.Strings(dependencies)
	return dependencies
}

func listGoPackages(root string) ([]listedPackage, error) {
	command := exec.Command("go", "list", "-json", "./...")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	var packages []listedPackage
	for {
		var pkg listedPackage
		if err := decoder.Decode(&pkg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		packages = append(packages, pkg)
	}
	sort.Slice(packages, func(a, b int) bool { return packages[a].ImportPath < packages[b].ImportPath })
	return packages, nil
}

func allGoRunnables(pkg listedPackage) ([]string, error) {
	files := append(append([]string{}, pkg.TestGoFiles...), pkg.XTestGoFiles...)
	set := map[string]bool{}
	for _, name := range files {
		path := filepath.Join(pkg.Dir, name)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return nil, err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Recv == nil && isGoRunnable(function.Name.Name) {
				set[function.Name.Name] = true
			}
		}
	}
	result := make([]string, 0, len(set))
	for name := range set {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func noInfrastructureTests(pkg listedPackage) ([]string, []Exclusion, error) {
	files := append(append(append([]string{}, pkg.GoFiles...), pkg.TestGoFiles...), pkg.XTestGoFiles...)
	functions := map[string]*testFunction{}
	for _, name := range files {
		path := filepath.Join(pkg.Dir, name)
		// Paths come from go list for packages inside the repository.
		//nolint:gosec
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, body, 0)
		if err != nil {
			return nil, nil, err
		}
		infrastructureImports := map[string]string{}
		commandImports := map[string]bool{}
		for _, spec := range file.Imports {
			importPath, _ := strconv.Unquote(spec.Path.Value)
			if importPath == "github.com/tetral-ai/tetral/internal/storage/storagetest" {
				name := "storagetest"
				if spec.Name != nil {
					name = spec.Name.Name
				}
				infrastructureImports[name] = "postgresql"
			}
			if importPath == "os/exec" {
				name := "exec"
				if spec.Name != nil {
					name = spec.Name.Name
				}
				commandImports[name] = true
			}
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			item := &testFunction{name: function.Name.Name}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if identifier, ok := node.(*ast.Ident); ok {
					item.calls = append(item.calls, identifier.Name)
					if identifier.Name == "EnvTestDatabaseURL" {
						item.infrastructure = true
						item.capability = "postgresql"
						item.reason = "uses the PostgreSQL test database contract"
					}
				}
				literal, ok := node.(*ast.BasicLit)
				if ok && literal.Kind == token.STRING {
					value, _ := strconv.Unquote(literal.Value)
					markExternalCapability(item, value)
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
					if identifier, ok := selector.X.(*ast.Ident); ok {
						if capability := infrastructureImports[identifier.Name]; capability != "" {
							item.infrastructure = true
							item.capability = capability
							item.reason = "calls the repository PostgreSQL test helper"
						}
						if commandImports[identifier.Name] && (selector.Sel.Name == "Command" || selector.Sel.Name == "CommandContext") {
							argument := 0
							if selector.Sel.Name == "CommandContext" {
								argument = 1
							}
							if argument < len(call.Args) {
								if literal, ok := call.Args[argument].(*ast.BasicLit); ok && literal.Kind == token.STRING {
									value, _ := strconv.Unquote(literal.Value)
									if value == "bun" {
										item.infrastructure = true
										item.capability = "bun-workspaces"
										item.reason = "executes a repository Bun composition fixture"
									}
								}
							}
						}
					}
					switch selector.Sel.Name {
					case "Getenv", "LookupEnv":
						for _, argument := range call.Args {
							if literal, ok := argument.(*ast.BasicLit); ok && literal.Kind == token.STRING {
								value, _ := strconv.Unquote(literal.Value)
								markExternalCapability(item, value)
							}
						}
					case "Skip", "Skipf", "SkipNow":
						// A conditional native skip is not itself an exclusion. Fast
						// rejects it unless the same function declares a known external
						// capability through its durable environment contract.
					}
				}
				return true
			})
			functions[item.name] = item
		}
	}
	changed := true
	for changed {
		changed = false
		for _, function := range functions {
			if function.infrastructure {
				continue
			}
			for _, call := range function.calls {
				if call == "startDependenciesWith" || call == "markExternalCapability" {
					continue
				}
				if called := functions[call]; called != nil && called.infrastructure {
					function.infrastructure = true
					function.capability = called.capability
					function.reason = "calls " + called.name + ", which " + defaultReason(called.reason)
					changed = true
					break
				}
			}
		}
	}
	var tests []string
	var excluded []Exclusion
	for name, function := range functions {
		if !isGoRunnable(name) {
			continue
		}
		if function.infrastructure {
			excluded = append(excluded, Exclusion{Runnable: name, Capability: defaultCapability(function.capability), Disposition: "not-applicable", Reason: defaultReason(function.reason)})
		} else {
			tests = append(tests, name)
		}
	}
	sort.Strings(tests)
	sort.Slice(excluded, func(a, b int) bool { return excluded[a].Runnable < excluded[b].Runnable })
	return tests, excluded, nil
}

func markExternalCapability(function *testFunction, value string) {
	capability := ""
	switch {
	case strings.Contains(value, "TETRAL_TEST_DATABASE_URL"):
		capability = "postgresql"
	case strings.Contains(value, "TETRAL_TEST_MINIO_"):
		capability = "minio"
	case strings.Contains(value, "TETRAL_TEST_DOCKER_AVAILABLE"):
		capability = "docker"
	case strings.Contains(value, "TETRAL_ENGINE_SDK_ROOT"):
		capability = "external-sdk-checkout"
	case strings.Contains(value, "TETRAL_RUN_GO_BUN_GRPC_INTEROP"):
		capability = "cross-language-integration"
	case strings.Contains(strings.ToLower(value), "live daytona"), strings.Contains(strings.ToLower(value), "live r2"), strings.Contains(value, "_LIVE"):
		capability = "live-external-service"
	case strings.Contains(strings.ToLower(value), "root ci lane"), strings.Contains(strings.ToLower(value), "production root helper"):
		capability = "root-linux"
	}
	if capability != "" {
		function.infrastructure = true
		function.capability = capability
		function.reason = "requires " + capability
	}
}

func isGoRunnable(name string) bool {
	return strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Example") || strings.HasPrefix(name, "Fuzz")
}

func defaultCapability(value string) string {
	if value == "" {
		return "conditional-native-capability"
	}
	return value
}

func defaultReason(value string) string {
	if value == "" {
		return "contains a conditional native skip without a repository capability declaration"
	}
	return value
}
