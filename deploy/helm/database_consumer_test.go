package helm_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Discover production Go service owners from their actual protected constructor
// references, independently of the chart's arithmetic and its DSN naming.
func protectedGoServiceOwners(t *testing.T) map[string][]string {
	t.Helper()
	root := engineRoot(t)
	owners := map[string][]string{}
	err := filepath.WalkDir(filepath.Join(root, "services"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "node_modules" || entry.Name() == "gen" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		aliases := map[string]bool{}
		for _, imp := range file.Imports {
			if imp.Path.Value == `"github.com/tetral-ai/tetral/internal/dbconnect"` {
				name := "dbconnect"
				if imp.Name != nil {
					name = imp.Name.Name
				}
				aliases[name] = true
			}
		}
		owns := false
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			alias, ok := selector.X.(*ast.Ident)
			if ok && aliases[alias.Name] && strings.HasPrefix(selector.Sel.Name, "OpenProtected") {
				owns = true
			}
			return true
		})
		if owns {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			role := strings.Split(filepath.ToSlash(rel), "/")[1]
			owners[role] = append(owners[role], filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return owners
}

func databaseConsumerTemplate(t *testing.T, object map[string]any) map[string]any {
	t.Helper()
	switch object["kind"] {
	case "Deployment":
		return transportMap(t, transportAt(t, object, "spec", "template"))
	case "CronJob":
		if transportAt(t, object, "spec", "concurrencyPolicy") != "Forbid" {
			t.Fatal("counted Cleanup job permits overlapping pools")
		}
		return transportMap(t, transportAt(t, object, "spec", "jobTemplate", "spec", "template"))
	default:
		return nil
	}
}

func TestProtectedDatabaseConsumerCensus(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy/helm/tetral")
	goOwners := protectedGoServiceOwners(t)
	wantGo := []string{"api", "auth", "bridge", "cleanup", "event-stream", "git-proxy", "job-runner", "queue", "sandbox"}
	gotGo := []string{}
	for role := range goOwners {
		gotGo = append(gotGo, role)
	}
	sort.Strings(gotGo)
	if !reflect.DeepEqual(gotGo, wantGo) {
		t.Fatalf("production protected Go service census=%v want%v; sources=%v", gotGo, wantGo, goOwners)
	}
	// Bun consumers use the same operation-owned native SQL constructor exercised
	// by the real protected PostgreSQL store fixture.
	bunOwners := []string{"provider-gateway", "mcp-connector"}
	for _, role := range bunOwners {
		source, err := os.ReadFile(filepath.Join(engineRoot(t), "services/gateway/packages", role, "src/command.ts"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(source), "await openPostgresSQLOwner(") {
			t.Fatalf("%s production command lost its SQL owner", role)
		}
	}
	for _, profile := range []string{"standard-routed", "hardened"} {
		t.Run(profile, func(t *testing.T) {
			objects := uniqueObjects(t, renderChart(t, helm, chart, "transport.profile="+profile, "transport.goPoolMax=7", "transport.bunPoolMax=3"))
			seen := map[string]bool{}
			deploymentRoles := map[string]int{}
			jobSlots := 0
			for _, object := range objects {
				template := databaseConsumerTemplate(t, object)
				if template == nil {
					continue
				}
				role := fmt.Sprint(transportAt(t, object, "metadata", "name"))
				for _, value := range transportList(t, transportAt(t, template, "spec", "containers")) {
					container := transportMap(t, value)
					env := map[string]string{}
					dsns := 0
					entries, _ := container["env"].([]any)
					for _, value := range entries {
						setting := transportMap(t, value)
						name := fmt.Sprint(setting["name"])
						if name == "TETRAL_DATABASE_URL" || name == "TETRAL_POSTGRES_DSN" || name == "TETRAL_EVENT_STREAM_DATABASE_URL" {
							dsns++
						}
						if literal, ok := setting["value"]; ok {
							env[name] = fmt.Sprint(literal)
						}
					}
					if dsns == 0 {
						continue
					}
					if dsns != 1 || seen[role] {
						t.Fatalf("%s ambiguous process/DSN ownership", role)
					}
					seen[role] = true
					generations, poolKey, poolMax := 1, "TETRAL_DB_MAX_OPEN_CONNS", "7"
					if _, ok := goOwners[role]; !ok {
						if role != "provider-gateway" && role != "mcp-connector" {
							t.Fatalf("uncensused native SQL consumer %s", role)
						}
						generations, poolKey, poolMax = 2, "TETRAL_DATABASE_POOL_MAX", "3"
					}
					if env["TETRAL_DATABASE_TLS_CA_PATH"] != "/var/run/tetral/store-trust/database-ca.crt" || env["TETRAL_DATABASE_TLS_SERVER_NAME"] != "tetral-postgres.tetral-system.svc.cluster.local" || env[poolKey] != poolMax {
						t.Fatalf("%s actual constructor trust/pool projection differs: %v", role, env)
					}
					if object["kind"] == "CronJob" {
						pool, err := strconv.Atoi(poolMax)
						if err != nil {
							t.Fatal(err)
						}
						jobSlots += pool * generations
					} else {
						deploymentRoles[role] = generations
					}
					t.Logf("protected SQL consumer=%s DSNs=%d pool=%s generations=%d", role, dsns, poolMax, generations)
				}
			}
			if len(seen) != len(goOwners)+len(bunOwners) {
				t.Fatalf("rendered protected consumers=%v; production Go=%v Bun=%v", seen, goOwners, bunOwners)
			}
			total, err := renderedPoolSlots(objects, deploymentRoles)
			if err != nil {
				t.Fatal(err)
			}
			total += jobSlots
			ledger := transportMap(t, transportAt(t, objects["v1|ConfigMap|tetral-system|tetral-database-connection-budget"], "data"))
			if fmt.Sprint(ledger["maximumOwnedPoolSlots"]) != strconv.Itoa(total) || total != 260 {
				t.Fatalf("independent eleven-consumer total=%d differs from ledger %v", total, ledger)
			}
		})
	}
}
