package workload

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

// observabilityTablePatterns find every component's observability.json: the
// two libraries, each Go service and command, and each TypeScript service
// package.
var observabilityTablePatterns = []string{
	"internal/workload/observability.json",
	"internal/ts-observability/observability.json",
	"services/*/observability.json",
	"services/*/packages/*/observability.json",
	"cmd/*/observability.json",
}

type managedInventories struct {
	events  map[string]map[string]EventDeclaration
	metrics map[string]map[string]MetricDeclaration
	spans   map[string]map[string]SpanDeclaration
}

func readJSONFile(t *testing.T, path string, target any) {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // G304: repository-relative test fixture path.
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func loadManagedInventories(t *testing.T, root string) managedInventories {
	t.Helper()
	var logging struct {
		ServiceEvents map[string]struct {
			Events map[string]EventDeclaration `json:"events"`
		} `json:"service_events"`
	}
	var metrics struct {
		Services map[string]map[string]MetricDeclaration `json:"services"`
	}
	var spans struct {
		Services map[string]map[string]SpanDeclaration `json:"services"`
	}
	readJSONFile(t, filepath.Join(root, "deploy/managed/logging-inventory.json"), &logging)
	readJSONFile(t, filepath.Join(root, "deploy/managed/metrics-inventory.json"), &metrics)
	readJSONFile(t, filepath.Join(root, "deploy/managed/span-inventory.json"), &spans)
	inventories := managedInventories{events: map[string]map[string]EventDeclaration{}, metrics: metrics.Services, spans: spans.Services}
	for service, entry := range logging.ServiceEvents {
		inventories.events[service] = entry.Events
	}
	return inventories
}

func loadObservabilityTables(t *testing.T, root string) map[string]*Declarations {
	t.Helper()
	tables := map[string]*Declarations{}
	for _, pattern := range observabilityTablePatterns {
		paths, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			data, err := os.ReadFile(path) //nolint:gosec // G304: path comes from a repository glob.
			if err != nil {
				t.Fatal(err)
			}
			table, err := ParseDeclarations(data)
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			if _, seen := tables[table.Service]; seen {
				t.Fatalf("%s declares service %s a second time", path, table.Service)
			}
			tables[table.Service] = table
		}
	}
	return tables
}

// inventoryDifferences lists every name, type, field, label, label value,
// bucket set, template and audience that one side holds and the other does
// not, every event field outside the shared allowlist, every event that
// carries the error tuple without declaring all of it, and every family that
// breaks the metric rules: a duration histogram outside the request and
// long-operation bucket sets, a lone _queued family.
func inventoryDifferences(tables map[string]*Declarations, inventories managedInventories) []string {
	var differences []string
	differ := func(format string, args ...any) { differences = append(differences, fmt.Sprintf(format, args...)) }
	services := map[string]bool{}
	for service := range tables {
		services[service] = true
	}
	for _, side := range []map[string]bool{keysOf(inventories.events), keysOf(inventories.metrics), keysOf(inventories.spans)} {
		for service := range side {
			services[service] = true
		}
	}
	for _, service := range sortedKeys(services) {
		table := tables[service]
		if table == nil {
			differ("%s: inventoried but has no observability.json", service)
			continue
		}
		compareEntries(differ, service, "event", table.Events, inventories.events[service])
		compareEntries(differ, service, "metric family", table.Metrics, inventories.metrics[service])
		compareEntries(differ, service, "span template", table.Spans, inventories.spans[service])
		for _, name := range sortedKeys(keysOf(table.Events)) {
			event := table.Events[name]
			for _, field := range sortedKeys(keysOf(event.Fields)) {
				if !approvedDiagnosticField(field) {
					differ("%s: event %s field %s is not in the shared field allowlist", service, name, field)
				}
			}
			// The writer completes the tuple on an ERROR record and on any
			// record that carries one member of it.
			carriesTuple := event.Level == "ERROR" || len(event.Errors) > 0
			for _, field := range errorTupleFields {
				_, declared := event.Fields[field]
				carriesTuple = carriesTuple || declared
			}
			for _, field := range errorTupleFields {
				if _, declared := event.Fields[field]; carriesTuple && !declared {
					differ("%s: event %s carries the error tuple but does not declare %s", service, name, field)
				}
			}
		}
		for name, metric := range table.Metrics {
			if metric.Type == "histogram" && strings.HasSuffix(name, "_seconds") {
				var named string
				if json.Unmarshal(metric.Buckets, &named) != nil || (named != "request" && named != "long") {
					differ("%s: duration histogram %s uses buckets %s, neither the request nor the long-operation set", service, name, metric.Buckets)
				}
			}
			if point, queued := strings.CutSuffix(name, "_queued"); queued {
				for _, sibling := range []string{point + "_oldest_queued_seconds", point + "_queue_wait_seconds"} {
					if _, ok := table.Metrics[sibling]; !ok {
						differ("%s: %s has no %s", service, name, sibling)
					}
				}
			}
		}
	}
	return differences
}

var errorTupleFields = []string{"error.class", "error.code", "error.message_safe"}

func compareEntries[T any](differ func(string, ...any), service, kind string, table, inventory map[string]T) {
	for _, name := range sortedKeys(keysOf(table)) {
		inventoried, ok := inventory[name]
		if !ok {
			differ("%s: %s %s is declared but not inventoried", service, kind, name)
			continue
		}
		if !reflect.DeepEqual(normalizedJSON(table[name]), normalizedJSON(inventoried)) {
			differ("%s: %s %s differs: table %s, inventory %s", service, kind, name, compactJSON(table[name]), compactJSON(inventoried))
		}
	}
	for _, name := range sortedKeys(keysOf(inventory)) {
		if _, ok := table[name]; !ok {
			differ("%s: %s %s is inventoried but not declared", service, kind, name)
		}
	}
}

func normalizedJSON(value any) any {
	data, _ := json.Marshal(value)
	var normalized any
	_ = json.Unmarshal(data, &normalized)
	return normalized
}

func compactJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func keysOf[T any](values map[string]T) map[string]bool {
	keys := map[string]bool{}
	for key := range values {
		keys[key] = true
	}
	return keys
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestObservationInventory(t *testing.T) {
	root := filepath.Clean("../..")
	tables := loadObservabilityTables(t, root)
	inventories := loadManagedInventories(t, root)
	for _, difference := range inventoryDifferences(tables, inventories) {
		t.Error(difference)
	}
	if len(tables) < 3 {
		t.Fatalf("found %d observability tables; the glob patterns missed the services", len(tables))
	}
}

func TestObservationInventoryRejectsEachKindOfDrift(t *testing.T) {
	root := filepath.Clean("../..")
	clean := func() (map[string]*Declarations, managedInventories) {
		return loadObservabilityTables(t, root), loadManagedInventories(t, root)
	}
	firstEvent := func(table *Declarations) string { return sortedKeys(keysOf(table.Events))[0] }
	for _, test := range []struct {
		name  string
		drift func(map[string]*Declarations, managedInventories)
	}{
		{"event removed from a table", func(tables map[string]*Declarations, _ managedInventories) {
			delete(tables["workload"].Events, firstEvent(tables["workload"]))
		}},
		{"label value added to a metric", func(tables map[string]*Declarations, _ managedInventories) {
			metric := tables["workload"].Metrics["cold_start_seconds"]
			labels := map[string]json.RawMessage{}
			for name, values := range metric.Labels {
				labels[name] = values
			}
			labels["scope"] = json.RawMessage(`["engine","total","cluster"]`)
			metric.Labels = labels
			tables["workload"].Metrics["cold_start_seconds"] = metric
		}},
		{"bucket set changed", func(_ map[string]*Declarations, inventories managedInventories) {
			metric := inventories.metrics["workload"]["tetral_operation_duration_seconds"]
			metric.Buckets = json.RawMessage(`"long"`)
			inventories.metrics["workload"]["tetral_operation_duration_seconds"] = metric
		}},
		{"duration histogram outside the request and long-operation bucket sets", func(tables map[string]*Declarations, inventories managedInventories) {
			drifted := MetricDeclaration{Type: "histogram", Audience: "operators", Labels: map[string]json.RawMessage{}, Buckets: json.RawMessage(`[0.001,0.01,1]`)}
			tables["workload"].Metrics["drifted_duration_seconds"] = drifted
			inventories.metrics["workload"]["drifted_duration_seconds"] = drifted
		}},
		{"event field outside the shared allowlist", func(tables map[string]*Declarations, inventories managedInventories) {
			name := firstEvent(tables["workload"])
			event := tables["workload"].Events[name]
			fields := map[string]string{"drifted.field": "string"}
			for field, kind := range event.Fields {
				fields[field] = kind
			}
			event.Fields = fields
			tables["workload"].Events[name] = event
			inventories.events["workload"][name] = event
		}},
		{"error code declared without the error fields", func(tables map[string]*Declarations, inventories managedInventories) {
			event := EventDeclaration{Level: "INFO", LogType: "app", Audience: "operators", Fields: map[string]string{"operation": "string"}, Errors: map[string]ErrorDeclaration{"drifted": {Class: "drifted", MessageSafe: "drifted"}}}
			tables["workload"].Events["drifted.event"] = event
			inventories.events["workload"]["drifted.event"] = event
		}},
		{"queued family without its siblings", func(tables map[string]*Declarations, inventories managedInventories) {
			queued := MetricDeclaration{Type: "gauge", Audience: "operators", Labels: map[string]json.RawMessage{}}
			tables["workload"].Metrics["drifted_queued"] = queued
			inventories.metrics["workload"]["drifted_queued"] = queued
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tables, inventories := clean()
			if differences := inventoryDifferences(tables, inventories); len(differences) != 0 {
				t.Fatalf("clean repository already differs: %v", differences)
			}
			test.drift(tables, inventories)
			if differences := inventoryDifferences(tables, inventories); len(differences) == 0 {
				t.Fatal("the drift went unnoticed")
			}
		})
	}
}

func TestLibraryBucketSetsAreTheRequestAndLongOperationSets(t *testing.T) {
	if !slices.Equal(RequestDurationBuckets, []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}) ||
		!slices.Equal(LongOperationDurationBuckets, []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800}) {
		t.Fatalf("bucket sets drifted: %v %v", RequestDurationBuckets, LongOperationDurationBuckets)
	}
}
