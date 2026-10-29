package helm_test

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type observedWorkload struct {
	namespace  string
	labels     map[string]string
	containers []map[string]any
	// metricsPort is zero for a workload that serves no metrics.
	metricsPort int
}

// Every workload serves /metrics on the listener its readiness probe uses, so
// the probe names the metrics port independently of the chart's own lists.
func observedWorkloads(t *testing.T, objects map[string]map[string]any) map[string]observedWorkload {
	t.Helper()
	result := map[string]observedWorkload{}
	for _, object := range objects {
		var template any
		switch object["kind"] {
		case "Deployment":
			template = configurationAt(object, "spec", "template")
		case "CronJob":
			template = configurationAt(object, "spec", "jobTemplate", "spec", "template")
		default:
			continue
		}
		name := fmt.Sprint(configurationAt(object, "metadata", "name"))
		workload := observedWorkload{namespace: fmt.Sprint(configurationAt(object, "metadata", "namespace")), labels: map[string]string{}}
		for key, value := range configurationMap(configurationAt(template, "metadata", "labels")) {
			workload.labels[key] = fmt.Sprint(value)
		}
		for _, raw := range configurationList(configurationAt(template, "spec", "containers")) {
			container := configurationMap(raw)
			workload.containers = append(workload.containers, container)
			probe := configurationAt(container, "readinessProbe", "httpGet", "port")
			if probe == nil {
				continue
			}
			if workload.metricsPort = workload.containerPort(probe); workload.metricsPort == 0 {
				t.Fatalf("%s readiness probe port %v names no container port", name, probe)
			}
		}
		if _, duplicate := result[name]; duplicate {
			t.Fatalf("duplicate workload %s", name)
		}
		result[name] = workload
	}
	if _, ok := result["cleanup"]; !ok || len(result) < 2 {
		t.Fatalf("render is missing workloads: %v", result)
	}
	return result
}

// containerPort resolves a port reference, by name or number, to the number a
// container declares; it returns zero when no container declares it.
func (w observedWorkload) containerPort(reference any) int {
	for _, container := range w.containers {
		for _, raw := range configurationList(container["ports"]) {
			port := configurationMap(raw)
			if number, ok := port["containerPort"].(int); ok && (port["name"] == reference || number == reference) {
				return number
			}
		}
	}
	return 0
}

func (w observedWorkload) selectedBy(selector map[string]any) bool {
	for key, value := range selector {
		if w.labels[key] != fmt.Sprint(value) {
			return false
		}
	}
	return len(selector) > 0
}

// takePolicyRules removes the rules of one direction whose peers and single
// TCP port equal the given ones, and reports how many it removed.
func takePolicyRules(policy map[string]any, direction, peerKey string, peers []any, port int) int {
	spec := configurationMap(policy["spec"])
	var kept []any
	taken := 0
	for _, raw := range configurationList(spec[direction]) {
		rule := configurationMap(raw)
		if reflect.DeepEqual(rule[peerKey], peers) && reflect.DeepEqual(rule["ports"], []any{map[string]any{"protocol": "TCP", "port": port}}) {
			taken++
			continue
		}
		kept = append(kept, raw)
	}
	if taken > 0 {
		spec[direction] = kept
	}
	return taken
}

func TestHelmObservabilitySettingsReachEveryWorkload(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	peer := func(namespace, name string) []any {
		return []any{map[string]any{
			"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": namespace}},
			"podSelector":       map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/name": name}},
		}}
	}
	collector, scraper := peer("observability", "otel-collector"), peer("monitoring", "prometheus")
	const endpoint, collectorPort = "http://otel-collector.observability:4317", 14317
	defaults := uniqueObjects(t, renderChart(t, helm, chart))
	scrapePeer := []string{
		`observability.metricsScrapePeers[0].namespaceSelector.matchLabels.kubernetes\.io/metadata\.name=monitoring`,
		`observability.metricsScrapePeers[0].podSelector.matchLabels.app\.kubernetes\.io/name=prometheus`,
	}
	for _, tc := range []struct {
		name, endpoint string
		// ratio and slowQuery, when a row sets them, are the values every
		// workload must receive.
		ratio, slowQuery string
		scraped          bool
		values           []string
	}{
		{name: "defaults"},
		{name: "lower bounds", ratio: "0", slowQuery: "1", values: []string{"observability.traceSampleRatio=0", "observability.slowQueryMs=1"}},
		{name: "scrape peers only", scraped: true, values: scrapePeer},
		{name: "exports", endpoint: endpoint, ratio: "0.25", slowQuery: "60001", scraped: true, values: []string{
			"observability.otlpEndpoint=" + endpoint,
			`observability.otlpPeers[0].namespaceSelector.matchLabels.kubernetes\.io/metadata\.name=observability`,
			`observability.otlpPeers[0].podSelector.matchLabels.app\.kubernetes\.io/name=otel-collector`,
			fmt.Sprintf("observability.otlpPort=%d", collectorPort),
			`observability.metricsScrapePeers[0].namespaceSelector.matchLabels.kubernetes\.io/metadata\.name=monitoring`,
			`observability.metricsScrapePeers[0].podSelector.matchLabels.app\.kubernetes\.io/name=prometheus`,
			"observability.traceSampleRatio=0.25",
			"observability.slowQueryMs=60001",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := uniqueObjects(t, renderChart(t, helm, chart, tc.values...))
			exports := tc.endpoint != ""
			for name, workload := range observedWorkloads(t, objects) {
				for _, container := range workload.containers {
					env := map[string]any{}
					for _, raw := range configurationList(container["env"]) {
						item := configurationMap(raw)
						env[fmt.Sprint(item["name"])] = item["value"]
					}
					for key, want := range map[string]string{"TETRAL_TRACE_SAMPLE_RATIO": tc.ratio, "TETRAL_DB_SLOW_QUERY_MS": tc.slowQuery} {
						if want != "" && env[key] != want {
							t.Errorf("%s/%v %s = %#v; want %q", name, container["name"], key, env[key], want)
						}
					}
					if got, present := env["OTEL_EXPORTER_OTLP_ENDPOINT"]; present != exports || (exports && got != tc.endpoint) {
						t.Errorf("%s/%v OTEL_EXPORTER_OTLP_ENDPOINT = %#v (present %v); want %q", name, container["name"], got, present, tc.endpoint)
					}
				}
				key := "networking.k8s.io/v1|NetworkPolicy|" + workload.namespace + "|" + name
				policy, ok := objects[key]
				if !ok {
					t.Fatalf("workload %s has no NetworkPolicy %s", name, key)
				}
				wantTrace, wantScrape := 0, 0
				if exports {
					wantTrace = 1
				}
				if tc.scraped && workload.metricsPort != 0 {
					wantScrape = 1
				}
				if got := takePolicyRules(policy, "egress", "to", collector, collectorPort); got != wantTrace {
					t.Errorf("%s has %d trace collector egress rules; want %d", key, got, wantTrace)
				}
				if got := takePolicyRules(policy, "ingress", "from", scraper, workload.metricsPort); got != wantScrape {
					t.Errorf("%s has %d scraper ingress rules on metrics port %d; want %d", key, got, workload.metricsPort, wantScrape)
				}
				if !reflect.DeepEqual(policy, defaults[key]) {
					t.Errorf("%s differs from its default beyond the observability rules:\n%#v\n%#v", key, policy, defaults[key])
				}
			}
		})
	}
}

func TestHelmServiceMonitorsCoverEveryMetricsService(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	defaults := renderChart(t, helm, chart)
	t.Run("disabled by default", func(t *testing.T) {
		for key, object := range uniqueObjects(t, defaults) {
			if object["kind"] == "ServiceMonitor" {
				t.Errorf("default render has %s", key)
			}
		}
	})
	t.Run("enabled", func(t *testing.T) {
		rendered := renderChart(t, helm, chart, "observability.serviceMonitor.enabled=true")
		objects := uniqueObjects(t, rendered)
		workloads := observedWorkloads(t, objects)
		var services []map[string]any
		hasMetricsService := map[string]bool{}
		for _, service := range objects {
			if service["kind"] != "Service" {
				continue
			}
			services = append(services, service)
			for name, workload := range workloads {
				if workload.namespace != configurationAt(service, "metadata", "namespace") || !workload.selectedBy(configurationMap(configurationAt(service, "spec", "selector"))) {
					continue
				}
				for _, raw := range configurationList(configurationAt(service, "spec", "ports")) {
					if workload.metricsPort != 0 && workload.containerPort(configurationMap(raw)["targetPort"]) == workload.metricsPort {
						hasMetricsService[name] = true
					}
				}
			}
		}
		monitored := map[string]bool{}
		var monitors []string
		for key, monitor := range objects {
			if monitor["kind"] != "ServiceMonitor" {
				continue
			}
			monitors = append(monitors, key)
			if !strings.HasPrefix(key, "monitoring.coreos.com/v1|") {
				t.Fatalf("%s has an unexpected API version", key)
			}
			endpoints := configurationList(configurationAt(monitor, "spec", "endpoints"))
			if len(endpoints) != 1 {
				t.Fatalf("%s has %d endpoints; want 1", key, len(endpoints))
			}
			endpoint := configurationMap(endpoints[0])
			if endpoint["honorLabels"] != true || endpoint["path"] != "/metrics" {
				t.Fatalf("%s endpoint = %v; want path /metrics with honorLabels true", key, endpoint)
			}
			namespace := configurationAt(monitor, "metadata", "namespace")
			scraped := map[string]bool{}
			for _, service := range services {
				if configurationAt(service, "metadata", "namespace") != namespace {
					continue
				}
				labels := map[string]string{}
				for label, value := range configurationMap(configurationAt(service, "metadata", "labels")) {
					labels[label] = fmt.Sprint(value)
				}
				matches, err := policySelectorMatches(configurationAt(monitor, "spec", "selector"), labels)
				if err != nil {
					t.Fatalf("%s selector: %v", key, err)
				}
				if !matches {
					continue
				}
				for _, raw := range configurationList(configurationAt(service, "spec", "ports")) {
					port := configurationMap(raw)
					if port["name"] != endpoint["port"] {
						continue
					}
					for name, workload := range workloads {
						if workload.namespace != namespace || !workload.selectedBy(configurationMap(configurationAt(service, "spec", "selector"))) {
							continue
						}
						if workload.metricsPort == 0 || workload.containerPort(port["targetPort"]) != workload.metricsPort {
							t.Fatalf("%s scrapes %s on a port other than its metrics port", key, name)
						}
						scraped[name] = true
					}
				}
			}
			if len(scraped) != 1 {
				t.Fatalf("%s scrapes %v; want exactly one workload", key, scraped)
			}
			for name := range scraped {
				if monitored[name] {
					t.Fatalf("workload %s has more than one ServiceMonitor", name)
				}
				monitored[name] = true
			}
		}
		for name := range workloads {
			if monitored[name] != hasMetricsService[name] {
				t.Errorf("workload %s has a ServiceMonitor = %v; want %v", name, monitored[name], hasMetricsService[name])
			}
		}
		if monitored["cleanup"] || hasMetricsService["cleanup"] {
			t.Error("cleanup has no metrics Service and keeps its push exporter")
		}
		requireToggleShape(t, rendered, defaults, monitors, nil)
	})
}

func TestHelmObservabilityRejectsInvalidValues(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	endpoint := `observability.otlpEndpoint="http://otel-collector.observability:4317"`
	collector := `observability.otlpPeers=[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"observability"}}}]`
	for _, tc := range []struct {
		name       string
		values     []string
		wantInFail string
	}{
		{name: "trace ratio above one", values: []string{"observability.traceSampleRatio=1.5"}, wantInFail: "observability.traceSampleRatio must be a decimal from 0 to 1"},
		{name: "negative trace ratio", values: []string{"observability.traceSampleRatio=-0.1"}, wantInFail: "observability.traceSampleRatio must be a decimal from 0 to 1"},
		{name: "zero slow query", values: []string{"observability.slowQueryMs=0"}, wantInFail: "observability.slowQueryMs must be a positive integer of milliseconds at most 9223372036854"},
		{name: "slow query over duration representation", values: []string{"observability.slowQueryMs=9223372036855"}, wantInFail: "observability.slowQueryMs must be a positive integer of milliseconds at most 9223372036854"},
		{name: "fractional slow query", values: []string{"observability.slowQueryMs=2.5"}, wantInFail: "observability.slowQueryMs must be a positive integer of milliseconds at most 9223372036854"},
		{name: "endpoint missing scheme", values: []string{`observability.otlpEndpoint="collector:4317"`, collector}, wantInFail: "observability.otlpEndpoint must be an http or https URL with a host"},
		{name: "endpoint unsupported scheme", values: []string{`observability.otlpEndpoint="ftp://collector:4317"`, collector}, wantInFail: "observability.otlpEndpoint must be an http or https URL with a host"},
		{name: "endpoint without host", values: []string{`observability.otlpEndpoint="http:///collector"`, collector}, wantInFail: "observability.otlpEndpoint must be an http or https URL with a host"},
		{name: "non-string empty endpoint", values: []string{`observability.otlpEndpoint=0`}, wantInFail: "observability.otlpEndpoint must be a string"},
		{name: "endpoint without collector peers", values: []string{endpoint}, wantInFail: "observability.otlpPeers must name the trace collector"},
		{name: "empty collector peer", values: []string{endpoint, `observability.otlpPeers=[{}]`}, wantInFail: "observability.otlpPeers[0] must be a non-empty peer object"},
		{name: "collector peer without namespace", values: []string{endpoint, `observability.otlpPeers=[{"podSelector":{"matchLabels":{"app.kubernetes.io/name":"otel-collector"}}}]`}, wantInFail: "observability.otlpPeers[0] must carry a namespaceSelector or an ipBlock"},
		{name: "null scrape peer", values: []string{`observability.metricsScrapePeers=[null]`}, wantInFail: "observability.metricsScrapePeers[0] must be a non-empty peer object"},
		{name: "collector port zero", values: []string{endpoint, collector, "observability.otlpPort=0"}, wantInFail: "observability.otlpPort must be a TCP port"},
		{name: "collector port above range", values: []string{endpoint, collector, "observability.otlpPort=65536"}, wantInFail: "observability.otlpPort must be a TCP port"},
		{name: "ServiceMonitor switch not boolean", values: []string{`observability.serviceMonitor={"enabled":"true"}`}, wantInFail: "observability.serviceMonitor.enabled must be a boolean"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := renderChartFailure(t, helm, chart, tc.values...)
			if err == nil {
				t.Fatalf("invalid values %v rendered successfully", tc.values)
			}
			if !strings.Contains(output, tc.wantInFail) {
				t.Fatalf("invalid values %v failure does not contain %q:\n%s", tc.values, tc.wantInFail, output)
			}
		})
	}
}

// A values file decodes YAML numerics differently from Helm's --set parser.
// The warning threshold must survive that actual operator-facing boundary.
func TestHelmDiagnosticIntegersFromYAML(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	for _, tc := range []struct{ value, want string }{
		{"1200000", "1200000"},
		{"9223372036854", "9223372036854"},
		{`"1200000"`, "1200000"},
		{"1200000.5", ""},
		{"9223372036855", ""},
	} {
		t.Run(tc.value, func(t *testing.T) {
			command := exec.Command(helm, "template", "tetral", chart, "-f", "-") //nolint:gosec // Test-owned Helm and chart paths.
			command.Stdin = strings.NewReader("observability:\n  logSummaryIntervalMs: 1200000\n  slowQueryMs: " + tc.value + "\n")
			output, err := command.CombinedOutput()
			if tc.want == "" {
				if err == nil || !strings.Contains(string(output), "observability.slowQueryMs must be a positive integer") {
					t.Fatalf("invalid YAML threshold rendered or failed elsewhere: %v\n%s", err, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("render valid YAML threshold: %v\n%s", err, output)
			}
			objects := uniqueObjects(t, decodeManifestObjects(t, bytes.NewReader(output), "helm template YAML"))
			for name, workload := range observedWorkloads(t, objects) {
				for _, container := range workload.containers {
					found := false
					summaryFound := false
					for _, raw := range configurationList(container["env"]) {
						item := configurationMap(raw)
						if item["name"] == "TETRAL_LOG_SUMMARY_INTERVAL_MS" {
							summaryFound = true
							if item["value"] != "1200000" {
								t.Errorf("%s/%v summary interval=%v; want 1200000", name, container["name"], item["value"])
							}
						}
						if item["name"] == "TETRAL_DB_SLOW_QUERY_MS" {
							found = true
							if item["value"] != tc.want {
								t.Errorf("%s/%v threshold=%v; want %s", name, container["name"], item["value"], tc.want)
							}
						}
					}
					if !found || !summaryFound {
						t.Errorf("%s/%v missing threshold or summary interval", name, container["name"])
					}
				}
			}
		})
	}
}
