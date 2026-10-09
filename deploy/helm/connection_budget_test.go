package helm_test

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Count processes and their implemented maximum pool generations, rather than
// worker concurrency or a second count of pool-backed listeners.
func renderedPoolSlots(objects map[string]map[string]any, roles map[string]int) (int, error) {
	total := 0
	for role, generations := range roles {
		d := objects["apps/v1|Deployment|tetral-system|"+role]
		if d == nil {
			return 0, fmt.Errorf("missing process %s", role)
		}
		spec := d["spec"].(map[string]any)
		replicas, err := strconv.Atoi(fmt.Sprint(spec["replicas"]))
		if err != nil || replicas <= 0 {
			return 0, fmt.Errorf("replica bound invalid")
		}
		for _, object := range objects {
			if object["kind"] == "HorizontalPodAutoscaler" {
				hpa := object["spec"].(map[string]any)
				target := hpa["scaleTargetRef"].(map[string]any)
				if object["metadata"].(map[string]any)["namespace"] == d["metadata"].(map[string]any)["namespace"] && target["apiVersion"] == d["apiVersion"] && target["kind"] == d["kind"] && target["name"] == role {
					maximum, err := strconv.Atoi(fmt.Sprint(hpa["maxReplicas"]))
					if err != nil || maximum <= 0 {
						return 0, fmt.Errorf("HPA replica bound invalid")
					}
					minimum, err := strconv.Atoi(fmt.Sprint(hpa["minReplicas"]))
					if err != nil || minimum <= 0 || minimum > maximum {
						return 0, fmt.Errorf("HPA minimum exceeds its valid replica range")
					}
					// Initial replicas exist independently of HPA reconciliation.
					if maximum > replicas {
						replicas = maximum
					}
				}
			}
		}
		surgeValue := fmt.Sprint(spec["strategy"].(map[string]any)["rollingUpdate"].(map[string]any)["maxSurge"])
		percent := strings.HasSuffix(surgeValue, "%")
		surge, err := strconv.Atoi(strings.TrimSuffix(surgeValue, "%"))
		if err != nil || surge <= 0 {
			return 0, fmt.Errorf("surge bound invalid")
		}
		if percent {
			surge = (replicas*surge + 99) / 100
		}
		containers := spec["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
		processes := 0
		for _, entry := range containers {
			container := entry.(map[string]any)
			env, _ := container["env"].([]any)
			max := 0
			ownsDB := false
			for _, entry := range env {
				v := entry.(map[string]any)
				if v["name"] == "TETRAL_DATABASE_URL" || v["name"] == "TETRAL_POSTGRES_DSN" || v["name"] == "TETRAL_EVENT_STREAM_DATABASE_URL" {
					ownsDB = true
				}
				if v["name"] == "TETRAL_DATABASE_POOL_MAX" || v["name"] == "TETRAL_DB_MAX_OPEN_CONNS" {
					max, err = strconv.Atoi(fmt.Sprint(v["value"]))
					if err != nil || max <= 0 {
						return 0, fmt.Errorf("pool bound invalid")
					}
				}
			}
			if ownsDB {
				if max == 0 {
					return 0, fmt.Errorf("pool bound missing")
				}
				processes++
				total += (replicas + surge) * max * generations
			}
		}
		if processes != 1 {
			return 0, fmt.Errorf("owning process count differs")
		}
	}
	return total, nil
}
func TestAggregatePostgreSQLConnectionBudget(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy/helm/tetral")
	values := []string{"autoscaling.providerGateway.enabled=false", "replicas.bridge=2", "replicas.jobRunner=2", "replicas.providerGateway=2", "replicas.mcpConnector=2", "transport.goPoolMax=20", "transport.bunPoolMax=20"}
	objects := uniqueObjects(t, renderChart(t, helm, chart, values...))
	publicScaled := uniqueObjects(t, renderChart(t, helm, chart, "replicas.api=3", "replicas.auth=2"))
	publicSlots, err := renderedPoolSlots(publicScaled, map[string]int{"api": 1, "auth": 1})
	if err != nil || publicSlots != 140 {
		t.Fatalf("independent public/auth replica+surge budget=%d err=%v", publicSlots, err)
	}
	for _, role := range []string{"api", "auth"} {
		requireManifestPathString(t, publicScaled["v1|Service|tetral-system|"+role], "None", "spec", "sessionAffinity")
	}
	goRoles := map[string]int{"bridge": 1, "job-runner": 1}
	slots, err := renderedPoolSlots(objects, goRoles)
	if err != nil || slots+8 != 128 {
		t.Fatalf("literal Go subset=%d +8 err=%v; want128", slots, err)
	}
	for _, available := range []int{128, 127} {
		if passes := slots+8 <= available; passes != (available == 128) {
			t.Fatalf("Go available%d pass=%v", available, passes)
		}
	}
	allRoles := map[string]int{"bridge": 1, "job-runner": 1, "provider-gateway": 2, "mcp-connector": 2}
	slots, err = renderedPoolSlots(objects, allRoles)
	if err != nil || slots+8 != 368 {
		t.Fatalf("literal complete subset=%d +8 err=%v; want368", slots, err)
	}
	for _, available := range []int{368, 367} {
		if passes := slots+8 <= available; passes != (available == 368) {
			t.Fatalf("complete available%d pass=%v", available, passes)
		}
	}
	percentage := uniqueObjects(t, renderChart(t, helm, chart, append(values, "replicas.bridge=3", "rollout.maxSurge=34%")...))
	slots, err = renderedPoolSlots(percentage, map[string]int{"bridge": 1})
	if err != nil || slots != 100 {
		t.Fatalf("percentage rounding=%d err=%v; want5*20", slots, err)
	}
	// Count separately configured old/new cohorts. Old20 and new10 slots, each
	// with three processes, require90; a homogenized cohort would be incorrect.
	oldSlots, err := renderedPoolSlots(objects, goRoles)
	if err != nil {
		t.Fatal(err)
	}
	newer := uniqueObjects(t, renderChart(t, helm, chart, append(values, "transport.goPoolMax=10")...))
	newSlots, err := renderedPoolSlots(newer, goRoles)
	if err != nil {
		t.Fatal(err)
	}
	if oldSlots/2+newSlots/2 != 90 {
		t.Fatal("different old/new cohort settings were discarded")
	}
	ledger := objects["v1|ConfigMap|tetral-system|tetral-database-connection-budget"]["data"].(map[string]any)
	if ledger["status"] != "unresolved" || ledger["externalConsumerSlots"] != "unknown" || ledger["availableServerConnections"] != "unknown" {
		t.Fatalf("unknown consumers/capacity claimed resolved: %v", ledger)
	}
	// A second Service listener does not introduce another SQL-owning process.
	before, _ := renderedPoolSlots(objects, goRoles)
	delete(objects, "v1|Service|tetral-system|bridge")
	after, _ := renderedPoolSlots(objects, goRoles)
	if before != after {
		t.Fatal("listener double-counted as another pool")
	}
	for _, bad := range []string{"transport.goPoolMax=0", "transport.bunPoolMax=unbounded", "rollout.maxSurge=0", "rollout.maxSurge=unbounded", "transport.databaseCapacity=0", "transport.externalDatabaseConnections=-1"} {
		t.Run(bad, func(t *testing.T) { requireRenderError(t, helm, chart, []string{bad}) })
	}
	gitSlots, err := renderedPoolSlots(objects, map[string]int{"git-proxy": 1})
	if err != nil || gitSlots != 220 {
		t.Fatalf("Git Proxy HPA maximum plus surge=%d err=%v", gitSlots, err)
	}
	gitPercentage, err := renderedPoolSlots(percentage, map[string]int{"git-proxy": 1})
	if err != nil || gitPercentage != 280 {
		t.Fatalf("Git Proxy HPA percentage surge=%d err=%v", gitPercentage, err)
	}
	// Sandbox is an independently scaled Go consumer using a nonstandard DSN key.
	sandbox := uniqueObjects(t, renderChart(t, helm, chart, "replicas.sandbox=3"))
	sandboxSlots, err := renderedPoolSlots(sandbox, map[string]int{"sandbox": 1})
	if err != nil || sandboxSlots != 80 {
		t.Fatalf("Sandbox three replicas plus surge=%d err=%v", sandboxSlots, err)
	}
	sandboxPercent := uniqueObjects(t, renderChart(t, helm, chart, "replicas.sandbox=3", "rollout.maxSurge=34%"))
	sandboxSlots, err = renderedPoolSlots(sandboxPercent, map[string]int{"sandbox": 1})
	if err != nil || sandboxSlots != 100 {
		t.Fatalf("Sandbox percentage surge=%d err=%v", sandboxSlots, err)
	}
	if sandbox["v1|ConfigMap|tetral-system|tetral-database-connection-budget"]["data"].(map[string]any)["maximumOwnedPoolSlots"] != "820" {
		t.Fatal("Sandbox scale was omitted from full ledger")
	}
	if sandboxPercent["v1|ConfigMap|tetral-system|tetral-database-connection-budget"]["data"].(map[string]any)["maximumOwnedPoolSlots"] != "960" {
		t.Fatal("Sandbox scale and percentage surge were omitted from full ledger")
	}
	for _, invalid := range []string{"replicas.sandbox=0", "replicas.sandbox=-1", "replicas.sandbox=1.5", "replicas.sandbox=unbounded"} {
		requireRenderError(t, helm, chart, []string{invalid})
	}
	requireRenderError(t, helm, chart, []string{"transport.databaseCapacity=748", "transport.externalDatabaseConnections=0"})
	// Resolve every counted owner and enforce the actual full-chart inequality.
	resolved := uniqueObjects(t, renderChart(t, helm, chart, "transport.databaseCapacity=788", "transport.externalDatabaseConnections=0"))
	data := resolved["v1|ConfigMap|tetral-system|tetral-database-connection-budget"]["data"].(map[string]any)
	if data["status"] != "resolved" || data["maximumOwnedPoolSlots"] != "780" {
		t.Fatalf("actual deployed inventory budget differs: %v", data)
	}
	requireRenderError(t, helm, chart, []string{"transport.databaseCapacity=787", "transport.externalDatabaseConnections=0"})
}
func requireRenderError(t *testing.T, helm, chart string, values []string, expectedReason ...string) {
	t.Helper()
	args := []string{"template", "tetral", chart}
	for _, value := range values {
		args = append(args, "--set", value)
	}
	command := exec.Command(helm, args...)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("invalid configuration rendered: %v", values)
	}
	for _, reason := range expectedReason {
		if !strings.Contains(string(output), reason) {
			t.Fatalf("configuration%v failed at wrong boundary: want%s actual%s", values, reason, output)
		}
	}
}

func providerBudgetChart(t *testing.T) string {
	t.Helper()
	return filepath.Join(engineRoot(t), "deploy/helm/tetral")
}

func TestProviderGatewayAutoscalingBounds(t *testing.T) {
	helm, chart := requireHelm(t), providerBudgetChart(t)
	for _, profile := range []string{"standard-routed", "hardened"} {
		t.Run(profile, func(t *testing.T) {
			base := []string{"transport.profile=" + profile}
			t.Run("enabled maximum below floor", func(t *testing.T) {
				requireRenderError(t, helm, chart, append(base, "autoscaling.providerGateway.maxReplicas=1"), "enabled HPA minimum of 2")
			})
			for _, maximum := range []string{"0", "-1", "1.5", "unbounded"} {
				t.Run("invalid maximum "+maximum, func(t *testing.T) {
					requireRenderError(t, helm, chart, append(base, "autoscaling.providerGateway.maxReplicas="+maximum), "autoscaling.providerGateway.maxReplicas")
				})
			}
			t.Run("enabled minimum", func(t *testing.T) {
				objects := uniqueObjects(t, renderChart(t, helm, chart, append(base, "autoscaling.providerGateway.maxReplicas=2")...))
				hpa := objects["autoscaling/v2|HorizontalPodAutoscaler|tetral-system|provider-gateway"]["spec"].(map[string]any)
				if hpa["minReplicas"] != 2 || hpa["maxReplicas"] != 2 {
					t.Fatalf("minimum enabled HPA changed: %v", hpa)
				}
			})
			t.Run("disabled one replica", func(t *testing.T) {
				objects := uniqueObjects(t, renderChart(t, helm, chart, append(base, "autoscaling.providerGateway.enabled=false", "autoscaling.providerGateway.maxReplicas=1", "replicas.providerGateway=1")...))
				if _, present := objects["autoscaling/v2|HorizontalPodAutoscaler|tetral-system|provider-gateway"]; present {
					t.Fatal("disabled Provider autoscaling rendered an HPA")
				}
				if objects["apps/v1|Deployment|tetral-system|provider-gateway"]["spec"].(map[string]any)["replicas"] != 1 {
					t.Fatal("disabled Provider lost one-replica support")
				}
			})
		})
	}
}

// Derive the complete default-pool ledger from rendered processes. Cleanup's
// one nonoverlapping pool is read from its actual CronJob, not from the ledger.
func renderedDefaultPoolLedger(t *testing.T, objects map[string]map[string]any) (int, error) {
	t.Helper()
	roles := map[string]int{"api": 1, "auth": 1, "bridge": 1, "job-runner": 1, "sandbox": 1, "queue": 1, "event-stream": 1, "git-proxy": 1, "provider-gateway": 2, "mcp-connector": 2}
	slots, err := renderedPoolSlots(objects, roles)
	if err != nil {
		return 0, err
	}
	cleanup := objects["batch/v1|CronJob|tetral-system|cleanup"]
	if transportAt(t, cleanup, "spec", "concurrencyPolicy") != "Forbid" {
		return 0, fmt.Errorf("cleanup pools can overlap")
	}
	containers := transportList(t, transportAt(t, cleanup, "spec", "jobTemplate", "spec", "template", "spec", "containers"))
	if len(containers) != 1 {
		return 0, fmt.Errorf("cleanup owning process count differs")
	}
	pool := 0
	for _, value := range transportList(t, transportAt(t, containers[0], "env")) {
		setting := transportMap(t, value)
		if setting["name"] == "TETRAL_DB_MAX_OPEN_CONNS" {
			pool, err = strconv.Atoi(fmt.Sprint(setting["value"]))
			if err != nil || pool <= 0 {
				return 0, fmt.Errorf("cleanup pool bound invalid")
			}
		}
	}
	if pool == 0 {
		return 0, fmt.Errorf("cleanup pool bound missing")
	}
	slots += pool
	ledger := transportMap(t, transportAt(t, objects["v1|ConfigMap|tetral-system|tetral-database-connection-budget"], "data"))
	if fmt.Sprint(ledger["maximumOwnedPoolSlots"]) != strconv.Itoa(slots) {
		return 0, fmt.Errorf("rendered pool ledger %v differs from actual process bounds %d", ledger, slots)
	}
	return slots, nil
}

func TestProviderGatewayConnectionBudgetBounds(t *testing.T) {
	helm, chart := requireHelm(t), providerBudgetChart(t)
	cases := []struct {
		name                      string
		initial, maximum          int
		enabled                   bool
		surge                     string
		owned, provider, capacity int
	}{
		{"enabled minimum", 1, 2, true, "1", 620, 60, 628},
		{"initial exceeds HPA", 10, 2, true, "1", 780, 220, 788},
		{"HPA exceeds initial", 1, 10, true, "1", 780, 220, 788},
		{"disabled ten initial", 10, 2, false, "1", 780, 220, 788},
		{"disabled one initial", 1, 1, false, "1", 600, 40, 608},
		{"percentage initial exceeds HPA", 10, 2, true, "34%", 900, 280, 908},
	}
	for _, profile := range []string{"standard-routed", "hardened"} {
		t.Run(profile, func(t *testing.T) {
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					values := []string{"transport.profile=" + profile, fmt.Sprintf("replicas.providerGateway=%d", c.initial), fmt.Sprintf("autoscaling.providerGateway.maxReplicas=%d", c.maximum), fmt.Sprintf("autoscaling.providerGateway.enabled=%t", c.enabled), "rollout.maxSurge=" + c.surge}
					objects := uniqueObjects(t, renderChart(t, helm, chart, values...))
					deployment := objects["apps/v1|Deployment|tetral-system|provider-gateway"]["spec"].(map[string]any)
					if deployment["replicas"] != c.initial {
						t.Fatalf("initial Deployment was capped: %v", deployment["replicas"])
					}
					hpa, present := objects["autoscaling/v2|HorizontalPodAutoscaler|tetral-system|provider-gateway"]
					if present != c.enabled {
						t.Fatal("Provider HPA presence differs from explicit setting")
					}
					if present && hpa["spec"].(map[string]any)["maxReplicas"] != c.maximum {
						t.Fatal("HPA maximum was replaced by initial Deployment replicas")
					}
					slots, err := renderedDefaultPoolLedger(t, objects)
					if err != nil || slots != c.owned {
						t.Fatalf("independent full owned slots=%d want%d: %v", slots, c.owned, err)
					}
					provider, err := renderedPoolSlots(objects, map[string]int{"provider-gateway": 2})
					if err != nil || provider != c.provider {
						t.Fatalf("Provider pool slots=%d want%d: %v", provider, c.provider, err)
					}
					// Literal expected totals include every process; reserve8 and
					// external0 are bound separately, never inferred from the ledger.
					bound := append(values, "transport.externalDatabaseConnections=0")
					resolved := uniqueObjects(t, renderChart(t, helm, chart, append(bound, fmt.Sprintf("transport.databaseCapacity=%d", c.capacity))...))
					if transportAt(t, resolved["v1|ConfigMap|tetral-system|tetral-database-connection-budget"], "data", "status") != "resolved" {
						t.Fatal("complete bound budget remained unresolved")
					}
					requireRenderError(t, helm, chart, append(bound, fmt.Sprintf("transport.databaseCapacity=%d", c.capacity-1)), fmt.Sprintf("aggregate connection budget %d", c.capacity))
				})
			}
			t.Run("previous falsely resolved capacity", func(t *testing.T) {
				requireRenderError(t, helm, chart, []string{"transport.profile=" + profile, "replicas.providerGateway=10", "autoscaling.providerGateway.maxReplicas=2", "transport.databaseCapacity=628", "transport.externalDatabaseConnections=0"}, "aggregate connection budget 788")
			})
			t.Run("independent oracle controls", func(t *testing.T) {
				objects := uniqueObjects(t, renderChart(t, helm, chart, "transport.profile="+profile, "replicas.providerGateway=10"))
				for _, mutation := range []string{"lower HPA maximum", "remove HPA", "wrong namespace", "wrong target name", "wrong target kind", "wrong target API"} {
					t.Run(mutation, func(t *testing.T) {
						changed := clonePolicyObjects(objects)
						hpa := changed["autoscaling/v2|HorizontalPodAutoscaler|tetral-system|provider-gateway"]
						spec := hpa["spec"].(map[string]any)
						target := spec["scaleTargetRef"].(map[string]any)
						switch mutation {
						case "lower HPA maximum":
							spec["maxReplicas"] = 2
						case "remove HPA":
							delete(changed, "autoscaling/v2|HorizontalPodAutoscaler|tetral-system|provider-gateway")
						case "wrong namespace":
							spec["maxReplicas"] = 20
							hpa["metadata"].(map[string]any)["namespace"] = "unrelated"
						case "wrong target name":
							spec["maxReplicas"] = 20
							target["name"] = "unrelated"
						case "wrong target kind":
							spec["maxReplicas"] = 20
							target["kind"] = "StatefulSet"
						case "wrong target API":
							spec["maxReplicas"] = 20
							target["apiVersion"] = "unrelated/v1"
						}
						if slots, err := renderedPoolSlots(changed, map[string]int{"provider-gateway": 2}); err != nil || slots != 220 {
							t.Fatalf("initial process bound changed through %s: %d %v", mutation, slots, err)
						}
					})
				}
				higher := clonePolicyObjects(objects)
				higher["autoscaling/v2|HorizontalPodAutoscaler|tetral-system|provider-gateway"]["spec"].(map[string]any)["maxReplicas"] = 20
				if slots, err := renderedPoolSlots(higher, map[string]int{"provider-gateway": 2}); err != nil || slots != 420 {
					t.Fatalf("matching higher HPA was not counted: %d %v", slots, err)
				}
				invalid := clonePolicyObjects(objects)
				invalid["autoscaling/v2|HorizontalPodAutoscaler|tetral-system|provider-gateway"]["spec"].(map[string]any)["maxReplicas"] = 1
				if _, err := renderedPoolSlots(invalid, map[string]int{"provider-gateway": 2}); err == nil {
					t.Fatal("oracle accepted an invalid matching HPA")
				}
				understated := clonePolicyObjects(objects)
				understated["v1|ConfigMap|tetral-system|tetral-database-connection-budget"]["data"].(map[string]any)["maximumOwnedPoolSlots"] = "620"
				if _, err := renderedDefaultPoolLedger(t, understated); err == nil {
					t.Fatal("oracle accepted the old understated ledger")
				}
			})
		})
	}
}
