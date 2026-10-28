package testinfra

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
)

const cliCacheAction = "./.github/actions/cli-build-cache"
const pinnedCLICache = "@55cc8345863c7cc4c66a329aec7e433d2d1c52a9"

// VerifyCLIBuildCacheWorkflows validates the separately owned Rust writer
// before the global cache policy grants this one job save access. It shares
// paths and compatible keys through the repository script, not YAML copies.
func VerifyCLIBuildCacheWorkflows(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github/workflows/cli-build-cache.yml"))
	if err != nil {
		return err
	}
	workflow := document.Content[0]
	triggers := mappingValue(workflow, "on")
	push := mappingValue(triggers, "push")
	if triggers == nil || triggers.Kind != workflowYAMLMapping || len(triggers.Content) != 4 || push == nil || len(push.Content) != 2 || !slices.Equal(sequenceScalars(mappingValue(push, "branches")), []string{"main"}) || mappingValue(triggers, "workflow_dispatch") == nil {
		return fmt.Errorf("CLI cache producer must run on main pushes and dispatch only")
	}
	jobs := mappingValue(workflow, "jobs")
	job := mappingValue(jobs, "build")
	if job == nil || len(jobs.Content) != 2 || scalar(mappingValue(job, "if")) != "github.ref == 'refs/heads/main'" {
		return fmt.Errorf("CLI cache producer must have one build job limited to main")
	}
	if !readOnlyPermissions(mappingValue(workflow, "permissions"), []string{"contents"}) || mappingValue(job, "permissions") != nil || strings.Contains(allScalars(workflow), "secrets.") {
		return fmt.Errorf("CLI cache producer must have read-only contents and no secrets")
	}
	if scalar(mappingValue(job, "cache-mode")) != "write" || mappingValue(workflow, "cache-mode") != nil {
		return fmt.Errorf("CLI cache producer alone must hold cache-mode write")
	}
	concurrency := mappingValue(workflow, "concurrency")
	if scalar(mappingValue(concurrency, "group")) != "cli-build-cache-${{ github.ref }}" || scalar(mappingValue(concurrency, "cancel-in-progress")) != "false" {
		return fmt.Errorf("CLI cache producer must queue without cancelling an upload")
	}
	steps := sequenceNodes(mappingValue(job, "steps"))
	if len(steps) != 21 {
		return fmt.Errorf("CLI cache producer must use the reviewed setup, lookup, restore, compile, measure, save and upload steps")
	}
	checkout, setup, bwrap, rust, prepare := steps[0], steps[1], steps[2], steps[3], steps[4]
	if scalar(mappingValue(checkout, "uses")) != "actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10" || scalar(mappingValue(mappingValue(checkout, "with"), "persist-credentials")) != "false" || mappingValue(mappingValue(checkout, "with"), "ref") != nil {
		return fmt.Errorf("CLI cache producer must check out its exact revision without persisted credentials")
	}
	if scalar(mappingValue(setup, "uses")) != "actions/setup-go@4a3601121dd01d1626a1e23e37211e3254c1c06c" || scalar(mappingValue(mappingValue(setup, "with"), "cache")) != "false" || scalar(mappingValue(mappingValue(setup, "with"), "go-version")) != "1.26.10" {
		return fmt.Errorf("CLI cache producer must use pinned Go without its own cache")
	}
	if scalar(mappingValue(bwrap, "run")) != "sudo apt-get update\nsudo apt-get install -y --no-install-recommends bubblewrap\n" || scalar(mappingValue(rust, "run")) != "rustup toolchain install" || scalar(mappingValue(rust, "working-directory")) != "cli" {
		return fmt.Errorf("CLI cache producer must install host bubblewrap and the CLI Rust toolchain")
	}
	if scalar(mappingValue(prepare, "id")) != "prepare" || scalar(mappingValue(prepare, "run")) != `./scripts/cli-build-cache.sh prepare all "$RUNNER_TEMP/cli-cache-evidence"` {
		return fmt.Errorf("CLI cache producer must use the shared key and path script")
	}
	for _, step := range steps[:5] {
		if mappingValue(step, "if") != nil {
			return fmt.Errorf("CLI cache setup must be unconditional")
		}
	}
	for i, component := range []string{"common", "digest", "release"} {
		lookup := steps[5+i]
		if scalar(mappingValue(lookup, "id")) != "lookup-"+component || scalar(mappingValue(lookup, "uses")) != "actions/cache/restore"+pinnedCLICache || scalar(mappingValue(mappingValue(lookup, "with"), "lookup-only")) != "true" || mappingValue(lookup, "if") != nil {
			return fmt.Errorf("CLI cache producer must look up each exact key before compiling")
		}
		if err := cliCacheInputs(lookup, component, false); err != nil {
			return err
		}
		build, measure, save := steps[11+i*3], steps[12+i*3], steps[13+i*3]
		miss := "steps.lookup-" + component + ".outputs.cache-hit != 'true'"
		profile := component
		if component == "release" {
			profile = "release-only"
		}
		if scalar(mappingValue(build, "run")) != `./scripts/cli-build-cache.sh build `+profile+` producer "$RUNNER_TEMP/cli-cache-evidence"` || scalar(mappingValue(build, "if")) != miss || mappingValue(build, "env") != nil || mappingValue(build, "continue-on-error") != nil {
			return fmt.Errorf("CLI cache producer must compile each missing component with the shared build recipe")
		}
		if scalar(mappingValue(measure, "run")) != `./scripts/cli-build-cache.sh record `+component+` "$RUNNER_TEMP/cli-cache-evidence/saved"` || scalar(mappingValue(measure, "if")) != miss {
			return fmt.Errorf("CLI cache producer must measure each component before saving")
		}
		if scalar(mappingValue(save, "uses")) != "actions/cache/save"+pinnedCLICache || scalar(mappingValue(save, "if")) != miss {
			return fmt.Errorf("CLI cache producer must save only after a successful missing-component build")
		}
		if err := cliCacheInputs(save, component, false); err != nil {
			return err
		}
	}
	if scalar(mappingValue(steps[8], "run")) != `./scripts/cli-build-cache.sh lookups "$RUNNER_TEMP/cli-cache-evidence"` {
		return fmt.Errorf("CLI cache producer must record exact-key lookups")
	}
	warm, release := steps[9], steps[10]
	if scalar(mappingValue(warm, "uses")) != cliCacheAction || scalar(mappingValue(mappingValue(warm, "with"), "profile")) != "functional" || scalar(mappingValue(warm, "if")) != "steps.prepare.outputs.restore-allowed == 'true' && (steps.lookup-common.outputs.cache-hit != 'true' || steps.lookup-digest.outputs.cache-hit != 'true' || steps.lookup-release.outputs.cache-hit != 'true')" {
		return fmt.Errorf("CLI cache producer must restore common and digest through the read-only consumer with a disk guard")
	}
	if scalar(mappingValue(release, "uses")) != "actions/cache/restore"+pinnedCLICache || scalar(mappingValue(release, "if")) != "steps.prepare.outputs.restore-allowed == 'true' && steps.lookup-release.outputs.cache-hit != 'true'" {
		return fmt.Errorf("CLI cache producer release restore must respect disk and exact-key lookup")
	}
	if err := cliCacheInputs(release, "release", true); err != nil {
		return err
	}
	upload := steps[20]
	if !strings.HasPrefix(scalar(mappingValue(upload, "uses")), "actions/upload-artifact@") || scalar(mappingValue(upload, "if")) != "always()" {
		return fmt.Errorf("CLI cache producer must retain failure evidence")
	}
	return verifyCLICacheConsumer(root)
}

func cliCacheInputs(step *workflowYAMLNode, component string, fallback bool) error {
	inputs := mappingValue(step, "with")
	for _, field := range []string{"path", "key"} {
		output := field
		if field == "path" {
			output = "paths"
		}
		if scalar(mappingValue(inputs, field)) != "${{ steps.prepare.outputs."+component+"-"+output+" }}" {
			return fmt.Errorf("CLI cache %s %s must use shared compatible keys and component paths", component, field)
		}
	}
	restore := mappingValue(inputs, "restore-keys")
	if fallback && scalar(restore) != "${{ steps.prepare.outputs."+component+"-prefix }}" || !fallback && restore != nil {
		return fmt.Errorf("CLI cache %s restore prefix differs from its compatible identity", component)
	}
	return nil
}

func verifyCLICacheConsumer(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github/actions/cli-build-cache/action.yml"))
	if err != nil {
		return err
	}
	action := document.Content[0]
	steps := sequenceNodes(mappingValue(mappingValue(action, "runs"), "steps"))
	if len(steps) != 7 {
		return fmt.Errorf("CLI cache consumer must prepare and restore/record only its common and selected variant components")
	}
	prepare := steps[0]
	if scalar(mappingValue(prepare, "id")) != "prepare" || scalar(mappingValue(prepare, "run")) != `./scripts/cli-build-cache.sh prepare "$PROFILE" "$EVIDENCE"` || scalar(mappingValue(mappingValue(prepare, "env"), "PROFILE")) != "${{ inputs.profile }}" {
		return fmt.Errorf("CLI cache consumer must use shared key and path preparation")
	}
	for i, component := range []string{"common", "digest", "release"} {
		restore, record := steps[1+i*2], steps[2+i*2]
		guard := "steps.prepare.outputs.restore-allowed == 'true'"
		recordGuard := "always()"
		if component != "common" {
			guard += " && steps.prepare.outputs.variant == '" + component + "'"
			recordGuard += " && steps.prepare.outputs.variant == '" + component + "'"
		}
		if scalar(mappingValue(restore, "uses")) != "actions/cache/restore"+pinnedCLICache || scalar(mappingValue(restore, "id")) != component || scalar(mappingValue(restore, "if")) != guard {
			return fmt.Errorf("CLI cache consumer must restore only common and its selected variant with a disk guard")
		}
		if err := cliCacheInputs(restore, component, true); err != nil {
			return err
		}
		if scalar(mappingValue(record, "run")) != `./scripts/cli-build-cache.sh record `+component+` "$EVIDENCE"` || scalar(mappingValue(record, "if")) != recordGuard {
			return fmt.Errorf("CLI cache consumer must retain restore evidence on failure")
		}
	}
	return nil
}
