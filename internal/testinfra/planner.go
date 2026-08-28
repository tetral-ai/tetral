package testinfra

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func BuildPlan(root string, profile Profile, base string) (Plan, error) {
	inventory, err := LoadInventory()
	if err != nil {
		return Plan{}, err
	}
	if profile != ProfileFast && profile != ProfileAffected && profile != ProfileFull {
		return Plan{}, fmt.Errorf("unknown profile %q", profile)
	}
	revision := inspectRevision(root, base)
	plan := Plan{Profile: profile, Revision: revision, CreatedAt: time.Now().UTC()}

	switch profile {
	case ProfileFast:
		plan.Selections = selectionsForGroups(inventory.GroupsForProfile("fast"), "fast profile")
	case ProfileFull:
		plan.Selections = selectionsForGroups(inventory.GroupsForProfile("full"), "full closed inventory")
	case ProfileAffected:
		plan.Selections, err = affectedSelections(root, inventory, &plan.Revision)
		if err != nil {
			return Plan{}, err
		}
	}
	if profile == ProfileFast {
		var expanded []Selection
		for _, selection := range plan.Selections {
			if selection.Group != "go" {
				selection.Dependencies = nil
				expanded = append(expanded, selection)
				continue
			}
			goSelections, exclusions, selectionErr := fastGoSelections(root)
			if selectionErr != nil {
				return Plan{}, fmt.Errorf("enumerate fast Go evidence: %w", selectionErr)
			}
			expanded = append(expanded, goSelections...)
			plan.Excluded = append(plan.Excluded, exclusions...)
		}
		plan.Selections = expanded
	} else if profile == ProfileFull {
		var exclusions []Exclusion
		plan.Selections, exclusions, err = expandFullGoSelection(root, plan.Selections, "full closed inventory")
		plan.Excluded = append(plan.Excluded, exclusions...)
		if err != nil {
			return Plan{}, err
		}
	} else if profile == ProfileAffected && plan.Revision.FullFallbackCause != "" {
		var exclusions []Exclusion
		plan.Selections, exclusions, err = expandFullGoSelection(root, plan.Selections, "full fallback: "+plan.Revision.FullFallbackCause)
		plan.Excluded = append(plan.Excluded, exclusions...)
		if err != nil {
			return Plan{}, err
		}
	} else if profile == ProfileAffected {
		plan.Selections, err = expandAffectedGoSelection(root, plan.Selections)
		if err != nil {
			return Plan{}, err
		}
	}
	plan.Selections, plan.Excluded, err = expandGatewaySelection(root, profile, plan.Selections, plan.Excluded)
	if err != nil {
		return Plan{}, err
	}
	plan.Dependencies = selectedDependencies(plan.Selections)
	if err := reconcilePlan(root, inventory, plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func expandGatewaySelection(root string, profile Profile, selections []Selection, exclusions []Exclusion) ([]Selection, []Exclusion, error) {
	for index := range selections {
		if selections[index].Group != "gateway" {
			continue
		}
		selected, excluded, err := gatewayTestFiles(filepath.Join(root, "services", "gateway"), profile)
		if err != nil {
			return nil, nil, err
		}
		selections[index].Tests = selected
		for _, item := range excluded {
			item.Group = "gateway"
			exclusions = append(exclusions, item)
		}
		for _, file := range selected {
			// Files are enumerated beneath the repository-owned Gateway test root.
			//nolint:gosec
			body, err := os.ReadFile(filepath.Join(root, "services", "gateway", filepath.FromSlash(file)))
			if err != nil {
				return nil, nil, err
			}
			if bytes.Contains(body, []byte("TETRAL_TEST_DATABASE_URL")) {
				selections[index].Dependencies = appendUnique(selections[index].Dependencies, "postgresql")
			}
		}
	}
	return selections, exclusions, nil
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func reconcilePlan(root string, inventory Inventory, plan Plan) error {
	knownGroups := map[string]bool{}
	for _, group := range inventory.Groups {
		knownGroups[group.ID] = true
	}
	seenPackages := map[string]bool{}
	seenRunnables := map[string]bool{}
	for _, selection := range plan.Selections {
		if !knownGroups[selection.Group] {
			return fmt.Errorf("plan contains unknown evidence group %q", selection.Group)
		}
		if selection.Group != "go" {
			continue
		}
		if len(selection.Packages) != 1 {
			return fmt.Errorf("go selection must identify exactly one logical package")
		}
		packageName := selection.Packages[0]
		if seenPackages[packageName] {
			return fmt.Errorf("go package %q is selected more than once", packageName)
		}
		seenPackages[packageName] = true
		for _, runnable := range selection.Tests {
			key := packageName + "\x00" + runnable
			if seenRunnables[key] {
				return fmt.Errorf("go runnable %q in %q is selected more than once", runnable, packageName)
			}
			seenRunnables[key] = true
		}
	}
	if plan.Profile == ProfileFast || plan.Profile == ProfileFull || plan.Revision.FullFallbackCause != "" {
		if err := reconcileGatewayPlan(root, plan); err != nil {
			return err
		}
	}
	if plan.Profile != ProfileFast && plan.Profile != ProfileFull && plan.Revision.FullFallbackCause == "" {
		return nil
	}
	packages, err := listGoPackages(root)
	if err != nil {
		return err
	}
	excluded := map[string]bool{}
	for _, item := range plan.Excluded {
		if item.Group != "go" || item.Package == "" || item.Runnable == "" {
			continue
		}
		key := item.Package + "\x00" + item.Runnable
		if excluded[key] {
			return fmt.Errorf("go runnable %q in %q is excluded more than once", item.Runnable, item.Package)
		}
		excluded[key] = true
	}
	for _, pkg := range packages {
		runnables, err := allGoRunnables(pkg)
		if err != nil {
			return err
		}
		if !seenPackages[pkg.ImportPath] {
			return fmt.Errorf("plan omitted Go package %q", pkg.ImportPath)
		}
		for _, runnable := range runnables {
			key := pkg.ImportPath + "\x00" + runnable
			count := 0
			if seenRunnables[key] {
				count++
			}
			if excluded[key] {
				count++
			}
			if count != 1 {
				return fmt.Errorf("go runnable %q in %q has %d evidence dispositions", runnable, pkg.ImportPath, count)
			}
		}
	}
	return nil
}

func reconcileGatewayPlan(root string, plan Plan) error {
	all, err := allBunTestFiles(filepath.Join(root, "services", "gateway"))
	if err != nil {
		return err
	}
	counts := map[string]int{}
	for _, selection := range plan.Selections {
		if selection.Group != "gateway" {
			continue
		}
		for _, file := range selection.Tests {
			counts[file]++
		}
	}
	for _, exclusion := range plan.Excluded {
		if exclusion.Group == "gateway" {
			counts[exclusion.Runnable]++
		}
	}
	for _, file := range all {
		if counts[file] != 1 {
			return fmt.Errorf("gateway test file %q has %d evidence dispositions", file, counts[file])
		}
		delete(counts, file)
	}
	for file, count := range counts {
		if count != 0 {
			return fmt.Errorf("gateway plan contains unexpected test file %q", file)
		}
	}
	return nil
}

func expandAffectedGoSelection(root string, selections []Selection) ([]Selection, error) {
	packages, err := listGoPackages(root)
	if err != nil {
		return nil, err
	}
	byImport := make(map[string]listedPackage, len(packages))
	for _, pkg := range packages {
		byImport[pkg.ImportPath] = pkg
	}
	var result []Selection
	for _, selection := range selections {
		if selection.Group != "go" {
			result = append(result, selection)
			continue
		}
		for _, importPath := range selection.Packages {
			pkg, ok := byImport[importPath]
			if !ok {
				return nil, fmt.Errorf("affected Go package %q is outside the enumerated universe", importPath)
			}
			runnables, err := allGoRunnables(pkg)
			if err != nil {
				return nil, err
			}
			dependencies, err := goPackageDependencies(pkg)
			if err != nil {
				return nil, err
			}
			result = append(result, Selection{Group: "go", Reason: selection.Reason, Packages: []string{importPath}, Tests: runnables, Mode: "race", Dependencies: dependencies})
		}
	}
	return result, nil
}

func expandFullGoSelection(root string, selections []Selection, reason string) ([]Selection, []Exclusion, error) {
	var result []Selection
	var resultExclusions []Exclusion
	for _, selection := range selections {
		if selection.Group != "go" {
			result = append(result, selection)
			continue
		}
		goSelections, exclusions, err := fullGoSelections(root, reason)
		if err != nil {
			return nil, nil, err
		}
		result = append(result, goSelections...)
		resultExclusions = append(resultExclusions, exclusions...)
	}
	return result, resultExclusions, nil
}

func affectedSelections(root string, inventory Inventory, revision *Revision) ([]Selection, error) {
	if revision.FullFallbackCause != "" {
		return selectionsForGroups(inventory.GroupsForProfile("full"), "full fallback: "+revision.FullFallbackCause), nil
	}
	if len(revision.ChangedPaths) == 0 {
		revision.FullFallbackCause = "no changed paths available for affected selection"
		return selectionsForGroups(inventory.GroupsForProfile("full"), "full fallback: "+revision.FullFallbackCause), nil
	}
	for _, path := range revision.ChangedPaths {
		if inventory.RequiresFull(path) {
			revision.FullFallbackCause = "broad evidence owner changed: " + path
			return selectionsForGroups(inventory.GroupsForProfile("full"), "full fallback: "+revision.FullFallbackCause), nil
		}
	}

	selected := map[string]Group{}
	for _, path := range revision.ChangedPaths {
		matches := inventory.MatchPath(path)
		if len(matches) == 0 {
			revision.FullFallbackCause = "unknown ownership: " + path
			return selectionsForGroups(inventory.GroupsForProfile("full"), "full fallback: "+revision.FullFallbackCause), nil
		}
		for _, group := range matches {
			selected[group.ID] = group
		}
	}
	var groups []Group
	for _, group := range selected {
		groups = append(groups, group)
	}
	sort.Slice(groups, func(a, b int) bool { return groups[a].ID < groups[b].ID })
	selections := selectionsForGroups(groups, "affected path closure")
	for index := range selections {
		if selections[index].Group == "go" {
			packages, err := affectedGoPackages(root, revision.ChangedPaths)
			if err != nil || len(packages) == 0 {
				revision.FullFallbackCause = "Go dependency closure unavailable"
				return selectionsForGroups(inventory.GroupsForProfile("full"), "full fallback: "+revision.FullFallbackCause), nil
			}
			selections[index].Packages = packages
			selections[index].Reason = "changed Go owners and repository-local reverse dependencies"
		}
	}
	return selections, nil
}

func selectionsForGroups(groups []Group, reason string) []Selection {
	selections := make([]Selection, 0, len(groups))
	for _, group := range groups {
		selections = append(selections, Selection{Group: group.ID, Reason: reason, Mode: modeForGroup(group.ID), Dependencies: append([]string(nil), group.Dependencies...)})
	}
	return selections
}

func selectedDependencies(selections []Selection) []string {
	set := map[string]bool{}
	for _, selection := range selections {
		for _, dependency := range selection.Dependencies {
			set[dependency] = true
		}
	}
	var dependencies []string
	for dependency := range set {
		dependencies = append(dependencies, dependency)
	}
	sort.Strings(dependencies)
	return dependencies
}

func modeForGroup(group string) string {
	if group == "go" {
		return "race"
	}
	return "ordinary"
}

type goListPackage struct {
	ImportPath   string
	Dir          string
	Imports      []string
	TestImports  []string
	XTestImports []string
}

func affectedGoPackages(root string, changedPaths []string) ([]string, error) {
	command := exec.Command("go", "list", "-json", "./...")
	command.Dir = root
	output, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(output)
	packages := map[string]goListPackage{}
	for {
		var pkg goListPackage
		if err := decoder.Decode(&pkg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			_ = command.Process.Kill()
			return nil, err
		}
		packages[pkg.ImportPath] = pkg
	}
	if err := command.Wait(); err != nil {
		return nil, err
	}
	direct := map[string]bool{}
	for importPath, pkg := range packages {
		relative, err := filepath.Rel(root, pkg.Dir)
		if err != nil {
			continue
		}
		relative = filepath.ToSlash(relative)
		for _, path := range changedPaths {
			if path == relative || strings.HasPrefix(path, relative+"/") {
				direct[importPath] = true
			}
		}
	}
	closure := map[string]bool{}
	for item := range direct {
		closure[item] = true
	}
	changed := true
	for changed {
		changed = false
		for importPath, pkg := range packages {
			if closure[importPath] {
				continue
			}
			for _, dependency := range append(append(pkg.Imports, pkg.TestImports...), pkg.XTestImports...) {
				if closure[dependency] {
					closure[importPath] = true
					changed = true
					break
				}
			}
		}
	}
	var selected []string
	for importPath := range closure {
		selected = append(selected, importPath)
	}
	sort.Strings(selected)
	return selected, nil
}
