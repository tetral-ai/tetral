package testinfra

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//go:embed actions.json
var actionInventoryJSON []byte

type ActionInventory struct {
	Version    int            `json:"version"`
	ReviewedAt string         `json:"reviewed_at"`
	Actions    []ActionRecord `json:"actions"`
}

type ActionRecord struct {
	Repository string `json:"repository"`
	SHA        string `json:"sha"`
	ReleaseTag string `json:"release_tag"`
	ObjectType string `json:"object_type"`
	TargetSHA  string `json:"target_sha"`
}

func LoadActionInventory() (ActionInventory, error) {
	var inventory ActionInventory
	if err := json.Unmarshal(actionInventoryJSON, &inventory); err != nil {
		return ActionInventory{}, fmt.Errorf("decode Action inventory: %w", err)
	}
	if inventory.Version != 1 || len(inventory.Actions) == 0 {
		return ActionInventory{}, fmt.Errorf("unsupported or empty Action inventory")
	}
	if _, err := time.Parse(time.DateOnly, inventory.ReviewedAt); err != nil {
		return ActionInventory{}, fmt.Errorf("action inventory review date is malformed")
	}
	seen := map[string]bool{}
	for _, action := range inventory.Actions {
		if action.Repository == "" || len(action.SHA) != 40 || len(action.TargetSHA) != 40 ||
			!isExactReleaseTag(action.ReleaseTag) || action.ObjectType != "commit" && action.ObjectType != "annotated_tag" || seen[action.Repository] {
			return ActionInventory{}, fmt.Errorf("invalid Action inventory row %q", action.Repository)
		}
		if action.ObjectType == "commit" && action.TargetSHA != action.SHA || action.ObjectType == "annotated_tag" && action.TargetSHA == action.SHA {
			return ActionInventory{}, fmt.Errorf("invalid Action provenance for %q", action.Repository)
		}
		seen[action.Repository] = true
	}
	return inventory, nil
}

func isExactReleaseTag(value string) bool {
	parts := strings.Split(strings.TrimPrefix(value, "v"), ".")
	if len(parts) != 3 || !strings.HasPrefix(value, "v") {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, character := range part {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}

// WorkflowActionReferences parses workflow and composite-action structure. It
// does not treat comments or arbitrary text as executable Action references.
func WorkflowActionReferences(root string) (map[string]string, error) {
	result := map[string]string{}
	for _, directory := range []string{filepath.Join(root, ".github", "workflows"), filepath.Join(root, ".github", "actions")} {
		err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || (filepath.Ext(path) != ".yml" && filepath.Ext(path) != ".yaml") {
				return nil
			}
			// Paths are rooted in the repository-owned workflow directories.
			//nolint:gosec
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			document, err := parseWorkflowYAML(body, filepath.Base(path))
			if err != nil {
				return err
			}
			return collectActionReferences(document, result)
		})
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	return result, nil
}

func collectActionReferences(node *workflowYAMLNode, result map[string]string) error {
	if node.Kind == workflowYAMLMapping {
		for index := 0; index+1 < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if key.Value == "uses" && value.Kind == workflowYAMLScalar {
				reference := value.Value
				if strings.HasPrefix(reference, "./") || strings.HasPrefix(reference, "docker://") {
					continue
				}
				parts := strings.Split(reference, "@")
				if len(parts) != 2 || len(parts[1]) != 40 {
					return fmt.Errorf("action reference %q is not a full SHA", reference)
				}
				if prior, exists := result[parts[0]]; exists && prior != parts[1] {
					return fmt.Errorf("action repository %q uses more than one SHA", parts[0])
				}
				result[parts[0]] = parts[1]
			}
		}
	}
	for _, child := range node.Content {
		if err := collectActionReferences(child, result); err != nil {
			return err
		}
	}
	return nil
}

func VerifyActionInventory(root string, inventory ActionInventory) error {
	references, err := WorkflowActionReferences(root)
	if err != nil {
		return err
	}
	wanted := map[string]string{}
	for _, action := range inventory.Actions {
		wanted[action.Repository] = action.SHA
	}
	for repository, sha := range references {
		if wanted[repository] != sha {
			return fmt.Errorf("action %s@%s is not approved", repository, sha)
		}
		delete(wanted, repository)
	}
	for repository := range wanted {
		return fmt.Errorf("approved Action %s is not used", repository)
	}
	return nil
}
