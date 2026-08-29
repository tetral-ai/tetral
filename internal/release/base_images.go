package release

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

//go:embed base_images.json
var baseInventoryJSON []byte

type BaseInventory struct {
	Schema   string               `json:"schema"`
	Platform string               `json:"platform"`
	Entries  []BaseInventoryEntry `json:"entries"`
}

type BaseInventoryEntry struct {
	Dockerfile     string `json:"dockerfile"`
	Stage          string `json:"stage"`
	Reference      string `json:"reference"`
	TopLevelDigest string `json:"top_level_digest"`
	ChildDigest    string `json:"linux_amd64_child_digest"`
}

type DockerBase struct {
	Dockerfile string
	Stage      string
	Reference  string
}

var argReferencePattern = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$`)

func LoadBaseInventory() (BaseInventory, error) {
	var inventory BaseInventory
	if err := json.Unmarshal(baseInventoryJSON, &inventory); err != nil {
		return BaseInventory{}, err
	}
	if inventory.Schema != "tetral.release-base-inventory/v1" || inventory.Platform != PlatformLinuxAMD64 || len(inventory.Entries) == 0 {
		return BaseInventory{}, fmt.Errorf("release base inventory is incomplete")
	}
	for _, entry := range inventory.Entries {
		if entry.Dockerfile == "" || entry.Stage == "" || !strings.Contains(entry.Reference, "@"+entry.TopLevelDigest) || !digestPattern.MatchString(entry.TopLevelDigest) || !digestPattern.MatchString(entry.ChildDigest) {
			return BaseInventory{}, fmt.Errorf("release base inventory entry is invalid")
		}
	}
	return inventory, nil
}

func VerifyBaseInventory(root string) error {
	inventory, err := LoadBaseInventory()
	if err != nil {
		return err
	}
	actual := map[string]DockerBase{}
	for _, path := range []string{"Dockerfile", "Dockerfile.sandbox", "services/gateway/Dockerfile", "services/agent-runtime/Dockerfile"} {
		bases, err := parseDockerfileBases(filepath.Join(root, path), path)
		if err != nil {
			return err
		}
		for _, base := range bases {
			actual[base.Dockerfile+"\x00"+base.Stage] = base
		}
	}
	if len(actual) != len(inventory.Entries) {
		return fmt.Errorf("effective Docker base count = %d; want %d", len(actual), len(inventory.Entries))
	}
	for _, entry := range inventory.Entries {
		key := entry.Dockerfile + "\x00" + entry.Stage
		base, ok := actual[key]
		if !ok || base.Reference != entry.Reference {
			return fmt.Errorf("docker base %s/%s differs from the release inventory", entry.Dockerfile, entry.Stage)
		}
	}
	return nil
}

func CandidateBases() ([]BaseIdentity, error) {
	inventory, err := LoadBaseInventory()
	if err != nil {
		return nil, err
	}
	result := make([]BaseIdentity, 0, len(inventory.Entries))
	for _, entry := range inventory.Entries {
		result = append(result, BaseIdentity{Reference: entry.Reference, TopLevelDigest: entry.TopLevelDigest, ChildDigest: entry.ChildDigest, Platform: Platform{OS: "linux", Architecture: "amd64"}})
	}
	return result, nil
}

func parseDockerfileBases(path, displayPath string) ([]DockerBase, error) {
	// Paths are the fixed four repository Dockerfiles selected by VerifyBaseInventory.
	//nolint:gosec
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	args := map[string]string{}
	aliases := map[string]bool{}
	var result []DockerBase
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		fields := strings.Fields(line)
		if len(fields) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "ARG":
			parts := strings.SplitN(fields[1], "=", 2)
			if len(parts) == 2 {
				args[parts[0]] = parts[1]
			}
		case "FROM":
			if len(fields) < 2 {
				return nil, fmt.Errorf("%s contains malformed FROM", displayPath)
			}
			reference := fields[1]
			if match := argReferencePattern.FindStringSubmatch(reference); match != nil {
				reference = args[match[1]]
			}
			stage := "final"
			if len(fields) >= 4 && strings.EqualFold(fields[len(fields)-2], "AS") {
				stage = fields[len(fields)-1]
				aliases[stage] = true
			}
			if aliases[reference] {
				continue
			}
			if reference == "" || !strings.Contains(reference, "@sha256:") {
				return nil, fmt.Errorf("%s stage %s uses a mutable base %q", displayPath, stage, reference)
			}
			result = append(result, DockerBase{Dockerfile: displayPath, Stage: stage, Reference: reference})
		}
	}
	return result, scanner.Err()
}
