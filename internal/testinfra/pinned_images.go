package testinfra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tetral-ai/tetral/internal/release"
)

// PostgreSQLImage and MinIOImage keep isolated transport fixtures on the same
// canonical images as the runner-owned database and object-store dependencies.
func PostgreSQLImage() string { return postgresImage }
func MinIOImage() string      { return minioImage }

// PinnedBunImage selects the production runtime base, not the host Bun binary.
func PinnedBunImage() (string, error) {
	inventory, err := release.LoadBaseInventory()
	if err != nil {
		return "", err
	}
	for _, entry := range inventory.Entries {
		if entry.Dockerfile == "services/gateway/Dockerfile" && entry.Stage == "final" {
			return entry.Reference, nil
		}
	}
	return "", errors.New("production Gateway Bun runtime image is absent from the base inventory")
}

// PinnedEnvoyImage reads the same immutable dependency used by deployment.
func PinnedEnvoyImage(root string) (string, error) {
	if root == "" {
		return "", errors.New("envoy dependency requires a repository root")
	}
	// The input is the repository root; the suffix is a fixed owned lockfile.
	//nolint:gosec
	body, err := os.ReadFile(filepath.Join(root, "deploy", "dependencies.lock.json"))
	if err != nil {
		return "", err
	}
	var lock struct {
		Schema string `json:"schema"`
		Istio  struct {
			Images []struct {
				Component string `json:"component"`
				Reference string `json:"reference"`
				Digest    string `json:"top_level_digest"`
			} `json:"images"`
		} `json:"istio"`
	}
	if err := json.Unmarshal(body, &lock); err != nil {
		return "", err
	}
	if lock.Schema != "tetral.deployment-dependencies/v1" {
		return "", errors.New("unsupported deployment dependency lock")
	}
	var image string
	for _, entry := range lock.Istio.Images {
		if entry.Component != "proxyv2" {
			continue
		}
		if image != "" || len(entry.Digest) != len("sha256:")+64 || !strings.HasPrefix(entry.Digest, "sha256:") || !strings.HasSuffix(entry.Reference, "@"+entry.Digest) {
			return "", errors.New("invalid or duplicate pinned Envoy image")
		}
		image = entry.Reference
	}
	if image == "" {
		return "", errors.New("pinned Envoy image is absent from deployment dependencies")
	}
	return image, nil
}

func (m *dependencyManager) preparePinnedImage(ctx context.Context, name string) error {
	if err := dockerAvailable(ctx); err != nil {
		return err
	}
	var reference string
	var err error
	switch name {
	case "envoy":
		reference, err = PinnedEnvoyImage(m.root)
	case "bun-image":
		reference, err = PinnedBunImage()
	default:
		return errors.New("unknown pinned test image")
	}
	if err != nil {
		return err
	}
	if err := runQuiet(ctx, "docker", "image", "inspect", reference); err != nil {
		if err := runQuiet(ctx, "docker", "pull", "--platform", "linux/amd64", reference); err != nil {
			return fmt.Errorf("prepare %s pinned image: %w", name, err)
		}
	}
	output, err := dockerOutput(ctx, "image", "inspect", "--format", "{{json .RepoDigests}}", reference)
	if err != nil {
		return err
	}
	var digests []string
	if err := json.Unmarshal([]byte(output), &digests); err != nil {
		return err
	}
	_, expected, found := strings.Cut(reference, "@")
	verified := false
	for _, digest := range digests {
		if found && strings.HasSuffix(digest, "@"+expected) {
			verified = true
		}
	}
	if !verified {
		return fmt.Errorf("%s image digest does not match its canonical lock", name)
	}
	m.evidence = append(m.evidence, DependencyEvidence{Name: name, Source: "canonical-pinned-image", Identity: reference})
	return nil
}
