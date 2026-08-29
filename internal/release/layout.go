package release

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	OCIImageIndexMediaType = "application/vnd.oci.image.index.v1+json"
	OCILayoutVersion       = "1.0.0"
)

type OCIIndex struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Manifests     []Descriptor `json:"manifests"`
}

// WriteOCILayout materializes the exact artifact bytes in the standard OCI
// image-layout shape. The caller may later copy the manifest by digest; no
// release transition is allowed to rebuild the layer or manifest.
func WriteOCILayout(root string, artifact OCIArtifact) error {
	if err := ValidateOCIArtifact(artifact, artifact.Manifest.ArtifactType, artifact.Manifest.Layers[0].MediaType); err != nil {
		return err
	}
	if root == "" {
		return fmt.Errorf("OCI layout root is required")
	}
	if err := os.MkdirAll(filepath.Join(root, "blobs", "sha256"), 0o700); err != nil {
		return err
	}
	for digest, body := range map[string][]byte{
		artifact.ManifestDigest:            artifact.ManifestJSON,
		artifact.Manifest.Config.Digest:    artifact.ConfigJSON,
		artifact.Manifest.Layers[0].Digest: artifact.Layer,
	} {
		path, err := ociBlobPath(root, digest)
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, body, 0o600); err != nil {
			return err
		}
	}
	index := OCIIndex{SchemaVersion: 2, MediaType: OCIImageIndexMediaType, Manifests: []Descriptor{{
		MediaType: OCIManifestMediaType, Digest: artifact.ManifestDigest, Size: int64(len(artifact.ManifestJSON)),
	}}}
	indexBody, err := CanonicalJSON(index)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "index.json"), indexBody, 0o600); err != nil {
		return err
	}
	layoutBody, err := CanonicalJSON(map[string]string{"imageLayoutVersion": OCILayoutVersion})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "oci-layout"), layoutBody, 0o600)
}

func ReadOCIArtifact(root, manifestDigest string) (OCIArtifact, error) {
	manifestPath, err := ociBlobPath(root, manifestDigest)
	if err != nil {
		return OCIArtifact{}, err
	}
	manifestBody, err := os.ReadFile(manifestPath) //nolint:gosec // Digest-addressed repository test or release artifact.
	if err != nil {
		return OCIArtifact{}, err
	}
	var manifest OCIArtifactManifest
	if err := json.Unmarshal(manifestBody, &manifest); err != nil {
		return OCIArtifact{}, fmt.Errorf("decode OCI artifact manifest: %w", err)
	}
	if len(manifest.Layers) != 1 {
		return OCIArtifact{}, fmt.Errorf("OCI artifact must contain one layer")
	}
	configPath, err := ociBlobPath(root, manifest.Config.Digest)
	if err != nil {
		return OCIArtifact{}, err
	}
	layerPath, err := ociBlobPath(root, manifest.Layers[0].Digest)
	if err != nil {
		return OCIArtifact{}, err
	}
	config, err := os.ReadFile(configPath) //nolint:gosec // Digest-addressed repository test or release artifact.
	if err != nil {
		return OCIArtifact{}, err
	}
	layer, err := os.ReadFile(layerPath) //nolint:gosec // Digest-addressed repository test or release artifact.
	if err != nil {
		return OCIArtifact{}, err
	}
	artifact := OCIArtifact{Manifest: manifest, ManifestJSON: manifestBody, ManifestDigest: manifestDigest, ConfigJSON: config, Layer: layer}
	if err := ValidateOCIArtifact(artifact, manifest.ArtifactType, manifest.Layers[0].MediaType); err != nil {
		return OCIArtifact{}, err
	}
	return artifact, nil
}

func ReadSingleOCIArtifact(root string) (OCIArtifact, error) {
	layoutBody, err := os.ReadFile(filepath.Join(root, "oci-layout")) //nolint:gosec // Explicit OCI layout input.
	if err != nil {
		return OCIArtifact{}, err
	}
	var layout map[string]string
	if err := json.Unmarshal(layoutBody, &layout); err != nil || len(layout) != 1 || layout["imageLayoutVersion"] != OCILayoutVersion {
		return OCIArtifact{}, fmt.Errorf("OCI layout version is invalid")
	}
	indexBody, err := os.ReadFile(filepath.Join(root, "index.json")) //nolint:gosec // Explicit OCI layout input.
	if err != nil {
		return OCIArtifact{}, err
	}
	var index OCIIndex
	if err := json.Unmarshal(indexBody, &index); err != nil {
		return OCIArtifact{}, fmt.Errorf("decode OCI index: %w", err)
	}
	if index.SchemaVersion != 2 || index.MediaType != OCIImageIndexMediaType || len(index.Manifests) != 1 || index.Manifests[0].MediaType != OCIManifestMediaType {
		return OCIArtifact{}, fmt.Errorf("OCI layout index must identify exactly one image manifest")
	}
	artifact, err := ReadOCIArtifact(root, index.Manifests[0].Digest)
	if err != nil {
		return OCIArtifact{}, err
	}
	if index.Manifests[0] != descriptor(OCIManifestMediaType, artifact.ManifestJSON) {
		return OCIArtifact{}, fmt.Errorf("OCI layout index descriptor differs from manifest bytes")
	}
	return artifact, nil
}

func ociBlobPath(root, digest string) (string, error) {
	if !digestPattern.MatchString(digest) {
		return "", fmt.Errorf("invalid OCI digest %q", digest)
	}
	return filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(digest, "sha256:")), nil
}
