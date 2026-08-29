package release

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
)

const (
	OCIManifestMediaType = "application/vnd.oci.image.manifest.v1+json"
	OCIEmptyConfigType   = "application/vnd.oci.empty.v1+json"
	ReservationType      = "application/vnd.tetral.release-reservation.v1+json"
	CandidateType        = "application/vnd.tetral.release-candidate.v1+json"
	HelmCandidateType    = "application/vnd.tetral.release-helm-candidate.v1"
	HelmChartLayerType   = "application/vnd.cncf.helm.chart.content.v1.tar+gzip"
	RehearsalType        = "application/vnd.tetral.rehearsal-evidence.v1+json"
	AuthorizationType    = "application/vnd.tetral.release-authorization.v1+json"
	DispositionType      = "application/vnd.tetral.release-disposition.v1+json"
)

type Descriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
}

type OCIArtifactManifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	ArtifactType  string       `json:"artifactType"`
	Config        Descriptor   `json:"config"`
	Layers        []Descriptor `json:"layers"`
}

type OCIArtifact struct {
	Manifest       OCIArtifactManifest
	ManifestJSON   []byte
	ManifestDigest string
	ConfigJSON     []byte
	Layer          []byte
}

func BuildOCIArtifact(artifactType, layerType string, layer []byte) (OCIArtifact, error) {
	if artifactType == "" || layerType == "" || len(layer) == 0 {
		return OCIArtifact{}, fmt.Errorf("OCI artifact media identity or layer is empty")
	}
	config := []byte("{}")
	manifest := OCIArtifactManifest{
		SchemaVersion: 2,
		MediaType:     OCIManifestMediaType,
		ArtifactType:  artifactType,
		Config:        descriptor(OCIEmptyConfigType, config),
		Layers:        []Descriptor{descriptor(layerType, layer)},
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return OCIArtifact{}, err
	}
	return OCIArtifact{Manifest: manifest, ManifestJSON: body, ManifestDigest: digestBytes(body), ConfigJSON: config, Layer: append([]byte(nil), layer...)}, nil
}

func BuildJSONArtifact(artifactType string, value any) (OCIArtifact, error) {
	layer, err := CanonicalJSON(value)
	if err != nil {
		return OCIArtifact{}, err
	}
	return BuildOCIArtifact(artifactType, artifactType, layer)
}

func ValidateOCIArtifact(artifact OCIArtifact, artifactType, layerType string) error {
	if artifact.Manifest.SchemaVersion != 2 || artifact.Manifest.MediaType != OCIManifestMediaType || artifact.Manifest.ArtifactType != artifactType || artifact.Manifest.Config.MediaType != OCIEmptyConfigType || len(artifact.Manifest.Layers) != 1 || artifact.Manifest.Layers[0].MediaType != layerType {
		return fmt.Errorf("OCI artifact shape is invalid")
	}
	if artifact.Manifest.Config != descriptor(OCIEmptyConfigType, artifact.ConfigJSON) || artifact.Manifest.Layers[0] != descriptor(layerType, artifact.Layer) || artifact.ManifestDigest != digestBytes(artifact.ManifestJSON) {
		return fmt.Errorf("OCI artifact descriptor digest differs from its bytes")
	}
	var parsed OCIArtifactManifest
	if err := json.Unmarshal(artifact.ManifestJSON, &parsed); err != nil || !reflect.DeepEqual(parsed, artifact.Manifest) {
		return fmt.Errorf("OCI artifact manifest bytes differ from the declared manifest")
	}
	return nil
}

func descriptor(mediaType string, body []byte) Descriptor {
	return Descriptor{MediaType: mediaType, Digest: digestBytes(body), Size: int64(len(body))}
}

func digestBytes(body []byte) string {
	digest := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(digest[:])
}
