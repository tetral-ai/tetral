package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	releasecontract "github.com/tetral-ai/tetral/internal/release"
)

func main() {
	if len(os.Args) < 2 {
		fail("usage: tetral-release <validate-version|validate-candidate|validate-rehearsal|validate-authorization|artifact|validate-layout|state|promotion-plan|cleanup-plan|verify-bases|environment-plan|package-preflight>")
	}
	var err error
	switch os.Args[1] {
	case "validate-version":
		err = validateVersion(os.Args[2:])
	case "validate-candidate":
		err = validateCandidate(os.Args[2:])
	case "validate-rehearsal":
		err = validateRehearsal(os.Args[2:])
	case "validate-authorization":
		err = validateAuthorization(os.Args[2:])
	case "artifact":
		err = buildArtifact(os.Args[2:])
	case "validate-layout":
		err = validateLayout(os.Args[2:])
	case "state":
		err = printState(os.Args[2:])
	case "promotion-plan":
		err = printPromotionPlan(os.Args[2:])
	case "cleanup-plan":
		err = printCleanupPlan(os.Args[2:])
	case "verify-bases":
		err = verifyBases(os.Args[2:])
	case "environment-plan":
		err = environmentPlan(os.Args[2:])
	case "package-preflight":
		err = packagePreflight(os.Args[2:])
	default:
		err = fmt.Errorf("unknown release command %q", os.Args[1])
	}
	if err != nil {
		fail(err.Error())
	}
}

func validateVersion(arguments []string) error {
	flags := flag.NewFlagSet("validate-version", flag.ContinueOnError)
	value := flags.String("version", "", "numeric Alpha Git version")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	version, err := releasecontract.ParseVersion(*value)
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, version)
}

func validateCandidate(arguments []string) error {
	flags := flag.NewFlagSet("validate-candidate", flag.ContinueOnError)
	input := flags.String("input", "", "candidate JSON")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	var candidate releasecontract.CandidateManifest
	if err := readJSON(*input, &candidate); err != nil {
		return err
	}
	if err := releasecontract.ValidateCandidate(candidate); err != nil {
		return err
	}
	return writeJSON(os.Stdout, candidate)
}

func validateRehearsal(arguments []string) error {
	flags := flag.NewFlagSet("validate-rehearsal", flag.ContinueOnError)
	candidatePath := flags.String("candidate", "", "candidate JSON")
	candidateDigest := flags.String("candidate-digest", "", "Candidate Manifest digest")
	evidencePath := flags.String("evidence", "", "rehearsal evidence JSON")
	nowValue := flags.String("now", "", "RFC3339 decision time")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	var candidate releasecontract.CandidateManifest
	var evidence releasecontract.RehearsalEvidence
	if err := readJSON(*candidatePath, &candidate); err != nil {
		return err
	}
	if err := readJSON(*evidencePath, &evidence); err != nil {
		return err
	}
	now, err := parseTime(*nowValue)
	if err != nil {
		return err
	}
	return releasecontract.ValidateRehearsal(candidate, *candidateDigest, evidence, now)
}

func validateAuthorization(arguments []string) error {
	flags := flag.NewFlagSet("validate-authorization", flag.ContinueOnError)
	candidatePath := flags.String("candidate", "", "candidate JSON")
	candidateDigest := flags.String("candidate-digest", "", "Candidate Manifest digest")
	evidenceDigest := flags.String("evidence-digest", "", "Rehearsal Evidence digest")
	authorizationPath := flags.String("authorization", "", "authorization JSON")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	var candidate releasecontract.CandidateManifest
	var authorization releasecontract.Authorization
	if err := readJSON(*candidatePath, &candidate); err != nil {
		return err
	}
	if err := readJSON(*authorizationPath, &authorization); err != nil {
		return err
	}
	return releasecontract.ValidateAuthorization(candidate, *candidateDigest, *evidenceDigest, authorization)
}

func buildArtifact(arguments []string) error {
	flags := flag.NewFlagSet("artifact", flag.ContinueOnError)
	kind := flags.String("kind", "", "reservation, candidate, rehearsal, authorization, disposition, or helm-candidate")
	input := flags.String("input", "", "canonical layer bytes")
	output := flags.String("output", "", "output directory")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	body, err := os.ReadFile(*input)
	if err != nil {
		return err
	}
	artifactType, layerType, err := artifactMedia(*kind)
	if err != nil {
		return err
	}
	if *kind != "helm-candidate" {
		var value any
		if err := json.Unmarshal(body, &value); err != nil {
			return fmt.Errorf("decode JSON artifact layer: %w", err)
		}
		body, err = releasecontract.CanonicalJSON(value)
		if err != nil {
			return err
		}
	}
	artifact, err := releasecontract.BuildOCIArtifact(artifactType, layerType, body)
	if err != nil {
		return err
	}
	if err := releasecontract.ValidateOCIArtifact(artifact, artifactType, layerType); err != nil {
		return err
	}
	if *output == "" {
		return fmt.Errorf("output directory is required")
	}
	if err := os.MkdirAll(*output, 0o700); err != nil {
		return err
	}
	if err := releasecontract.WriteOCILayout(*output, artifact); err != nil {
		return err
	}
	return writeJSON(os.Stdout, map[string]string{"manifest_digest": artifact.ManifestDigest, "layout": filepath.Clean(*output)})
}

func validateLayout(arguments []string) error {
	flags := flag.NewFlagSet("validate-layout", flag.ContinueOnError)
	kind := flags.String("kind", "", "reservation, candidate, rehearsal, authorization, disposition, or helm-candidate")
	root := flags.String("root", "", "OCI layout root")
	output := flags.String("output-layer", "", "validated layer output")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	artifactType, layerType, err := artifactMedia(*kind)
	if err != nil {
		return err
	}
	artifact, err := releasecontract.ReadSingleOCIArtifact(*root)
	if err != nil {
		return err
	}
	if err := releasecontract.ValidateOCIArtifact(artifact, artifactType, layerType); err != nil {
		return err
	}
	if *kind != "helm-candidate" {
		if err := releasecontract.ValidateCanonicalJSONLayer(artifact); err != nil {
			return err
		}
	}
	if *output == "" {
		return fmt.Errorf("validated layer output is required")
	}
	if err := os.WriteFile(*output, artifact.Layer, 0o600); err != nil {
		return err
	}
	return writeJSON(os.Stdout, map[string]string{"manifest_digest": artifact.ManifestDigest, "layer_digest": artifact.Manifest.Layers[0].Digest})
}

func printState(arguments []string) error {
	facts, now, err := factsFlags("state", arguments)
	if err != nil {
		return err
	}
	state, err := releasecontract.Reconstruct(facts, now)
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, map[string]any{"state": state, "facts": facts})
}

func printPromotionPlan(arguments []string) error {
	facts, now, err := factsFlags("promotion-plan", arguments)
	if err != nil {
		return err
	}
	steps, err := releasecontract.PromotionPlan(facts, now)
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, map[string]any{"steps": steps})
}

func printCleanupPlan(arguments []string) error {
	flags := flag.NewFlagSet("cleanup-plan", flag.ContinueOnError)
	input := flags.String("input", "", "cleanup candidates JSON")
	nowValue := flags.String("now", "", "RFC3339 decision time")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	var candidates []releasecontract.CleanupCandidate
	if err := readJSON(*input, &candidates); err != nil {
		return err
	}
	now, err := parseTime(*nowValue)
	if err != nil {
		return err
	}
	selected, err := releasecontract.CleanupPlan(candidates, now)
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, map[string]any{"dry_run": true, "candidates": selected})
}

func verifyBases(arguments []string) error {
	flags := flag.NewFlagSet("verify-bases", flag.ContinueOnError)
	root := flags.String("repository-root", ".", "repository root")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := releasecontract.VerifyBaseInventory(*root); err != nil {
		return err
	}
	bases, err := releasecontract.CandidateBases()
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, map[string]any{"platform": releasecontract.PlatformLinuxAMD64, "bases": bases})
}

func environmentPlan(arguments []string) error {
	flags := flag.NewFlagSet("environment-plan", flag.ContinueOnError)
	repository := flags.String("repository", "", "owner/repository")
	reviewerType := flags.String("reviewer-type", "User", "GitHub reviewer type")
	reviewerID := flags.Int64("reviewer-id", 0, "GitHub reviewer database ID")
	preState := flags.String("pre-state", "", "normalized release environment snapshot")
	output := flags.String("output", "", "output bundle")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	var pre releasecontract.EnvironmentSnapshot
	if err := readJSON(*preState, &pre); err != nil {
		return err
	}
	bundle, err := releasecontract.BuildEnvironmentBundle(*repository, releasecontract.EnvironmentReviewer{Type: *reviewerType, ID: *reviewerID}, pre)
	if err != nil {
		return err
	}
	if err := releasecontract.VerifyEnvironmentBundle(bundle); err != nil {
		return err
	}
	body, err := releasecontract.CanonicalJSON(bundle)
	if err != nil {
		return err
	}
	if *output == "" {
		return fmt.Errorf("output path is required")
	}
	return os.WriteFile(*output, append(body, '\n'), 0o600)
}

func packagePreflight(arguments []string) error {
	flags := flag.NewFlagSet("package-preflight", flag.ContinueOnError)
	input := flags.String("input", "", "normalized package inventory")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	var preflight releasecontract.PackagePreflight
	if err := readJSON(*input, &preflight); err != nil {
		return err
	}
	if err := releasecontract.ValidatePackagePreflights(preflight); err != nil {
		return err
	}
	return writeJSON(os.Stdout, preflight)
}

func factsFlags(name string, arguments []string) (releasecontract.Facts, time.Time, error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	input := flags.String("facts", "", "release facts JSON")
	nowValue := flags.String("now", "", "RFC3339 decision time")
	if err := flags.Parse(arguments); err != nil {
		return releasecontract.Facts{}, time.Time{}, err
	}
	var facts releasecontract.Facts
	if err := readJSON(*input, &facts); err != nil {
		return releasecontract.Facts{}, time.Time{}, err
	}
	now, err := parseTime(*nowValue)
	return facts, now, err
}

func parseTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("explicit RFC3339 decision time is required")
	}
	return time.Parse(time.RFC3339, value)
}

func artifactMedia(kind string) (string, string, error) {
	switch kind {
	case "reservation":
		return releasecontract.ReservationType, releasecontract.ReservationType, nil
	case "candidate":
		return releasecontract.CandidateType, releasecontract.CandidateType, nil
	case "rehearsal":
		return releasecontract.RehearsalType, releasecontract.RehearsalType, nil
	case "authorization":
		return releasecontract.AuthorizationType, releasecontract.AuthorizationType, nil
	case "disposition":
		return releasecontract.DispositionType, releasecontract.DispositionType, nil
	case "helm-candidate":
		return releasecontract.HelmCandidateType, releasecontract.HelmChartLayerType, nil
	default:
		return "", "", fmt.Errorf("unknown OCI artifact kind %q", kind)
	}
}

func readJSON(path string, destination any) error {
	if path == "" {
		return fmt.Errorf("input path is required")
	}
	body, err := os.ReadFile(path) //nolint:gosec // Operator-selected CLI input is the command's explicit contract.
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, destination); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func writeJSON(file *os.File, value any) error {
	body, err := releasecontract.CanonicalJSON(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(file, "%s\n", body)
	return err
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
