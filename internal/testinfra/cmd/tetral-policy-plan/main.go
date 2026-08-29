package main

import (
	"bytes"
	"crypto/sha1" //nolint:gosec // Git object identity is SHA-1 by repository format, not a security signature.
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	repository := flag.String("repository", "", "GitHub owner/repository")
	repositoryRoot := flag.String("repository-root", ".", "local repository root containing the archived source commit")
	ruleset := flag.String("ruleset", "", "captured repository ruleset JSON")
	repositorySettings := flag.String("repository-settings", "", "captured repository settings JSON")
	actions := flag.String("actions", "", "captured repository Actions policy JSON")
	legacyArchive := flag.String("legacy-archive", "", "complete git archive of the legacy-capable source tree")
	legacyProof := flag.String("legacy-proof-result", "", "passing Full-profile result from the exact archived source tree")
	legacyWorkflow := flag.String("legacy-workflow", ".github/workflows/engine-ci.yml", "legacy workflow in the current source tree")
	sourceCommit := flag.String("source-commit", "", "exact source commit archived for rollback")
	treeSHA := flag.String("tree-sha", "", "exact source tree archived for rollback")
	output := flag.String("output", "github-policy-cutover.json", "ordered cutover bundle")
	flag.Parse()
	if *repository == "" || *ruleset == "" || *repositorySettings == "" || *actions == "" || *legacyArchive == "" || *legacyProof == "" || *sourceCommit == "" || *treeSHA == "" {
		fatal(fmt.Errorf("repository, three pre-state captures, archive, Full proof, source commit, and tree SHA are required"))
	}
	pre, err := testinfra.ReadPolicyPreState(*ruleset, *repositorySettings, *actions)
	if err != nil {
		fatal(err)
	}
	workflowBody, err := gitOutput(*repositoryRoot, "show", *sourceCommit+":"+*legacyWorkflow)
	if err != nil {
		fatal(err)
	}
	archiveBody, err := os.ReadFile(*legacyArchive) //nolint:gosec // explicit operator-created immutable git archive.
	if err != nil {
		fatal(err)
	}
	if err := verifyArchiveIdentity(*repositoryRoot, *sourceCommit, *treeSHA, archiveBody); err != nil {
		fatal(err)
	}
	archiveDigest := sha256.Sum256(archiveBody)
	proofBody, err := os.ReadFile(*legacyProof) //nolint:gosec // explicit operator-owned result artifact.
	if err != nil {
		fatal(err)
	}
	var proof testinfra.Result
	if err := json.Unmarshal(proofBody, &proof); err != nil || proof.Status != "pass" || proof.Plan.Profile != testinfra.ProfileFull ||
		proof.Plan.Revision.Head != *sourceCommit || proof.Plan.Revision.WorktreeDirty {
		fatal(fmt.Errorf("legacy workflow proof is not a passing Full result for the exact clean source commit"))
	}
	proofDigest := sha256.Sum256(proofBody)
	gitObject := append([]byte(fmt.Sprintf("blob %d\x00", len(workflowBody))), workflowBody...)
	blobDigest := sha1.Sum(gitObject) //nolint:gosec // Git object identity uses the repository's SHA-1 format.
	bundle, err := testinfra.BuildGitHubPolicyBundle(*repository, pre, testinfra.LegacyWorkflowArchive{
		SourceCommit: *sourceCommit, TreeSHA: *treeSHA, Path: *legacyWorkflow,
		BlobSHA: hex.EncodeToString(blobDigest[:]), ArchiveSHA: "sha256:" + hex.EncodeToString(archiveDigest[:]),
		ProofResultSHA: "sha256:" + hex.EncodeToString(proofDigest[:]), ProofCommand: []string{"make", "test-full"},
		RequiredContexts: testinfra.LegacyRequiredChecks(),
	})
	if err != nil {
		fatal(err)
	}
	if err := testinfra.VerifyGitHubPolicyBundle(bundle); err != nil {
		fatal(err)
	}
	for failurePoint := 0; failurePoint <= len(bundle.Transitions); failurePoint++ {
		if err := testinfra.RehearseGitHubPolicyRollback(bundle, failurePoint); err != nil {
			fatal(err)
		}
	}
	if err := testinfra.RehearseFinalStateRecovery(bundle); err != nil {
		fatal(err)
	}
	body, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*output, append(body, '\n'), 0o600); err != nil { //nolint:gosec // explicit operator-owned bundle path.
		fatal(err)
	}
}

func verifyArchiveIdentity(repositoryRoot, sourceCommit, treeSHA string, archiveBody []byte) error {
	head, err := gitOutput(repositoryRoot, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(head)) != sourceCommit {
		return fmt.Errorf("source commit is not the current final HEAD")
	}
	status, err := gitOutput(repositoryRoot, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(status)) != "" {
		return fmt.Errorf("repository must be clean before creating the rollback archive")
	}
	resolvedCommit, err := gitOutput(repositoryRoot, "rev-parse", sourceCommit+"^{commit}")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(resolvedCommit)) != sourceCommit {
		return fmt.Errorf("source commit is not the exact resolved commit")
	}
	resolvedTree, err := gitOutput(repositoryRoot, "rev-parse", sourceCommit+"^{tree}")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(resolvedTree)) != treeSHA {
		return fmt.Errorf("source tree does not match the archived commit")
	}
	expectedArchive, err := gitOutput(repositoryRoot, "archive", "--format=tar", sourceCommit)
	if err != nil {
		return err
	}
	if !bytes.Equal(expectedArchive, archiveBody) {
		return fmt.Errorf("legacy archive bytes do not match git archive for the source commit")
	}
	return nil
}

func gitOutput(repositoryRoot string, args ...string) ([]byte, error) {
	command := exec.Command("git", append([]string{"-C", repositoryRoot}, args...)...) //nolint:gosec // fixed executable with operator-supplied repository identity.
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return output, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
