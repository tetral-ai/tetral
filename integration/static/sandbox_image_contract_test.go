package static

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/runtimeidentity"
)

const sandboxRuntimeBaseReference = "ghcr.io/tetral-ai/mirror/ubuntu:24.04@sha256:ee4d23cbccd3aa96de85ab491b0404aecdda1d458931e9ef1aaba733a1128ba9"

// The projection mount passes --vfs-cache-min-free-space, which Ubuntu's
// rclone (v1.60.1) does not have. Building the sandbox image from the
// distribution package makes every resource projection fail with rclone's
// generic "mount not ready" once its daemon-wait expires, with no other
// diagnostic anywhere. The image therefore installs rclone from upstream,
// and this gate keeps the two in step.
func TestSandboxImageInstallsUpstreamRclone(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	dockerfile, err := os.ReadFile(filepath.Join(engineRoot, "Dockerfile.sandbox"))
	if err != nil {
		t.Fatalf("read Dockerfile.sandbox: %v", err)
	}
	text := string(dockerfile)

	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), "\\"))
		if trimmed == "rclone" {
			t.Fatal("Dockerfile.sandbox installs rclone from the distribution; " +
				"the archived version predates --vfs-cache-min-free-space")
		}
	}
	if !strings.Contains(text, "downloads.rclone.org") {
		t.Fatal("Dockerfile.sandbox does not install rclone from upstream")
	}
	if !strings.Contains(text, "sha256sum -c -") {
		t.Fatal("the upstream rclone download is not checksum-verified")
	}
	if !strings.Contains(text, "--vfs-cache-min-free-space") {
		t.Fatal("the image build does not assert the mount flag the projection requires")
	}
	if strings.Contains(text, "ARG RCLONE_VERSION") {
		t.Fatal("RCLONE_VERSION collides with rclone's own --version flag; use another name")
	}
}

func TestSandboxImageRuntimeIdentityMatchesEngineContract(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	dockerfile := finalArchitectureReadText(t, filepath.Join(engineRoot, "Dockerfile.sandbox"))
	releaseWorkflow := finalArchitectureReadText(t, filepath.Join(engineRoot, ".github", "workflows", "engine-release.yml"))
	proofScript := finalArchitectureReadText(t, filepath.Join(engineRoot, "scripts", "run-helper-privilege-ci.sh"))
	if err := assertSandboxRuntimeBaseJoin(dockerfile, releaseWorkflow, proofScript); err != nil {
		t.Fatal(err)
	}
	if err := assertSandboxRuntimeIdentity(dockerfile); err != nil {
		t.Fatal(err)
	}
}

func TestSandboxImageProofRejectsReleaseRuntimeBaseSubstitution(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	dockerfile := finalArchitectureReadText(t, filepath.Join(engineRoot, "Dockerfile.sandbox"))
	mutated := strings.Replace(dockerfile,
		"ARG SANDBOX_RUNTIME_BASE_IMAGE="+sandboxRuntimeBaseReference,
		"ARG SANDBOX_RUNTIME_BASE_IMAGE=ghcr.io/tetral-ai/mirror/golang:1.25.12",
		1,
	)
	if mutated == dockerfile {
		t.Fatal("runtime-base mutation did not alter Dockerfile.sandbox")
	}
	if err := assertSandboxReleaseRuntimeBase(mutated); err == nil {
		t.Fatal("runtime-base substitution preserved the release-image proof")
	}
}

func assertSandboxReleaseRuntimeBase(dockerfile string) error {
	defaultCount := 0
	lastFrom := ""
	for _, instruction := range dockerfileLogicalInstructions(dockerfile) {
		fields := strings.Fields(instruction)
		if len(fields) == 2 && strings.EqualFold(fields[0], "ARG") && strings.HasPrefix(fields[1], "SANDBOX_RUNTIME_BASE_IMAGE=") {
			if strings.TrimPrefix(fields[1], "SANDBOX_RUNTIME_BASE_IMAGE=") != sandboxRuntimeBaseReference {
				return fmt.Errorf("Sandbox release runtime base is not the selected immutable Ubuntu reference")
			}
			defaultCount++
		}
		if len(fields) > 0 && strings.EqualFold(fields[0], "FROM") {
			lastFrom = instruction
		}
	}
	if defaultCount != 1 || !strings.EqualFold(lastFrom, "FROM ${SANDBOX_RUNTIME_BASE_IMAGE}") {
		return fmt.Errorf("Sandbox release runtime base must be the single immutable Ubuntu default consumed by the final image stage")
	}
	return nil
}

func TestSandboxRuntimeBaseJoinRejectsIndependentReleaseAndProofMutations(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	dockerfile := finalArchitectureReadText(t, filepath.Join(engineRoot, "Dockerfile.sandbox"))
	releaseWorkflow := finalArchitectureReadText(t, filepath.Join(engineRoot, ".github", "workflows", "engine-release.yml"))
	proofScript := finalArchitectureReadText(t, filepath.Join(engineRoot, "scripts", "run-helper-privilege-ci.sh"))
	wrongDigest := strings.Repeat("0", 64)
	wrongReference := "ghcr.io/tetral-ai/mirror/ubuntu:24.04@sha256:" + wrongDigest

	mutations := map[string]struct {
		dockerfile string
		release    string
		proof      string
	}{
		"same_name_wrong_digest": {
			dockerfile: strings.Replace(dockerfile, sandboxRuntimeBaseReference, wrongReference, 1),
			release:    releaseWorkflow,
			proof:      proofScript,
		},
		"release_only": {
			dockerfile: dockerfile,
			release:    strings.Replace(releaseWorkflow, sandboxRuntimeBaseReference, wrongReference, 1),
			proof:      proofScript,
		},
		"proof_only": {
			dockerfile: dockerfile,
			release:    releaseWorkflow,
			proof:      strings.Replace(proofScript, sandboxRuntimeBaseReference, wrongReference, 1),
		},
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			if mutation.dockerfile == dockerfile && mutation.release == releaseWorkflow && mutation.proof == proofScript {
				t.Fatal("mutation did not alter an owner")
			}
			if err := assertSandboxRuntimeBaseJoin(mutation.dockerfile, mutation.release, mutation.proof); err == nil {
				t.Fatal("runtime-base owner mismatch preserved the digest join")
			}
		})
	}
}

func assertSandboxRuntimeBaseJoin(dockerfile string, releaseWorkflow string, proofScript string) error {
	if err := assertSandboxReleaseRuntimeBase(dockerfile); err != nil {
		return err
	}
	releasePattern := regexp.MustCompile(`(?m)^\s*build_args:\s*SANDBOX_RUNTIME_BASE_IMAGE=([^\s]+)\s*$`)
	releaseMatches := releasePattern.FindStringSubmatch(releaseWorkflow)
	if len(releaseMatches) != 2 || releaseMatches[1] != sandboxRuntimeBaseReference {
		return fmt.Errorf("engine-release Sandbox build does not consume the selected runtime-base reference")
	}
	if !strings.Contains(releaseWorkflow, `echo "runtime-base=${BUILD_ARGS#SANDBOX_RUNTIME_BASE_IMAGE=}"`) {
		return fmt.Errorf("engine-release does not report the selected Sandbox runtime-base reference")
	}
	proofPattern := regexp.MustCompile(`(?m)^sandbox_runtime_base_reference="([^\"]+)"$`)
	proofMatches := proofPattern.FindStringSubmatch(proofScript)
	if len(proofMatches) != 2 || proofMatches[1] != sandboxRuntimeBaseReference {
		return fmt.Errorf("Sandbox proof does not consume the selected runtime-base reference")
	}
	return nil
}

func TestSandboxImageRuntimeIdentityJoinRejectsAccountMutations(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	dockerfile := finalArchitectureReadText(t, filepath.Join(engineRoot, "Dockerfile.sandbox"))
	finalAccount := "WORKDIR /workspace\nUSER daytona"
	mutations := map[string]string{
		"user":                 strings.Replace(dockerfile, finalAccount, "RUN usermod --login runner daytona\nWORKDIR /workspace\nUSER runner", 1),
		"home":                 strings.Replace(dockerfile, finalAccount, "RUN usermod --home /srv/daytona daytona\n"+finalAccount, 1),
		"shell":                strings.Replace(dockerfile, finalAccount, "RUN usermod --shell /bin/sh daytona\n"+finalAccount, 1),
		"uppercase_final_user": strings.Replace(dockerfile, "USER daytona\nCMD", "USER root\nCMD", 1),
		"lowercase_final_user": strings.Replace(dockerfile, "USER daytona\nCMD", "USER daytona\nuser root\nCMD", 1),
		"lowercase_run":        strings.Replace(dockerfile, finalAccount, "run usermod --shell /bin/sh daytona\n"+finalAccount, 1),
	}
	for name, mutated := range mutations {
		t.Run(name, func(t *testing.T) {
			if mutated == dockerfile {
				t.Fatalf("%s mutation did not alter Dockerfile.sandbox", name)
			}
			if err := assertSandboxRuntimeIdentity(mutated); err == nil {
				t.Fatalf("%s mutation preserved the Sandbox runtime identity join", name)
			}
		})
	}
}

func TestSandboxImageRuntimeIdentityIgnoresMatchingBuilderAccount(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	dockerfile := finalArchitectureReadText(t, filepath.Join(engineRoot, "Dockerfile.sandbox"))
	mutated := strings.Replace(
		dockerfile,
		"RUN useradd --create-home --shell /bin/bash daytona",
		"RUN useradd --create-home --shell /bin/sh runner",
		1,
	)
	mutated = strings.Replace(
		mutated,
		"WORKDIR /src",
		"RUN useradd --create-home --shell /bin/bash daytona\nWORKDIR /src",
		1,
	)
	if mutated == dockerfile {
		t.Fatal("builder-stage mutation did not alter Dockerfile.sandbox")
	}
	if err := assertSandboxRuntimeIdentity(mutated); err == nil {
		t.Fatal("matching builder-stage account satisfied the final-stage identity contract")
	}
}

type sandboxImageAccount struct {
	user  string
	home  string
	shell string
}

func assertSandboxRuntimeIdentity(dockerfile string) error {
	account, err := effectiveSandboxRuntimeAccount(dockerfile)
	if err != nil {
		return err
	}
	if account.user != runtimeidentity.User || account.home != runtimeidentity.Home || account.shell != runtimeidentity.Shell {
		return fmt.Errorf(
			"Sandbox runtime account = %s/%s/%s; want %s/%s/%s",
			account.user, account.home, account.shell,
			runtimeidentity.User, runtimeidentity.Home, runtimeidentity.Shell,
		)
	}
	return nil
}

// effectiveSandboxRuntimeAccount evaluates only the last Dockerfile stage. The
// release publishes that stage, so builder-stage accounts cannot authorize the
// runtime identity selected by its final USER instruction.
func effectiveSandboxRuntimeAccount(dockerfile string) (sandboxImageAccount, error) {
	accounts := map[string]sandboxImageAccount{}
	finalUser := ""
	instructions, err := finalDockerfileStageInstructions(dockerfile)
	if err != nil {
		return sandboxImageAccount{}, err
	}
	for _, instruction := range instructions {
		fields := strings.Fields(instruction)
		if len(fields) < 2 {
			continue
		}
		if strings.EqualFold(fields[0], "USER") {
			finalUser = strings.SplitN(fields[1], ":", 2)[0]
			continue
		}
		if !strings.EqualFold(fields[0], "RUN") {
			continue
		}
		for index := 1; index < len(fields); index++ {
			switch fields[index] {
			case "useradd":
				account, next, err := parseUseraddAccount(fields, index+1)
				if err != nil {
					return sandboxImageAccount{}, err
				}
				accounts[account.user] = account
				index = next - 1
			case "usermod":
				mutation, next, err := parseUsermodAccount(fields, index+1)
				if err != nil {
					return sandboxImageAccount{}, err
				}
				account, ok := accounts[mutation.user]
				if !ok {
					return sandboxImageAccount{}, fmt.Errorf("Sandbox usermod target %q has no account definition", mutation.user)
				}
				delete(accounts, mutation.user)
				if mutation.home != "" {
					account.home = mutation.home
				}
				if mutation.shell != "" {
					account.shell = mutation.shell
				}
				if mutation.rename != "" {
					account.user = mutation.rename
				}
				accounts[account.user] = account
				index = next - 1
			}
		}
	}
	if finalUser == "" {
		return sandboxImageAccount{}, errors.New("Dockerfile.sandbox has no final USER")
	}
	account, ok := accounts[finalUser]
	if !ok {
		return sandboxImageAccount{}, fmt.Errorf("final Sandbox USER %q has no effective account definition", finalUser)
	}
	return account, nil
}

func parseUseraddAccount(fields []string, start int) (sandboxImageAccount, int, error) {
	account := sandboxImageAccount{}
	createHome := false
	index := start
	for index < len(fields) {
		token := strings.Trim(fields[index], " ;")
		if token == "" {
			index++
			continue
		}
		if token == "&&" {
			break
		}
		switch token {
		case "--create-home":
			createHome = true
		case "--shell":
			index++
			if index >= len(fields) {
				return sandboxImageAccount{}, index, errors.New("Sandbox useradd shell option has no value")
			}
			account.shell = strings.Trim(fields[index], " ;")
		case "--home-dir":
			index++
			if index >= len(fields) {
				return sandboxImageAccount{}, index, errors.New("Sandbox useradd home option has no value")
			}
			account.home = strings.Trim(fields[index], " ;")
		default:
			if strings.HasPrefix(token, "-") {
				return sandboxImageAccount{}, index, fmt.Errorf("unsupported Sandbox useradd option %q", token)
			}
			account.user = token
		}
		index++
	}
	if account.user == "" || account.shell == "" || !createHome {
		return sandboxImageAccount{}, index, fmt.Errorf("Sandbox useradd account is incomplete: %+v", account)
	}
	if account.home == "" {
		account.home = "/home/" + account.user
	}
	return account, index, nil
}

type sandboxAccountMutation struct {
	user   string
	rename string
	home   string
	shell  string
}

func parseUsermodAccount(fields []string, start int) (sandboxAccountMutation, int, error) {
	mutation := sandboxAccountMutation{}
	index := start
	for index < len(fields) {
		token := strings.Trim(fields[index], " ;")
		if token == "" {
			index++
			continue
		}
		if token == "&&" {
			break
		}
		switch token {
		case "--login":
			index++
			if index >= len(fields) {
				return sandboxAccountMutation{}, index, errors.New("Sandbox usermod login option has no value")
			}
			mutation.rename = strings.Trim(fields[index], " ;")
		case "--home":
			index++
			if index >= len(fields) {
				return sandboxAccountMutation{}, index, errors.New("Sandbox usermod home option has no value")
			}
			mutation.home = strings.Trim(fields[index], " ;")
		case "--shell":
			index++
			if index >= len(fields) {
				return sandboxAccountMutation{}, index, errors.New("Sandbox usermod shell option has no value")
			}
			mutation.shell = strings.Trim(fields[index], " ;")
		default:
			if strings.HasPrefix(token, "-") {
				return sandboxAccountMutation{}, index, fmt.Errorf("unsupported Sandbox usermod option %q", token)
			}
			mutation.user = token
		}
		index++
	}
	if mutation.user == "" {
		return sandboxAccountMutation{}, index, errors.New("Sandbox usermod has no target account")
	}
	return mutation, index, nil
}

func finalDockerfileStageInstructions(dockerfile string) ([]string, error) {
	var finalStage []string
	sawStage := false
	for _, instruction := range dockerfileLogicalInstructions(dockerfile) {
		fields := strings.Fields(instruction)
		if len(fields) > 0 && strings.EqualFold(fields[0], "FROM") {
			sawStage = true
			finalStage = nil
			continue
		}
		if sawStage {
			finalStage = append(finalStage, instruction)
		}
	}
	if !sawStage {
		return nil, errors.New("Dockerfile.sandbox has no image stage")
	}
	return finalStage, nil
}

func dockerfileLogicalInstructions(dockerfile string) []string {
	var instructions []string
	current := ""
	for _, sourceLine := range strings.Split(dockerfile, "\n") {
		line := strings.TrimSpace(sourceLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		continued := strings.HasSuffix(line, "\\")
		line = strings.TrimSpace(strings.TrimSuffix(line, "\\"))
		if current != "" {
			current += " "
		}
		current += line
		if !continued {
			instructions = append(instructions, current)
			current = ""
		}
	}
	if current != "" {
		instructions = append(instructions, current)
	}
	return instructions
}
