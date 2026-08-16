package runtimeidentity

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSandboxRuntimeIdentityJoinsDockerfileAndHelperHarness(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..", "..")
	dockerfile := readIdentityContractFile(t, filepath.Join(repositoryRoot, "Dockerfile.sandbox"))
	account := parseDockerfileRuntimeAccount(t, dockerfile)
	if account.user != User || account.home != Home || account.shell != Shell {
		t.Fatalf("Dockerfile runtime account = %s/%s/%s; want %s/%s/%s", account.user, account.home, account.shell, User, Home, Shell)
	}
	if account.userDirective != User {
		t.Fatalf("Dockerfile final USER = %s; want %s", account.userDirective, User)
	}

	harness := readIdentityContractFile(t, filepath.Join(repositoryRoot, "scripts", "run-helper-privilege-ci.sh"))
	for _, token := range []string{
		"TETRAL_TEST_RUNTIME_USER",
		"TETRAL_TEST_RUNTIME_HOME",
		"TETRAL_TEST_RUNTIME_SHELL",
		"TETRAL_TEST_RUNTIME_UID",
		"TETRAL_TEST_RUNTIME_GID",
		"internal/sandbox/runtimeidentity/identity.go",
		"Dockerfile.sandbox",
	} {
		if !strings.Contains(harness, token) {
			t.Fatalf("Helper identity harness does not derive %q from an owning source", token)
		}
	}
	for _, forbidden := range []string{
		"daytona:x:1000:1000:Tetral runtime:/home/daytona:/bin/sh",
		"daytona:x:1000:",
	} {
		if strings.Contains(harness, forbidden) {
			t.Fatalf("Helper identity harness retains fabricated account literal %q", forbidden)
		}
	}
	if strconv.Itoa(UID) == "" || strconv.Itoa(GID) == "" {
		t.Fatal("runtime numeric identity is unavailable")
	}
}

func TestSandboxRuntimeIdentityJoinRejectsIndependentDockerfileMutations(t *testing.T) {
	dockerfile := readIdentityContractFile(t, filepath.Join("..", "..", "..", "Dockerfile.sandbox"))
	mutations := map[string]string{
		"user":  strings.Replace(dockerfile, "--shell /bin/bash daytona", "--shell /bin/bash runner", 1),
		"home":  strings.Replace(dockerfile, "--create-home", "--create-home --home-dir /srv/daytona", 1),
		"shell": strings.Replace(dockerfile, "--shell /bin/bash", "--shell /bin/sh", 1),
	}
	for name, mutated := range mutations {
		t.Run(name, func(t *testing.T) {
			account := parseDockerfileRuntimeAccount(t, mutated)
			if account.user == User && account.home == Home && account.shell == Shell && account.userDirective == User {
				t.Fatalf("%s mutation did not break the Dockerfile/runtime identity join", name)
			}
		})
	}
}

type dockerfileRuntimeAccount struct {
	user          string
	home          string
	shell         string
	userDirective string
}

func parseDockerfileRuntimeAccount(t *testing.T, dockerfile string) dockerfileRuntimeAccount {
	t.Helper()
	logicalLines := dockerfileLogicalLines(dockerfile)
	var account dockerfileRuntimeAccount
	for _, line := range logicalLines {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "USER" {
			account.userDirective = fields[1]
		}
		if len(fields) < 3 || fields[0] != "RUN" || fields[1] != "useradd" {
			continue
		}
		createHome := false
		for index := 2; index < len(fields); index++ {
			switch fields[index] {
			case "&&":
				index = len(fields)
			case "--create-home":
				createHome = true
			case "--shell":
				index++
				if index >= len(fields) {
					t.Fatal("Dockerfile runtime user has no shell value")
				}
				account.shell = fields[index]
			case "--home-dir":
				index++
				if index >= len(fields) {
					t.Fatal("Dockerfile runtime user has no home value")
				}
				account.home = fields[index]
			default:
				if account.user == "" && !strings.HasPrefix(fields[index], "-") {
					account.user = fields[index]
				}
			}
		}
		if !createHome {
			t.Fatal("Dockerfile runtime user does not create its home")
		}
		if account.home == "" {
			account.home = "/home/" + account.user
		}
	}
	if account.user == "" || account.shell == "" || account.userDirective == "" {
		t.Fatalf("Dockerfile runtime account is incomplete: %+v", account)
	}
	return account
}

func dockerfileLogicalLines(dockerfile string) []string {
	var logical []string
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
			logical = append(logical, current)
			current = ""
		}
	}
	if current != "" {
		logical = append(logical, current)
	}
	return logical
}

func readIdentityContractFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // Fixed repository contract path.
	if err != nil {
		t.Fatalf("read identity contract %s: %v", path, err)
	}
	return string(body)
}
