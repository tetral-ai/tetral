package testinfra

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const EnvTestEGCTL = "TETRAL_TEST_EGCTL"

//nolint:gosec // G101: environment-variable name for a test executable path, not a credential.
const EnvTestEGCTLSecretTranslator = "TETRAL_TEST_EGCTL_SECRET_TRANSLATOR"

type edgeDependencyLock struct {
	Schema  string `json:"schema"`
	Gateway struct {
		SecretHelper struct {
			Directory string `json:"module_directory"`
			Module    string `json:"upstream_module"`
			Version   string `json:"upstream_version"`
			Toolchain string `json:"go_toolchain"`
		} `json:"secret_helper"`
		Version string `json:"version"`
		Images  []struct {
			Component string `json:"component"`
			Reference string `json:"reference"`
			Digest    string `json:"top_level_digest"`
		} `json:"images"`
		Tools []struct {
			OS      string `json:"os"`
			Arch    string `json:"arch"`
			Version string `json:"version"`
			URL     string `json:"url"`
			SHA256  string `json:"sha256"`
		} `json:"egctl"`
	} `json:"envoy_gateway"`
}

func loadEdgeDependencyLock(root string) (edgeDependencyLock, error) {
	var lock edgeDependencyLock
	if root == "" {
		return lock, errors.New("edge prerequisite requires a repository root")
	}
	//nolint:gosec // Fixed repository-owned lock relative to the runner's root.
	body, err := os.ReadFile(filepath.Join(root, "deploy", "dependencies.lock.json"))
	if err != nil {
		return lock, err
	}
	if err := json.Unmarshal(body, &lock); err != nil {
		return lock, err
	}
	if lock.Schema != "tetral.deployment-dependencies/v1" || lock.Gateway.Version != "1.9.2" {
		return lock, errors.New("unsupported edge prerequisite lock")
	}
	return lock, nil
}

// PinnedEdgeEnvoyImage is distinct from the existing Istio proxy dependency.
func PinnedEdgeEnvoyImage(root string) (string, error) {
	lock, err := loadEdgeDependencyLock(root)
	if err != nil {
		return "", err
	}
	image := ""
	for _, entry := range lock.Gateway.Images {
		if entry.Component != "proxy" {
			continue
		}
		if image != "" || len(entry.Digest) != 71 || !strings.HasPrefix(entry.Digest, "sha256:") || !strings.HasSuffix(entry.Reference, "@"+entry.Digest) {
			return "", errors.New("invalid or duplicate edge Envoy image")
		}
		image = entry.Reference
	}
	if image == "" {
		return "", errors.New("edge Envoy image is missing from lock")
	}
	return image, nil
}

func (m *dependencyManager) prepareEGCTL(ctx context.Context) error {
	lock, err := loadEdgeDependencyLock(m.root)
	if err != nil {
		return err
	}
	selected := -1
	for index, tool := range lock.Gateway.Tools {
		if tool.OS == runtime.GOOS && tool.Arch == runtime.GOARCH {
			if selected != -1 {
				return errors.New("duplicate egctl platform lock")
			}
			selected = index
		}
	}
	if selected == -1 {
		return errors.New("egctl platform is unsupported by the deployment lock")
	}
	tool := lock.Gateway.Tools[selected]
	if tool.Version != lock.Gateway.Version || len(tool.SHA256) != 64 || !strings.HasPrefix(tool.URL, "https://github.com/envoyproxy/gateway/releases/download/v"+tool.Version+"/") {
		return errors.New("invalid egctl source lock")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, tool.URL, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Minute}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("download locked egctl: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("locked egctl download status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 128<<20))
	if err != nil {
		return err
	}
	digest := sha256.Sum256(body)
	if hex.EncodeToString(digest[:]) != tool.SHA256 {
		return errors.New("egctl archive checksum differs from lock")
	}
	gzipReader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = gzipReader.Close() }()
	tarReader := tar.NewReader(gzipReader)
	wanted := "bin/" + runtime.GOOS + "/" + runtime.GOARCH + "/egctl"
	var executable []byte
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if strings.TrimPrefix(header.Name, "./") != wanted {
			continue
		}
		if header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > 256<<20 || executable != nil {
			return errors.New("invalid egctl archive executable")
		}
		executable, err = io.ReadAll(io.LimitReader(tarReader, header.Size+1))
		if err != nil || int64(len(executable)) != header.Size {
			return errors.New("incomplete egctl executable")
		}
	}
	if len(executable) == 0 {
		return errors.New("egctl archive lacks the selected executable")
	}
	directory, err := os.MkdirTemp("", "tetral-egctl-")
	if err != nil {
		return err
	}
	m.directories = append(m.directories, directory)
	m.directoryDependencies = append(m.directoryDependencies, "egctl")
	path := filepath.Join(directory, "egctl")
	//nolint:gosec // G306: checksum-verified egctl executable needs owner execute permission in the runner-owned temporary directory.
	if err := os.WriteFile(path, executable, 0700); err != nil {
		return err
	}
	//nolint:gosec // Checksum-verified locked executable, fixed version argument.
	output, err := exec.CommandContext(ctx, path, "version").CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "client: v"+tool.Version {
		return errors.New("egctl executable version differs from lock")
	}
	m.environment = append(withoutEnvironmentVariable(m.environment, EnvTestEGCTL), EnvTestEGCTL+"="+path)
	m.evidence = append(m.evidence, DependencyEvidence{Name: "egctl", Source: "canonical-pinned-archive", Identity: "v" + tool.Version + "/" + tool.OS + "/" + tool.Arch + "@sha256:" + tool.SHA256})
	helper := lock.Gateway.SecretHelper
	if helper.Directory != "integration/envoy-gateway-secret-helper" || helper.Module != "github.com/envoyproxy/gateway" || helper.Version != "v"+lock.Gateway.Version || helper.Toolchain != "go1.26.8" {
		return errors.New("unsupported egctl SecretType helper lock")
	}
	helperDirectory := filepath.Join(m.root, helper.Directory)
	sourceDigest, err := helperSourceDigest(helperDirectory)
	if err != nil {
		return err
	}
	helperPath := filepath.Join(directory, "secret-translator")
	//nolint:gosec // G204: fixed Go build arguments, validated repository helper directory and runner-owned temporary output; no shell.
	command := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-p", "2", "-o", helperPath, ".")
	command.Dir = helperDirectory
	command.Env = append(withoutEnvironmentVariable(m.environment, "GOTOOLCHAIN"), "GOTOOLCHAIN="+helper.Toolchain)
	if err := command.Run(); err != nil {
		return errors.New("pinned upstream SecretType helper build failed")
	}
	afterDigest, err := helperSourceDigest(helperDirectory)
	if err != nil || sourceDigest != afterDigest {
		return errors.New("SecretType helper source changed during build")
	}
	info, err := buildinfo.ReadFile(helperPath)
	if err != nil || info.GoVersion != helper.Toolchain {
		return errors.New("SecretType helper actual Go toolchain differs from lock")
	}
	foundModule := false
	for _, dependency := range info.Deps {
		if dependency.Path == helper.Module && dependency.Version == helper.Version && dependency.Replace == nil {
			foundModule = true
		}
	}
	if !foundModule {
		return errors.New("SecretType helper actual upstream module differs from lock")
	}
	//nolint:gosec // Fixed helper path owned by this dependency directory.
	helperBytes, err := os.ReadFile(helperPath)
	if err != nil {
		return err
	}
	helperDigest := sha256.Sum256(helperBytes)
	m.environment = append(withoutEnvironmentVariable(m.environment, EnvTestEGCTLSecretTranslator), EnvTestEGCTLSecretTranslator+"="+helperPath)
	m.evidence = append(m.evidence, DependencyEvidence{Name: "egctl", Source: "pinned-upstream-secret-translator", Identity: helper.Module + "@" + helper.Version + "/" + helper.Toolchain + "/source-sha256:" + sourceDigest + "/binary-sha256:" + hex.EncodeToString(helperDigest[:])})
	return nil
}

// helperSourceDigest binds the complete authored Go source and module graph.
// Upstream dependency bytes are independently protected by go.sum and readonly
// module resolution; build metadata verifies the actual upstream/toolchain.
func helperSourceDigest(directory string) (string, error) {
	names := []string{}
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("SecretType helper source cannot be a symlink")
		}
		if strings.HasSuffix(path, ".go") || entry.Name() == "go.mod" || entry.Name() == "go.sum" {
			names = append(names, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(names)
	digest := sha256.New()
	for _, path := range names {
		name, err := filepath.Rel(directory, path)
		if err != nil {
			return "", err
		}
		//nolint:gosec // G304: WalkDir admits only Go/module source below the owned helper directory and rejects symlink entries.
		body, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		_, _ = digest.Write([]byte(name))
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(body)
		_, _ = digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
