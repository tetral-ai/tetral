package static

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// enginePackage is a package of one of Engine's Go modules as the go tool
// lists it, named by its directory under the repository root.
type enginePackage struct {
	Directory  string
	Production []string
	Tests      []string
}

// engineModules are the directories of Engine's Go modules: the repository's
// own and the Envoy Gateway secret helper's.
var engineModules = []string{".", "integration/envoy-gateway-secret-helper"}

func enginePackages(t *testing.T) []enginePackage {
	t.Helper()
	root, err := filepath.Abs(finalArchitectureEngineRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	var packages []enginePackage
	for _, module := range engineModules {
		command := exec.Command("go", "list", "-json=Dir,Imports,TestImports,XTestImports", "./...")
		command.Dir = filepath.Join(root, module)
		var stderr bytes.Buffer
		command.Stderr = &stderr
		output, err := command.Output()
		if err != nil {
			t.Fatalf("go list in %s: %v\n%s", module, err, stderr.String())
		}
		for decoder := json.NewDecoder(bytes.NewReader(output)); ; {
			var listed struct {
				Dir                                string
				Imports, TestImports, XTestImports []string
			}
			if err := decoder.Decode(&listed); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("decode go list in %s: %v", module, err)
			}
			directory, err := filepath.Rel(root, listed.Dir)
			if err != nil {
				t.Fatal(err)
			}
			packages = append(packages, enginePackage{
				Directory:  filepath.ToSlash(directory),
				Production: listed.Imports,
				Tests:      append(listed.TestImports, listed.XTestImports...),
			})
		}
	}
	if len(packages) == 0 {
		t.Fatal("go list named no package")
	}
	return packages
}

// dependencyOwner names the packages that may import a dependency. The
// packages below an owner may as well. Test files are bound only where
// testsToo says so, and then testOwners may import it from their tests.
type dependencyOwner struct {
	dependency string
	exact      bool
	owners     []string
	testsToo   bool
	testOwners []string
}

func (rule dependencyOwner) matches(imported string) bool {
	if rule.exact {
		return imported == rule.dependency
	}
	return strings.HasPrefix(imported, rule.dependency)
}

// The first rule that matches an import decides it.
var dependencyOwners = []dependencyOwner{
	// Object storage is reached through the BlobStore interface.
	{dependency: "github.com/aws/aws-sdk-go", owners: []string{"internal/blob"}, testsToo: true},
	{dependency: "github.com/aws/smithy-go", owners: []string{"internal/blob"}, testsToo: true},
	{dependency: "golang.org/x/text/unicode/norm", owners: []string{"internal/pathvalidation"}, testsToo: true},
	{dependency: "gopkg.in/yaml.v3", owners: []string{"internal/skill", "internal/drive/skillpackage", "internal/testinfra", "integration/transporttest"}},
	{dependency: "github.com/pdfcpu/pdfcpu", owners: []string{"internal/sandbox/helper/internal/media", "internal/files/pdfcount"}},
	// The Envoy translator test builds one extension policy input.
	{dependency: "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured", exact: true, owners: []string{"internal/kubernetes", "internal/internalgrpc/auth"},
		testsToo: true, testOwners: []string{"integration/envoy-gateway-secret-helper"}},
	{dependency: "k8s.io/", owners: []string{"internal/kubernetes", "internal/internalgrpc/auth"}, testsToo: true},
	// net/http wants a *log.Logger for its server's ErrorLog; every other
	// record is written through log/slog.
	{dependency: "log", exact: true, owners: []string{"internal/workload"}},
	{dependency: "go.opentelemetry.io/", owners: []string{"internal/workload"}},
	{dependency: "github.com/Azure/"},
	{dependency: "cloud.google.com/go/"},
	{dependency: "google.golang.org/api/"},
	{dependency: "github.com/GoogleCloudPlatform/cloud-sql-proxy"},
	{dependency: "github.com/jackc/pgx-gcp"},
	{dependency: "github.com/jackc/pgx-azure"},
	{dependency: "github.com/prometheus/"},
	{dependency: "github.com/rs/zerolog"},
	{dependency: "go.uber.org/zap"},
	{dependency: "github.com/sirupsen/logrus"},
	{dependency: "github.com/go-logr/logr"},
	{dependency: "github.com/cenkalti/backoff"},
	{dependency: "github.com/avast/retry-go"},
	{dependency: "github.com/hashicorp/go-retryablehttp"},
}

// packageBoundaries are imports a package's production code never has.
var packageBoundaries = map[string][]dependencyOwner{
	// Admission neither dispatches over gRPC nor logs keys or event content.
	"internal/sessionevent": {
		{dependency: "github.com/tetral-ai/tetral/internal/internalgrpc"},
		{dependency: "log", exact: true},
		{dependency: "log/slog", exact: true},
	},
}

func TestDependenciesStayWithTheirOwners(t *testing.T) {
	packages := enginePackages(t)
	for _, violation := range dependencyOwnerViolations(packages, dependencyOwners, packageBoundaries) {
		t.Error(violation)
	}
	listed := map[string]bool{}
	for _, one := range packages {
		listed[one.Directory] = true
	}
	for _, rule := range dependencyOwners {
		for _, owner := range slices.Concat(rule.owners, rule.testOwners) {
			if !listed[owner] {
				t.Errorf("%s names the owner %s, which is no package", rule.dependency, owner)
			}
		}
	}
	for directory := range packageBoundaries {
		if !listed[directory] {
			t.Errorf("a boundary names %s, which is no package", directory)
		}
	}
}

func dependencyOwnerViolations(packages []enginePackage, owners []dependencyOwner, boundaries map[string][]dependencyOwner) []string {
	within := func(directory string, allowed []string) bool {
		return slices.ContainsFunc(allowed, func(owner string) bool {
			return directory == owner || strings.HasPrefix(directory, owner+"/")
		})
	}
	var violations []string
	for _, one := range packages {
		for _, imported := range one.Production {
			for _, rule := range owners {
				if rule.matches(imported) {
					if !within(one.Directory, rule.owners) {
						violations = append(violations, one.Directory+" imports "+imported)
					}
					break
				}
			}
			for _, rule := range boundaries[one.Directory] {
				if rule.matches(imported) {
					violations = append(violations, one.Directory+" imports "+imported)
				}
			}
		}
		for _, imported := range one.Tests {
			for _, rule := range owners {
				if rule.matches(imported) {
					if rule.testsToo && !within(one.Directory, slices.Concat(rule.owners, rule.testOwners)) {
						violations = append(violations, "the tests of "+one.Directory+" import "+imported)
					}
					break
				}
			}
		}
	}
	slices.Sort(violations)
	return violations
}

func TestDependencyOwnerRulesNameEachImportOutsideItsOwners(t *testing.T) {
	owners := []dependencyOwner{
		{dependency: "example.com/storage", owners: []string{"internal/blob"}, testsToo: true},
		{dependency: "example.com/kube/unstructured", exact: true, owners: []string{"internal/kube"}, testsToo: true, testOwners: []string{"tools/translator"}},
		{dependency: "example.com/kube/", owners: []string{"internal/kube"}, testsToo: true},
		{dependency: "example.com/yaml", owners: []string{"internal/skill"}},
		{dependency: "log", exact: true},
	}
	boundaries := map[string][]dependencyOwner{"internal/admission": {{dependency: "log/slog", exact: true}}}
	packages := []enginePackage{
		{Directory: "internal/blob", Production: []string{"example.com/storage/s3"}},
		{Directory: "internal/blob/blobtest", Production: []string{"example.com/storage"}, Tests: []string{"example.com/storage/s3"}},
		{Directory: "internal/blobber", Production: []string{"example.com/storage/s3"}},
		{Directory: "internal/files", Tests: []string{"example.com/storage"}},
		{Directory: "tools/translator", Production: []string{"example.com/kube/unstructured"}, Tests: []string{"example.com/kube/unstructured", "example.com/kube/client"}},
		{Directory: "deploy", Production: []string{"log/slog"}, Tests: []string{"example.com/yaml", "log"}},
		{Directory: "internal/api", Production: []string{"example.com/yaml", "log"}},
		{Directory: "internal/admission", Production: []string{"log/slog", "log/syslog"}},
	}
	want := []string{
		"internal/admission imports log/slog",
		"internal/api imports example.com/yaml",
		"internal/api imports log",
		"internal/blobber imports example.com/storage/s3",
		"the tests of internal/files import example.com/storage",
		"the tests of tools/translator import example.com/kube/client",
		"tools/translator imports example.com/kube/unstructured",
	}
	if got := dependencyOwnerViolations(packages, owners, boundaries); !slices.Equal(got, want) {
		t.Fatalf("violations:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
