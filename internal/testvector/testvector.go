// Package testvector pins one component's real output as a file another
// component's tests read as their input. The producer's test owns the file:
// it compares its output with the file and rewrites it only when
// TETRAL_UPDATE_VECTORS=1 is set, which CI never does.
package testvector

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
)

// UpdateVariable is the variable that lets a producer's test rewrite its
// vectors.
const UpdateVariable = "TETRAL_UPDATE_VECTORS"

// generatedIdentifier is the random or derived part of the identifiers the
// stores mint, such as evt_<16 hex> and delivery_<32 hex>.
var generatedIdentifier = regexp.MustCompile(`_([0-9a-f]{32}|[0-9a-f]{16})\b`)

// PinJSON compares the JSON value with the vector at path, relative to the
// repository root, or writes it there under UpdateVariable. The file holds
// the value indented, keys in the producer's order, with each generated
// identifier renamed to its order of first appearance, so equal runs give
// equal files and equal identifiers stay equal.
func PinJSON(t testing.TB, path string, value []byte) {
	t.Helper()
	recordPin(t, path)
	renamed := map[string]string{}
	value = generatedIdentifier.ReplaceAllFunc(value, func(match []byte) []byte {
		name, ok := renamed[string(match)]
		if !ok {
			name = fmt.Sprintf("_%0*x", len(match)-1, len(renamed)+1)
			renamed[string(match)] = name
		}
		return []byte(name)
	})
	var indented bytes.Buffer
	if err := json.Indent(&indented, value, "", "  "); err != nil {
		t.Fatalf("vector %s is not JSON: %v", path, err)
	}
	indented.WriteByte('\n')
	full := filepath.Join(RepositoryRoot(t), path)
	if os.Getenv(UpdateVariable) == "1" {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s is set in CI; vectors are rewritten only by hand", UpdateVariable)
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, indented.Bytes(), 0o644); err != nil { //nolint:gosec // G306: a vector is a checked-in repository file.
			t.Fatal(err)
		}
		return
	}
	pinned, err := os.ReadFile(full) //nolint:gosec // G304: the path is a fixed repository vector.
	if err != nil {
		t.Fatalf("read vector %s: %v (rewrite it with %s=1)", path, err, UpdateVariable)
	}
	if !bytes.Equal(pinned, indented.Bytes()) {
		t.Fatalf("output differs from vector %s; its readers' tests hold the old shape. If the change is intended, rewrite it with %s=1 and run its readers.\nwant:\n%s\ngot:\n%s", path, UpdateVariable, pinned, indented.Bytes())
	}
}

// ManifestPath, relative to a producer package, lists each vector the package
// pins with the top-level test that pins it. A reader's completeness check
// cannot see a vector its producer stopped pinning; Main can.
const ManifestPath = "testdata/pinned-vectors.json"

// ManifestRow is one vector of a producer's manifest.
type ManifestRow struct {
	Path string `json:"path"`
	Test string `json:"test"`
}

var pins struct {
	sync.Mutex
	declared map[string]string // vector path -> top-level test
	pinned   map[string]string
}

// recordPin refuses a vector the package's manifest does not give to the
// calling top-level test; under UpdateVariable, Main writes the manifest
// from what was pinned.
func recordPin(t testing.TB, path string) {
	t.Helper()
	test, _, _ := strings.Cut(t.Name(), "/")
	pins.Lock()
	defer pins.Unlock()
	if err := loadManifest(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv(UpdateVariable) != "1" && pins.declared[path] != test {
		t.Fatalf("vector %s is not declared for %s in %s; rewrite the vectors with %s=1", path, test, ManifestPath, UpdateVariable)
	}
	pins.pinned[path] = test
}

func loadManifest() error {
	if pins.declared != nil {
		return nil
	}
	pins.declared, pins.pinned = map[string]string{}, map[string]string{}
	rows, err := ReadManifest(ManifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	for _, row := range rows {
		pins.declared[row.Path] = row.Test
	}
	return err
}

// ReadManifest reads the manifest file at path.
func ReadManifest(path string) ([]ManifestRow, error) {
	body, err := os.ReadFile(path) //nolint:gosec // G304: the path is a fixed repository manifest.
	if err != nil {
		return nil, err
	}
	var rows []ManifestRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return rows, nil
}

// Main is the TestMain of a producer package. After a passing run it fails
// when a test the -run and -skip patterns selected whole did not pin a vector
// the manifest gives it; under UpdateVariable it writes the manifest instead,
// keeping the rows of tests the run did not select.
func Main(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if err := settlePins(flag.Lookup("test.run").Value.String(), flag.Lookup("test.skip").Value.String()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			code = 1
		}
	}
	os.Exit(code)
}

func settlePins(run, skip string) error {
	pins.Lock()
	defer pins.Unlock()
	if err := loadManifest(); err != nil {
		return err
	}
	if os.Getenv(UpdateVariable) == "1" {
		if os.Getenv("CI") != "" {
			return fmt.Errorf("%s is set in CI; vectors are rewritten only by hand", UpdateVariable)
		}
		rows := []ManifestRow{}
		for path, test := range pins.declared {
			if _, pinned := pins.pinned[path]; !pinned && !SelectsWhole(run, skip, test) {
				rows = append(rows, ManifestRow{path, test})
			}
		}
		for path, test := range pins.pinned {
			rows = append(rows, ManifestRow{path, test})
		}
		slices.SortFunc(rows, func(a, b ManifestRow) int { return strings.Compare(a.Path, b.Path) })
		body, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(ManifestPath), 0o750); err != nil {
			return err
		}
		return os.WriteFile(ManifestPath, append(body, '\n'), 0o644) //nolint:gosec // G306: the manifest is a checked-in repository file.
	}
	var missing []string
	for path, test := range pins.declared {
		if _, pinned := pins.pinned[path]; !pinned && SelectsWhole(run, skip, test) {
			missing = append(missing, fmt.Sprintf("%s did not pin %s", test, path))
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return fmt.Errorf("%s declares vectors their tests no longer pin; pin them again, or delete the vector with its row:\n%s", ManifestPath, strings.Join(missing, "\n"))
	}
	return nil
}

// SelectsWhole reports whether go test runs the top-level test with every
// subtest under these -run and -skip patterns. Like go test, a pattern splits
// into alternatives at unbracketed "|" and into levels at unbracketed "/",
// and each level is an unanchored regular expression.
func SelectsWhole(run, skip, test string) bool {
	matches := func(pattern string) bool {
		ok, err := regexp.MatchString(pattern, test)
		return err == nil && ok
	}
	if run != "" && !slices.ContainsFunc(splitPattern(run), func(levels []string) bool {
		return len(levels) == 1 && matches(levels[0])
	}) {
		return false
	}
	return skip == "" || !slices.ContainsFunc(splitPattern(skip), func(levels []string) bool {
		return matches(levels[0])
	})
}

func splitPattern(pattern string) [][]string {
	var alternatives [][]string
	var levels []string
	brackets, parentheses, start := 0, 0, 0
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '[':
			brackets++
		case ']':
			brackets = max(brackets-1, 0)
		case '(':
			if brackets == 0 {
				parentheses++
			}
		case ')':
			if brackets == 0 {
				parentheses--
			}
		case '\\':
			i++
		case '/', '|':
			if brackets == 0 && parentheses == 0 {
				levels = append(levels, pattern[start:i])
				start = i + 1
				if pattern[i] == '|' {
					alternatives, levels = append(alternatives, levels), nil
				}
			}
		}
	}
	return append(alternatives, append(levels, pattern[start:]))
}

// RepositoryRoot is the directory holding the repository's go.mod.
func RepositoryRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "services")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root not found")
		}
		dir = parent
	}
}
