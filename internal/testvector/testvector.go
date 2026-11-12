// Package testvector pins one component's real output as a file another
// component's tests read as their input. The producer's test owns the file:
// it compares its output with the file and rewrites it only when
// TETRAL_UPDATE_VECTORS=1 is set, which CI never does.
package testvector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
