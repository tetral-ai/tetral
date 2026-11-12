package kubernetesmanifest_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Every manifest is read through a YAML decoder, and a check names what it
// wants as a YAML fragment: a document holds a fragment when one of its nodes
// has every field of the fragment. The indentation, quoting and key order of
// a manifest are part of no check.

// decodeDocuments are the YAML documents of the file at path.
func decodeDocuments(t *testing.T, path string) []map[string]any {
	t.Helper()
	source, err := os.Open(path) //nolint:gosec // A manifest of this repository or of a test's rendering.
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	defer func() { _ = source.Close() }()
	var documents []map[string]any
	for decoder := yaml.NewDecoder(source); ; {
		var document map[string]any
		if err := decoder.Decode(&document); errors.Is(err, io.EOF) {
			return documents
		} else if err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		if document != nil {
			documents = append(documents, document)
		}
	}
}

// manifestDocumentsOf are the documents of the file at path, recorded under file.
func manifestDocumentsOf(t *testing.T, path string, file string) manifestDocuments {
	t.Helper()
	var documents manifestDocuments
	for _, object := range decodeDocuments(t, path) {
		kind, name := text(object, "kind"), text(object, "metadata", "name")
		if kind == "" || name == "" {
			t.Fatalf("%s has a document without kind or metadata.name: %v", path, object)
		}
		documents = append(documents, manifestDocument{file: file, kind: kind, name: name, object: object})
	}
	return documents
}

// fragment decodes a YAML fragment a check wants.
func fragment(t *testing.T, source string) any {
	t.Helper()
	var want any
	if err := yaml.Unmarshal([]byte(source), &want); err != nil {
		t.Fatalf("fragment %q: %v", source, err)
	}
	return want
}

// holds reports whether node has every field of want. A mapping holds a
// mapping whose keys it has with values it holds; a list holds a list that
// its leading items hold in order; a key without a value is held by any
// value.
func holds(node any, want any) bool {
	switch want := want.(type) {
	case nil:
		return true
	case map[string]any:
		actual, ok := node.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range want {
			if child, present := actual[key]; !present || !holds(child, value) {
				return false
			}
		}
		return true
	case []any:
		actual, ok := node.([]any)
		if !ok || len(actual) < len(want) {
			return false
		}
		for index, value := range want {
			if !holds(actual[index], value) {
				return false
			}
		}
		return true
	default:
		return node == want
	}
}

// anywhere reports whether node or a node under it holds want. A list that
// is wanted on its own is held by any run of a list's items.
func anywhere(node any, want any) bool {
	wanted, run := want.([]any)
	switch node := node.(type) {
	case map[string]any:
		if !run && holds(node, want) {
			return true
		}
		for _, child := range node {
			if anywhere(child, want) {
				return true
			}
		}
	case []any:
		for index, child := range node {
			if run && holds(node[index:], wanted) || anywhere(child, want) {
				return true
			}
		}
	}
	return false
}

// mentions reports whether a key or a scalar under node contains token.
func mentions(node any, token string) bool {
	switch node := node.(type) {
	case map[string]any:
		for key, child := range node {
			if strings.Contains(key, token) || mentions(child, token) {
				return true
			}
		}
		return false
	case []any:
		for _, child := range node {
			if mentions(child, token) {
				return true
			}
		}
		return false
	case nil:
		return false
	default:
		return strings.Contains(fmt.Sprint(node), token)
	}
}

// has reports whether the document holds the fragment: a mapping or a list
// as anywhere holds it, a bare word as a key or scalar that contains it.
func has(t *testing.T, node any, source string) bool {
	t.Helper()
	want := fragment(t, source)
	switch want.(type) {
	case map[string]any, []any:
		return anywhere(node, want)
	}
	return mentions(node, source)
}

// at is the node under node at path, nil when a step is missing.
func at(node any, path ...string) any {
	for _, key := range path {
		mapping, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		node = mapping[key]
	}
	return node
}

// fields is the mapping at path; a missing mapping has no fields.
func fields(node any, path ...string) map[string]any {
	mapping, _ := at(node, path...).(map[string]any)
	return mapping
}

// items is the list at path; a missing list has no items.
func items(node any, path ...string) []any {
	list, _ := at(node, path...).([]any)
	return list
}

// text is the scalar at path as a string; a missing scalar is empty.
func text(node any, path ...string) string {
	switch value := at(node, path...).(type) {
	case nil, map[string]any, []any:
		return ""
	default:
		return fmt.Sprint(value)
	}
}

// named is the item of list whose name is name, nil when there is none.
func named(list []any, name string) map[string]any {
	for _, item := range list {
		if text(item, "name") == name {
			return item.(map[string]any)
		}
	}
	return nil
}

// podSpec is the pod specification a workload document carries.
func podSpec(t *testing.T, document *manifestDocument) map[string]any {
	t.Helper()
	path := []string{"spec", "template", "spec"}
	if document.kind == "CronJob" {
		path = []string{"spec", "jobTemplate", "spec", "template", "spec"}
	}
	spec := fields(document.object, path...)
	if spec == nil {
		t.Fatalf("%s %s/%s has no pod specification", document.file, document.kind, document.name)
	}
	return spec
}

// requireContainer is the container called name of a workload document.
func requireContainer(t *testing.T, document *manifestDocument, name string) map[string]any {
	t.Helper()
	container := named(items(podSpec(t, document), "containers"), name)
	if container == nil {
		t.Fatalf("%s %s/%s has no container %s", document.file, document.kind, document.name, name)
	}
	return container
}

// environment is the variable called name of any container of a workload
// document, nil when no container declares it.
func environment(t *testing.T, document *manifestDocument, name string) map[string]any {
	t.Helper()
	for _, container := range items(podSpec(t, document), "containers") {
		if variable := named(items(container, "env"), name); variable != nil {
			return variable
		}
	}
	return nil
}

// serviceLocalDocument is the document of kind in the file at path.
func serviceLocalDocument(t *testing.T, path string, kind string) *manifestDocument {
	t.Helper()
	documents := manifestDocumentsOf(t, path, path)
	for index := range documents {
		if documents[index].kind == kind {
			return &documents[index]
		}
	}
	t.Fatalf("%s has no %s", path, kind)
	return nil
}

// serviceLocalWorkload is the Deployment or CronJob of a service's own
// manifest, named by its path under services.
func serviceLocalWorkload(t *testing.T, path string) *manifestDocument {
	t.Helper()
	documents := manifestDocumentsOf(t, filepath.Join("..", "..", "services", path), path)
	for index := range documents {
		if kind := documents[index].kind; kind == "Deployment" || kind == "CronJob" {
			return &documents[index]
		}
	}
	t.Fatalf("%s has no workload", path)
	return nil
}
