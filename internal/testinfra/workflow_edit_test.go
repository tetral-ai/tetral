package testinfra

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// A seeded defect is an edit of the parsed workflow, so a case names what it
// changes by job, step and key and does not depend on how the file is laid
// out.

// editWorkflowFile applies change to the parsed file below root and writes
// the result back.
func editWorkflowFile(t *testing.T, root, file string, change func(t *testing.T, workflow *workflowYAMLNode)) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(file))
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	document, err := parseWorkflowYAML(body, file)
	if err != nil {
		t.Fatal(err)
	}
	change(t, document.Content[0])
	changed, err := yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, file, string(changed))
}

// yamlAt walks from node. A step of the path is a mapping's key, or in a
// sequence an index, "last", or a selector: field=value picks the item whose
// field, which may be dotted, has the value, field^=value the item whose
// field starts with it, and =value the item that is the value.
func yamlAt(t *testing.T, node *workflowYAMLNode, path ...string) *workflowYAMLNode {
	t.Helper()
	for position, step := range path {
		next := yamlChild(node, step)
		if next == nil {
			t.Fatalf("the workflow has no %s", strings.Join(path[:position+1], " / "))
		}
		node = next
	}
	return node
}

func yamlChild(node *workflowYAMLNode, step string) *workflowYAMLNode {
	if node.Kind == workflowYAMLMapping {
		return mappingValue(node, step)
	}
	if node.Kind != workflowYAMLSequence {
		return nil
	}
	if step == "last" && len(node.Content) > 0 {
		return node.Content[len(node.Content)-1]
	}
	if index, err := strconv.Atoi(step); err == nil && index < len(node.Content) {
		return node.Content[index]
	}
	field, value, selects := strings.Cut(step, "=")
	prefix := strings.HasSuffix(field, "^")
	field = strings.TrimSuffix(field, "^")
	for _, item := range node.Content {
		found := item
		for _, key := range strings.FieldsFunc(field, func(r rune) bool { return r == '.' }) {
			if found = mappingValue(found, key); found == nil {
				break
			}
		}
		if selects && found != nil && (found.Value == value || prefix && strings.HasPrefix(found.Value, value)) {
			return item
		}
	}
	return nil
}

// yamlSet gives the scalar at path the value, adding the last key when its
// mapping lacks it.
func yamlSet(t *testing.T, node *workflowYAMLNode, value string, path ...string) {
	t.Helper()
	parent := yamlAt(t, node, path[:len(path)-1]...)
	if existing := yamlChild(parent, path[len(path)-1]); existing != nil {
		*existing = workflowYAMLNode{Kind: workflowYAMLScalar, Value: value, Style: existing.Style}
		return
	}
	if parent.Kind != workflowYAMLMapping {
		t.Fatalf("the workflow has no %s", strings.Join(path, " / "))
	}
	parent.Content = append(parent.Content,
		&workflowYAMLNode{Kind: workflowYAMLScalar, Value: path[len(path)-1]},
		&workflowYAMLNode{Kind: workflowYAMLScalar, Value: value})
}

// yamlRemove takes the key or the item at path out of its parent.
func yamlRemove(t *testing.T, node *workflowYAMLNode, path ...string) {
	t.Helper()
	parent := yamlAt(t, node, path[:len(path)-1]...)
	target := yamlAt(t, parent, path[len(path)-1])
	for index, child := range parent.Content {
		if child != target {
			continue
		}
		first := index
		if parent.Kind == workflowYAMLMapping {
			first = index - 1
		}
		parent.Content = append(parent.Content[:first], parent.Content[index+1:]...)
		return
	}
}

// yamlPut gives the key at path what the YAML text holds, adding the key
// when its mapping lacks it.
func yamlPut(t *testing.T, node *workflowYAMLNode, text string, path ...string) {
	t.Helper()
	parent := yamlAt(t, node, path[:len(path)-1]...)
	if existing := yamlChild(parent, path[len(path)-1]); existing != nil {
		*existing = *yamlValue(t, text)
		return
	}
	parent.Content = append(parent.Content, &workflowYAMLNode{Kind: workflowYAMLScalar, Value: path[len(path)-1]}, yamlValue(t, text))
}

// yamlExtend adds text to the end of the scalar at path, such as a line to a
// step's script.
func yamlExtend(t *testing.T, node *workflowYAMLNode, text string, path ...string) {
	t.Helper()
	yamlAt(t, node, path...).Value += text
}

// yamlAppend adds to the sequence at path what the YAML text holds.
func yamlAppend(t *testing.T, node *workflowYAMLNode, text string, path ...string) {
	t.Helper()
	sequence := yamlAt(t, node, path...)
	sequence.Content = append(sequence.Content, yamlValue(t, text))
}

func yamlValue(t *testing.T, text string) *workflowYAMLNode {
	t.Helper()
	document, err := parseWorkflowYAML([]byte(text), "an edit")
	if err != nil {
		t.Fatal(err)
	}
	return document.Content[0]
}
