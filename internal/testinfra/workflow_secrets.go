package testinfra

import (
	"strings"
	"unicode"
)

// WorkflowReferencesSecrets reports whether the workflow in body reads the
// secrets context.
func WorkflowReferencesSecrets(body []byte, source string) (bool, error) {
	workflow, err := parseWorkflowYAML(body, source)
	if err != nil {
		return false, err
	}
	return referencesSecrets(workflow), nil
}

// referencesSecrets reports whether a parsed workflow reads the secrets
// context: the context named in an expression, where an if value is an
// expression with or without ${{ }}, or a secrets key passed to a called
// reusable workflow.
func referencesSecrets(node *workflowYAMLNode) bool {
	if node == nil {
		return false
	}
	switch node.Kind {
	case workflowYAMLMapping:
		for index := 0; index+1 < len(node.Content); index += 2 {
			key, value := node.Content[index].Value, node.Content[index+1]
			if key == "if" && value.Kind == workflowYAMLScalar && expressionNamesSecrets(value.Value) {
				return true
			}
			if key == "secrets" && mappingValue(node, "uses") != nil {
				return true
			}
		}
	case workflowYAMLScalar:
		rest := node.Value
		for {
			start := strings.Index(rest, "${{")
			if start < 0 {
				break
			}
			end := strings.Index(rest[start:], "}}")
			if end < 0 {
				end = len(rest) - start
			}
			if expressionNamesSecrets(rest[start+len("${{") : start+end]) {
				return true
			}
			rest = rest[start+end:]
		}
	}
	for _, child := range node.Content {
		if referencesSecrets(child) {
			return true
		}
	}
	return false
}

// expressionNamesSecrets reports whether an expression names the secrets
// context: the identifier secrets, not a property after a dot and not inside
// a string literal. Context names are case-insensitive.
func expressionNamesSecrets(expression string) bool {
	expression = strings.NewReplacer("${{", " ", "}}", " ").Replace(expression)
	previous := ' '
	for index := 0; index < len(expression); {
		character := rune(expression[index])
		switch {
		case character == '\'':
			// A string literal; '' is an escaped quote inside it.
			index++
			for index < len(expression) {
				if expression[index] == '\'' {
					if index+1 < len(expression) && expression[index+1] == '\'' {
						index += 2
						continue
					}
					break
				}
				index++
			}
			index++
			previous = '\''
		case unicode.IsLetter(character) || character == '_':
			start := index
			for index < len(expression) && (unicode.IsLetter(rune(expression[index])) || unicode.IsDigit(rune(expression[index])) || expression[index] == '_' || expression[index] == '-') {
				index++
			}
			if previous != '.' && strings.EqualFold(expression[start:index], "secrets") {
				return true
			}
			previous = 'a'
		default:
			if !unicode.IsSpace(character) {
				previous = character
			}
			index++
		}
	}
	return false
}
