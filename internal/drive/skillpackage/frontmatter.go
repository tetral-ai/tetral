package skillpackage

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const (
	// MaxFrontmatterBytes bounds the bytes between the `---` lines of SKILL.md.
	MaxFrontmatterBytes = 32_768
	MaxNameRunes        = 64
	MaxDescriptionRunes = 1024
)

// frontmatterReadBytes is the most of SKILL.md a check reads: the opening
// line, the largest frontmatter block and the closing line, each line with
// CRLF.
const frontmatterReadBytes = MaxFrontmatterBytes + 2*len("---\r\n")

// Frontmatter is the name and description of a valid SKILL.md.
type Frontmatter struct {
	Name        string
	Description string
}

// ReadFrontmatter checks the frontmatter at the start of a SKILL.md body. A
// failed rule is a *ValidationError naming the rule, never the file's text;
// any other error is the reader's.
func ReadFrontmatter(body io.Reader) (Frontmatter, error) {
	head, err := io.ReadAll(io.LimitReader(body, int64(frontmatterReadBytes)))
	if err != nil {
		return Frontmatter{}, err
	}
	block, err := frontmatterBlock(head, len(head) == frontmatterReadBytes)
	if err != nil {
		return Frontmatter{}, err
	}
	return parseFrontmatter(block)
}

// frontmatterBlock returns the bytes between the opening and closing `---`
// lines. truncated reports that head is a prefix of a longer file.
func frontmatterBlock(head []byte, truncated bool) ([]byte, error) {
	line, rest, found := bytes.Cut(head, []byte("\n"))
	if len(head) == 0 {
		return nil, &ValidationError{Message: "SKILL.md frontmatter is missing"}
	}
	if !found || !isDelimiter(line) {
		return nil, &ValidationError{Message: "SKILL.md must begin with a `---` frontmatter delimiter"}
	}
	for offset := 0; ; {
		if offset > MaxFrontmatterBytes {
			return nil, &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter exceeds %d bytes", MaxFrontmatterBytes)}
		}
		line, _, found := bytes.Cut(rest[offset:], []byte("\n"))
		if !found && truncated {
			return nil, &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter exceeds %d bytes", MaxFrontmatterBytes)}
		}
		if isDelimiter(line) {
			return rest[:offset], nil
		}
		if !found {
			return nil, &ValidationError{Message: "SKILL.md frontmatter is missing the closing `---` delimiter"}
		}
		offset += len(line) + 1
	}
}

// isDelimiter matches a line that is exactly `---`, with CRLF tolerated.
func isDelimiter(line []byte) bool {
	return string(bytes.TrimSuffix(line, []byte("\r"))) == "---"
}

// parseFrontmatter decodes one YAML mapping without anchors, aliases, tags or
// merge keys. name and description must be plain strings; other keys are
// ignored whatever their values.
func parseFrontmatter(block []byte) (Frontmatter, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(block))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return Frontmatter{}, &ValidationError{Message: "SKILL.md frontmatter is not valid YAML"}
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return Frontmatter{}, &ValidationError{Message: "SKILL.md frontmatter must contain exactly one YAML document"}
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return Frontmatter{}, &ValidationError{Message: "SKILL.md frontmatter must be a single YAML mapping"}
	}
	mapping := document.Content[0]
	if err := rejectYAMLAttributes(mapping, "frontmatter root"); err != nil {
		return Frontmatter{}, err
	}
	if mapping.Kind != yaml.MappingNode || len(mapping.Content)%2 != 0 {
		return Frontmatter{}, &ValidationError{Message: "SKILL.md frontmatter must be a YAML mapping"}
	}
	var fm Frontmatter
	for i := 0; i < len(mapping.Content); i += 2 {
		key, value := mapping.Content[i], mapping.Content[i+1]
		if err := rejectYAMLAttributes(key, "frontmatter key"); err != nil {
			return Frontmatter{}, err
		}
		if err := rejectYAMLAttributes(value, "frontmatter value"); err != nil {
			return Frontmatter{}, err
		}
		if key.Kind != yaml.ScalarNode {
			return Frontmatter{}, &ValidationError{Message: "SKILL.md frontmatter keys must be plain scalar strings"}
		}
		var target *string
		switch key.Value {
		case "<<":
			return Frontmatter{}, &ValidationError{Message: "SKILL.md frontmatter must not use YAML merge keys"}
		case "name":
			target = &fm.Name
		case "description":
			target = &fm.Description
		default:
			continue
		}
		if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
			return Frontmatter{}, &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter `%s` must be a scalar string", key.Value)}
		}
		*target = value.Value
	}
	return fm, checkFrontmatterRules(fm)
}

// rejectYAMLAttributes refuses an alias, an anchor or an explicit tag. The
// resolved Tag field cannot tell an explicit tag, since yaml.v3 also sets it
// for implicit scalars; TaggedStyle can.
func rejectYAMLAttributes(node *yaml.Node, where string) error {
	switch {
	case node.Kind == yaml.AliasNode:
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter must not use YAML aliases (%s)", where)}
	case node.Anchor != "":
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter must not declare YAML anchors (%s)", where)}
	case node.Style&yaml.TaggedStyle != 0:
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter must not use explicit YAML tags (%s)", where)}
	}
	return nil
}

func checkFrontmatterRules(fm Frontmatter) error {
	switch n := utf8.RuneCountInString(fm.Name); {
	case fm.Name == "":
		return &ValidationError{Message: "SKILL.md frontmatter `name` is required"}
	case n > MaxNameRunes:
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter `name` must be at most %d characters (got %d)", MaxNameRunes, n)}
	case strings.Trim(fm.Name, "abcdefghijklmnopqrstuvwxyz0123456789-") != "":
		return &ValidationError{Message: "SKILL.md frontmatter `name` must contain only lowercase letters, digits, and hyphens"}
	case containsXMLTag(fm.Name):
		return &ValidationError{Message: "SKILL.md frontmatter `name` must not contain XML tags"}
	case strings.Contains(strings.ToLower(fm.Name), "anthropic") || strings.Contains(strings.ToLower(fm.Name), "claude"):
		return &ValidationError{Message: "SKILL.md frontmatter `name` must not contain reserved vendor tokens"}
	}
	switch n := utf8.RuneCountInString(fm.Description); {
	case fm.Description == "":
		return &ValidationError{Message: "SKILL.md frontmatter `description` is required"}
	case n > MaxDescriptionRunes:
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter `description` must be at most %d characters (got %d)", MaxDescriptionRunes, n)}
	case containsXMLTag(fm.Description):
		return &ValidationError{Message: "SKILL.md frontmatter `description` must not contain XML tags"}
	case strings.IndexByte(fm.Description, 0) >= 0:
		return &ValidationError{Message: "SKILL.md frontmatter `description` must not contain U+0000"}
	}
	return nil
}

// containsXMLTag reports any `<` followed later by `>`.
func containsXMLTag(s string) bool {
	open := strings.IndexByte(s, '<')
	return open >= 0 && strings.IndexByte(s[open:], '>') >= 0
}
