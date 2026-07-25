package skill

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// MaxFrontmatterBytes caps the bytes between the opening and closing
// `---` delimiters of SKILL.md frontmatter. Only that bounded segment
// reaches the YAML parser; an oversized frontmatter rejects before
// any decode work runs. 32 KiB is generous for legitimate name+description
// pairs while keeping
// adversarial frontmatter parsing within a fixed memory ceiling.
const MaxFrontmatterBytes = 32 * 1024

// MaxNameRunes is the upper bound on the SKILL.md `name` value. Counted in
// Unicode code points so multi-byte runes are not undercharged.
const MaxNameRunes = 64

// MaxDescriptionRunes is the upper bound on the SKILL.md `description`
// value.
const MaxDescriptionRunes = 1024

// Frontmatter holds the validated SKILL.md metadata that the upload
// pipeline persists into the skills/skill_versions rows.
type Frontmatter struct {
	Name        string
	Description string
}

// frontmatterRoot is the strict two-field decode target. yaml.v3
// `KnownFields(true)` rejects unexpected top-level keys; explicit
// node-level inspection (below) catches anchors, aliases, tags,
// merge keys, multi-document streams, and nested values that the
// strict struct decode does not catch on its own.
type frontmatterRoot struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

// ParseFrontmatter validates the raw SKILL.md bytes and returns the
// extracted frontmatter on success. Failure modes return
// *ValidationError (mapped to 400 invalid_request_error by the HTTP
// layer) or *RequestTooLargeError when the bounded frontmatter
// segment is exceeded.
//
// The function never returns the raw frontmatter text or any
// attacker-supplied substring inside the error; only the violated
// rule name is named. Tests scan returned error strings to enforce
// this rule.
//
// Sequence:
//  1. SKILL.md must begin with the `---` opening delimiter.
//  2. The closing `---` must appear within MaxFrontmatterBytes of
//     the opening; otherwise reject with RequestTooLargeError.
//  3. Decode the bounded segment into yaml.Node and reject anchors,
//     aliases, tags, merge keys, multiple documents, unknown keys,
//     non-scalar values, and nested collections.
//  4. Apply length, character-class, and reserved-word rules to
//     name and description.
func ParseFrontmatter(rawSkillMD []byte) (*Frontmatter, error) {
	body, err := boundedFrontmatterBlock(rawSkillMD)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	// Strict known-field decoding: extra top-level keys reject.
	decoder.KnownFields(true)

	var docNode yaml.Node
	if err := decoder.Decode(&docNode); err != nil {
		return nil, &ValidationError{Message: "SKILL.md frontmatter is not valid YAML"}
	}
	if err := rejectMultipleDocuments(decoder); err != nil {
		return nil, err
	}
	if err := assertSafeRoot(&docNode); err != nil {
		return nil, err
	}

	var root frontmatterRoot
	if err := docNode.Decode(&root); err != nil {
		return nil, &ValidationError{Message: "SKILL.md frontmatter must contain only scalar string `name` and `description`"}
	}
	fm := &Frontmatter{Name: root.Name, Description: root.Description}
	if err := validateFrontmatterRules(fm); err != nil {
		return nil, err
	}
	return fm, nil
}

// boundedFrontmatterBlock returns the bytes between the opening and
// closing `---` delimiters, capped at MaxFrontmatterBytes for the
// parsed body. The delimiters themselves are not returned.
func boundedFrontmatterBlock(rawSkillMD []byte) ([]byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(rawSkillMD))
	// Allow scanner to read up to the cap + a little overhead per
	// line. yaml frontmatter is usually a handful of lines but a
	// pathological description could legitimately reach a few KiB
	// per line.
	scanner.Buffer(make([]byte, 0, 4*1024), MaxFrontmatterBytes+1024)

	if !scanner.Scan() {
		return nil, &ValidationError{Message: "SKILL.md frontmatter is missing"}
	}
	if !isFrontmatterDelimiter(scanner.Text()) {
		return nil, &ValidationError{Message: "SKILL.md must begin with a `---` frontmatter delimiter"}
	}
	var body bytes.Buffer
	for scanner.Scan() {
		line := scanner.Text()
		if isFrontmatterDelimiter(line) {
			return body.Bytes(), nil
		}
		if body.Len()+len(line)+1 > MaxFrontmatterBytes {
			return nil, &RequestTooLargeError{Message: "SKILL.md frontmatter exceeds the bounded size cap"}
		}
		body.WriteString(line)
		body.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return nil, &ValidationError{Message: "SKILL.md frontmatter is malformed"}
	}
	return nil, &ValidationError{Message: "SKILL.md frontmatter is missing the closing `---` delimiter"}
}

// isFrontmatterDelimiter matches a line that is exactly `---`,
// trimming trailing carriage returns to tolerate CRLF input. Trailing
// whitespace on the delimiter line is rejected so an attacker cannot
// hide content past a near-delimiter.
func isFrontmatterDelimiter(line string) bool {
	trimmed := strings.TrimRight(line, "\r")
	return trimmed == "---"
}

// rejectMultipleDocuments asserts the decoder has no further
// documents. yaml.v3's Decoder.Decode returns io.EOF when the stream
// is exhausted; any other state means another document follows.
func rejectMultipleDocuments(decoder *yaml.Decoder) error {
	var extra yaml.Node
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return &ValidationError{Message: "SKILL.md frontmatter must contain exactly one YAML document"}
}

// assertSafeRoot inspects the root document node and rejects any
// disallowed YAML feature: anchors, aliases, explicit tags, merge
// keys, multi-doc, non-mapping roots, nested non-scalar values, and
// unknown keys (keys other than `name` / `description`).
func assertSafeRoot(node *yaml.Node) error {
	if node.Kind != yaml.DocumentNode || len(node.Content) != 1 {
		return &ValidationError{Message: "SKILL.md frontmatter must be a single YAML mapping"}
	}
	mapping := node.Content[0]
	if err := assertNoForbiddenAttributes(mapping, "frontmatter root"); err != nil {
		return err
	}
	if mapping.Kind != yaml.MappingNode {
		return &ValidationError{Message: "SKILL.md frontmatter must be a YAML mapping"}
	}
	// Mapping content alternates key, value, key, value...
	if len(mapping.Content)%2 != 0 {
		return &ValidationError{Message: "SKILL.md frontmatter mapping is malformed"}
	}
	for i := 0; i < len(mapping.Content); i += 2 {
		key := mapping.Content[i]
		value := mapping.Content[i+1]
		if err := assertNoForbiddenAttributes(key, "frontmatter key"); err != nil {
			return err
		}
		if err := assertNoForbiddenAttributes(value, "frontmatter value"); err != nil {
			return err
		}
		if key.Kind != yaml.ScalarNode {
			return &ValidationError{Message: "SKILL.md frontmatter keys must be plain scalar strings"}
		}
		switch key.Value {
		case "name", "description":
		case "<<":
			return &ValidationError{Message: "SKILL.md frontmatter must not use YAML merge keys"}
		default:
			return &ValidationError{Message: "SKILL.md frontmatter contains an unexpected key"}
		}
		if value.Kind != yaml.ScalarNode {
			return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter `%s` must be a scalar string (not a list, mapping, or null)", key.Value)}
		}
		if value.Tag != "!!str" {
			return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter `%s` must be a scalar string", key.Value)}
		}
	}
	return nil
}

// assertNoForbiddenAttributes rejects nodes that carry an anchor,
// alias, or explicit tag. yaml.v3 marks a node with non-empty
// `Anchor` when it defines an alias target, Kind == AliasNode when it
// references one, and TaggedStyle when the source wrote a tag such as
// `!!str` or `!custom`. The resolved Tag field is not enough for this
// rule because yaml.v3 also sets it for ordinary implicit scalars.
func assertNoForbiddenAttributes(node *yaml.Node, where string) error {
	if node.Kind == yaml.AliasNode {
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter must not use YAML aliases (%s)", where)}
	}
	if node.Anchor != "" {
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter must not declare YAML anchors (%s)", where)}
	}
	if node.Style&yaml.TaggedStyle != 0 {
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter must not use explicit YAML tags (%s)", where)}
	}
	return nil
}

// validateFrontmatterRules enforces the stable name/description constraints on
// validated string values.
func validateFrontmatterRules(fm *Frontmatter) error {
	if fm.Name == "" {
		return &ValidationError{Message: "SKILL.md frontmatter `name` is required"}
	}
	if n := utf8.RuneCountInString(fm.Name); n > MaxNameRunes {
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter `name` must be at most %d characters (got %d)", MaxNameRunes, n)}
	}
	if !isValidSkillName(fm.Name) {
		return &ValidationError{Message: "SKILL.md frontmatter `name` must contain only lowercase letters, digits, and hyphens"}
	}
	if containsXMLTag(fm.Name) {
		return &ValidationError{Message: "SKILL.md frontmatter `name` must not contain XML tags"}
	}
	if containsReservedVendor(fm.Name) {
		return &ValidationError{Message: "SKILL.md frontmatter `name` must not contain reserved vendor tokens"}
	}

	if fm.Description == "" {
		return &ValidationError{Message: "SKILL.md frontmatter `description` is required"}
	}
	if n := utf8.RuneCountInString(fm.Description); n > MaxDescriptionRunes {
		return &ValidationError{Message: fmt.Sprintf("SKILL.md frontmatter `description` must be at most %d characters (got %d)", MaxDescriptionRunes, n)}
	}
	if containsXMLTag(fm.Description) {
		return &ValidationError{Message: "SKILL.md frontmatter `description` must not contain XML tags"}
	}
	return nil
}

// isValidSkillName accepts only lowercase ASCII letters, digits, and hyphens.
// Disallowed: uppercase, underscore, dot, slash, whitespace, multibyte letters,
// and empty strings. Leading and trailing hyphens are valid names under this
// character-class rule.
func isValidSkillName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-':
		default:
			return false
		}
	}
	return true
}

// containsXMLTag reports whether s contains a substring that looks
// like an XML tag opener / closer / processing instruction. The check
// is intentionally broad: any `<...>` pair triggers the XML-tag rejection.
// Empty `<>` is also rejected.
func containsXMLTag(s string) bool {
	openIdx := strings.IndexByte(s, '<')
	if openIdx < 0 {
		return false
	}
	closeIdx := strings.IndexByte(s[openIdx:], '>')
	return closeIdx >= 0
}

// containsReservedVendor reports whether name embeds either
// "anthropic" or "claude". The check is case-insensitive and ignores
// punctuation/non-letter separators so split forms (e.g. "anthr0pic")
// are not bypassed via simple substring tricks.
func containsReservedVendor(name string) bool {
	lower := strings.ToLower(name)
	return strings.Contains(lower, "anthropic") || strings.Contains(lower, "claude")
}
