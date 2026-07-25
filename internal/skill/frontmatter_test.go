package skill_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/skill"
)

func TestParseFrontmatterAcceptsValidScalarPair(t *testing.T) {
	body := []byte("---\nname: data-analyzer\ndescription: Analyze CSV files and produce reports.\n---\n# heading\n")
	fm, err := skill.ParseFrontmatter(body)
	if err != nil {
		t.Fatalf("ParseFrontmatter: %v", err)
	}
	if fm.Name != "data-analyzer" {
		t.Errorf("Name = %q; want data-analyzer", fm.Name)
	}
	if fm.Description != "Analyze CSV files and produce reports." {
		t.Errorf("Description = %q", fm.Description)
	}
}

func TestParseFrontmatterAcceptsLeadingAndTrailingHyphenNames(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "-report"},
		{name: "report-"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte("---\nname: " + tt.name + "\ndescription: ok\n---\n")
			fm, err := skill.ParseFrontmatter(body)
			if err != nil {
				t.Fatalf("ParseFrontmatter: %v", err)
			}
			if fm.Name != tt.name {
				t.Errorf("Name = %q; want %q", fm.Name, tt.name)
			}
		})
	}
}

func TestParseFrontmatterAcceptsPlainDescriptionPunctuation(t *testing.T) {
	tests := []struct {
		name        string
		description string
	}{
		{
			name:        "bang",
			description: "Great!",
		},
		{
			name:        "ampersand",
			description: "Research & development reports",
		},
		{
			name:        "asterisk",
			description: "Use * to mark required fields",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte("---\nname: ok\ndescription: " + tt.description + "\n---\n")
			fm, err := skill.ParseFrontmatter(body)
			if err != nil {
				t.Fatalf("ParseFrontmatter: %v", err)
			}
			if fm.Description != tt.description {
				t.Errorf("Description = %q; want %q", fm.Description, tt.description)
			}
		})
	}
}

func TestParseFrontmatterAcceptsBlockScalarDescriptionPunctuation(t *testing.T) {
	body := []byte("---\nname: ok\ndescription: |\n  Great!\n  Research & development reports\n  Use * to mark required fields\n  ! leading bang\n  & leading ampersand\n  * leading asterisk\n---\n")
	fm, err := skill.ParseFrontmatter(body)
	if err != nil {
		t.Fatalf("ParseFrontmatter: %v", err)
	}
	want := "Great!\nResearch & development reports\nUse * to mark required fields\n! leading bang\n& leading ampersand\n* leading asterisk\n"
	if fm.Description != want {
		t.Errorf("Description = %q; want %q", fm.Description, want)
	}
}

func TestParseFrontmatterRejectsMissingOpeningDelimiter(t *testing.T) {
	body := []byte("name: hello\ndescription: world\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected validation error for missing opening delimiter")
	}
	if !errors.As(err, new(*skill.ValidationError)) {
		t.Errorf("expected *skill.ValidationError; got %T", err)
	}
}

func TestParseFrontmatterRejectsMissingClosingDelimiter(t *testing.T) {
	body := []byte("---\nname: hello\ndescription: world\nstill-going\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected validation error for missing closing delimiter")
	}
	var vErr *skill.ValidationError
	if !errors.As(err, &vErr) {
		t.Fatalf("expected *skill.ValidationError; got %T", err)
	}
	if !strings.Contains(vErr.Error(), "closing") {
		t.Errorf("error must mention closing delimiter, got %q", vErr.Error())
	}
}

func TestParseFrontmatterRejectsOversizedFrontmatter(t *testing.T) {
	var b strings.Builder
	b.WriteString("---\nname: ok\ndescription: ")
	// Fill a description that exceeds MaxFrontmatterBytes by itself.
	b.WriteString(strings.Repeat("A", skill.MaxFrontmatterBytes+10))
	b.WriteString("\n---\n")
	_, err := skill.ParseFrontmatter([]byte(b.String()))
	if err == nil {
		t.Fatal("expected oversized error")
	}
	var tooLarge *skill.RequestTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("expected *skill.RequestTooLargeError; got %T (%v)", err, err)
	}
}

func TestParseFrontmatterRejectsExtraKeys(t *testing.T) {
	const sentinel = "__frontmatter_extra_key_must_not_echo__"
	body := []byte("---\nname: ok\ndescription: ok\n" + sentinel + ": yes\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for extra key")
	}
	var vErr *skill.ValidationError
	if !errors.As(err, &vErr) {
		t.Fatalf("expected *skill.ValidationError; got %T", err)
	}
	if !strings.Contains(vErr.Error(), "unexpected key") {
		t.Errorf("error must describe the unexpected-key rule; got %q", vErr.Error())
	}
	if strings.Contains(vErr.Error(), sentinel) {
		t.Errorf("error must not echo the unexpected key; got %q", vErr.Error())
	}
}

func TestParseFrontmatterRejectsNonScalarValue(t *testing.T) {
	body := []byte("---\nname: ok\ndescription:\n  - line one\n  - line two\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for nested value")
	}
	if !errors.As(err, new(*skill.ValidationError)) {
		t.Errorf("expected *skill.ValidationError; got %T", err)
	}
}

func TestParseFrontmatterRejectsAnchorOrAlias(t *testing.T) {
	body := []byte("---\nname: &anchor ok\ndescription: *anchor\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for anchor/alias usage")
	}
}

func TestParseFrontmatterRejectsExplicitTag(t *testing.T) {
	body := []byte("---\nname: !!str ok\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for explicit YAML tag on value")
	}
}

func TestParseFrontmatterRejectsExplicitDescriptionTags(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "custom",
			body: "---\nname: ok\ndescription: !custom ok\n---\n",
		},
		{
			name: "explicit-string",
			body: "---\nname: ok\ndescription: !!str ok\n---\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := skill.ParseFrontmatter([]byte(tt.body))
			if err == nil {
				t.Fatal("expected reject for explicit YAML tag on description")
			}
		})
	}
}

func TestParseFrontmatterRejectsMergeKey(t *testing.T) {
	body := []byte("---\nbase: &b\n  description: shared\nname: ok\n<<: *b\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for merge key")
	}
}

func TestParseFrontmatterRejectsMultipleDocuments(t *testing.T) {
	// Use YAML's `...` end-of-document marker followed by another
	// document so the multi-doc trigger lives inside the bounded
	// block (the `---` delimiter would close the block early).
	body := []byte("---\nname: ok\ndescription: ok\n...\nname: oops\ndescription: nope\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for multiple YAML documents")
	}
}

func TestParseFrontmatterRejectsNonStringName(t *testing.T) {
	body := []byte("---\nname: 42\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for non-string name")
	}
}

func TestParseFrontmatterRejectsEmptyName(t *testing.T) {
	body := []byte("---\nname: \"\"\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for empty name")
	}
}

func TestParseFrontmatterRejectsOverlongName(t *testing.T) {
	body := []byte("---\nname: " + strings.Repeat("a", skill.MaxNameRunes+1) + "\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for overlong name")
	}
}

func TestParseFrontmatterRejectsUppercaseName(t *testing.T) {
	body := []byte("---\nname: DataAnalyzer\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for uppercase name")
	}
}

func TestParseFrontmatterRejectsUnderscoreName(t *testing.T) {
	body := []byte("---\nname: data_analyzer\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for underscore name")
	}
}

func TestParseFrontmatterRejectsSpaceName(t *testing.T) {
	body := []byte("---\nname: data analyzer\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for space in name")
	}
}

func TestParseFrontmatterRejectsDotName(t *testing.T) {
	body := []byte("---\nname: data.analyzer\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for dot in name")
	}
}

func TestParseFrontmatterRejectsSlashName(t *testing.T) {
	body := []byte("---\nname: data/analyzer\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for slash in name")
	}
}

func TestParseFrontmatterRejectsMultibyteName(t *testing.T) {
	body := []byte("---\nname: résumé\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for multibyte name")
	}
}

func TestParseFrontmatterRejectsXMLTagInName(t *testing.T) {
	body := []byte("---\nname: \"data<script>analyzer\"\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for XML tags in name")
	}
}

func TestParseFrontmatterRejectsXMLTagInDescription(t *testing.T) {
	body := []byte("---\nname: ok\ndescription: \"hello <script>alert('x')</script>\"\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for XML tags in description")
	}
}

func TestParseFrontmatterRejectsReservedAnthropic(t *testing.T) {
	body := []byte("---\nname: anthropic-skill\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for anthropic in name")
	}
}

func TestParseFrontmatterRejectsReservedClaude(t *testing.T) {
	body := []byte("---\nname: claude-helper\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for claude in name")
	}
}

func TestParseFrontmatterRejectsOverlongDescription(t *testing.T) {
	body := []byte("---\nname: ok\ndescription: " + strings.Repeat("x", skill.MaxDescriptionRunes+1) + "\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for overlong description")
	}
}

func TestParseFrontmatterErrorDoesNotEchoFrontmatter(t *testing.T) {
	// Use a recognizable sentinel inside an invalid frontmatter and
	// assert it does not appear in the returned error.
	const sentinel = "__do_not_echo_me__"
	body := []byte("---\nname: " + sentinel + "_uppercase_INVALID\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Errorf("frontmatter content leaked into error: %q", err.Error())
	}
}

func TestParseFrontmatterRejectsInvalidYAML(t *testing.T) {
	body := []byte("---\nname: \"unterminated\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject for invalid YAML")
	}
}

func TestParseFrontmatterCRLFDelimiterAccepted(t *testing.T) {
	body := []byte("---\r\nname: ok\r\ndescription: ok\r\n---\r\n")
	_, err := skill.ParseFrontmatter(body)
	if err != nil {
		t.Fatalf("CRLF should be tolerated; got %v", err)
	}
}

func TestParseFrontmatterRejectsMissingNameKey(t *testing.T) {
	body := []byte("---\ndescription: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject when name is omitted")
	}
	var vErr *skill.ValidationError
	if !errors.As(err, &vErr) {
		t.Fatalf("expected *skill.ValidationError; got %T", err)
	}
	if !strings.Contains(vErr.Error(), "name") {
		t.Errorf("error must mention name; got %q", vErr.Error())
	}
}

func TestParseFrontmatterRejectsMissingDescriptionKey(t *testing.T) {
	body := []byte("---\nname: ok\n---\n")
	_, err := skill.ParseFrontmatter(body)
	if err == nil {
		t.Fatal("expected reject when description is omitted")
	}
	var vErr *skill.ValidationError
	if !errors.As(err, &vErr) {
		t.Fatalf("expected *skill.ValidationError; got %T", err)
	}
	if !strings.Contains(vErr.Error(), "description") {
		t.Errorf("error must mention description; got %q", vErr.Error())
	}
}

func TestParseFrontmatterRejectsDeeplyNestedYAML(t *testing.T) {
	// Build a deeply nested flow-style mapping that fits inside the
	// 32 KiB bounded segment. The reject path here is the structural
	// "value must be scalar" rule, not the size cap — that is, the
	// non-scalar nested structure is what trips assertSafeRoot's
	// scalar-only check.
	const depth = 200
	body := "---\nname: ok\ndescription: " +
		strings.Repeat("{a:", depth) +
		"x" +
		strings.Repeat("}", depth) +
		"\n---\n"
	if len(body) > skill.MaxFrontmatterBytes {
		t.Fatalf("test fixture too large for bounded segment: %d", len(body))
	}
	_, err := skill.ParseFrontmatter([]byte(body))
	if err == nil {
		t.Fatal("expected reject for deeply nested YAML in description")
	}
	var vErr *skill.ValidationError
	if !errors.As(err, &vErr) {
		t.Fatalf("expected *skill.ValidationError; got %T", err)
	}
	// Either yaml.v3's own depth/structural limit fires ("is not
	// valid YAML") or our scalar-only assertion fires
	// ("must be a scalar string"). Both close the security goal:
	// deeply-nested YAML rejects without resource exhaustion.
	msg := vErr.Error()
	if !strings.Contains(msg, "scalar") &&
		!strings.Contains(msg, "valid YAML") &&
		!strings.Contains(msg, "mapping") {
		t.Errorf("error must reflect a structural rejection; got %q", msg)
	}
}

func TestParseFrontmatterRejectsLargeInvalidYAML(t *testing.T) {
	// Construct a frontmatter that fits inside the bounded segment
	// but contains pathological invalid YAML structure (unterminated
	// flow mapping). The parser must reject quickly without echoing
	// the body.
	var b strings.Builder
	b.WriteString("---\nname: ok\ndescription: {")
	b.WriteString(strings.Repeat("a:1,", 1000))
	b.WriteString("\n---\n")
	_, err := skill.ParseFrontmatter([]byte(b.String()))
	if err == nil {
		t.Fatal("expected reject for large invalid YAML")
	}
}
