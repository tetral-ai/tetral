package skillpackage_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/drive/skillpackage"
)

func TestFrontmatterRules(t *testing.T) {
	for _, tc := range []struct{ name, block, want string }{
		{"ordinary", "name: report\ndescription: Writes reports.\n", ""},
		{"reserved vendor", "name: claude-report\ndescription: valid\n", "reserved vendor tokens"},
		{"name charset", "name: Report\ndescription: valid\n", "lowercase letters, digits, and hyphens"},
		{"anchor", "name: &n report\ndescription: valid\n", "YAML anchors"},
		{"alias", "name: report\ndescription: *n\n", "not valid YAML"},
		{"tag", "name: !!str report\ndescription: valid\n", "explicit YAML tags"},
		{"NUL description", "name: report\ndescription: \"a\\0b\"\n", "U+0000"},
		{"extra top-level data", "name: report\ndescription: valid\nmetadata: {team: reports}\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := skillpackage.ReadFrontmatter(strings.NewReader("---\n" + tc.block + "---\nbody"))
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}

var errReadBeyondFrontmatter = errors.New("read beyond permitted frontmatter window")

type boundedEndlessReader struct {
	prefix          string
	remaining, read int
}

func (r *boundedEndlessReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, errReadBeyondFrontmatter
	}
	n := min(len(p), r.remaining)
	for i := range n {
		if r.read < len(r.prefix) {
			p[i] = r.prefix[r.read]
		} else {
			p[i] = 'x'
		}
		r.read++
	}
	r.remaining -= n
	return n, nil
}
func TestFrontmatterConsumptionIsBounded(t *testing.T) {
	for _, tc := range []struct {
		name, prefix string
		valid        bool
	}{{"valid header before endless body", skillText, true}, {"endless unterminated header", "---\n", false}} {
		t.Run(tc.name, func(t *testing.T) {
			// The public rule permits CRLF on both delimiters. The source refuses a
			// read beyond that bound, so removing the limiter fails promptly.
			const bound = skillpackage.MaxFrontmatterBytes + 2*len("---\r\n")
			reader := &boundedEndlessReader{prefix: tc.prefix, remaining: bound}
			_, err := skillpackage.ReadFrontmatter(reader)
			if errors.Is(err, errReadBeyondFrontmatter) {
				t.Fatal("consumed beyond frontmatter window")
			}
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid && (err == nil || !strings.Contains(err.Error(), "exceeds")) {
				t.Fatalf("unterminated header: %v", err)
			}
			if reader.read > bound {
				t.Fatalf("consumed %d > %d", reader.read, bound)
			}
		})
	}
}
