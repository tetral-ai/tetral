package webconnector

import (
	"strings"
	"testing"
)

func TestFormatterGoldensMatchSearchOpenFindAndErrorVocabulary(t *testing.T) {
	t.Parallel()
	search := formatSearch([]string{"example.com"}, []SearchHit{{URL: "https://example.com/", Title: "Example", Description: "Description"}}, []Ref{{ID: "r_example"}})
	wantSearch := "Search results, sites: example.com and subdomains:\n\n1. [date unknown] Example\n   https://example.com/   (ref: r_example)\n   Description\n(1 results; open a ref or a URL to read content)"
	if search != wantSearch {
		t.Fatalf("search:\n%s\nwant:\n%s", search, wantSearch)
	}
	if strings.Contains(search, "docs") {
		t.Fatalf("search result echoed request query: %q", search)
	}
	open, err := formatWindow("r_example", Page{URL: "https://example.com/", Title: "Example", Lines: []string{"first", "second"}, SourceIncomplete: true}, 1)
	if err != nil {
		t.Fatal(err)
	}
	wantOpen := "[r_example] Example\nlines 1-2 of 2; source truncated at capture\n\nfirst\nsecond"
	if open.text != wantOpen {
		t.Fatalf("open=%q want=%q", open.text, wantOpen)
	}
	find, err := formatFind("r_example", "first", Page{Lines: []string{"first", "second"}})
	if err != nil {
		t.Fatal(err)
	}
	wantFind := "[r_example] 1 match in 2 lines\nL1: first"
	if find != wantFind {
		t.Fatalf("find=%q want=%q", find, wantFind)
	}
	if strings.Contains(find, "pattern first") {
		t.Fatalf("find result echoed request pattern: %q", find)
	}
	if _, err := formatWindow("r_example", Page{Lines: []string{"only"}}, 2); err == nil || err.Error() != "lineno out of range: document has 1 lines" {
		t.Fatalf("range error=%v", err)
	}
	secretPattern := "(?P<private-token>"
	if _, err := formatFind("r_example", secretPattern, Page{Lines: []string{"only"}}); err == nil || err.Error() != "pattern is invalid" || strings.Contains(err.Error(), secretPattern) {
		t.Fatalf("regex error=%q; want fixed safe reason", err)
	}
	for _, text := range []string{search, open.text, find, "URL could not be fetched", "web backend temporarily unavailable", "tool delivery conflict"} {
		for _, forbidden := range []string{"jina.ai", "TETRAL_BLOB", "fixture-key", "base64"} {
			if containsFold(text, forbidden) {
				t.Fatalf("model-visible text %q contains %q", text, forbidden)
			}
		}
	}
}

func containsFold(value, part string) bool {
	return len(part) > 0 && strings.Contains(strings.ToLower(value), strings.ToLower(part))
}
