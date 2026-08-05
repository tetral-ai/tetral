package webconnector

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

func formatSearch(domains []string, hits []SearchHit, refs []Ref) string {
	var b strings.Builder
	b.WriteString("Search results")
	if len(domains) > 0 {
		b.WriteString(", sites: ")
		for i, domain := range domains {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(domain)
			b.WriteString(" and subdomains")
		}
	}
	b.WriteString(":\n\n")
	for i, hit := range hits {
		date := hit.Date
		if date == "" {
			date = "date unknown"
		}
		fmt.Fprintf(&b, "%d. [%s] %s\n   %s   (ref: %s)\n   %s\n", i+1, date, hit.Title, hit.URL, refs[i].ID, hit.Description)
	}
	fmt.Fprintf(&b, "(%d results; open a ref or a URL to read content)", len(hits))
	return b.String()
}

type windowResult struct {
	text      string
	ref       Ref
	next      *int32
	truncated bool
}

func formatWindow(id string, page Page, lineno int32) (windowResult, error) {
	lines := page.Lines
	if len(lines) == 0 {
		lines, _ = normalizeContent(page.Content)
	}
	total := int32(len(lines))
	if lineno < 1 || lineno > total {
		return windowResult{}, fmt.Errorf("lineno out of range: document has %d lines", total)
	}
	start := int(lineno - 1)
	end := start
	used := 0
	for end < len(lines) && end-start < maxWindowLines {
		extra := len([]byte(lines[end]))
		if end > start {
			extra++
		}
		if end > start && used+extra > maxWindowBytes {
			break
		}
		used += extra
		end++
	}
	lineEnd := int32(end)
	truncated := end < len(lines)
	var next *int32
	if truncated {
		value := lineEnd + 1
		next = &value
	}
	title := page.Title
	if title == "" {
		title = page.URL
	}
	suffix := ""
	if page.SourceIncomplete {
		suffix = "; source truncated at capture"
	}
	body := strings.Join(lines[start:end], "\n")
	text := fmt.Sprintf("[%s] %s\nlines %d-%d of %d%s\n\n%s", id, title, lineno, lineEnd, total, suffix, body)
	if truncated {
		text += fmt.Sprintf("\n\n[truncated — continue with open(ref_id: %s, lineno: %d)]", id, *next)
	}
	return windowResult{text: text, ref: Ref{ID: id, URL: page.URL, Title: page.Title, LineStart: lineno, LineEnd: lineEnd, TotalLines: total}, next: next, truncated: truncated}, nil
}

func formatFind(id, pattern string, page Page) (string, error) {
	if len([]byte(pattern)) > maxPatternBytes {
		return "", fmt.Errorf("pattern exceeds %d bytes", maxPatternBytes)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}
	lines := page.Lines
	if len(lines) == 0 {
		lines, _ = normalizeContent(page.Content)
	}
	type match struct {
		number int
		text   string
	}
	matches := make([]match, 0)
	total := 0
	for index, line := range lines {
		if re.MatchString(line) {
			total++
			if len(matches) < maxFindMatches {
				matches = append(matches, match{index + 1, truncateRunes(line, maxFindMatchChars)})
			}
		}
	}
	var b strings.Builder
	word := "matches"
	if total == 1 {
		word = "match"
	}
	fmt.Fprintf(&b, "[%s] %d %s in %d lines", id, total, word, len(lines))
	for _, item := range matches {
		fmt.Fprintf(&b, "\nL%d: %s", item.number, item.text)
	}
	if total > len(matches) {
		fmt.Fprintf(&b, "\n(showing first %d of %d)", maxFindMatches, total)
	}
	return b.String(), nil
}
func truncateRunes(value string, limit int) string {
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	r := []rune(value)
	return string(r[:limit])
}
