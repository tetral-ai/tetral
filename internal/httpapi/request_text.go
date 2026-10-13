package httpapi

import (
	"bytes"
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"
)

// Engine stores request text in PostgreSQL, whose text and jsonb types cannot
// hold U+0000. PublicOperationHandler checks the path and query, and each JSON
// body reader its body, so a request carrying U+0000 (or, in the path and
// query, invalid UTF-8) gets 400 before any lookup or write.

// rejectUnstorableJSONText reports U+0000 in any string or object key of a
// JSON body. Malformed JSON is left to the route's strict decoder.
func rejectUnstorableJSONText(body []byte) error {
	// Valid JSON can carry U+0000 only as the escape \u0000.
	if !bytes.Contains(body, []byte(`\u0000`)) {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil
		}
		if text, ok := token.(string); ok && strings.IndexByte(text, 0) >= 0 {
			return &ValidationError{Message: "request body must not contain U+0000"}
		}
	}
}

// rejectUnstorableRequestTarget reports a decoded path, query key or query
// value that is not valid UTF-8 or contains U+0000. A malformed query escape
// is left to the route's query parsing; the pairs that did parse are checked.
func rejectUnstorableRequestTarget(r *http.Request) error {
	if !storableText(r.URL.Path) {
		return &ValidationError{Message: "request path must be valid UTF-8 without U+0000"}
	}
	values, _ := url.ParseQuery(r.URL.RawQuery)
	for _, key := range slices.Sorted(maps.Keys(values)) {
		if !storableText(key) {
			return &ValidationError{Message: "query parameter names must be valid UTF-8 without U+0000"}
		}
		for _, value := range values[key] {
			if !storableText(value) {
				return &ValidationError{Message: "query parameter " + key + " must be valid UTF-8 without U+0000"}
			}
		}
	}
	return nil
}

func storableText(text string) bool {
	return utf8.ValidString(text) && strings.IndexByte(text, 0) < 0
}
