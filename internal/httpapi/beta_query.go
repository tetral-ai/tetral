package httpapi

import (
	"net/http"
	"net/url"
)

func requireBetaQuery(r *http.Request, allowed ...string) (url.Values, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, &ValidationError{Message: "invalid query string"}
	}
	allowedSet := map[string]struct{}{"beta": {}}
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, rawValues := range values {
		if _, ok := allowedSet[key]; !ok {
			return nil, &ValidationError{Message: "unknown query parameter " + key}
		}
		if len(rawValues) != 1 {
			return nil, &ValidationError{Message: "duplicate query parameter " + key}
		}
		if rawValues[0] == "" && !emptyQueryValueAllowed(key) {
			return nil, &ValidationError{Message: "empty query parameter " + key}
		}
	}
	if rawValues, ok := values["beta"]; !ok || rawValues[0] != "true" {
		return nil, &ValidationError{Message: "beta must be true"}
	}
	values.Del("beta")
	return values, nil
}

// emptyQueryValueAllowed reports whether key may carry an empty value. The SDK
// sends a null page cursor as "page=", which asks for the first page.
func emptyQueryValueAllowed(key string) bool {
	return key == "page"
}
