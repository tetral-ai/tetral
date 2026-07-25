// Package memory is the control-plane domain for memory stores, memories, and
// their immutable version history, together with the list query-option decoding
// and opaque page tokens that drive the memory list endpoints.
//
// OWNS:
//   - Durable tables memory_stores, memories, memory_versions.
//   - List query-option decoding: DecodeListStoresOptions,
//     DecodeListMemoriesOptions, DecodeListMemoryVersionsOptions.
//   - Opaque list page tokens (pagination.go), base64 JSON that is checked for
//     field-consistency against the current request, not cryptographically
//     signed; they pin the query shape (path_prefix, depth, view, order_by,
//     order) across a paginated sequence.
//
// STATE MACHINE (memory-list depth: DecodeListMemoriesOptions -> normalizeListMemoriesOptions -> projectMemoryList):
//
//	depth omitted            -> Depth=0, DepthSet=false -> no depth rollup; matching memories at any nesting are listed directly
//	depth == 0               -> Depth=0, DepthSet=false -> same unbounded listing (zero is an alias of omitted)
//	depth  > 0               -> Depth=n, DepthSet=true  -> paths deeper than n are collapsed into prefix entries
//	depth  < 0               -> rejected ("depth must be positive")
//	depth present but empty  -> rejected ("depth must be an integer")
//
// INVARIANTS:
//   - depth zero and omitted are the same request: both leave DepthSet false and
//     take the unbounded path, so "depth must be positive" applies to negatives
//     only. normalizeListMemoriesOptions also collapses a zero Depth to
//     DepthSet=false; only DepthSet with Depth>0 bounds the walk.
//   - A page token replays only when its embedded Depth and DepthSet match the
//     re-decoded request, so the zero/omitted aliasing stays fixed across pages.
//
// UPDATE-WITH:
//   - internal/memory/query.go (depth and limit decoding)
//   - internal/memory/memory.go (ListMemoriesOptions.Depth, ListMemoriesOptions.DepthSet)
//   - internal/memory/postgresql_store.go (normalizeListMemoriesOptions zero-collapse, projectMemoryList recursion selection)
//   - internal/memory/pagination.go (memoryPageToken Depth/DepthSet round-trip)
package memory

import (
	"net/url"
	"strconv"
)

func DecodeListStoresOptions(values url.Values) (ListStoresOptions, error) {
	var options ListStoresOptions
	if rawLimit := values.Get("limit"); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil {
			return ListStoresOptions{}, &ValidationError{Message: "limit must be an integer"}
		}
		if limit <= 0 {
			return ListStoresOptions{}, &ValidationError{Message: "limit must be between 1 and 100"}
		}
		options.Limit = limit
		options.LimitSet = true
	}
	options.Page = values.Get("page")
	options.CreatedAtGTE = values.Get("created_at[gte]")
	options.CreatedAtLTE = values.Get("created_at[lte]")
	if rawArchived := values.Get("include_archived"); rawArchived != "" {
		includeArchived, err := strconv.ParseBool(rawArchived)
		if err != nil {
			return ListStoresOptions{}, &ValidationError{Message: "include_archived must be a boolean"}
		}
		options.IncludeArchived = includeArchived
	}
	return options, nil
}

func DecodeListMemoriesOptions(values url.Values) (ListMemoriesOptions, error) {
	var options ListMemoriesOptions
	if rawLimit, ok := singleQueryValue(values, "limit"); ok {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil {
			return ListMemoriesOptions{}, &ValidationError{Message: "limit must be an integer"}
		}
		if limit <= 0 {
			return ListMemoriesOptions{}, &ValidationError{Message: "limit must be positive"}
		}
		options.Limit = limit
		options.LimitSet = true
	}
	if rawDepth, ok := singleQueryValue(values, "depth"); ok {
		depth, err := strconv.Atoi(rawDepth)
		if err != nil {
			return ListMemoriesOptions{}, &ValidationError{Message: "depth must be an integer"}
		}
		if depth < 0 {
			return ListMemoriesOptions{}, &ValidationError{Message: "depth must be positive"}
		}
		if depth > 0 {
			options.Depth = depth
			options.DepthSet = true
		}
	}
	options.Page = values.Get("page")
	options.PathPrefix = values.Get("path_prefix")
	options.View = values.Get("view")
	options.OrderBy = values.Get("order_by")
	options.Order = values.Get("order")
	return options, nil
}

func DecodeListMemoryVersionsOptions(values url.Values) (ListMemoryVersionsOptions, error) {
	var options ListMemoryVersionsOptions
	if rawLimit, ok := singleQueryValue(values, "limit"); ok {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil {
			return ListMemoryVersionsOptions{}, &ValidationError{Message: "limit must be an integer"}
		}
		if limit <= 0 {
			return ListMemoryVersionsOptions{}, &ValidationError{Message: "limit must be positive"}
		}
		options.Limit = limit
		options.LimitSet = true
	}
	options.Page = values.Get("page")
	options.MemoryID = values.Get("memory_id")
	options.Operation = values.Get("operation")
	options.SessionID = values.Get("session_id")
	options.APIKeyID = values.Get("api_key_id")
	options.CreatedAtGTE = values.Get("created_at[gte]")
	options.CreatedAtLTE = values.Get("created_at[lte]")
	options.View = values.Get("view")
	return options, nil
}

func singleQueryValue(values url.Values, key string) (string, bool) {
	rawValues, ok := values[key]
	if !ok {
		return "", false
	}
	if len(rawValues) != 1 || rawValues[0] == "" {
		return "", true
	}
	return rawValues[0], true
}
