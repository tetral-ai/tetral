// Package api holds Engine's OpenAPI file, the contract of its public API
// (README.md).
package api

import "embed"

// File is the OpenAPI file's name, and Unreachable the name of the list of
// what the file declares and no test can reach.
const (
	File        = "engine-v1.openapi.json"
	Unreachable = "unreachable.json"
)

// Files holds the OpenAPI file and the list.
//
//go:embed engine-v1.openapi.json unreachable.json
var Files embed.FS
