// Package api holds Engine's OpenAPI file, the contract of its public API
// (README.md).
package api

import "embed"

// File is the OpenAPI file's name.
const File = "engine-v1.openapi.json"

// Files holds the OpenAPI file.
//
//go:embed engine-v1.openapi.json
var Files embed.FS
