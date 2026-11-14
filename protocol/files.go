// Package protocol holds the files of the contract between the CLI and
// Engine (README.md) for Engine's Go tests: the schemas, their fixtures and
// the case files.
package protocol

import "embed"

//go:embed *.json
var Files embed.FS
