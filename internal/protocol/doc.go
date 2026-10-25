// Package protocol holds Engine's types for the client protocol between
// Engine and the CLI. README.md states the decoding, encoding and error
// rules; this comment indexes the code.
//
// Where each concern lives:
//   - decode.go: the token pass and the building blocks every decoder uses.
//   - encode.go: Optional, Nullable, RawObject and the encoder.
//   - errors.go: ErrorCode and DecodeError.
//   - tool_execution.go: the tool execution types, their decoders and their
//     relation checks.
//   - fixtures_test.go runs every fixture in protocol/; decode_test.go covers
//     the decoding rules no fixture shows.
package protocol
