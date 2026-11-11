package enginetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/dlclark/regexp2"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/tetral-ai/tetral/internal/protocol"
	contractfiles "github.com/tetral-ai/tetral/protocol"
)

// The schema files of protocol/, named without their .schema.json.
const (
	connectionFile = "connection-v1"
	toolFile       = "tool-execution-v1"
	driveFile      = "drive-transfer-v1"
)

// definition names one definition of a schema file.
type definition struct {
	file string
	name string
}

func (d definition) String() string { return d.file + ".schema.json#/$defs/" + d.name }

// exchange is one request with its answer. Checking a part the CLI sends
// records it here, and the parts received afterwards are checked against
// it: an error body against the status and the endpoint, a page against its
// query, a commit's results against its changes, an upload's answer against
// its URL and length, a range against the one requested.
type exchange struct {
	status       int
	endpoint     protocol.DriveTransferEndpoint
	filesQuery   protocol.Optional[protocol.ListFilesQuery]
	changesQuery protocol.Optional[protocol.ListChangesQuery]
	contentPath  protocol.Optional[protocol.ContentPathParams]
	upload       protocol.Optional[protocol.UploadContentRequestHeaders]
	download     protocol.Optional[protocol.DownloadContentRequestHeaders]
	commit       protocol.Optional[protocol.CommitRequest]
}

// part is how one definition is checked beyond its schema: who sends it,
// and its decoder of internal/protocol. A query, path parameters and headers
// are objects of strings, as the fixtures hold them.
type part struct {
	sent       bool
	body       func(x *exchange, data []byte) error
	parameters func(x *exchange, values map[string][]string) error
}

func sentBody[T any](decode func([]byte) (T, error)) part {
	return part{sent: true, body: func(_ *exchange, data []byte) error { _, err := decode(data); return err }}
}

func receivedBody[T any](decode func([]byte) (T, error)) part {
	return part{body: func(_ *exchange, data []byte) error { _, err := decode(data); return err }}
}

func single(values map[string][]string) map[string]string {
	params := make(map[string]string, len(values))
	for name, value := range values {
		params[name] = value[0]
	}
	return params
}

var parts = map[definition]part{
	{connectionFile, "EventsQuery"}: {sent: true, parameters: func(_ *exchange, v map[string][]string) error {
		_, err := protocol.DecodeEventsQuery(url.Values(v))
		return err
	}},
	{connectionFile, "Ready"}:               sentBody(protocol.DecodeReady),
	{connectionFile, "Heartbeat"}:           sentBody(protocol.DecodeHeartbeat),
	{connectionFile, "SessionStatus"}:       sentBody(protocol.DecodeSessionStatus),
	{connectionFile, "McpTools"}:            sentBody(protocol.DecodeMcpTools),
	{connectionFile, "CredentialRequest"}:   sentBody(protocol.DecodeCredentialRequest),
	{connectionFile, "GitTicketRequest"}:    sentBody(protocol.DecodeGitTicketRequest),
	{connectionFile, "DisconnectRequest"}:   sentBody(protocol.DecodeDisconnectRequest),
	{connectionFile, "ToolStatus"}:          sentBody(protocol.DecodeToolStatus),
	{connectionFile, "DriveSyncReport"}:     sentBody(protocol.DecodeDriveSyncReport),
	{connectionFile, "ProtocolErrorReport"}: sentBody(protocol.DecodeProtocolErrorReport),
	{toolFile, "ToolStatus"}:                sentBody(protocol.DecodeToolStatus),
	{toolFile, "DriveSyncReport"}:           sentBody(protocol.DecodeDriveSyncReport),
	{toolFile, "ProtocolErrorReport"}:       sentBody(protocol.DecodeProtocolErrorReport),
	{toolFile, "ReadyExtension"}:            sentBody(protocol.DecodeReadyExtension),

	{connectionFile, "Downstream"}:           receivedBody(protocol.DecodeDownstream),
	{connectionFile, "Acknowledgement"}:      receivedBody(protocol.DecodeAcknowledgement),
	{connectionFile, "GrantAcknowledgement"}: receivedBody(protocol.DecodeGrantAcknowledgement),
	{connectionFile, "CredentialResponse"}:   receivedBody(protocol.DecodeCredentialResponse),
	{connectionFile, "GitTicketResponse"}:    receivedBody(protocol.DecodeGitTicketResponse),
	{connectionFile, "ErrorBody"}: {body: func(x *exchange, data []byte) error {
		_, err := protocol.DecodeErrorBody(x.status, data)
		return err
	}},
	{toolFile, "Downstream"}:      receivedBody(protocol.DecodeToolDownstream),
	{toolFile, "Acknowledgement"}: receivedBody(protocol.DecodeAcknowledgement),
	{toolFile, "SandboxPolicy"}:   receivedBody(protocol.DecodeSandboxPolicy),

	{driveFile, "ListFilesQuery"}: {sent: true, parameters: func(x *exchange, v map[string][]string) error {
		query, err := protocol.DecodeListFilesQuery(url.Values(v))
		x.filesQuery = protocol.Some(query)
		return err
	}},
	{driveFile, "ListChangesQuery"}: {sent: true, parameters: func(x *exchange, v map[string][]string) error {
		query, err := protocol.DecodeListChangesQuery(url.Values(v))
		x.changesQuery = protocol.Some(query)
		return err
	}},
	{driveFile, "CommonQuery"}: {sent: true, parameters: func(_ *exchange, v map[string][]string) error {
		_, err := protocol.DecodeCommonQuery(url.Values(v))
		return err
	}},
	{driveFile, "DrivePathParams"}: {sent: true, parameters: func(_ *exchange, v map[string][]string) error {
		_, err := protocol.DecodeDrivePathParams(single(v))
		return err
	}},
	{driveFile, "ContentPathParams"}: {sent: true, parameters: func(x *exchange, v map[string][]string) error {
		params, err := protocol.DecodeContentPathParams(single(v))
		x.contentPath = protocol.Some(params)
		return err
	}},
	{driveFile, "UploadContentRequestHeaders"}: {sent: true, parameters: func(x *exchange, v map[string][]string) error {
		headers, err := protocol.DecodeUploadContentRequestHeaders(http.Header(v))
		if err != nil {
			return err
		}
		x.upload = protocol.Some(headers)
		return protocol.CheckUploadLength(headers)
	}},
	{driveFile, "DownloadContentRequestHeaders"}: {sent: true, parameters: func(x *exchange, v map[string][]string) error {
		headers, err := protocol.DecodeDownloadContentRequestHeaders(http.Header(v))
		x.download = protocol.Some(headers)
		return err
	}},
	{driveFile, "CommitRequest"}: {sent: true, body: func(x *exchange, data []byte) error {
		request, err := protocol.DecodeCommitRequest(data)
		if err != nil {
			return err
		}
		x.commit = protocol.Some(request)
		return protocol.CheckCommitChanges(request)
	}},

	{driveFile, "PartialContentResponseHeaders"}: {parameters: func(x *exchange, v map[string][]string) error {
		_, err := protocol.DecodePartialContentResponseHeaders(http.Header(v), protocol.PartialContentResponseContext{RequestHeaders: x.download})
		return err
	}},
	{driveFile, "RateLimitedResponseHeaders"}: {parameters: func(_ *exchange, v map[string][]string) error {
		_, err := protocol.DecodeRateLimitedResponseHeaders(http.Header(v))
		return err
	}},
	{driveFile, "DriveChanged"}: receivedBody(protocol.DecodeDriveChanged),
	{driveFile, "ListFilesResponse"}: {body: func(x *exchange, data []byte) error {
		_, err := protocol.DecodeListFilesResponse(data, protocol.ListFilesResponseContext{Query: x.filesQuery})
		return err
	}},
	{driveFile, "ListChangesResponse"}: {body: func(x *exchange, data []byte) error {
		_, err := protocol.DecodeListChangesResponse(data, protocol.ListChangesResponseContext{Query: x.changesQuery})
		return err
	}},
	{driveFile, "UploadContentResponse"}: {body: func(x *exchange, data []byte) error {
		_, err := protocol.DecodeUploadContentResponse(data, protocol.UploadContentResponseContext{PathParams: x.contentPath, RequestHeaders: x.upload})
		return err
	}},
	{driveFile, "CommitResponse"}: {body: func(x *exchange, data []byte) error {
		_, err := protocol.DecodeCommitResponse(data, protocol.CommitResponseContext{Request: x.commit})
		return err
	}},
	{driveFile, "Error"}: {body: func(x *exchange, data []byte) error {
		_, err := protocol.DecodeDriveTransferError(x.endpoint, x.status, data)
		return err
	}},
}

// sent checks a part the CLI sends, as JSON, against its definition's
// schema and decoder, and records it for the answer.
func (x *exchange) sent(d definition, value []byte) error { return x.check(d, value, true) }

// received checks a part the CLI receives, as JSON, against its
// definition's schema and decoder, given the request recorded so far.
func (x *exchange) received(d definition, value []byte) error { return x.check(d, value, false) }

func (x *exchange) check(d definition, value []byte, sent bool) error {
	p, err := partOf(d, sent)
	if err != nil {
		return err
	}
	if err := validate(d, value); err != nil {
		return err
	}
	if p.body != nil {
		return p.body(x, value)
	}
	var params map[string]string
	if err := json.Unmarshal(value, &params); err != nil {
		return fmt.Errorf("%s: not an object of strings: %w", d, err)
	}
	values := make(map[string][]string, len(params))
	for name, value := range params {
		values[name] = []string{value}
	}
	return p.parameters(x, values)
}

// sentValues and receivedValues check a query, path parameters or headers
// as HTTP carries them.
func (x *exchange) sentValues(d definition, values map[string][]string) error {
	return x.checkValues(d, values, true)
}

func (x *exchange) receivedValues(d definition, values map[string][]string) error {
	return x.checkValues(d, values, false)
}

func (x *exchange) checkValues(d definition, values map[string][]string, sent bool) error {
	p, err := partOf(d, sent)
	if err != nil {
		return err
	}
	if p.parameters == nil {
		return fmt.Errorf("%s is a body", d)
	}
	if err := validate(d, parameters(values)); err != nil {
		return err
	}
	return p.parameters(x, values)
}

func partOf(d definition, sent bool) (part, error) {
	p, ok := parts[d]
	switch {
	case !ok:
		return p, fmt.Errorf("%s has no check", d)
	case p.sent && !sent:
		return p, fmt.Errorf("%s is sent by the CLI, not received", d)
	case !p.sent && sent:
		return p, fmt.Errorf("%s is received by the CLI, not sent", d)
	}
	return p, nil
}

// parameters is the JSON object of a query's, a path's or a header's
// first values, the form their schemas and fixtures have.
func parameters(values map[string][]string) []byte {
	params := make(map[string]string, len(values))
	for name, value := range values {
		if len(value) > 0 {
			params[name] = value[0]
		}
	}
	data, _ := json.Marshal(params)
	return data
}

// ecmaScript matches a schema pattern as ECMA 262 does, which JSON Schema
// names for patterns: the contract's use lookaheads, which Go's regexp
// lacks.
type ecmaScript regexp2.Regexp

func (r *ecmaScript) MatchString(s string) bool {
	matched, err := (*regexp2.Regexp)(r).MatchString(s)
	return err == nil && matched
}

func (r *ecmaScript) String() string { return (*regexp2.Regexp)(r).String() }

var schemas struct {
	sync.Mutex
	compiler *jsonschema.Compiler
	compiled map[definition]*jsonschema.Schema
}

func compileECMAScript(pattern string) (jsonschema.Regexp, error) {
	compiled, err := regexp2.Compile(pattern, regexp2.ECMAScript|regexp2.Unicode)
	return (*ecmaScript)(compiled), err
}

const schemaBase = "file:///protocol/"

func schemaOf(d definition) (*jsonschema.Schema, error) {
	schemas.Lock()
	defer schemas.Unlock()
	if schemas.compiler == nil {
		compiler := jsonschema.NewCompiler()
		compiler.UseRegexpEngine(compileECMAScript)
		for _, file := range []string{connectionFile, toolFile, driveFile} {
			data, err := contractfiles.Files.ReadFile(file + ".schema.json")
			if err != nil {
				return nil, err
			}
			document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
			if err != nil {
				return nil, fmt.Errorf("%s.schema.json: %w", file, err)
			}
			if err := compiler.AddResource(schemaBase+file+".schema.json", document); err != nil {
				return nil, err
			}
		}
		schemas.compiler, schemas.compiled = compiler, map[definition]*jsonschema.Schema{}
	}
	if schema, ok := schemas.compiled[d]; ok {
		return schema, nil
	}
	schema, err := schemas.compiler.Compile(schemaBase + d.file + ".schema.json#/$defs/" + d.name)
	if err != nil {
		return nil, err
	}
	schemas.compiled[d] = schema
	return schema, nil
}

// validate checks value, as JSON, against d's schema.
func validate(d definition, value []byte) error {
	schema, err := schemaOf(d)
	if err != nil {
		return err
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(value))
	if err != nil {
		return fmt.Errorf("%s: not JSON: %w", d, err)
	}
	if err := schema.Validate(instance); err != nil {
		// The first line of the validator's text names the schema's
		// location again; the others say what fails where.
		_, causes, _ := strings.Cut(err.Error(), "\n")
		return fmt.Errorf("%s: %s", d, strings.ReplaceAll(strings.TrimPrefix(causes, "- "), "\n- ", "; "))
	}
	return nil
}
