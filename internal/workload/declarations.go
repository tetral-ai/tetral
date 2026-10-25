package workload

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
)

// Declarations is one component's observability table: the events it writes,
// the operations it observes, its metric families and its span templates. The
// file is observability.json at the root of the component's package.
type Declarations struct {
	Service    string                          `json:"service"`
	Events     map[string]EventDeclaration     `json:"events"`
	Operations map[string]OperationDeclaration `json:"operations"`
	Metrics    map[string]MetricDeclaration    `json:"metrics"`
	Spans      map[string]SpanDeclaration      `json:"spans"`
}

type EventDeclaration struct {
	Level    string                      `json:"level"`
	LogType  string                      `json:"log_type"`
	Audience string                      `json:"audience"`
	Fields   map[string]string           `json:"fields"`
	Errors   map[string]ErrorDeclaration `json:"errors,omitempty"`
}

// ErrorDeclaration fixes the safe text a record carries for one error.code.
type ErrorDeclaration struct {
	Class       string `json:"class"`
	MessageSafe string `json:"message_safe"`
}

type OperationDeclaration struct {
	Long   bool     `json:"long"`
	Errors []string `json:"errors,omitempty"`
}

// MetricDeclaration lists each label's allowed values, or, as a JSON string,
// the name of a closed domain fixed in code (such as registered route
// patterns); Buckets is "request", "long" or an explicit list.
type MetricDeclaration struct {
	Type     string                     `json:"type"`
	Audience string                     `json:"audience"`
	Labels   map[string]json.RawMessage `json:"labels"`
	Buckets  json.RawMessage            `json:"buckets,omitempty"`
}

type SpanDeclaration struct {
	Attributes []string `json:"attributes"`
}

//go:embed observability.json
var libraryDeclarationsJSON []byte

// LibraryDeclarations are the records and metrics this package and the
// boundaries every Go service shares (HTTP, internal gRPC, database, queue
// lease, transport security, cold start, pressure points) write for every
// service; a service's own table must not repeat them.
var LibraryDeclarations = mustParseDeclarations(libraryDeclarationsJSON)

var declarationRegistry = struct {
	sync.RWMutex
	tables map[string]*Declarations
}{tables: map[string]*Declarations{}}

// RegisterDeclarations installs a service's table for its loggers and
// observations. Services call it from an init function with their embedded
// observability.json; a malformed or overlapping table panics there.
func RegisterDeclarations(service string, data []byte) {
	table := mustParseDeclarations(data)
	if table.Service != service {
		panic("workload: observability table for " + strconv.Quote(service) + " names service " + strconv.Quote(table.Service))
	}
	for name := range table.Events {
		if _, shared := LibraryDeclarations.Events[name]; shared {
			panic("workload: service " + service + " redeclares library event " + name)
		}
	}
	for name := range table.Operations {
		if _, shared := LibraryDeclarations.Operations[name]; shared {
			panic("workload: service " + service + " redeclares library operation " + name)
		}
	}
	declarationRegistry.Lock()
	defer declarationRegistry.Unlock()
	declarationRegistry.tables[service] = table
}

// RegisteredDeclarations returns the table registered for service, or nil.
func RegisteredDeclarations(service string) *Declarations {
	declarationRegistry.RLock()
	defer declarationRegistry.RUnlock()
	return declarationRegistry.tables[service]
}

// ParseDeclarations reads and validates one observability.json.
func ParseDeclarations(data []byte) (*Declarations, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var table Declarations
	if err := decoder.Decode(&table); err != nil {
		return nil, err
	}
	if table.Service == "" {
		return nil, fmt.Errorf("observability table has no service")
	}
	for name, event := range table.Events {
		if safeDiagnosticEvent(name) != name {
			return nil, fmt.Errorf("event %q is not a valid event name", name)
		}
		if !slices.Contains([]string{"DEBUG", "INFO", "WARN", "ERROR"}, event.Level) {
			return nil, fmt.Errorf("event %q has level %q", name, event.Level)
		}
		if event.LogType != "app" && event.LogType != "audit" {
			return nil, fmt.Errorf("event %q has log_type %q", name, event.LogType)
		}
		if !validAudience(event.Audience) {
			return nil, fmt.Errorf("event %q has audience %q", name, event.Audience)
		}
		for field, kind := range event.Fields {
			if !slices.Contains([]string{"string", "integer", "number", "boolean"}, kind) {
				return nil, fmt.Errorf("event %q field %q has type %q", name, field, kind)
			}
		}
		for code, declared := range event.Errors {
			if code == "" || declared.Class == "" || declared.MessageSafe == "" {
				return nil, fmt.Errorf("event %q has an incomplete error %q", name, code)
			}
		}
	}
	for name, metric := range table.Metrics {
		if !slices.Contains([]string{"counter", "gauge", "histogram", "summary"}, metric.Type) {
			return nil, fmt.Errorf("metric %q has type %q", name, metric.Type)
		}
		if !validAudience(metric.Audience) {
			return nil, fmt.Errorf("metric %q has audience %q", name, metric.Audience)
		}
		if (metric.Type == "histogram") != (len(metric.Buckets) > 0) {
			return nil, fmt.Errorf("metric %q: only a histogram declares buckets", name)
		}
	}
	return &table, nil
}

func validAudience(audience string) bool {
	return audience == "operators" || audience == "benchmarks" || audience == "developers"
}

func mustParseDeclarations(data []byte) *Declarations {
	table, err := ParseDeclarations(data)
	if err != nil {
		panic("workload: invalid observability table: " + err.Error())
	}
	return table
}

// eventDeclared reports whether a logger of service may write event. Loggers
// of services without a registered table are not checked.
func eventDeclared(service, event string) bool {
	if _, ok := LibraryDeclarations.Events[event]; ok {
		return true
	}
	table := RegisteredDeclarations(service)
	if table == nil {
		return true
	}
	_, ok := table.Events[event]
	return ok
}

// lookupEvent returns the declaration of event for service.
func lookupEvent(service, event string) (EventDeclaration, bool) {
	if declared, ok := LibraryDeclarations.Events[event]; ok {
		return declared, true
	}
	if table := RegisteredDeclarations(service); table != nil {
		declared, ok := table.Events[event]
		return declared, ok
	}
	return EventDeclaration{}, false
}

// lookupOperation finds operation for service; with no service, any
// registered table that declares it.
func lookupOperation(service, operation string) (OperationDeclaration, bool) {
	if declared, ok := LibraryDeclarations.Operations[operation]; ok {
		return declared, true
	}
	declarationRegistry.RLock()
	defer declarationRegistry.RUnlock()
	if table := declarationRegistry.tables[service]; table != nil {
		declared, ok := table.Operations[operation]
		return declared, ok
	}
	for _, table := range declarationRegistry.tables {
		if declared, ok := table.Operations[operation]; ok {
			return declared, true
		}
	}
	return OperationDeclaration{}, false
}

// failOnUndeclared is true in test binaries, where a name missing from a
// table fails the test at once; production records diagnostic.invalid_event.
var failOnUndeclared = testing.Testing()

func undeclared(kind, name string) {
	if failOnUndeclared {
		panic("workload: undeclared " + kind + " " + strconv.Quote(name) + "; declare it in the service's observability.json")
	}
}
