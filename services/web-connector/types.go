package webconnector

import (
	"context"
	"time"
)

const (
	// MaxStoredLineBytes bounds the size of one stored line: storage.go
	// normalizeContent wraps any longer physical line into successive stored
	// lines of at most this many bytes. The value is held strictly below
	// maxWindowBytes (4 KiB < 50 KiB) so a single stored line can never exceed
	// one window's byte budget. That coupling is the window-progress guarantee:
	// format.go formatWindow therefore always admits at least the first whole
	// stored line, so every window advances, next_lineno strictly increases, and
	// every continuation chain terminates. Windows never split a stored line.
	// UPDATE-WITH: storage.go normalizeContent, format.go formatWindow.
	MaxStoredLineBytes = 4 * 1024
	maxSnapshotBytes   = 1024 * 1024
	maxWindowLines     = 2000
	maxWindowBytes     = 50 * 1024
	maxOperations      = 8
	maxSearchHits      = 10
	maxSearchDomains   = 4
	maxFindMatches     = 250
	maxFindMatchChars  = 250
	maxPatternBytes    = 1024
)

type Scope struct {
	WorkspaceID string
	SessionID   string
	ThreadID    string
}

type SearchHit struct {
	URL         string
	Title       string
	Description string
	Date        string
}

type Page struct {
	URL              string
	Title            string
	PublishedTime    string
	Content          string
	TargetHTTPStatus int32
	Tokens           int64
	SourceIncomplete bool
	Lines            []string
	FetchedAt        time.Time
}

type Ref struct {
	ID         string
	URL        string
	Title      string
	LineStart  int32
	LineEnd    int32
	TotalLines int32
}

type BackendOutcomeKind int

const (
	BackendSuccess BackendOutcomeKind = iota
	BackendToolError
	BackendRuntimeError
	BackendUnavailable
)

type BackendOutcome struct {
	Kind             BackendOutcomeKind
	TargetHTTPStatus *int32
	Message          string
	Tokens           int64
	Requests         int64
}

type Backend interface {
	Search(context.Context, string, []string) ([]SearchHit, BackendOutcome)
	Fetch(context.Context, string) (Page, BackendOutcome)
}
