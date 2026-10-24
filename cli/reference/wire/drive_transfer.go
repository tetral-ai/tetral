package wire

import (
	"slices"
	"strings"
)

// The limits of DriveTransferLimits that the messages below are checked
// against.
const (
	ListPageMaxItems            = 1_000
	ListPageMaxJSONBytes        = 4_194_304
	CommitMinChanges            = 1
	CommitMaxChanges            = 10_000
	CommitRequestMaxJSONBytes   = 16_777_216
	CommitResponseMaxJSONBytes  = 16_777_216
	RuleMaxUTF8Bytes            = 512
	FileMaxBytes                = 500_000_000
	DriveChangedSSEMaxJSONBytes = 65_536
	RetryAfterMinSeconds        = 1
	RetryAfterMaxSeconds        = 60
)

type CommonQuery struct {
	ProtocolVersion string `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID     `json:"connection_id"`
	BindingID       ID     `json:"binding_id"`
}

func (CommonQuery) MaxBytes() int { return MessageMaxBytes }

type ListFilesQuery struct {
	ProtocolVersion string `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID     `json:"connection_id"`
	BindingID       ID     `json:"binding_id"`
	// AfterPath is the last path of the previous page, absent for the first
	// page.
	AfterPath Opt[string] `json:"after_path,omitzero"`
}

func (ListFilesQuery) MaxBytes() int { return MessageMaxBytes }

type ListChangesQuery struct {
	ProtocolVersion string  `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID      `json:"connection_id"`
	BindingID       ID      `json:"binding_id"`
	AfterSeq        Decimal `json:"after_seq"`
	// AfterIndex continues inside the commit AfterSeq names.
	AfterIndex Opt[Decimal] `json:"after_index,omitzero"`
}

func (ListChangesQuery) MaxBytes() int { return MessageMaxBytes }

type DrivePathParams struct {
	_       Closed
	DriveID ID `json:"drive_id"`
}

func (DrivePathParams) MaxBytes() int { return MessageMaxBytes }

type ContentPathParams struct {
	_       Closed
	DriveID ID     `json:"drive_id"`
	Sha256  Sha256 `json:"sha256"`
}

func (ContentPathParams) MaxBytes() int { return MessageMaxBytes }

type UploadContentRequestHeaders struct {
	ContentType string `json:"Content-Type" wire:"const=application/octet-stream"`
	// ContentLength is required: Engine refuses an upload without it.
	ContentLength Decimal `json:"Content-Length"`
}

func (UploadContentRequestHeaders) MaxBytes() int { return MessageMaxBytes }

func (headers *UploadContentRequestHeaders) check() error {
	return rule(headers.ContentLength.Value() <= FileMaxBytes, "Content-Length over 500,000,000")
}

// ByteRange is one range of a Range header: `first-last`, `first-` (Last
// -1) or the suffix `-n` (First -1, Last n).
type ByteRange struct{ First, Last int64 }

// RangeHeader is a Range value of the form `bytes=<range>[, <range>…]`.
type RangeHeader string

func (header *RangeHeader) check() error {
	_, ok := byteRanges(string(*header))
	return want(ok, "a byte range")
}

func byteRanges(text string) ([]ByteRange, bool) {
	specs, isBytes := strings.CutPrefix(text, "bytes=")
	if !isBytes {
		return nil, false
	}
	parts := strings.Split(specs, ",")
	ranges := make([]ByteRange, 0, len(parts))
	for position, spec := range parts {
		// Spaces and tabs may stand only next to a comma.
		if position > 0 {
			spec = strings.TrimLeft(spec, " \t")
		}
		if position < len(parts)-1 {
			spec = strings.TrimRight(spec, " \t")
		}
		first, last, hasDash := strings.Cut(spec, "-")
		if !hasDash || (first == "" && last == "") {
			return nil, false
		}
		byteRange := ByteRange{First: -1, Last: -1}
		for _, bound := range []struct {
			text  string
			value *int64
		}{{first, &byteRange.First}, {last, &byteRange.Last}} {
			if bound.text == "" {
				continue
			}
			if strings.Trim(bound.text, "0123456789") != "" {
				return nil, false
			}
			value, ok := digits(bound.text)
			if !ok {
				return nil, false
			}
			*bound.value = value
		}
		ranges = append(ranges, byteRange)
	}
	return ranges, true
}

// digits reads a run of decimal digits, leading zeros allowed.
func digits(text string) (int64, bool) {
	var value int64
	for _, digit := range []byte(text) {
		if value > (1<<63-1-int64(digit-'0'))/10 {
			return 0, false
		}
		value = value*10 + int64(digit-'0')
	}
	return value, true
}

// Answer is the bytes first to last that a 206 answer to the range over
// total bytes holds; ok is false where the answer is 416.
func (byteRange ByteRange) Answer(total int64) (first, last int64, ok bool) {
	switch {
	case byteRange.First < 0:
		first, last = max(total-byteRange.Last, 0), total-1
		if byteRange.Last == 0 {
			last = -1
		}
	case byteRange.Last < 0:
		first, last = byteRange.First, total-1
	default:
		first, last = byteRange.First, min(byteRange.Last, total-1)
	}
	return first, last, first < total && first <= last
}

type DownloadContentRequestHeaders struct {
	Range Opt[RangeHeader] `json:"Range,omitzero"`
}

func (DownloadContentRequestHeaders) MaxBytes() int { return MessageMaxBytes }

func (headers *DownloadContentRequestHeaders) check() error {
	if !headers.Range.Set {
		return nil
	}
	ranges, _ := byteRanges(string(headers.Range.Value))
	for _, byteRange := range ranges {
		if byteRange.First >= 0 && byteRange.Last >= 0 && byteRange.First > byteRange.Last {
			return rule(false, "range end before its start")
		}
	}
	return rule(len(ranges) == 1, "more than one range")
}

// ContentRange is a Content-Range value `bytes <first>-<last>/<total>`.
type ContentRange string

func (contentRange *ContentRange) check() error {
	_, _, _, ok := contentRange.Bounds()
	return want(ok, "a content range")
}

// Bounds are the first byte, the last byte and the content's total size.
func (contentRange ContentRange) Bounds() (first, last, total int64, ok bool) {
	rest, isBytes := strings.CutPrefix(string(contentRange), "bytes ")
	firstText, rest, hasDash := strings.Cut(rest, "-")
	lastText, totalText, hasSlash := strings.Cut(rest, "/")
	if !isBytes || !hasDash || !hasSlash {
		return 0, 0, 0, false
	}
	first, okFirst := decimal(firstText)
	last, okLast := decimal(lastText)
	total, okTotal := decimal(totalText)
	return first, last, total, okFirst && okLast && okTotal
}

type PartialContentResponseHeaders struct {
	ContentRange ContentRange `json:"Content-Range"`
}

func (PartialContentResponseHeaders) MaxBytes() int { return MessageMaxBytes }

func (headers *PartialContentResponseHeaders) check() error {
	first, last, total, _ := headers.ContentRange.Bounds()
	return rule(first <= last && last < total && total <= FileMaxBytes, "content range outside the content")
}

func (headers *PartialContentResponseHeaders) checkAnswer(request DownloadContentRequestHeaders) error {
	first, last, total, _ := headers.ContentRange.Bounds()
	ranges, _ := byteRanges(string(request.Range.Value))
	if !request.Range.Set || len(ranges) != 1 {
		return rule(false, "content range not the one requested")
	}
	askedFirst, askedLast, satisfiable := ranges[0].Answer(total)
	return rule(satisfiable && askedFirst == first && askedLast == last, "content range not the one requested")
}

// RetryAfter is a Retry-After value: whole seconds from 1 to 60.
type RetryAfter string

func (retryAfter *RetryAfter) check() error {
	seconds, ok := decimal(string(*retryAfter))
	return want(ok && seconds >= RetryAfterMinSeconds && seconds <= RetryAfterMaxSeconds,
		"Retry-After seconds from 1 to 60")
}

type RateLimitedResponseHeaders struct {
	RetryAfter RetryAfter `json:"Retry-After"`
}

func (RateLimitedResponseHeaders) MaxBytes() int { return MessageMaxBytes }

type FileEntry struct {
	Path          DrivePath `json:"path"`
	VersionID     ID        `json:"version_id"`
	ContentSha256 Sha256    `json:"content_sha256"`
	SizeBytes     int64     `json:"size_bytes" wire:"min=0,max=500000000"`
	Executable    bool      `json:"executable"`
}

type ListFilesResponse struct {
	Files   []FileEntry `json:"files" wire:"max=1000"`
	HasMore bool        `json:"has_more"`
	// StartSeq is the drive's change sequence at the first page, from which
	// catch-up lists changes; only the first page carries it.
	StartSeq Opt[int64] `json:"start_seq,omitzero" wire:"min=0"`
}

func (ListFilesResponse) MaxBytes() int { return ListPageMaxJSONBytes }

func (page *ListFilesResponse) check() error {
	paths := make([]string, len(page.Files))
	for index, entry := range page.Files {
		paths[index] = string(entry.Path)
	}
	// Strictly increasing, so no path is listed twice.
	for index := 1; index < len(paths); index++ {
		if paths[index-1] >= paths[index] {
			return rule(false, "files not in path order")
		}
	}
	if err := rule(uniqueBy(paths, caseFolded), "paths that differ only in case"); err != nil {
		return err
	}
	for _, path := range paths {
		for slash := strings.IndexByte(path, '/'); slash >= 0; slash = nextSlash(path, slash) {
			if _, listed := slices.BinarySearch(paths, path[:slash]); listed {
				return rule(false, "a file listed above another file")
			}
		}
	}
	return rule(!page.HasMore || len(paths) > 0, "has_more on an empty page")
}

func nextSlash(path string, after int) int {
	next := strings.IndexByte(path[after+1:], '/')
	if next < 0 {
		return -1
	}
	return after + 1 + next
}

func (page *ListFilesResponse) checkAnswer(query ListFilesQuery) error {
	if err := rule(page.StartSeq.Set != query.AfterPath.Set, "start_seq exactly on the first page"); err != nil {
		return err
	}
	if query.AfterPath.Set && len(page.Files) > 0 {
		return rule(string(page.Files[0].Path) > query.AfterPath.Value, "page not after its cursor")
	}
	return nil
}

// ChangeRecord is one change of a drive: commit Seq's change Index.
type ChangeRecord struct {
	Seq       int64     `json:"seq" wire:"min=1"`
	Index     int64     `json:"index" wire:"min=0"`
	Path      DrivePath `json:"path"`
	Operation string    `json:"operation"`
	VersionID ID        `json:"version_id"`
	// ContentSha256 is null exactly for a deletion.
	ContentSha256 Nullable[Sha256] `json:"content_sha256"`
	SizeBytes     int64            `json:"size_bytes" wire:"min=0,max=500000000"`
	Executable    bool             `json:"executable"`
}

func (record *ChangeRecord) check() error {
	if record.Operation == "deleted" {
		return want(!record.ContentSha256.Valid && record.SizeBytes == 0 && !record.Executable, "a deletion without content")
	}
	if err := oneOf(record.Operation, "created", "modified", "deleted"); err != nil {
		return err
	}
	return want(record.ContentSha256.Valid, "a write with content")
}

func (record ChangeRecord) after(seq, index int64) bool {
	return record.Seq > seq || (record.Seq == seq && record.Index > index)
}

type ListChangesResponse struct {
	Records []ChangeRecord `json:"records" wire:"max=1000"`
	HasMore bool           `json:"has_more"`
}

func (ListChangesResponse) MaxBytes() int { return ListPageMaxJSONBytes }

func (page *ListChangesResponse) check() error {
	// Within one commit, records count up from index 0, deletions first.
	for index := 1; index < len(page.Records); index++ {
		earlier, later := page.Records[index-1], page.Records[index]
		if !later.after(earlier.Seq, earlier.Index) {
			return rule(false, "records not in order")
		}
		if earlier.Seq == later.Seq {
			if later.Index != earlier.Index+1 {
				return rule(false, "index gap")
			}
			if earlier.Operation != "deleted" && later.Operation == "deleted" {
				return rule(false, "a deletion after a write")
			}
		} else if later.Index != 0 {
			return rule(false, "index gap")
		}
	}
	return rule(!page.HasMore || len(page.Records) > 0, "has_more on an empty page")
}

func (page *ListChangesResponse) checkAnswer(query ListChangesQuery) error {
	if len(page.Records) == 0 {
		return nil
	}
	first, seq := page.Records[0], query.AfterSeq.Value()
	if !query.AfterIndex.Set {
		if err := rule(first.Seq > seq, "page not after its cursor"); err != nil {
			return err
		}
		return rule(first.Index == 0, "index gap")
	}
	index := query.AfterIndex.Value.Value()
	if err := rule(first.after(seq, index), "page not after its cursor"); err != nil {
		return err
	}
	next := int64(0)
	if first.Seq == seq {
		next = index + 1
	}
	return rule(first.Index == next, "index gap")
}

// DriveChanged is a drive.changed event of the connection's stream.
type DriveChanged struct{ Downstream }

func (event *DriveChanged) decodeTree(tree any) error {
	if err := event.Downstream.decodeTree(tree); err != nil {
		return err
	}
	return want(event.Type == "drive.changed", "a drive.changed event")
}

func (event DriveChanged) MarshalJSON() ([]byte, error) { return event.Downstream.MarshalJSON() }

func (DriveChanged) MaxBytes() int { return DriveChangedSSEMaxJSONBytes }

type UploadContentResponse struct {
	ContentSha256 Sha256 `json:"content_sha256"`
	SizeBytes     int64  `json:"size_bytes" wire:"min=0,max=500000000"`
}

func (UploadContentResponse) MaxBytes() int { return MessageMaxBytes }

// UploadRequest is what an upload response answers: the content's URL
// parameters and its headers.
type UploadRequest struct {
	Path    ContentPathParams
	Headers UploadContentRequestHeaders
}

func (response *UploadContentResponse) checkAnswer(request UploadRequest) error {
	if err := rule(response.ContentSha256 == request.Path.Sha256, "hash differs from the URL"); err != nil {
		return err
	}
	return rule(response.SizeBytes == request.Headers.ContentLength.Value(), "size differs from Content-Length")
}

// CommitChange is one change of a commit, a WriteChange or a DeleteChange
// named by the member `change`. Both are closed objects: a member the hub
// does not know, such as a writer, is refused rather than ignored.
type CommitChange struct{ Change commitChange }

type commitChange interface {
	variant
	path() string
	base() (Sha256, bool)
}

type WriteChange struct {
	_ Closed
	// Path is any string; the hub refuses a path that breaks the path rules
	// in that change's result.
	Path string `json:"path"`
	// BaseSha256 is the content this machine last saw, null for a new file.
	BaseSha256    Nullable[Sha256] `json:"base_sha256"`
	ContentSha256 Sha256           `json:"content_sha256"`
	Executable    bool             `json:"executable"`
}

type DeleteChange struct {
	_          Closed
	Path       string `json:"path"`
	BaseSha256 Sha256 `json:"base_sha256"`
}

func (*WriteChange) variantName() string  { return "write" }
func (*DeleteChange) variantName() string { return "delete" }

func (change *WriteChange) path() string  { return change.Path }
func (change *DeleteChange) path() string { return change.Path }

func (change *WriteChange) base() (Sha256, bool) {
	return change.BaseSha256.Value, change.BaseSha256.Valid
}
func (change *DeleteChange) base() (Sha256, bool) { return change.BaseSha256, true }

func (change *CommitChange) decodeTree(tree any) (err error) {
	change.Change, err = decodeVariant[commitChange](tree, "change", &WriteChange{}, &DeleteChange{})
	return err
}

func (change CommitChange) MarshalJSON() ([]byte, error) {
	return encodeVariant("change", change.Change)
}

type CommitRequest struct {
	_               Closed
	ProtocolVersion int64          `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID             `json:"connection_id"`
	BindingID       ID             `json:"binding_id"`
	Changes         []CommitChange `json:"changes" wire:"min=1,max=10000"`
}

func (CommitRequest) MaxBytes() int { return CommitRequestMaxJSONBytes }

func (request *CommitRequest) check() error {
	return rule(uniqueBy(request.Changes, func(change CommitChange) string { return change.Change.path() }),
		"path twice in one commit")
}

// CurrentVersion is a path's current version where a change was rejected.
type CurrentVersion struct {
	VersionID ID `json:"version_id"`
	// ContentSha256 is null when the current version is a deletion.
	ContentSha256 Nullable[Sha256] `json:"content_sha256"`
	SizeBytes     int64            `json:"size_bytes" wire:"min=0,max=500000000"`
	Executable    bool             `json:"executable"`
	Writer        DriveWriter      `json:"writer"`
}

func (current *CurrentVersion) check() error {
	return want(current.ContentSha256.Valid || (current.SizeBytes == 0 && !current.Executable),
		"a current deletion without content")
}

// CommitResult is one change's result, an AcceptedResult or a
// RejectedResult named by the member `status`.
type CommitResult struct{ Result commitResult }

type commitResult interface{ variant }

type AcceptedResult struct {
	// VersionID is null only for the deletion of a path that holds no file.
	VersionID Nullable[ID] `json:"version_id"`
}

type RejectedResult struct {
	Reason string `json:"reason"`
	// Rule is the rule broken, for path_rule and limit.
	Rule    Opt[NonEmpty]       `json:"rule,omitzero"`
	Current Opt[CurrentVersion] `json:"current,omitzero"`
}

func (*AcceptedResult) variantName() string { return "accepted" }
func (*RejectedResult) variantName() string { return "rejected" }

func (rejected *RejectedResult) check() error {
	if err := oneOf(rejected.Reason, "conflict", "path_rule", "limit"); err != nil {
		return err
	}
	if err := want(rejected.Reason == "conflict" || rejected.Rule.Set, "a rule with a path_rule or limit rejection"); err != nil {
		return err
	}
	return rule(len(rejected.Rule.Value) <= RuleMaxUTF8Bytes, "rule over 512 bytes")
}

func (result *CommitResult) decodeTree(tree any) (err error) {
	result.Result, err = decodeVariant[commitResult](tree, "status", &AcceptedResult{}, &RejectedResult{})
	return err
}

func (result CommitResult) MarshalJSON() ([]byte, error) {
	return encodeVariant("status", result.Result)
}

// checkDecision holds a result to its change and the path's current
// version: a rejection says why the hub did not take a change it could not
// have taken, in the order the hub decides.
func (result CommitResult) checkDecision(change commitChange) error {
	_, isWrite := change.(*WriteChange)
	rejected, isRejected := result.Result.(*RejectedResult)
	if !isRejected {
		accepted := result.Result.(*AcceptedResult)
		return rule(!isWrite || accepted.VersionID.Valid, "an accepted write without a version")
	}
	current := rejected.Current
	held, holds := current.Value.ContentSha256.Value, current.Set && current.Value.ContentSha256.Valid
	if write, isWrite := change.(*WriteChange); isWrite {
		already := holds && held == write.ContentSha256 && current.Value.Executable == write.Executable
		if err := rule(!already, "a write rejected where the drive already holds it"); err != nil {
			return err
		}
	} else if err := rule(holds && rejected.Reason != "limit",
		"a deletion rejected where it could not have failed"); err != nil {
		return err
	}
	base, hasBase := change.base()
	matches := holds == hasBase && (!holds || held == base)
	switch rejected.Reason {
	case "conflict":
		return rule(!matches, "a conflict on a matching base")
	case "limit":
		return rule(matches, "a limit on a conflicting base")
	}
	return nil
}

type CommitResponse struct {
	// Seq is the drive's new change sequence, null when the commit made no
	// new version.
	Seq     Nullable[int64] `json:"seq" wire:"min=1"`
	Results []CommitResult  `json:"results" wire:"min=1,max=10000"`
}

func (CommitResponse) MaxBytes() int { return CommitResponseMaxJSONBytes }

func (response *CommitResponse) check() error {
	accepted := slices.ContainsFunc(response.Results, func(result CommitResult) bool {
		_, isAccepted := result.Result.(*AcceptedResult)
		return isAccepted
	})
	return rule(!response.Seq.Valid || accepted, "seq without a new version")
}

func (response *CommitResponse) checkAnswer(request CommitRequest) error {
	if err := rule(len(response.Results) == len(request.Changes), "result count differs from change count"); err != nil {
		return err
	}
	for index, result := range response.Results {
		if err := result.checkDecision(request.Changes[index].Change); err != nil {
			return err
		}
	}
	return nil
}
