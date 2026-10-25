package protocol

import (
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/text/cases"
)

// Limits of drive-transfer-v1; byte sizes count the compact encoding.
const (
	ListPageMaxItems             = 1000
	ListPageMaxJSONBytes         = 4194304
	CommitMaxChanges             = 10000
	CommitRequestMaxJSONBytes    = 16777216
	CommitResponseMaxJSONBytes   = 16777216
	RuleMaxUTF8Bytes             = 512
	FileMaxBytes                 = 500000000
	DriveChangedSSEMaxJSONBytes  = 65536
	retryAfterMaxSeconds         = 60
	driveTransferProtocolVersion = "1"
)

// canonicalDecimal is the Decimal pattern: digits without a leading zero.
func canonicalDecimal(s string) string {
	if !isDigits(s) || len(s) > 1 && s[0] == '0' {
		return "not a canonical decimal integer"
	}
	return ""
}

var decimalText = stringWith(canonicalDecimal)

// decimal is the Decimal definition: a canonical decimal integer in a query
// or header, parsed into int64.
func decimal(d *decoder, ptr string, v *value) int64 {
	s := decimalText(d, ptr, v)
	if d.failed() {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		d.fail(ptr, "integer %s is outside the signed 64-bit range", s)
	}
	return n
}

// transferPath is the DrivePath of drive-transfer-v1, a path Engine returns.
var transferPath = stringWith(relativePath(false))

var fileSize = integer(0, FileMaxBytes)

// ListFilesQuery is the query of a list files request.
type ListFilesQuery struct {
	ConnectionID string
	BindingID    string
	AfterPath    Optional[string] // the last path of the previous page
}

func listFilesQuery(d *decoder, ptr string, v *value) ListFilesQuery {
	o := d.object(ptr, v)
	required(o, "protocol_version", constString(driveTransferProtocolVersion))
	q := ListFilesQuery{
		ConnectionID: required(o, "connection_id", identity),
		BindingID:    required(o, "binding_id", identity),
		AfterPath:    optional(o, "after_path", anyString),
	}
	o.open()
	return q
}

func (q ListFilesQuery) Query() map[string][]string {
	values := transferQuery(q.ConnectionID, q.BindingID)
	setOptional(values, "after_path", q.AfterPath)
	return values
}

func transferQuery(connectionID, bindingID string) map[string][]string {
	return map[string][]string{
		"protocol_version": {driveTransferProtocolVersion},
		"connection_id":    {connectionID},
		"binding_id":       {bindingID},
	}
}

// ListChangesQuery is the query of a list changes request: after the whole
// commit AfterSeq, or after one of its records when AfterIndex is set.
type ListChangesQuery struct {
	ConnectionID string
	BindingID    string
	AfterSeq     int64
	AfterIndex   Optional[int64]
}

func listChangesQuery(d *decoder, ptr string, v *value) ListChangesQuery {
	o := d.object(ptr, v)
	required(o, "protocol_version", constString(driveTransferProtocolVersion))
	q := ListChangesQuery{
		ConnectionID: required(o, "connection_id", identity),
		BindingID:    required(o, "binding_id", identity),
		AfterSeq:     required(o, "after_seq", decimal),
		AfterIndex:   optional(o, "after_index", decimal),
	}
	o.open()
	return q
}

func (q ListChangesQuery) Query() map[string][]string {
	values := transferQuery(q.ConnectionID, q.BindingID)
	values["after_seq"] = []string{strconv.FormatInt(q.AfterSeq, 10)}
	if q.AfterIndex.Set {
		values["after_index"] = []string{strconv.FormatInt(q.AfterIndex.Value, 10)}
	}
	return values
}

// CommonQuery is the query of a content upload or download.
type CommonQuery struct {
	ConnectionID string
	BindingID    string
}

func commonQuery(d *decoder, ptr string, v *value) CommonQuery {
	o := d.object(ptr, v)
	required(o, "protocol_version", constString(driveTransferProtocolVersion))
	q := CommonQuery{
		ConnectionID: required(o, "connection_id", identity),
		BindingID:    required(o, "binding_id", identity),
	}
	o.open()
	return q
}

func (q CommonQuery) Query() map[string][]string {
	return transferQuery(q.ConnectionID, q.BindingID)
}

// DrivePathParams are the path parameters of the list and commit endpoints.
type DrivePathParams struct {
	DriveID string
}

func drivePathParams(d *decoder, ptr string, v *value) DrivePathParams {
	o := d.object(ptr, v)
	p := DrivePathParams{DriveID: required(o, "drive_id", identity)}
	o.closed()
	return p
}

func (p DrivePathParams) Params() map[string]string {
	return map[string]string{"drive_id": p.DriveID}
}

// ContentPathParams are the path parameters of a content upload or download.
type ContentPathParams struct {
	DriveID string
	SHA256  string
}

func contentPathParams(d *decoder, ptr string, v *value) ContentPathParams {
	o := d.object(ptr, v)
	p := ContentPathParams{
		DriveID: required(o, "drive_id", identity),
		SHA256:  required(o, "sha256", sha256Hex),
	}
	o.closed()
	return p
}

func (p ContentPathParams) Params() map[string]string {
	return map[string]string{"drive_id": p.DriveID, "sha256": p.SHA256}
}

// UploadContentRequestHeaders are the headers of a content upload, whose body
// is the raw content.
type UploadContentRequestHeaders struct {
	ContentLength Optional[int64]
}

func uploadContentRequestHeaders(d *decoder, ptr string, v *value) UploadContentRequestHeaders {
	o := d.object(ptr, v)
	required(o, "Content-Type", constString("application/octet-stream"))
	length, lengthPtr := o.member("Content-Length")
	var h UploadContentRequestHeaders
	if length != nil {
		// A canonical length above the signed 64-bit range is above the file
		// limit as well, so it is kept as the largest int64 for
		// CheckUploadLength to answer.
		text := decimalText(d, lengthPtr, length)
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			h.ContentLength = Some(n)
		} else {
			h.ContentLength = Some(int64(math.MaxInt64))
		}
	}
	o.open()
	return h
}

// CheckUploadLength applies the upload's length rules, which the hub answers
// after the connection, binding and drive access checks and before reading
// any byte: a missing Content-Length gets 400 invalid_message, and one above
// the file limit 413 file_too_large.
func CheckUploadLength(h UploadContentRequestHeaders) error {
	switch {
	case !h.ContentLength.Set:
		return invalid("", "missing Content-Length")
	case h.ContentLength.Value > FileMaxBytes:
		return &DecodeError{Pointer: "/Content-Length", Status: http.StatusRequestEntityTooLarge, Code: ErrorFileTooLarge,
			Reason: "Content-Length above 500000000 bytes"}
	}
	return nil
}

func (h UploadContentRequestHeaders) Header() map[string][]string {
	header := map[string][]string{"Content-Type": {"application/octet-stream"}}
	if h.ContentLength.Set {
		header["Content-Length"] = []string{strconv.FormatInt(h.ContentLength.Value, 10)}
	}
	return header
}

// ByteRange is one RFC 9110 byte range: First-Last, First- to the end, or the
// suffix -Last, the content's last Last bytes.
type ByteRange struct {
	First Optional[int64]
	Last  Optional[int64]
}

func (r ByteRange) String() string {
	var b strings.Builder
	b.WriteString("bytes=")
	if r.First.Set {
		b.WriteString(strconv.FormatInt(r.First.Value, 10))
	}
	b.WriteByte('-')
	if r.Last.Set {
		b.WriteString(strconv.FormatInt(r.Last.Value, 10))
	}
	return b.String()
}

// Answer returns the bytes First..Last of a content of total bytes that the
// range selects, or false for 416 range_not_satisfiable.
func (r ByteRange) Answer(total int64) (first, last int64, ok bool) {
	switch {
	case !r.First.Set:
		first, last = max(0, total-r.Last.Value), total-1
		if r.Last.Value == 0 {
			last = -1
		}
	case !r.Last.Set || r.Last.Value >= total:
		first, last = r.First.Value, total-1
	default:
		first, last = r.First.Value, r.Last.Value
	}
	return first, last, first < total && first <= last
}

// DownloadContentRequestHeaders are the headers of a content download; Range
// names at most one range.
type DownloadContentRequestHeaders struct {
	Range Optional[ByteRange]
}

// rangeSpecs is the Range pattern: "bytes=" and a list of a-b, a- or -n
// separated by commas with optional blanks.
func rangeSpecs(s string) ([]string, bool) {
	list, ok := strings.CutPrefix(s, "bytes=")
	if !ok {
		return nil, false
	}
	specs := strings.Split(list, ",")
	for i, spec := range specs {
		if i > 0 {
			spec = strings.TrimLeft(spec, " \t")
		}
		if i < len(specs)-1 {
			spec = strings.TrimRight(spec, " \t")
		}
		first, last, found := strings.Cut(spec, "-")
		if !found || first == "" && !isDigits(last) || first != "" && (!isDigits(first) || last != "" && !isDigits(last)) {
			return nil, false
		}
		specs[i] = spec
	}
	return specs, true
}

func downloadContentRequestHeaders(d *decoder, ptr string, v *value) DownloadContentRequestHeaders {
	o := d.object(ptr, v)
	var h DownloadContentRequestHeaders
	text := optional(o, "Range", stringWith(func(s string) string {
		if _, ok := rangeSpecs(s); !ok {
			return "not a byte range"
		}
		return ""
	}))
	o.open()
	if !text.Set || d.failed() {
		return h
	}
	specs, _ := rangeSpecs(text.Value)
	ranges := make([]ByteRange, len(specs))
	for i, spec := range specs {
		first, last, _ := strings.Cut(spec, "-")
		for _, part := range []struct {
			text string
			into *Optional[int64]
		}{{first, &ranges[i].First}, {last, &ranges[i].Last}} {
			if part.text == "" {
				continue
			}
			n, err := strconv.ParseInt(part.text, 10, 64)
			if err != nil {
				d.fail(pointer(ptr, "Range"), "range bound %s is outside the signed 64-bit range", part.text)
				return h
			}
			*part.into = Some(n)
		}
	}
	h.Range = Some(ranges[0])
	for _, r := range ranges {
		if r.First.Set && r.Last.Set && r.First.Value > r.Last.Value {
			d.fail(pointer(ptr, "Range"), "range ends before it starts")
			return h
		}
	}
	if len(ranges) > 1 {
		d.err = &DecodeError{Pointer: pointer(ptr, "Range"), Status: http.StatusRequestedRangeNotSatisfiable,
			Code: ErrorRangeNotSatisfiable, Reason: "more than one range"}
	}
	return h
}

func (h DownloadContentRequestHeaders) Header() map[string][]string {
	if !h.Range.Set {
		return map[string][]string{}
	}
	return map[string][]string{"Range": {h.Range.Value.String()}}
}

// PartialContentResponseHeaders are the headers of a 206 download: the bytes
// First..Last of a content of Total bytes.
type PartialContentResponseHeaders struct {
	First int64
	Last  int64
	Total int64
}

// PartialContentResponseContext is the request a 206 answers.
type PartialContentResponseContext struct {
	RequestHeaders Optional[DownloadContentRequestHeaders]
}

func partialContentResponseHeaders(d *decoder, ptr string, v *value) PartialContentResponseHeaders {
	o := d.object(ptr, v)
	var h PartialContentResponseHeaders
	text := required(o, "Content-Range", stringWith(func(s string) string {
		if _, ok := parseContentRange(s); !ok {
			return "not a byte content range"
		}
		return ""
	}))
	o.open()
	if d.failed() {
		return h
	}
	bounds, _ := parseContentRange(text)
	var numbers [3]int64
	for i, b := range bounds {
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil {
			d.fail(pointer(ptr, "Content-Range"), "bound %s is outside the signed 64-bit range", b)
			return h
		}
		numbers[i] = n
	}
	return PartialContentResponseHeaders{First: numbers[0], Last: numbers[1], Total: numbers[2]}
}

// parseContentRange is the pattern "bytes a-b/n" with canonical decimals.
func parseContentRange(s string) ([3]string, bool) {
	rest, ok := strings.CutPrefix(s, "bytes ")
	first, rest, ok1 := strings.Cut(rest, "-")
	last, total, ok2 := strings.Cut(rest, "/")
	bounds := [3]string{first, last, total}
	for _, b := range bounds {
		if canonicalDecimal(b) != "" {
			return bounds, false
		}
	}
	return bounds, ok && ok1 && ok2
}

func checkPartialContent(ctx PartialContentResponseContext) func(PartialContentResponseHeaders, *value) error {
	return func(h PartialContentResponseHeaders, _ *value) error {
		if h.First > h.Last || h.Last >= h.Total || h.Total > FileMaxBytes {
			return invalid("/Content-Range", "content range outside the content")
		}
		if ctx.RequestHeaders.Set {
			r := ctx.RequestHeaders.Value.Range
			if !r.Set {
				return invalid("/Content-Range", "content range not requested")
			}
			first, last, ok := r.Value.Answer(h.Total)
			if !ok || first != h.First || last != h.Last {
				return invalid("/Content-Range", "content range not requested")
			}
		}
		return nil
	}
}

func (h PartialContentResponseHeaders) Header() map[string][]string {
	return map[string][]string{"Content-Range": {"bytes " + strconv.FormatInt(h.First, 10) + "-" +
		strconv.FormatInt(h.Last, 10) + "/" + strconv.FormatInt(h.Total, 10)}}
}

// RateLimitedResponseHeaders are the headers of a 429 upload: the seconds
// until the upload budget holds the request's count.
type RateLimitedResponseHeaders struct {
	RetryAfter int64
}

func rateLimitedResponseHeaders(d *decoder, ptr string, v *value) RateLimitedResponseHeaders {
	o := d.object(ptr, v)
	text := required(o, "Retry-After", stringWith(func(s string) string {
		if n, err := strconv.Atoi(s); err != nil || n < 1 || n > retryAfterMaxSeconds || strconv.Itoa(n) != s {
			return "not a whole number of seconds from 1 to 60"
		}
		return ""
	}))
	o.open()
	n, _ := strconv.ParseInt(text, 10, 64)
	return RateLimitedResponseHeaders{RetryAfter: n}
}

func (h RateLimitedResponseHeaders) Header() map[string][]string {
	return map[string][]string{"Retry-After": {strconv.FormatInt(h.RetryAfter, 10)}}
}

// FileEntry is one current file of a drive.
type FileEntry struct {
	Path          string
	VersionID     string
	ContentSHA256 string
	SizeBytes     int64
	Executable    bool
}

func fileEntry(d *decoder, ptr string, v *value) FileEntry {
	o := d.object(ptr, v)
	f := FileEntry{
		Path:          required(o, "path", transferPath),
		VersionID:     required(o, "version_id", identity),
		ContentSHA256: required(o, "content_sha256", sha256Hex),
		SizeBytes:     required(o, "size_bytes", fileSize),
		Executable:    required(o, "executable", boolean),
	}
	o.open()
	return f
}

func (f FileEntry) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", f.Path)
	e.stringMember("version_id", f.VersionID)
	e.stringMember("content_sha256", f.ContentSHA256)
	e.integerMember("size_bytes", f.SizeBytes)
	e.boolMember("executable", f.Executable)
	e.endObject()
}

// ListFilesResponse is one page of a drive's current files in UTF-8 byte
// order of path; the first page, requested without after_path, carries the
// seq it was read at.
type ListFilesResponse struct {
	Files    []FileEntry
	HasMore  bool
	StartSeq Optional[int64]
}

// ListFilesResponseContext is the request a list files page answers.
type ListFilesResponseContext struct {
	Query Optional[ListFilesQuery]
}

func listFilesResponse(d *decoder, ptr string, v *value) ListFilesResponse {
	o := d.object(ptr, v)
	r := ListFilesResponse{
		Files:    required(o, "files", arrayOf(fileEntry, 0, ListPageMaxItems)),
		HasMore:  required(o, "has_more", boolean),
		StartSeq: optional(o, "start_seq", nonNegative),
	}
	o.open()
	return r
}

func (r ListFilesResponse) encode(e *encoder) {
	e.beginObject()
	e.key("files")
	list(e, r.Files)
	e.boolMember("has_more", r.HasMore)
	optionalMember(e, "start_seq", r.StartSeq, writeInteger)
	e.endObject()
}

// foldCherokee completes cases.Fold as Unicode full case folding: cases.Fold
// folds the Cherokee letters to their small forms, while CaseFolding.txt folds
// the small forms to the capitals.
func foldCherokee(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case 0xab70 <= r && r <= 0xabbf:
			return r - 0xab70 + 0x13a0
		case 0x13f8 <= r && r <= 0x13fd:
			return r - 8
		}
		return r
	}, s)
}

// checkListFiles applies the relations of a files page: at most 4 MiB, the
// path rules, paths strictly increasing, distinct under Unicode full case
// folding and none above another file, has_more only on a page with files,
// and against its request start_seq exactly without after_path and a first
// path after the cursor.
func checkListFiles(ctx ListFilesResponseContext) func(ListFilesResponse, *value) error {
	return func(r ListFilesResponse, v *value) error {
		if compactSize(v) > ListPageMaxJSONBytes {
			return invalid("", "list page over 4 MiB")
		}
		present := make(map[string]bool, len(r.Files))
		folded := make(map[string]bool, len(r.Files))
		fold := cases.Fold()
		for i, f := range r.Files {
			at := "/files/" + strconv.Itoa(i) + "/path"
			if err := checkDrivePathRules(at, f.Path); err != nil {
				return err
			}
			if present[f.Path] {
				return invalid(at, "duplicate path")
			}
			if i > 0 && r.Files[i-1].Path >= f.Path {
				return invalid(at, "files not in path order")
			}
			key := foldCherokee(fold.String(f.Path))
			if folded[key] {
				return invalid(at, "paths differ only in case")
			}
			folded[key] = true
			present[f.Path] = true
		}
		for i, f := range r.Files {
			for j := 0; j < len(f.Path); j++ {
				if f.Path[j] == '/' && present[f.Path[:j]] {
					return invalid("/files/"+strconv.Itoa(i)+"/path", "a file above another file")
				}
			}
		}
		if r.HasMore && len(r.Files) == 0 {
			return invalid("/has_more", "has_more on an empty page")
		}
		if ctx.Query.Set {
			q := ctx.Query.Value
			if r.StartSeq.Set == q.AfterPath.Set {
				return invalid("/start_seq", "start_seq exactly on the page requested without after_path")
			}
			if q.AfterPath.Set && len(r.Files) > 0 && r.Files[0].Path <= q.AfterPath.Value {
				return invalid("/files/0/path", "page not after its cursor")
			}
		}
		return nil
	}
}

// ChangeRecord is one change of a drive: a commit's records are numbered from
// 0, deletions first.
type ChangeRecord struct {
	Seq           int64
	Index         int64
	Path          string
	Operation     string // created, modified or deleted
	VersionID     string
	ContentSHA256 Nullable[string] // null exactly for a deletion
	SizeBytes     int64
	Executable    bool
}

func changeRecord(d *decoder, ptr string, v *value) ChangeRecord {
	o := d.object(ptr, v)
	r := ChangeRecord{
		Seq:       required(o, "seq", positiveInteger),
		Index:     required(o, "index", nonNegative),
		Path:      required(o, "path", transferPath),
		Operation: required(o, "operation", enum("created", "modified", "deleted")),
		VersionID: required(o, "version_id", identity),
	}
	if r.Operation == "deleted" {
		r.ContentSHA256 = requiredNullable(o, "content_sha256", func(d *decoder, ptr string, v *value) string {
			d.fail(ptr, "expected null, got %s", v.kind)
			return ""
		})
		r.SizeBytes = required(o, "size_bytes", constInteger(0))
		r.Executable = required(o, "executable", constBool(false))
	} else {
		r.ContentSHA256 = Value(required(o, "content_sha256", sha256Hex))
		r.SizeBytes = required(o, "size_bytes", fileSize)
		r.Executable = required(o, "executable", boolean)
	}
	o.open()
	return r
}

func (r ChangeRecord) encode(e *encoder) {
	e.beginObject()
	e.integerMember("seq", r.Seq)
	e.integerMember("index", r.Index)
	e.stringMember("path", r.Path)
	e.stringMember("operation", r.Operation)
	e.stringMember("version_id", r.VersionID)
	nullableMember(e, "content_sha256", r.ContentSHA256, writeString)
	e.integerMember("size_bytes", r.SizeBytes)
	e.boolMember("executable", r.Executable)
	e.endObject()
}

// ListChangesResponse is one page of change records in (seq, index) order
// after the request's cursor.
type ListChangesResponse struct {
	Records []ChangeRecord
	HasMore bool
}

// ListChangesResponseContext is the request a list changes page answers.
type ListChangesResponseContext struct {
	Query Optional[ListChangesQuery]
}

func listChangesResponse(d *decoder, ptr string, v *value) ListChangesResponse {
	o := d.object(ptr, v)
	r := ListChangesResponse{
		Records: required(o, "records", arrayOf(changeRecord, 0, ListPageMaxItems)),
		HasMore: required(o, "has_more", boolean),
	}
	o.open()
	return r
}

func (r ListChangesResponse) encode(e *encoder) {
	e.beginObject()
	e.key("records")
	list(e, r.Records)
	e.boolMember("has_more", r.HasMore)
	e.endObject()
}

// checkListChanges applies the relations of a changes page: at most 4 MiB,
// the path rules, records strictly in (seq, index) order with no index gap and
// no deletion after a write within a commit, has_more only on a page with
// records, and a first record right after the request's cursor.
func checkListChanges(ctx ListChangesResponseContext) func(ListChangesResponse, *value) error {
	return func(r ListChangesResponse, v *value) error {
		if compactSize(v) > ListPageMaxJSONBytes {
			return invalid("", "list page over 4 MiB")
		}
		for i, rec := range r.Records {
			at := "/records/" + strconv.Itoa(i)
			if err := checkDrivePathRules(at+"/path", rec.Path); err != nil {
				return err
			}
			if i == 0 {
				continue
			}
			prev := r.Records[i-1]
			switch {
			case prev.Seq > rec.Seq || prev.Seq == rec.Seq && prev.Index >= rec.Index:
				return invalid(at, "records not in order")
			case prev.Seq == rec.Seq && rec.Index != prev.Index+1:
				return invalid(at+"/index", "index gap")
			case prev.Seq == rec.Seq && prev.Operation != "deleted" && rec.Operation == "deleted":
				return invalid(at, "deletion after a write")
			case prev.Seq != rec.Seq && rec.Index != 0:
				return invalid(at+"/index", "index gap")
			}
		}
		if r.HasMore && len(r.Records) == 0 {
			return invalid("/has_more", "has_more on an empty page")
		}
		if ctx.Query.Set && len(r.Records) > 0 {
			q, first := ctx.Query.Value, r.Records[0]
			if q.AfterIndex.Set {
				index := q.AfterIndex.Value
				if first.Seq < q.AfterSeq || first.Seq == q.AfterSeq && first.Index <= index {
					return invalid("/records/0", "page not after its cursor")
				}
				want := int64(0)
				if first.Seq == q.AfterSeq {
					want = index + 1
				}
				if first.Index != want {
					return invalid("/records/0/index", "index gap")
				}
			} else {
				if first.Seq <= q.AfterSeq {
					return invalid("/records/0", "page not after its cursor")
				}
				if first.Index != 0 {
					return invalid("/records/0/index", "index gap")
				}
			}
		}
		return nil
	}
}

// DriveChanged is the drive.changed notice: a Downstream whose payload is a
// DriveChangedPayload. It has no ACK.
type DriveChanged Downstream

func driveChanged(d *decoder, ptr string, v *value) DriveChanged {
	m := connectionDownstream(d, ptr, v)
	if !d.failed() {
		if _, ok := m.Payload.(DriveChangedPayload); !ok {
			d.fail(pointer(ptr, "type"), "expected drive.changed")
		}
	}
	return DriveChanged(m)
}

func (n DriveChanged) encode(e *encoder) { Downstream(n).encode(e) }

// UploadContentResponse is the answer of a content upload.
type UploadContentResponse struct {
	ContentSHA256 string
	SizeBytes     int64
}

// UploadContentResponseContext is the request an upload answers.
type UploadContentResponseContext struct {
	PathParams     Optional[ContentPathParams]
	RequestHeaders Optional[UploadContentRequestHeaders]
}

func uploadContentResponse(d *decoder, ptr string, v *value) UploadContentResponse {
	o := d.object(ptr, v)
	r := UploadContentResponse{
		ContentSHA256: required(o, "content_sha256", sha256Hex),
		SizeBytes:     required(o, "size_bytes", fileSize),
	}
	o.open()
	return r
}

func (r UploadContentResponse) encode(e *encoder) {
	e.beginObject()
	e.stringMember("content_sha256", r.ContentSHA256)
	e.integerMember("size_bytes", r.SizeBytes)
	e.endObject()
}

func checkUploadResponse(ctx UploadContentResponseContext) func(UploadContentResponse, *value) error {
	return func(r UploadContentResponse, _ *value) error {
		if ctx.PathParams.Set && r.ContentSHA256 != ctx.PathParams.Value.SHA256 {
			return invalid("/content_sha256", "hash differs from the URL")
		}
		if ctx.RequestHeaders.Set && ctx.RequestHeaders.Value.ContentLength != Some(r.SizeBytes) {
			return invalid("/size_bytes", "size differs from Content-Length")
		}
		return nil
	}
}

// CommitChange is one change of a commit: a WriteChange or a DeleteChange.
type CommitChange interface {
	encodable
	ChangePath() string
}

// WriteChange writes a file whose content the drive holds. A null base means
// the file must not exist.
type WriteChange struct {
	Path          string
	BaseSHA256    Nullable[string]
	ContentSHA256 string
	Executable    bool
}

func (c WriteChange) ChangePath() string { return c.Path }

// DeleteChange deletes a file whose content is the base.
type DeleteChange struct {
	Path       string
	BaseSHA256 string
}

func (c DeleteChange) ChangePath() string { return c.Path }

// commitChange reads a change; its path is any string, since the commit
// decides the path rules per change and a path never fails the request.
func commitChange(d *decoder, ptr string, v *value) CommitChange {
	o := d.object(ptr, v)
	var c CommitChange
	switch required(o, "change", enum("write", "delete")) {
	case "write":
		c = WriteChange{
			Path:          required(o, "path", anyString),
			BaseSHA256:    requiredNullable(o, "base_sha256", sha256Hex),
			ContentSHA256: required(o, "content_sha256", sha256Hex),
			Executable:    required(o, "executable", boolean),
		}
	case "delete":
		c = DeleteChange{
			Path:       required(o, "path", anyString),
			BaseSHA256: required(o, "base_sha256", sha256Hex),
		}
	}
	o.closed()
	if d.failed() {
		return nil
	}
	return c
}

func (c WriteChange) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", c.Path)
	e.stringMember("change", "write")
	if c.BaseSHA256.Present {
		e.stringMember("base_sha256", c.BaseSHA256.Value)
	} else {
		e.key("base_sha256")
		e.null()
	}
	e.stringMember("content_sha256", c.ContentSHA256)
	e.boolMember("executable", c.Executable)
	e.endObject()
}

func (c DeleteChange) encode(e *encoder) {
	e.beginObject()
	e.stringMember("path", c.Path)
	e.stringMember("change", "delete")
	e.stringMember("base_sha256", c.BaseSHA256)
	e.endObject()
}

// CommitRequest is the closed body of a commit.
type CommitRequest struct {
	ConnectionID string
	BindingID    string
	Changes      []CommitChange
}

func commitRequest(d *decoder, ptr string, v *value) CommitRequest {
	o := d.object(ptr, v)
	required(o, "protocol_version", constInteger(connectionProtocolVersionInt))
	r := CommitRequest{
		ConnectionID: required(o, "connection_id", identity),
		BindingID:    required(o, "binding_id", identity),
		Changes:      required(o, "changes", arrayOf(commitChange, 1, -1)),
	}
	o.closed()
	return r
}

func (r CommitRequest) encode(e *encoder) {
	e.beginObject()
	e.integerMember("protocol_version", connectionProtocolVersionInt)
	e.stringMember("connection_id", r.ConnectionID)
	e.stringMember("binding_id", r.BindingID)
	e.key("changes")
	list(e, r.Changes)
	e.endObject()
}

func checkCommitRequest(r CommitRequest, _ *value) error {
	seen := make(map[string]bool, len(r.Changes))
	for i, c := range r.Changes {
		if seen[c.ChangePath()] {
			return invalid("/changes/"+strconv.Itoa(i)+"/path", "duplicate path")
		}
		seen[c.ChangePath()] = true
	}
	return nil
}

// CurrentVersion is the current version of a rejected change's path; a
// deletion has a null hash, no bytes and is not executable.
type CurrentVersion struct {
	VersionID     string
	ContentSHA256 Nullable[string]
	SizeBytes     int64
	Executable    bool
	Writer        DriveWriter
}

func currentVersion(d *decoder, ptr string, v *value) CurrentVersion {
	o := d.object(ptr, v)
	c := CurrentVersion{
		VersionID:     required(o, "version_id", identity),
		ContentSHA256: requiredNullable(o, "content_sha256", sha256Hex),
	}
	if c.ContentSHA256.Null {
		c.SizeBytes = required(o, "size_bytes", constInteger(0))
		c.Executable = required(o, "executable", constBool(false))
	} else {
		c.SizeBytes = required(o, "size_bytes", fileSize)
		c.Executable = required(o, "executable", boolean)
	}
	c.Writer = required(o, "writer", driveWriter)
	o.open()
	return c
}

func (c CurrentVersion) encode(e *encoder) {
	e.beginObject()
	e.stringMember("version_id", c.VersionID)
	nullableMember(e, "content_sha256", c.ContentSHA256, writeString)
	e.integerMember("size_bytes", c.SizeBytes)
	e.boolMember("executable", c.Executable)
	e.valueMember("writer", c.Writer)
	e.endObject()
}

// CommitResult is the outcome of one change: an AcceptedResult or a
// RejectedResult.
type CommitResult interface {
	encodable
	commitResult()
}

// AcceptedResult is an accepted change. VersionID is the new version, or the
// current one when the path already held the change; it is null only for a
// delete of a path of which the drive keeps no version.
type AcceptedResult struct {
	VersionID Nullable[string]
}

func (AcceptedResult) commitResult() {}

// RejectedResult is a rejected change. Current is absent when the drive keeps
// no version of the path.
type RejectedResult struct {
	Reason  string // conflict, path_rule or limit
	Rule    Optional[string]
	Current Optional[CurrentVersion]
}

func (RejectedResult) commitResult() {}

func commitResult(d *decoder, ptr string, v *value) CommitResult {
	o := d.object(ptr, v)
	var r CommitResult
	switch required(o, "status", enum("accepted", "rejected")) {
	case "accepted":
		r = AcceptedResult{VersionID: requiredNullable(o, "version_id", identity)}
	case "rejected":
		rejected := RejectedResult{Reason: required(o, "reason", enum("conflict", "path_rule", "limit"))}
		rule := stringWith(minLength(1))
		if rejected.Reason == "path_rule" || rejected.Reason == "limit" {
			rejected.Rule = Some(required(o, "rule", rule))
		} else {
			rejected.Rule = optional(o, "rule", rule)
		}
		rejected.Current = optional(o, "current", currentVersion)
		r = rejected
	}
	o.open()
	if d.failed() {
		return nil
	}
	return r
}

func (r AcceptedResult) encode(e *encoder) {
	e.beginObject()
	e.stringMember("status", "accepted")
	if r.VersionID.Present {
		e.stringMember("version_id", r.VersionID.Value)
	} else {
		e.key("version_id")
		e.null()
	}
	e.endObject()
}

func (r RejectedResult) encode(e *encoder) {
	e.beginObject()
	e.stringMember("status", "rejected")
	e.stringMember("reason", r.Reason)
	optionalMember(e, "rule", r.Rule, writeString)
	optionalMember(e, "current", r.Current, writeValue)
	e.endObject()
}

// CommitResponse answers a commit with one result per change in request
// order; Seq is null when the commit created no version.
type CommitResponse struct {
	Seq     Nullable[int64]
	Results []CommitResult
}

// CommitResponseContext is the commit a response answers.
type CommitResponseContext struct {
	Request Optional[CommitRequest]
}

func commitResponse(d *decoder, ptr string, v *value) CommitResponse {
	o := d.object(ptr, v)
	r := CommitResponse{
		Seq:     requiredNullable(o, "seq", positiveInteger),
		Results: required(o, "results", arrayOf(commitResult, 1, CommitMaxChanges)),
	}
	o.open()
	return r
}

func (r CommitResponse) encode(e *encoder) {
	e.beginObject()
	if r.Seq.Present {
		e.integerMember("seq", r.Seq.Value)
	} else {
		e.key("seq")
		e.null()
	}
	e.key("results")
	list(e, r.Results)
	e.endObject()
}

// checkCommitResponse applies the relations of a commit response: at most
// 16 MiB, each rule at most 512 bytes, a seq only with an accepted change,
// and against its request one result per change, decided in the commit's
// rule order.
func checkCommitResponse(ctx CommitResponseContext) func(CommitResponse, *value) error {
	return func(r CommitResponse, v *value) error {
		if compactSize(v) > CommitResponseMaxJSONBytes {
			return invalid("", "commit response over 16 MiB")
		}
		// An accepted result's rule is a member its schema allows without
		// defining, so the bound reads every result's rule from the tree.
		results, _ := v.get("results")
		for i, item := range results.items {
			if rule, ok := item.get("rule"); ok && rule.kind == kindString && len(rule.text) > RuleMaxUTF8Bytes {
				return invalid("/results/"+strconv.Itoa(i)+"/rule", "rule over 512 bytes")
			}
		}
		accepted := slices.ContainsFunc(r.Results, func(result CommitResult) bool {
			_, ok := result.(AcceptedResult)
			return ok
		})
		if r.Seq.Present && !accepted {
			return invalid("/seq", "seq without a new version")
		}
		if !ctx.Request.Set {
			return nil
		}
		changes := ctx.Request.Value.Changes
		if len(changes) != len(r.Results) {
			return invalid("/results", "result count differs from the changes")
		}
		for i, change := range changes {
			if reason := decisionOrder(change, r.Results[i]); reason != "" {
				return invalid("/results/"+strconv.Itoa(i), reason)
			}
		}
		return nil
	}
}

// decisionOrder checks a result against its change and the path's current
// version: an accepted write names a version; a rejection is the first rule
// that matches, so a change the current state already holds is never
// rejected, a deletion is never a limit, a conflict has a base other than the
// current state and a limit the current state as its base.
func decisionOrder(change CommitChange, result CommitResult) string {
	write, isWrite := change.(WriteChange)
	if accepted, ok := result.(AcceptedResult); ok {
		if isWrite && !accepted.VersionID.Present {
			return "null version for a write"
		}
		return ""
	}
	rejected := result.(RejectedResult)
	held, holds := "", false
	if rejected.Current.Set && rejected.Current.Value.ContentSHA256.Present {
		held, holds = rejected.Current.Value.ContentSHA256.Value, true
	}
	var base Nullable[string]
	if isWrite {
		if rejected.Current.Set && holds && held == write.ContentSHA256 && rejected.Current.Value.Executable == write.Executable {
			return "decision order"
		}
		base = write.BaseSHA256
	} else {
		if !holds || rejected.Reason == "limit" {
			return "decision order"
		}
		base = Value(change.(DeleteChange).BaseSHA256)
	}
	sameAsBase := holds == base.Present && (!holds || held == base.Value)
	switch rejected.Reason {
	case "conflict":
		if sameAsBase {
			return "decision order"
		}
	case "limit":
		if !sameAsBase {
			return "decision order"
		}
	}
	return ""
}

// DriveTransferEndpoint names one endpoint of the drive transfer endpoint
// table.
type DriveTransferEndpoint string

const (
	EndpointListFiles       DriveTransferEndpoint = "list_files"
	EndpointListChanges     DriveTransferEndpoint = "list_changes"
	EndpointUploadContent   DriveTransferEndpoint = "upload_content"
	EndpointCommit          DriveTransferEndpoint = "commit"
	EndpointDownloadContent DriveTransferEndpoint = "download_content"
)

// DecodeListFilesQuery decodes the query of a list files request.
func DecodeListFilesQuery(query url.Values) (ListFilesQuery, error) {
	return decodeParameters(query, listFilesQuery)
}

// DecodeListChangesQuery decodes the query of a list changes request.
func DecodeListChangesQuery(query url.Values) (ListChangesQuery, error) {
	return decodeParameters(query, listChangesQuery)
}

// DecodeCommonQuery decodes the query of a content upload or download.
func DecodeCommonQuery(query url.Values) (CommonQuery, error) {
	return decodeParameters(query, commonQuery)
}

// DecodeDrivePathParams decodes the path parameters of the list and commit
// endpoints.
func DecodeDrivePathParams(params map[string]string) (DrivePathParams, error) {
	return decodeParameters(singleValues(params), drivePathParams)
}

// DecodeContentPathParams decodes the path parameters of a content upload or
// download.
func DecodeContentPathParams(params map[string]string) (ContentPathParams, error) {
	return decodeParameters(singleValues(params), contentPathParams)
}

// DecodeUploadContentRequestHeaders decodes the headers of a content upload
// and checks only their form. The length rules are left to CheckUploadLength,
// which the hub applies after the connection, binding and drive access checks.
func DecodeUploadContentRequestHeaders(header http.Header) (UploadContentRequestHeaders, error) {
	return decodeCheckedParameters(header, uploadContentRequestHeaders, nil)
}

// DecodeDownloadContentRequestHeaders decodes the headers of a content
// download.
func DecodeDownloadContentRequestHeaders(header http.Header) (DownloadContentRequestHeaders, error) {
	return decodeCheckedParameters(header, downloadContentRequestHeaders, nil)
}

// DecodePartialContentResponseHeaders decodes the headers of a 206 download.
func DecodePartialContentResponseHeaders(header http.Header, ctx PartialContentResponseContext) (PartialContentResponseHeaders, error) {
	return decodeCheckedParameters(header, partialContentResponseHeaders, checkPartialContent(ctx))
}

// DecodeRateLimitedResponseHeaders decodes the headers of a 429 upload.
func DecodeRateLimitedResponseHeaders(header http.Header) (RateLimitedResponseHeaders, error) {
	return decodeCheckedParameters(header, rateLimitedResponseHeaders, nil)
}

// driveBodyChecked decodes a drive transfer body with f and check, requiring
// protocol_version to be an integer token wherever the body carries it.
func driveBodyChecked[T any](data []byte, f decodeFunc[T], check func(T, *value) error) (T, error) {
	return decodeChecked(data, f, integerTokens(check, "protocol_version"))
}

// DecodeListFilesResponse decodes a files page against the request it
// answers.
func DecodeListFilesResponse(data []byte, ctx ListFilesResponseContext) (ListFilesResponse, error) {
	return driveBodyChecked(data, listFilesResponse, checkListFiles(ctx))
}

// DecodeListChangesResponse decodes a changes page against the request it
// answers.
func DecodeListChangesResponse(data []byte, ctx ListChangesResponseContext) (ListChangesResponse, error) {
	return driveBodyChecked(data, listChangesResponse, checkListChanges(ctx))
}

func DecodeDriveChanged(data []byte) (DriveChanged, error) {
	return driveBodyChecked(data, driveChanged, messageSizeCheck[DriveChanged](DriveChangedSSEMaxJSONBytes, "notice over 64 KiB"))
}

// DecodeUploadContentResponse decodes an upload's answer against its request.
func DecodeUploadContentResponse(data []byte, ctx UploadContentResponseContext) (UploadContentResponse, error) {
	return driveBodyChecked(data, uploadContentResponse, checkUploadResponse(ctx))
}

// DecodeCommitRequest answers a body over 16 MiB with 413 request_too_large
// before any other rule. A body within the bound also compacts within it, so
// the size rule is checked once, on the body. The change count is left to
// CheckCommitChanges, which the hub applies after the connection, binding and
// drive access checks.
func DecodeCommitRequest(data []byte) (CommitRequest, error) {
	if len(data) > CommitRequestMaxJSONBytes {
		return CommitRequest{}, tooLarge("", "commit over 16 MiB")
	}
	return driveBodyChecked(data, commitRequest, checkCommitRequest)
}

// CheckCommitChanges answers a commit of more than 10,000 changes with 413
// request_too_large.
func CheckCommitChanges(r CommitRequest) error {
	if len(r.Changes) > CommitMaxChanges {
		return tooLarge("/changes", "more than 10,000 changes")
	}
	return nil
}

// DecodeCommitResponse decodes a commit's answer against its request.
func DecodeCommitResponse(data []byte, ctx CommitResponseContext) (CommitResponse, error) {
	return driveBodyChecked(data, commitResponse, checkCommitResponse(ctx))
}

// DecodeDriveTransferError decodes the error body of a drive transfer
// endpoint, rejecting a status and code pair the endpoint table does not list
// for the endpoint.
func DecodeDriveTransferError(endpoint DriveTransferEndpoint, status int, data []byte) (ErrorBody, error) {
	return driveBodyChecked(data, errorBody, checkDriveTransferError(endpoint, status))
}

func checkDriveTransferError(endpoint DriveTransferEndpoint, status int) func(ErrorBody, *value) error {
	return func(b ErrorBody, v *value) error {
		if err := checkErrorBody(status)(b, v); err != nil {
			return err
		}
		for _, answer := range DriveTransferEndpoints[endpoint] {
			if answer.Status == status && answer.Code == b.Code {
				return nil
			}
		}
		return invalid("/code", "code not listed for the endpoint")
	}
}
