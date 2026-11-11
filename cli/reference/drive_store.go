package reference

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// drive is one drive of the store: what `protocol/README.md`, "Drive
// transfer", says a drive holds, and nothing the contract leaves to Engine.
type drive struct {
	id string
	// seq is the seq of the latest commit that made a version, 0 before
	// the first.
	seq int64
	// current is each path's current version, a deletion included while the
	// drive keeps one. setCurrent keeps what follows it in step: folded is
	// each current file's path by its case-folded form, below counts the
	// current files under each directory, and usedBytes and usedFiles are
	// the size and number of the current files.
	current              map[string]*driveVersion
	folded               map[string]string
	below                map[string]int
	usedBytes, usedFiles int64
	versions             map[string]*driveVersion
	// records are the change records still listed, in (seq, index) order.
	records []wire.ChangeRecord
	// expired is the newest record that ageing removed.
	expired *wire.ChangeRecord
	// contents is the content the drive holds, uploaded or committed, by
	// its hash.
	contents map[string]driveContent
	// maxBytes and maxFiles are the drive's own limits, nil for none.
	maxBytes, maxFiles *int64
}

// driveVersion is one version of a path: a file's content or a deletion.
type driveVersion struct {
	id   string
	path string
	// sha256 is empty for a deletion.
	sha256     string
	size       int64
	executable bool
	writer     wire.DriveWriter
	// removed is set once the drive no longer keeps the version.
	removed bool
	// made orders the drive's versions as they were made.
	made int
}

func (version *driveVersion) isFile() bool { return version != nil && version.sha256 != "" }

// driveContent is content a drive holds: its bytes, or for generated content
// its size and the seed its bytes follow from.
type driveContent struct {
	size      int64
	data      []byte
	generated bool
	seed      string
}

// reader reads count bytes of the content from first.
func (content driveContent) reader(first, count int64) io.Reader {
	if !content.generated {
		return bytes.NewReader(content.data[first : first+count])
	}
	// AES in counter mode gives bytes that depend on their offset, so that a
	// range served from the wrong offset changes the content's hash, and that
	// any range is made without the bytes before it.
	key := sha256.Sum256([]byte(content.seed))
	block, _ := aes.NewCipher(key[:])
	var counter [aes.BlockSize]byte
	binary.BigEndian.PutUint64(counter[8:], uint64(first/aes.BlockSize))
	stream := cipher.NewCTR(block, counter[:])
	skipped := make([]byte, first%aes.BlockSize)
	stream.XORKeyStream(skipped, skipped)
	return &keystream{stream: stream, remaining: count}
}

type keystream struct {
	stream    cipher.Stream
	remaining int64
}

func (source *keystream) Read(buffer []byte) (int, error) {
	if source.remaining == 0 {
		return 0, io.EOF
	}
	count := min(int64(len(buffer)), source.remaining)
	clear(buffer[:count])
	source.stream.XORKeyStream(buffer[:count], buffer[:count])
	source.remaining -= count
	return int(count), nil
}

// generatedContent is size bytes that follow from seed, with their hash.
func generatedContent(size int64, seed string) (driveContent, string) {
	content := driveContent{size: size, generated: true, seed: seed}
	digest := sha256.New()
	_, _ = io.Copy(digest, content.reader(0, size))
	return content, hex.EncodeToString(digest.Sum(nil))
}

// driveOf is the drive with id, made empty when first named.
func (server *Server) driveOf(id string) *drive {
	found := server.drives[id]
	if found == nil {
		found = &drive{
			id: id, current: map[string]*driveVersion{}, folded: map[string]string{}, below: map[string]int{},
			versions: map[string]*driveVersion{}, contents: map[string]driveContent{},
		}
		server.drives[id] = found
	}
	return found
}

// files are the drive's current files in UTF-8 byte order of their paths.
func (target *drive) files() []*driveVersion {
	var listed []*driveVersion
	for _, path := range slices.Sorted(maps.Keys(target.current)) {
		if version := target.current[path]; version.isFile() {
			listed = append(listed, version)
		}
	}
	return listed
}

// directories are the directories above path, nearest last.
func directories(path string) []string {
	var above []string
	for index, char := range []byte(path) {
		if char == '/' {
			above = append(above, path[:index])
		}
	}
	return above
}

// setCurrent makes version the current version of path; nil leaves the
// drive without a version of the path.
func (target *drive) setCurrent(path string, version *driveVersion) {
	if previous := target.current[path]; previous.isFile() {
		delete(target.folded, wire.CaseFolded(path))
		for _, directory := range directories(path) {
			target.below[directory]--
		}
		target.usedBytes -= previous.size
		target.usedFiles--
	}
	if version == nil {
		delete(target.current, path)
		return
	}
	target.current[path] = version
	if version.isFile() {
		target.folded[wire.CaseFolded(path)] = path
		for _, directory := range directories(path) {
			target.below[directory]++
		}
		target.usedBytes += version.size
		target.usedFiles++
	}
}

// keeps says whether a kept version of the drive names the content.
func (target *drive) keeps(sha256 string) bool {
	for _, version := range target.versions {
		if version.sha256 == sha256 && !version.removed {
			return true
		}
	}
	return false
}

// pathRule gives the drive path rule a new file at path would break, or "".
// A rule names no path, since a path may be longer than a rule may be.
func (target *drive) pathRule(path string) string {
	if err := wire.CheckDrivePath(path); err != nil {
		return "the path is not a drive path: " + err.Error()
	}
	if other, held := target.folded[wire.CaseFolded(path)]; held && other != path {
		return "the path differs only in case from another file's"
	}
	for _, directory := range directories(path) {
		if target.current[directory].isFile() {
			return "the path is below a file"
		}
	}
	if target.below[path] > 0 {
		return "the path is above another file"
	}
	return ""
}

// limitRule gives the drive limit that a write of size bytes at path would
// raise usage beyond, or "". A write that raises neither bytes nor files
// breaks none, also while usage is above a lowered limit.
func (target *drive) limitRule(path string, size int64) string {
	bytes, files := target.usedBytes, target.usedFiles
	previous := target.current[path]
	addedFiles, addedBytes := int64(1), size
	if previous.isFile() {
		addedFiles, addedBytes = 0, size-previous.size
	}
	switch {
	case target.maxFiles != nil && addedFiles > 0 && files+addedFiles > *target.maxFiles:
		return fmt.Sprintf("the drive holds at most %d files", *target.maxFiles)
	case target.maxBytes != nil && addedBytes > 0 && bytes+addedBytes > *target.maxBytes:
		return fmt.Sprintf("the drive holds at most %d bytes", *target.maxBytes)
	}
	return ""
}

// pendingCommit collects the versions one commit makes; they become visible
// together under one seq when it ends.
type pendingCommit struct {
	server *Server
	target *drive
	writer wire.DriveWriter
	made   []*driveVersion
	// operations holds each made version's operation, in the same order.
	operations []string
}

// put makes path's new version, a deletion when sha256 is empty.
func (commit *pendingCommit) put(path, sha256 string, executable bool) *driveVersion {
	target := commit.target
	operation := "deleted"
	version := &driveVersion{id: commit.server.newID("ver"), path: path, writer: commit.writer, made: len(target.versions)}
	if sha256 != "" {
		operation = "modified"
		if !target.current[path].isFile() {
			operation = "created"
		}
		version.sha256, version.size, version.executable = sha256, target.contents[sha256].size, executable
	}
	target.setCurrent(path, version)
	target.versions[version.id] = version
	commit.made = append(commit.made, version)
	commit.operations = append(commit.operations, operation)
	return version
}

// end appends the commit's change records under one new seq and sends the
// drive's notice; a commit that made no version takes no seq.
func (commit *pendingCommit) end() wire.Nullable[int64] {
	target := commit.target
	if len(commit.made) == 0 {
		return wire.Nullable[int64]{}
	}
	target.seq++
	for index, version := range commit.made {
		record := wire.ChangeRecord{
			Seq: target.seq, Index: int64(index), Path: wire.DrivePath(version.path),
			Operation: commit.operations[index], VersionID: wire.ID(version.id),
			SizeBytes: version.size, Executable: version.executable,
		}
		if version.isFile() {
			record.ContentSha256 = wire.NonNull(wire.Sha256(version.sha256))
		}
		target.records = append(target.records, record)
	}
	commit.server.notifyDrive(target)
	return wire.NonNull(target.seq)
}

// currentVersion is a rejection's `current`: the path's current version,
// absent when the drive keeps none.
func currentVersion(version *driveVersion) wire.Opt[wire.CurrentVersion] {
	if version == nil {
		return wire.Opt[wire.CurrentVersion]{}
	}
	current := wire.CurrentVersion{
		VersionID: wire.ID(version.id), SizeBytes: version.size, Executable: version.executable, Writer: version.writer,
	}
	if version.isFile() {
		current.ContentSha256 = wire.NonNull(wire.Sha256(version.sha256))
	}
	return wire.Some(current)
}

func rejected(reason, rule string, current *driveVersion) wire.CommitResult {
	result := &wire.RejectedResult{Reason: reason, Current: currentVersion(current)}
	if rule != "" {
		result.Rule = wire.Some(wire.NonEmpty(rule))
	}
	return wire.CommitResult{Result: result}
}

func accepted(version *driveVersion) wire.CommitResult {
	result := &wire.AcceptedResult{}
	if version != nil {
		result.VersionID = wire.NonNull(wire.ID(version.id))
	}
	return wire.CommitResult{Result: result}
}

// decide decides one change of a machine's commit by the first rule of
// "Commit", "Decision", b, that matches.
func (commit *pendingCommit) decide(change wire.CommitChange) wire.CommitResult {
	target := commit.target
	switch change := change.Change.(type) {
	case *wire.DeleteChange:
		current := target.current[change.Path]
		switch {
		case !current.isFile():
			return accepted(current)
		case current.sha256 != string(change.BaseSha256):
			return rejected("conflict", "", current)
		}
		return accepted(commit.put(change.Path, "", false))
	case *wire.WriteChange:
		current := target.current[change.Path]
		if current.isFile() && current.sha256 == string(change.ContentSha256) && current.executable == change.Executable {
			return accepted(current)
		}
		if rule := target.pathRule(change.Path); rule != "" {
			return rejected("path_rule", rule, current)
		}
		if current.isFile() != change.BaseSha256.Valid || (current.isFile() && current.sha256 != string(change.BaseSha256.Value)) {
			return rejected("conflict", "", current)
		}
		if rule := target.limitRule(change.Path, target.contents[string(change.ContentSha256)].size); rule != "" {
			return rejected("limit", rule, current)
		}
		return accepted(commit.put(change.Path, string(change.ContentSha256), change.Executable))
	}
	panic("a commit change that is neither a write nor a delete")
}

// commitMachine runs a machine's commit: the content check, then every
// deletion and then every write, each group in request order, and the results
// in request order. With content missing nothing is applied and missing
// lists the hashes.
func (server *Server) commitMachine(target *drive, writer wire.DriveWriter, changes []wire.CommitChange) (response wire.CommitResponse, missing []wire.Sha256) {
	for _, change := range changes {
		if write, isWrite := change.Change.(*wire.WriteChange); isWrite {
			if _, held := target.contents[string(write.ContentSha256)]; !held && !slices.Contains(missing, write.ContentSha256) {
				missing = append(missing, write.ContentSha256)
			}
		}
	}
	if len(missing) > 0 {
		return response, missing
	}
	commit := &pendingCommit{server: server, target: target, writer: writer}
	response.Results = make([]wire.CommitResult, len(changes))
	for _, deletions := range []bool{true, false} {
		for index, change := range changes {
			if _, isDelete := change.Change.(*wire.DeleteChange); isDelete == deletions {
				response.Results[index] = commit.decide(change)
			}
		}
	}
	response.Seq = commit.end()
	return response, nil
}

// uploadBudget is the workspace's upload budget: it holds at most perMinute
// bytes, starts full and refills continuously at perMinute bytes every 60
// seconds.
type uploadBudget struct {
	perMinute int64
	level     float64
	at        time.Time
}

const (
	// DefaultUploadBudget is the budget's bytes per minute until the
	// drive_budget control sets another amount.
	DefaultUploadBudget = 500_000_000
	// uploadBudgetMinCount is the least an upload counts.
	uploadBudgetMinCount = 65_536
)

// take takes an upload of contentLength bytes from the budget at now, or
// gives the whole seconds until the budget holds its count.
func (budget *uploadBudget) take(contentLength int64, now time.Time) (retryAfter int64) {
	perSecond := float64(budget.perMinute) / 60
	budget.level = min(float64(budget.perMinute), budget.level+now.Sub(budget.at).Seconds()*perSecond)
	budget.at = now
	count := float64(min(max(contentLength, uploadBudgetMinCount), budget.perMinute))
	if budget.level >= count {
		budget.level -= count
		return 0
	}
	seconds := int64((count - budget.level) / perSecond)
	if float64(seconds)*perSecond < count-budget.level {
		seconds++
	}
	return max(seconds, 1)
}

// notifyDrive writes the drive's notice with its current seq on every
// connection that takes it.
func (server *Server) notifyDrive(target *drive) {
	for _, conn := range server.connections {
		if conn.stream != nil && conn.noticed(target.id) {
			server.sendNotice(conn, target)
		}
	}
}

func (server *Server) sendNotice(conn *connection, target *drive) {
	data, messageID := server.event(conn, "drive.changed", wire.DriveChangedPayload{DriveID: wire.ID(target.id), Seq: target.seq})
	if _, err := wire.Decode[wire.DriveChanged](data); err != nil {
		server.fail("a drive.changed notice fails its schema: " + err.Error())
		return
	}
	conn.stream.send(data, messageID, "drive.changed")
}

// noticed says whether the connection takes the notices of a drive: its
// ready declared drive mounting, and a prepared or closing binding of it
// holds the drive.
func (conn *connection) noticed(driveID string) bool {
	if conn.ended || !conn.readyAccepted || !conn.mountsDrives {
		return false
	}
	for _, bound := range conn.bindings {
		if bound.usable() && bound.access(driveID, false) {
			return true
		}
	}
	return false
}

// startNotices writes, on a stream that starts, one notice for each drive
// with a commit that the connection takes notices of, in the order of their
// drive IDs.
func (server *Server) startNotices(conn *connection) {
	for _, id := range slices.Sorted(maps.Keys(server.drives)) {
		if target := server.drives[id]; target.seq > 0 && conn.noticed(id) {
			server.sendNotice(conn, target)
		}
	}
}

// driveSet is the drives and access one session.prepare or session.drives
// sent to a binding.
type driveSet struct {
	messageID string
	drives    []wire.DriveBinding
}

// usable says whether the binding is prepared or closing: its prepared
// report is stored and its closed report is not.
func (bound *binding) usable() bool { return bound.prepared && !bound.closed }

// access says whether the binding's drive set holds the drive, with
// read_write access when write is set. The set is what the binding was last
// sent, and what it was sent before each session.drives whose applied
// report is not stored.
func (bound *binding) access(driveID string, write bool) bool {
	for _, set := range bound.sets {
		for _, held := range set.drives {
			if string(held.DriveID) == driveID && (!write || held.Access == "read_write") {
				return true
			}
		}
	}
	return false
}

// applied drops, once the applied report of a session.drives is stored, the
// drive sets sent before it.
func (bound *binding) applied(messageID string) {
	for index, set := range bound.sets {
		if set.messageID == messageID {
			bound.sets = bound.sets[index:]
			return
		}
	}
}

// fail records a request or a message that the server does not implement
// or cannot make within the contract; the state control lists them, and a
// test that finds one fails.
func (server *Server) fail(reason string) {
	server.failures = append(server.failures, reason)
}
