package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	helpercapture "github.com/tetral-ai/tetral/internal/sandbox/helper/internal/capture"
	helperexecute "github.com/tetral-ai/tetral/internal/sandbox/helper/internal/execute"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/filetool"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/health"
	helpermedia "github.com/tetral-ai/tetral/internal/sandbox/helper/internal/media"
	helperpatch "github.com/tetral-ai/tetral/internal/sandbox/helper/internal/patch"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	helpersearch "github.com/tetral-ai/tetral/internal/sandbox/helper/internal/search"
	helpertask "github.com/tetral-ai/tetral/internal/sandbox/helper/internal/task"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
	"github.com/tetral-ai/tetral/internal/sandbox/runtimeidentity"
)

const Version = "0.1.0"

// maxPayloadBytes is the 4 MiB hard cap on the whole payload file; a larger
// payload is invalid_input. Because the apply_patch patch text arrives inside
// the payload, this cap transitively bounds the largest patch the helper will
// accept.
const maxPayloadBytes = 4 * 1024 * 1024
const defaultPayloadRoot = "/tmp/tetral-runtime/tool-payloads"

// helperIDPattern constrains tool_use_event_id and task_id to
// ^[A-Za-z0-9_-]{1,64}$. The shape matters for safety, not just tidiness: these
// IDs become path components of the payload and task directories, so allowing
// no '/', '.', or other separators is what keeps an ID from traversing out of
// its payload or task path.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/task/supervisor.go (same pattern gates
//     task_id)
var helperIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

var (
	currentEUID         = os.Geteuid
	statRuntimeIdentity = statOwnerIdentity
	setRuntimeGID       = syscall.Setgid
	setRuntimeUID       = syscall.Setuid
	clearRuntimeGroups  = func() error { return syscall.Setgroups([]int{}) }
	normalizeRuntimeEnv = runtimeidentity.NormalizeProcessEnvironment
	payloadRoot         = defaultPayloadRoot
)

func Main(ctx context.Context, args []string, stdout io.Writer) int {
	redirectStderr()
	if len(args) > 0 && args[0] == "__supervise" {
		if !helpertask.SupervisorEntrypointAuthorized() {
			return 1
		}
		helpertask.ConsumeSupervisorEntrypointAuth()
		return helpertask.RunSupervisor(args[1:])
	}
	if len(args) > 0 && args[0] == "__supervise-bootstrap" {
		if !helpertask.SupervisorEntrypointAuthorized() {
			return 1
		}
		return helpertask.RunSupervisorBootstrap(args[1:])
	}
	if len(args) > 0 && args[0] == "__capture" {
		sourcePath, maxBytes, maxEntries, maxNameBytes, ok := parseCaptureArgs(args)
		if !ok {
			return 1
		}
		return runCapture(stdout, sourcePath, maxBytes, maxEntries, maxNameBytes)
	}
	if len(args) == 1 && args[0] == "health" {
		return runHealth(ctx, stdout)
	}
	if len(args) != 3 || args[1] != "--payload" {
		return 1
	}
	switch args[0] {
	case "exec":
		return runExec(stdout, args[2])
	case "stdin":
		return runStdin(stdout, args[2])
	case "poll":
		return runPoll(stdout, args[2])
	case "cancel":
		return runCancel(stdout, args[2])
	case "read":
		return runRead(stdout, args[2])
	case "write":
		return runWrite(stdout, args[2])
	case "edit":
		return runEdit(stdout, args[2])
	case "apply_patch":
		return runApplyPatch(stdout, args[2])
	case "grep":
		return runGrep(stdout, args[2])
	case "glob":
		return runGlob(stdout, args[2])
	case "view_image":
		return runViewImage(stdout, args[2])
	default:
		return 1
	}
}

func parseCaptureArgs(args []string) (string, int64, int, int64, bool) {
	if len(args) != 5 && len(args) != 9 {
		return "", 0, 0, 0, false
	}
	if args[0] != "__capture" || args[1] != "--path" || args[3] != "--max-bytes" {
		return "", 0, 0, 0, false
	}
	maxBytes, err := strconv.ParseInt(args[4], 10, 64)
	if err != nil {
		return "", 0, 0, 0, false
	}
	maxEntries := protocol.CaptureDefaultMaxEntries
	maxNameBytes := int64(protocol.CaptureDefaultMaxNameBytes)
	if len(args) == 9 {
		if args[5] != "--max-entries" || args[7] != "--max-name-bytes" {
			return "", 0, 0, 0, false
		}
		parsedEntries, err := strconv.Atoi(args[6])
		if err != nil {
			return "", 0, 0, 0, false
		}
		parsedNameBytes, err := strconv.ParseInt(args[8], 10, 64)
		if err != nil {
			return "", 0, 0, 0, false
		}
		maxEntries = parsedEntries
		maxNameBytes = parsedNameBytes
	}
	if maxBytes < 0 || maxEntries < 0 || maxNameBytes < 0 {
		return "", 0, 0, 0, false
	}
	return args[2], maxBytes, maxEntries, maxNameBytes, true
}

func runCapture(stdout io.Writer, sourcePath string, maxBytes int64, maxEntries int, maxNameBytes int64) int {
	envelope := helpercapture.RunWithBounds(sourcePath, maxBytes, maxEntries, maxNameBytes)
	if err := json.NewEncoder(stdout).Encode(envelope); err != nil {
		return 1
	}
	return 0
}

func runHealth(ctx context.Context, stdout io.Writer) int {
	startedAt := time.Now()
	result := health.NewChecker(Version).Run(ctx)
	var envelope protocol.Envelope
	var err error
	if health.Failed(result) {
		envelope, err = protocol.NewToolErrorEnvelope("health", protocol.ToolError{Kind: health.HelperFailureKind, Message: "sandbox helper health check failed"}, result, false, startedAt)
	} else {
		envelope, err = protocol.NewToolResultEnvelope("health", result, false, startedAt)
	}
	if err != nil {
		return writeToolError(stdout, "health", protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode health result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, false); err != nil {
		return 1
	}
	return 0
}

func runExec(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "exec")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "exec", *toolErr, startedAt)
	}
	response := helperexecute.Run(payload)
	return writeCommandResponse(stdout, "exec", response, startedAt)
}

func runStdin(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "stdin")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "stdin", *toolErr, startedAt)
	}
	response := helpertask.RunStdin(payload)
	return writeCommandResponse(stdout, "stdin", response, startedAt)
}

func runPoll(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "poll")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "poll", *toolErr, startedAt)
	}
	response := helpertask.RunPoll(payload)
	return writeCommandResponse(stdout, "poll", response, startedAt)
}

func runCancel(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "cancel")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "cancel", *toolErr, startedAt)
	}
	response := helpertask.RunCancel(payload)
	return writeCommandResponse(stdout, "cancel", response, startedAt)
}

func writeCommandResponse(stdout io.Writer, tool string, response helpertask.Response, startedAt time.Time) int {
	if response.HelperFailure {
		return 1
	}
	truncated := response.Result.Stdout.Truncated || response.Result.Stderr.Truncated
	var envelope protocol.Envelope
	var err error
	if response.Status == protocol.ToolStatusError {
		if response.Error == nil {
			return 1
		}
		toolErr := *response.Error
		envelope, err = protocol.NewToolErrorEnvelope(tool, toolErr, response.Result, truncated, startedAt)
	} else {
		envelope, err = protocol.NewToolResultEnvelope(tool, response.Result, truncated, startedAt)
		envelope.Status = response.Status
	}
	if err != nil {
		return writeToolError(stdout, tool, protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode command result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, false); err != nil {
		return 1
	}
	return 0
}

func runRead(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "read")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "read", *toolErr, startedAt)
	}
	result, toolErr, err := filetool.Read(payload)
	if err != nil {
		return 1
	}
	if toolErr != nil {
		return writePayloadLoadError(stdout, "read", *toolErr, startedAt)
	}
	readMedia := result.Media != nil
	if !readMedia {
		result = filetool.FitReadResultForEnvelope(result, "read", startedAt)
	}
	envelope, err := protocol.NewToolResultEnvelope("read", result, result.Truncated, startedAt)
	if err != nil {
		return writeToolError(stdout, "read", protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode read result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, readMedia); err != nil {
		return 1
	}
	return 0
}

func runWrite(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "write")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "write", *toolErr, startedAt)
	}
	result, toolErr := filetool.Write(payload)
	if toolErr != nil {
		return writePayloadLoadError(stdout, "write", *toolErr, startedAt)
	}
	envelope, err := protocol.NewToolResultEnvelope("write", result, false, startedAt)
	if err != nil {
		return writeToolError(stdout, "write", protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode write result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, false); err != nil {
		return 1
	}
	return 0
}

func runEdit(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "edit")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "edit", *toolErr, startedAt)
	}
	result, toolErr := filetool.Edit(payload)
	if toolErr != nil {
		return writePayloadLoadError(stdout, "edit", *toolErr, startedAt)
	}
	envelope, err := protocol.NewToolResultEnvelope("edit", result, false, startedAt)
	if err != nil {
		return writeToolError(stdout, "edit", protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode edit result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, false); err != nil {
		return 1
	}
	return 0
}

func runApplyPatch(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "apply_patch")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "apply_patch", *toolErr, startedAt)
	}
	result, toolErr := helperpatch.Apply(payload)
	if toolErr != nil {
		return writePayloadLoadError(stdout, "apply_patch", *toolErr, startedAt)
	}
	envelope, err := protocol.NewToolResultEnvelope("apply_patch", result, false, startedAt)
	if err != nil {
		return writeToolError(stdout, "apply_patch", protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode apply_patch result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, false); err != nil {
		return 1
	}
	return 0
}

func runGrep(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "grep")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "grep", *toolErr, startedAt)
	}
	result, toolErr := helpersearch.Grep(payload)
	if toolErr != nil {
		return writePayloadLoadError(stdout, "grep", *toolErr, startedAt)
	}
	envelope, err := protocol.NewToolResultEnvelope("grep", result, result.Truncated, startedAt)
	if err != nil {
		return writeToolError(stdout, "grep", protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode grep result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, false); err != nil {
		return 1
	}
	return 0
}

func runGlob(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "glob")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "glob", *toolErr, startedAt)
	}
	result, toolErr := helpersearch.Glob(payload)
	if toolErr != nil {
		return writePayloadLoadError(stdout, "glob", *toolErr, startedAt)
	}
	envelope, err := protocol.NewToolResultEnvelope("glob", result, result.Truncated, startedAt)
	if err != nil {
		return writeToolError(stdout, "glob", protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode glob result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, false); err != nil {
		return 1
	}
	return 0
}

func runViewImage(stdout io.Writer, payloadPath string) int {
	startedAt := time.Now()
	payload, toolErr := loadPayloadAndDrop(payloadPath, "view_image")
	if toolErr != nil {
		return writePayloadLoadError(stdout, "view_image", *toolErr, startedAt)
	}
	result, toolErr := helpermedia.ViewImage(payload)
	if toolErr != nil {
		return writePayloadLoadError(stdout, "view_image", *toolErr, startedAt)
	}
	envelope, err := protocol.NewToolResultEnvelope("view_image", result, false, startedAt)
	if err != nil {
		return writeToolError(stdout, "view_image", protocol.ToolError{Kind: health.HelperFailureKind, Message: "encode view_image result failed"}, startedAt)
	}
	if err := writeEnvelope(stdout, envelope, true); err != nil {
		return 1
	}
	return 0
}

func loadPayloadAndDrop(payloadPath string, subcommand string) (protocol.Payload, *protocol.ToolError) {
	payload, toolErr := loadPayload(payloadPath, subcommand)
	if toolErr != nil {
		return protocol.Payload{}, toolErr
	}
	if subcommand == "exec" && detachedExecRequested(payload.Input) {
		if err := helpertask.InitializeSupervisorEntrypointAuth(); err != nil {
			return protocol.Payload{}, &protocol.ToolError{Kind: health.HelperFailureKind, Message: "supervisor authorization initialization failed"}
		}
	}
	// Root owns only the protected-payload transport and detached-supervisor
	// capability setup. The shared boundary below irreversibly adopts the
	// prepared workspace owner's credentials and the fixed runtime environment
	// before any payload-backed Agent Tool effect is dispatched.
	if toolErr := dropPrivilegesForTool(payload.WorkspaceRoot); toolErr != nil {
		return protocol.Payload{}, toolErr
	}
	return payload, nil
}

func detachedExecRequested(raw json.RawMessage) bool {
	var input struct {
		OnWaitExpiry string `json:"on_wait_expiry"`
	}
	return json.Unmarshal(raw, &input) == nil && input.OnWaitExpiry == "detach"
}

func writePayloadLoadError(stdout io.Writer, tool string, toolError protocol.ToolError, startedAt time.Time) int {
	if toolError.Kind == health.HelperFailureKind {
		return 1
	}
	return writeToolError(stdout, tool, toolError, startedAt)
}

func loadPayload(payloadPath string, subcommand string) (protocol.Payload, *protocol.ToolError) {
	cleanPayloadPath, pathID, toolErr := validatePayloadPath(payloadPath)
	if toolErr != nil {
		return protocol.Payload{}, toolErr
	}
	file, rootDir, relativePayloadPath, err := openProtectedPayload(cleanPayloadPath, pathID)
	if err != nil {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "payload is not readable"}
	}
	defer func() { _ = rootDir.Close() }()
	body, readErr := io.ReadAll(io.LimitReader(file, maxPayloadBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "payload could not be read"}
	}
	if closeErr != nil {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "payload could not be closed"}
	}
	if len(body) > maxPayloadBytes {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "payload exceeds 4 MiB"}
	}
	var payload protocol.Payload
	if err := json.Unmarshal(body, &payload); err != nil {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "payload is malformed JSON"}
	}
	_ = unix.Unlinkat(int(rootDir.Fd()), relativePayloadPath, 0)
	if payload.SchemaVersion != protocol.SchemaVersion {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "unsupported payload schema_version"}
	}
	if payload.Tool != subcommand {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "payload tool does not match subcommand"}
	}
	if !helperIDPattern.MatchString(payload.ToolUseEventID) {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "tool_use_event_id has invalid shape"}
	}
	if payload.ToolUseEventID != pathID {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "payload path does not match tool_use_event_id"}
	}
	if toolErr := validatePayloadCommon(payload); toolErr != nil {
		return protocol.Payload{}, toolErr
	}
	if len(payload.Input) == 0 || string(payload.Input) == "null" {
		return protocol.Payload{}, &protocol.ToolError{Kind: "invalid_input", Message: "payload input is required"}
	}
	return payload, nil
}

func openProtectedPayload(cleanPayloadPath string, pathID string) (*os.File, *os.File, string, error) {
	root := filepath.Clean(payloadRoot)
	rootDir, err := os.Open(root) //nolint:gosec // the fixed helper-private root is verified before relative open.
	if err != nil {
		return nil, nil, "", err
	}
	closeRoot := true
	defer func() {
		if closeRoot {
			_ = rootDir.Close()
		}
	}()
	var rootStat unix.Stat_t
	if err := unix.Fstat(int(rootDir.Fd()), &rootStat); err != nil || rootStat.Mode&unix.S_IFMT != unix.S_IFDIR || rootStat.Uid != uint32(currentEUID()) || rootStat.Mode&0o022 != 0 {
		return nil, nil, "", errors.New("payload root is not protected")
	}
	relativePayloadPath := filepath.Join(pathID, "payload.json")
	if filepath.Join(root, relativePayloadPath) != cleanPayloadPath {
		return nil, nil, "", errors.New("payload path changed during validation")
	}
	fd, err := unix.Openat2(int(rootDir.Fd()), relativePayloadPath, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return nil, nil, "", err
	}
	file := os.NewFile(uintptr(fd), relativePayloadPath)
	if file == nil {
		_ = unix.Close(fd)
		return nil, nil, "", errors.New("payload file descriptor is unavailable")
	}
	var fileStat unix.Stat_t
	if err := unix.Fstat(fd, &fileStat); err != nil || fileStat.Mode&unix.S_IFMT != unix.S_IFREG || fileStat.Uid != uint32(currentEUID()) || fileStat.Mode&0o077 != 0 {
		_ = file.Close()
		return nil, nil, "", errors.New("payload file is not protected")
	}
	closeRoot = false
	return file, rootDir, relativePayloadPath, nil
}

func validatePayloadPath(payloadPath string) (string, string, *protocol.ToolError) {
	if payloadPath == "" {
		return "", "", &protocol.ToolError{Kind: "invalid_input", Message: "payload path is required"}
	}
	cleanPayloadPath := filepath.Clean(payloadPath)
	if !filepath.IsAbs(cleanPayloadPath) {
		return "", "", &protocol.ToolError{Kind: "invalid_input", Message: "payload path must be absolute"}
	}
	root := filepath.Clean(payloadRoot)
	rel, err := filepath.Rel(root, cleanPayloadPath)
	if err != nil || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", "", &protocol.ToolError{Kind: "invalid_input", Message: "payload path is outside helper payload root"}
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 2 || parts[1] != "payload.json" || !helperIDPattern.MatchString(parts[0]) {
		return "", "", &protocol.ToolError{Kind: "invalid_input", Message: "payload path has invalid shape"}
	}
	return cleanPayloadPath, parts[0], nil
}

func dropPrivilegesForTool(runtimeRoot string) *protocol.ToolError {
	if currentEUID == nil || currentEUID() != 0 {
		return nil
	}
	if statRuntimeIdentity == nil {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime identity lookup is unavailable"}
	}
	identity, err := statRuntimeIdentity(runtimeRoot)
	if err != nil {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime identity lookup failed"}
	}
	if identity.uid == 0 {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime identity must not be root"}
	}
	if clearRuntimeGroups == nil || setRuntimeGID == nil || setRuntimeUID == nil {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime privilege drop is unavailable"}
	}
	if err := clearRuntimeGroups(); err != nil {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime supplementary group drop failed"}
	}
	if err := setRuntimeGID(identity.gid); err != nil {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime gid drop failed"}
	}
	if err := setRuntimeUID(identity.uid); err != nil {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime uid drop failed"}
	}
	if normalizeRuntimeEnv == nil {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime environment normalization is unavailable"}
	}
	if err := normalizeRuntimeEnv(); err != nil {
		return &protocol.ToolError{Kind: health.HelperFailureKind, Message: "runtime environment normalization failed"}
	}
	return nil
}

type runtimeIdentity struct {
	uid int
	gid int
}

func statOwnerIdentity(path string) (runtimeIdentity, error) {
	info, err := os.Stat(path)
	if err != nil {
		return runtimeIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return runtimeIdentity{}, errors.New("runtime root stat lacks uid/gid")
	}
	return runtimeIdentity{uid: int(stat.Uid), gid: int(stat.Gid)}, nil
}

func writeToolError(stdout io.Writer, tool string, toolError protocol.ToolError, startedAt time.Time) int {
	if toolError.Kind == health.HelperFailureKind && tool != "health" {
		return 1
	}
	envelope, err := protocol.NewToolErrorEnvelope(tool, toolError, nil, false, startedAt)
	if err != nil {
		return 1
	}
	if err := writeEnvelope(stdout, envelope, false); err != nil {
		return 1
	}
	return 0
}

func writeEnvelope(stdout io.Writer, envelope protocol.Envelope, allowOversize bool) error {
	body, err := protocol.MarshalEnvelope(envelope, allowOversize)
	if err != nil {
		return err
	}
	if _, err := stdout.Write(body); err != nil {
		return err
	}
	return nil
}

func validatePayloadCommon(payload protocol.Payload) *protocol.ToolError {
	err := (pathsafe.Resolver{WorkspaceRoot: payload.WorkspaceRoot, Roots: payload.Roots}).Validate()
	if err == nil {
		return nil
	}
	var pathErr *pathsafe.Error
	if errors.As(err, &pathErr) {
		return &protocol.ToolError{Kind: pathErr.Kind, Message: pathErr.Message, Path: pathErr.Path}
	}
	return &protocol.ToolError{Kind: "invalid_input", Message: "payload roots are invalid"}
}

func redirectStderr() {
	logPath := filepath.Join(health.RuntimeRoot, "logs", "helper.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		redirectStderrToDevNull()
		return
	}
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // Helper log path is fixed under the runtime root.
	if err != nil {
		redirectStderrToDevNull()
		return
	}
	_ = os.Stderr.Close()
	os.Stderr = file
}

func redirectStderrToDevNull() {
	file, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return
	}
	_ = os.Stderr.Close()
	os.Stderr = file
}
