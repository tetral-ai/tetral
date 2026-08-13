package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	// helperPath is the in-sandbox absolute path every helper subcommand is
	// invoked at; it must match the path the helper binary is installed to.
	// UPDATE-WITH: internal/sandbox/helper/cmd/sandbox (helper entrypoint);
	// Makefile build-sandbox-helper and install-sandbox-helper (SANDBOX_HELPER_INSTALL_PATH).
	helperPath = "/usr/local/bin/sandbox"
	// The helper starts as root only long enough to read/unlink root-owned
	// payload files and initialize protected supervisor transport. Its shared
	// payload boundary then drops credentials and establishes RuntimeUser's
	// identity environment before any Agent Tool effect.
	helperUser = "root"
	// Payload staging is two-rooted because the two transports act as
	// different identities: Daytona's filesystem API writes as the runtime
	// user (it cannot produce root-owned files), while the helper's
	// openProtectedPayload refuses any payload whose root, directory, or file
	// is not root-owned with group/other bits clear. Each call therefore
	// uploads into the runtime-user stage root and a sudo freeze command
	// moves the payload directory into the root-owned final root.
	payloadRootPath      = "/tmp/tetral-runtime/tool-payloads"
	payloadStageRootPath = "/tmp/tetral-runtime/tool-payloads-stage"
	payloadFileName      = "payload.json"

	helperSubcommandExec       = "exec"
	helperSubcommandStdin      = "stdin"
	helperSubcommandPoll       = "poll"
	helperSubcommandCancel     = "cancel"
	helperSubcommandRead       = "read"
	helperSubcommandWrite      = "write"
	helperSubcommandEdit       = "edit"
	helperSubcommandApplyPatch = "apply_patch"
	helperSubcommandGrep       = "grep"
	helperSubcommandGlob       = "glob"
	helperSubcommandViewImage  = "view_image"

	helperWorkspaceRoot      = "/workspace"
	helperSessionUploadsRoot = "/mnt/session/uploads"
	helperSessionOutputsRoot = "/mnt/session/outputs"
	helperMemoryRoot         = "/mnt/memory"
	helperSkillsRoot         = "/skills"
	helperVisibleBytes       = 51200
	helperVisibleLines       = 2000
)

var helperCommandNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
var helperPayloadIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type DaytonaHelperExecutor struct {
	client         daytonaSandboxGetter
	commandTimeout time.Duration
}

type daytonaSandboxGetter interface {
	Get(context.Context, string) (daytonaSandboxHandle, error)
}

type daytonaRawSandboxGetter interface {
	Get(context.Context, string) (*daytona.Sandbox, error)
}

type daytonaSandboxHandle struct {
	FileSystem daytonaFileSystem
	Process    daytonaProcess
}

type daytonaFileSystem interface {
	CreateFolder(context.Context, string, ...func(*options.CreateFolder)) error
	UploadFileStream(context.Context, io.Reader, string, ...daytona.UploadStreamOption) error
	DeleteFile(context.Context, string, bool) error
	DownloadFileStream(context.Context, string, ...daytona.DownloadStreamOption) (io.ReadCloser, error)
}

type daytonaProcess interface {
	ExecuteCommand(context.Context, string, ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error)
}

// DaytonaSandboxServices is the narrow process/filesystem boundary used by
// the helper executor. It permits hermetic lifecycle integration tests to run
// the real helper without substituting the driver's payload translation.
type DaytonaSandboxServices struct {
	FileSystem DaytonaSandboxFileSystem
	Process    DaytonaSandboxProcess
}

type DaytonaSandboxFileSystem interface {
	CreateFolder(context.Context, string, ...func(*options.CreateFolder)) error
	UploadFileStream(context.Context, io.Reader, string, ...daytona.UploadStreamOption) error
	DeleteFile(context.Context, string, bool) error
	DownloadFileStream(context.Context, string, ...daytona.DownloadStreamOption) (io.ReadCloser, error)
}

type DaytonaSandboxProcess interface {
	ExecuteCommand(context.Context, string, ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error)
}

type daytonaSandboxServicesGetter struct {
	get func(context.Context, string) (DaytonaSandboxServices, error)
}

func (g daytonaSandboxServicesGetter) Get(ctx context.Context, providerSandboxID string) (daytonaSandboxHandle, error) {
	services, err := g.get(ctx, providerSandboxID)
	if err != nil {
		return daytonaSandboxHandle{}, err
	}
	return daytonaSandboxHandle{FileSystem: services.FileSystem, Process: services.Process}, nil
}

type daytonaClientSandboxGetter struct {
	client daytonaRawSandboxGetter
}

func (g daytonaClientSandboxGetter) Get(ctx context.Context, providerSandboxID string) (daytonaSandboxHandle, error) {
	got, err := g.client.Get(ctx, providerSandboxID)
	if err != nil || got == nil {
		return daytonaSandboxHandle{}, err
	}
	return daytonaSandboxHandle{FileSystem: got.FileSystem, Process: got.Process}, nil
}

// NewDaytonaHelperExecutorForSDKClient binds helper operations to the
// process-wide Daytona client owned by the Sandbox Service adapter.
func NewDaytonaHelperExecutorForSDKClient(client *daytona.Client, timeout time.Duration) (*DaytonaHelperExecutor, error) {
	if client == nil || timeout <= 0 {
		return nil, errors.New("daytona helper client and command timeout are required")
	}
	return &DaytonaHelperExecutor{client: daytonaClientSandboxGetter{client: client}, commandTimeout: timeout}, nil
}

func NewDaytonaHelperExecutorForClient(client daytonaSandboxGetter) *DaytonaHelperExecutor {
	return &DaytonaHelperExecutor{client: client}
}

func NewDaytonaHelperExecutorForSandboxServices(get func(context.Context, string) (DaytonaSandboxServices, error)) *DaytonaHelperExecutor {
	return &DaytonaHelperExecutor{client: daytonaSandboxServicesGetter{get: get}}
}

func NewDaytonaHelperExecutorForClientWithCommandTimeout(client daytonaSandboxGetter, timeout time.Duration) *DaytonaHelperExecutor {
	return &DaytonaHelperExecutor{client: client, commandTimeout: timeout}
}

// PrepareTool validates and stages one deterministic helper payload. It may
// be repeated for the same Tool Use after worker loss and does not invoke the
// user-facing helper subcommand.
func (e *DaytonaHelperExecutor) PrepareTool(ctx context.Context, invocation ToolInvocation) (PreparedToolExecution, error) {
	if invocation.ToolUseEventID == "" || invocation.ToolName == "" {
		return PreparedToolExecution{}, errors.New("tool invocation is incomplete")
	}
	helperCommand, err := helperSubcommandForToolName(invocation.ToolName)
	if err != nil {
		return PreparedToolExecution{}, err
	}
	input, composition, err := helperRunToolInputForInvocation(helperCommand, invocation.ToolName, invocation.InputJSON, invocation.ToolUseEventID)
	if err != nil {
		var preHelperResult *preHelperToolResult
		if errors.As(err, &preHelperResult) {
			result := ToolExecution{ResultJSON: preHelperResult.resultJSON}
			return PreparedToolExecution{immediateResult: &result}, nil
		}
		return PreparedToolExecution{}, err
	}
	payload, err := newHelperPayload(invocation.Target, helperCommand, invocation.ToolUseEventID, input)
	if err != nil {
		return PreparedToolExecution{}, err
	}
	limits := helperLimits(helperCommand, input)
	payloadPath, process, err := e.stageHelperPayload(ctx, invocation.Target, invocation.ToolUseEventID, helperCommand, payload)
	if err != nil {
		return PreparedToolExecution{}, mapDaytonaError(sandbox.StageExecuteTool, err)
	}
	return PreparedToolExecution{
		target: invocation.Target, process: process, toolUseEventID: invocation.ToolUseEventID,
		helperCommand: helperCommand, payloadPath: payloadPath,
		pollUntilTerminal: composition.pollUntilTerminal,
		visibleBytes:      limits.VisibleBytes, visibleLines: limits.VisibleLines,
	}, nil
}

// SubmitPreparedTool invokes the user-facing helper command once and returns
// immediately with durable observation state when a foreground task remains
// running. Callers must persist that state before polling it.
func (e *DaytonaHelperExecutor) SubmitPreparedTool(ctx context.Context, prepared PreparedToolExecution) (ToolExecution, error) {
	if prepared.immediateResult != nil {
		return *prepared.immediateResult, nil
	}
	result, err := e.executePreparedHelper(ctx, prepared.process, prepared.helperCommand, prepared.payloadPath)
	if err != nil {
		return ToolExecution{}, mapDaytonaError(sandbox.StageExecuteTool, err)
	}
	if prepared.pollUntilTerminal {
		return newForegroundCommandSubmission(prepared.target, prepared.toolUseEventID, protocol.Limits{
			VisibleBytes: prepared.visibleBytes,
			VisibleLines: prepared.visibleLines,
		}, result)
	}
	var backgroundTask *BackgroundTask
	if prepared.helperCommand == helperSubcommandExec {
		backgroundTask = synthesizeHelperBackgroundTask(prepared.target, result)
	}
	return ToolExecution{ResultJSON: result.ResultJSON, BackgroundTask: backgroundTask}, nil
}

func newForegroundCommandSubmission(target ToolTarget, sourceToolUseEventID string, limits protocol.Limits, first helperResult) (ToolExecution, error) {
	task := synthesizeHelperBackgroundTask(target, first)
	if task == nil {
		return ToolExecution{ResultJSON: first.ResultJSON}, nil
	}
	accumulator := newForegroundCommandAccumulator(limits.VisibleBytes, limits.VisibleLines)
	resultJSON, err := accumulator.add(first.ResultJSON)
	if err != nil {
		return ToolExecution{}, err
	}
	observation := accumulator.observation(CommandReference{
		Target:          target,
		Task:            *task,
		ToolUseEventID:  sourceToolUseEventID,
		MaxOutputTokens: limits.VisibleBytes / 4,
	}, limits)
	return ToolExecution{ResultJSON: resultJSON, ForegroundObservation: &observation}, nil
}

// ObserveForegroundTool performs one observation call for a previously
// submitted foreground command. A running response returns updated durable
// state; a terminal response returns the complete bounded aggregate.
func (e *DaytonaHelperExecutor) ObserveForegroundTool(ctx context.Context, observation ForegroundCommandObservation) (ToolExecution, error) {
	result, err := e.ReadCommandResult(ctx, observation.Reference)
	if err != nil {
		return ToolExecution{}, err
	}
	accumulator := foregroundCommandAccumulatorFromObservation(observation)
	resultJSON, err := accumulator.add(result.ResultJSON)
	if err != nil {
		return ToolExecution{}, err
	}
	if result.TerminalStatus != "" {
		return ToolExecution{ResultJSON: resultJSON}, nil
	}
	next := accumulator.observation(observation.Reference, protocol.Limits{
		VisibleBytes: observation.Limits.VisibleBytes,
		VisibleLines: observation.Limits.VisibleLines,
	})
	return ToolExecution{ResultJSON: resultJSON, ForegroundObservation: &next}, nil
}

type foregroundCommandAccumulator struct {
	stdout *foregroundStreamAccumulator
	stderr *foregroundStreamAccumulator
}

func newForegroundCommandAccumulator(visibleBytes int, visibleLines int) *foregroundCommandAccumulator {
	return &foregroundCommandAccumulator{
		stdout: newForegroundStreamAccumulator(visibleBytes, visibleLines),
		stderr: newForegroundStreamAccumulator(visibleBytes, visibleLines),
	}
}

func foregroundCommandAccumulatorFromObservation(observation ForegroundCommandObservation) *foregroundCommandAccumulator {
	return &foregroundCommandAccumulator{
		stdout: foregroundStreamAccumulatorFromObservation(observation.Stdout, observation.Limits),
		stderr: foregroundStreamAccumulatorFromObservation(observation.Stderr, observation.Limits),
	}
}

func (a *foregroundCommandAccumulator) observation(reference CommandReference, limits protocol.Limits) ForegroundCommandObservation {
	return ForegroundCommandObservation{
		Reference: reference,
		Stdout:    a.stdout.observation(),
		Stderr:    a.stderr.observation(),
		Limits: ForegroundObservationLimits{
			VisibleBytes: limits.VisibleBytes,
			VisibleLines: limits.VisibleLines,
		},
	}
}

func (a *foregroundCommandAccumulator) add(resultJSON string) (string, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resultJSON), &envelope); err != nil {
		return resultJSON, err
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(envelope["result"], &result); err != nil {
		return resultJSON, err
	}
	for field, accumulator := range map[string]*foregroundStreamAccumulator{
		"stdout": a.stdout,
		"stderr": a.stderr,
	} {
		raw, ok := result[field]
		if !ok {
			continue
		}
		var snapshot foregroundStreamSnapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return resultJSON, fmt.Errorf("invalid %s stream snapshot: %w", field, err)
		}
		updated := accumulator.add(snapshot)
		encoded, err := json.Marshal(updated)
		if err != nil {
			return resultJSON, err
		}
		result[field] = encoded
	}
	encodedResult, err := json.Marshal(result)
	if err != nil {
		return resultJSON, err
	}
	envelope["result"] = encodedResult
	encodedEnvelope, err := json.Marshal(envelope)
	if err != nil {
		return resultJSON, err
	}
	return string(encodedEnvelope), nil
}

type foregroundStreamSnapshot struct {
	Text          string `json:"text"`
	TotalBytes    int64  `json:"total_bytes"`
	TotalLines    int64  `json:"total_lines"`
	ReturnedBytes int    `json:"returned_bytes"`
	Truncated     bool   `json:"truncated"`
}

type foregroundStreamAccumulator struct {
	head          []byte
	tail          []byte
	capturedBytes int64
	totalBytes    int64
	totalLines    int64
	truncated     bool
	visibleBytes  int
	visibleLines  int
}

func newForegroundStreamAccumulator(visibleBytes int, visibleLines int) *foregroundStreamAccumulator {
	if visibleBytes <= 0 || visibleBytes > helperVisibleBytes {
		visibleBytes = helperVisibleBytes
	}
	if visibleLines <= 0 || visibleLines > helperVisibleLines {
		visibleLines = helperVisibleLines
	}
	return &foregroundStreamAccumulator{visibleBytes: visibleBytes, visibleLines: visibleLines}
}

func foregroundStreamAccumulatorFromObservation(state ForegroundStreamObservationState, limits ForegroundObservationLimits) *foregroundStreamAccumulator {
	accumulator := newForegroundStreamAccumulator(limits.VisibleBytes, limits.VisibleLines)
	accumulator.head = append([]byte(nil), state.Head...)
	accumulator.tail = append([]byte(nil), state.Tail...)
	accumulator.capturedBytes = state.CapturedBytes
	accumulator.totalBytes = state.TotalBytes
	accumulator.totalLines = state.TotalLines
	accumulator.truncated = state.Truncated
	return accumulator
}

func (a *foregroundStreamAccumulator) observation() ForegroundStreamObservationState {
	return ForegroundStreamObservationState{
		Head:          append([]byte(nil), a.head...),
		Tail:          append([]byte(nil), a.tail...),
		CapturedBytes: a.capturedBytes,
		TotalBytes:    a.totalBytes,
		TotalLines:    a.totalLines,
		Truncated:     a.truncated,
	}
}

func (a *foregroundStreamAccumulator) add(latest foregroundStreamSnapshot) foregroundStreamSnapshot {
	captured := latest.Text
	if latest.Truncated {
		if head, tail, ok := splitForegroundTruncatedSnapshot(latest.Text, latest.ReturnedBytes); ok {
			captured = head + tail
		}
	}
	a.write([]byte(captured))
	if latest.TotalBytes > a.totalBytes {
		a.totalBytes = latest.TotalBytes
	}
	if latest.TotalLines > a.totalLines {
		a.totalLines = latest.TotalLines
	}
	a.truncated = a.truncated || latest.Truncated
	return a.snapshot()
}

func (a *foregroundStreamAccumulator) write(value []byte) {
	if len(value) == 0 {
		return
	}
	headCap := a.visibleBytes / 2
	tailCap := a.visibleBytes - headCap
	if len(a.head) < headCap {
		n := min(len(value), headCap-len(a.head))
		a.head = append(a.head, value[:n]...)
	}
	a.tail = append(a.tail, value...)
	if len(a.tail) > tailCap {
		a.tail = append([]byte(nil), a.tail[len(a.tail)-tailCap:]...)
	}
	a.capturedBytes += int64(len(value))
}

func (a *foregroundStreamAccumulator) snapshot() foregroundStreamSnapshot {
	head := foregroundPrefixBounded(a.head, a.visibleBytes/2, a.visibleLines/2)
	tail, tailOffset := foregroundSuffixBounded(a.tail, a.visibleBytes-a.visibleBytes/2, a.visibleLines-a.visibleLines/2)
	tailStart := a.capturedBytes - int64(len(a.tail)) + int64(tailOffset)
	if overlap := int64(len(head)) - tailStart; overlap > 0 {
		drop := min(len(tail), int(overlap))
		tail = tail[drop:]
	}
	returnedBytes := len(head) + len(tail)
	omitted := a.totalBytes - int64(returnedBytes)
	if omitted < 0 {
		omitted = 0
	}
	text := append([]byte(nil), head...)
	truncated := a.truncated || omitted > 0
	if truncated {
		text = append(text, []byte(foregroundTruncationMarker(omitted))...)
	}
	text = append(text, tail...)
	return foregroundStreamSnapshot{
		Text:          string(text),
		TotalBytes:    a.totalBytes,
		TotalLines:    a.totalLines,
		ReturnedBytes: returnedBytes,
		Truncated:     truncated,
	}
}

func splitForegroundTruncatedSnapshot(text string, returnedBytes int) (string, string, bool) {
	markerBytes := len(text) - returnedBytes
	if markerBytes <= 0 {
		return "", "", false
	}
	const prefix = "\n[... "
	const suffix = " bytes truncated ...]\n"
	for start := 0; start+markerBytes <= len(text); {
		relative := strings.Index(text[start:], prefix)
		if relative < 0 {
			break
		}
		start += relative
		end := start + markerBytes
		countText, hasPrefix := strings.CutPrefix(text[start:end], prefix)
		countText, hasSuffix := strings.CutSuffix(countText, suffix)
		if hasPrefix && hasSuffix {
			if count, err := strconv.ParseInt(countText, 10, 64); err == nil && count >= 0 {
				return text[:start], text[end:], true
			}
		}
		start += len(prefix)
	}
	return "", "", false
}

func foregroundTruncationMarker(omitted int64) string {
	return "\n[... " + strconv.FormatInt(omitted, 10) + " bytes truncated ...]\n"
}

func foregroundPrefixBounded(value []byte, maxBytes int, maxLines int) []byte {
	if maxBytes <= 0 || maxLines == 0 || len(value) == 0 {
		return nil
	}
	limit := min(len(value), maxBytes)
	if maxLines > 0 {
		lines := 0
		for index := 0; index < limit; index++ {
			if value[index] == '\n' {
				lines++
				if lines >= maxLines {
					limit = index + 1
					break
				}
			}
		}
	}
	for limit > 0 && !utf8.Valid(value[:limit]) {
		limit--
	}
	return value[:limit]
}

func foregroundSuffixBounded(value []byte, maxBytes int, maxLines int) ([]byte, int) {
	if maxBytes <= 0 || maxLines == 0 || len(value) == 0 {
		return nil, len(value)
	}
	start := len(value) - min(len(value), maxBytes)
	if maxLines > 0 {
		lines := 0
		index := len(value) - 1
		if index >= start && value[index] == '\n' {
			index--
		}
		for ; index >= start; index-- {
			if value[index] == '\n' {
				lines++
				if lines >= maxLines {
					start = index + 1
					break
				}
			}
		}
	}
	for start < len(value) && !utf8.Valid(value[start:]) {
		start++
	}
	return value[start:], start
}

func (e *DaytonaHelperExecutor) CheckHealth(ctx context.Context, target ToolTarget) error {
	if e == nil || e.client == nil {
		return errors.New("daytona sandbox client is unavailable")
	}
	if target.ProviderSandboxID == "" {
		return errors.New("provider sandbox id is required")
	}
	sandboxHandle, err := e.client.Get(ctx, target.ProviderSandboxID)
	if err != nil {
		return mapDaytonaError(sandbox.StageCheckBaseTemplate, err)
	}
	if sandboxHandle.Process == nil {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnavailable, true, 0, "daytona sandbox is missing process service", nil)
	}
	response, err := sandboxHandle.Process.ExecuteCommand(ctx, runtimeUserShellCommand(shellQuote(helperPath)+" health"))
	if err != nil {
		return mapDaytonaError(sandbox.StageCheckBaseTemplate, err)
	}
	if response == nil {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnknown, true, 0, "sandbox helper health returned no response", nil)
	}
	return providerErrorForHealthResponse(response.Result, response.ExitCode)
}

func (e *DaytonaHelperExecutor) ReadCommandResult(ctx context.Context, reference CommandReference) (CommandResult, error) {
	input := map[string]any{"task_id": reference.Task.TaskID}
	if reference.MaxOutputTokens > 0 {
		input["max_output_tokens"] = reference.MaxOutputTokens
	}
	return e.executeCommandHelper(ctx, reference, helperSubcommandPoll, input)
}

func (e *DaytonaHelperExecutor) SendCommandInput(ctx context.Context, input CommandInput) (CommandResult, error) {
	stdinInput, err := helperStdinInput(input.Task.TaskID, input.InputJSON)
	if err != nil {
		return CommandResult{}, err
	}
	return e.executeCommandHelper(ctx, input.CommandReference, helperSubcommandStdin, stdinInput)
}

func (e *DaytonaHelperExecutor) CancelCommand(ctx context.Context, cancel CommandCancel) (CommandResult, error) {
	input := map[string]any{"task_id": cancel.Task.TaskID}
	if cancel.Reason != "" {
		input["reason"] = cancel.Reason
	}
	return e.executeCommandHelper(ctx, cancel.CommandReference, helperSubcommandCancel, input)
}

func (e *DaytonaHelperExecutor) RunDaytonaCommand(ctx context.Context, target DaytonaCommandTarget, command string, env map[string]string, timeout time.Duration) error {
	if e == nil || e.client == nil {
		return errors.New("daytona sandbox client is unavailable")
	}
	if target.ProviderSandboxID == "" {
		return errors.New("provider sandbox id is required")
	}
	if strings.TrimSpace(command) == "" {
		return errors.New("daytona command is required")
	}
	if timeout <= 0 {
		return errors.New("daytona command timeout is required")
	}
	if e.commandTimeout <= 0 || timeout != e.commandTimeout {
		return errors.New("daytona command timeout does not match configured server-side timeout")
	}
	sandboxHandle, err := e.client.Get(ctx, target.ProviderSandboxID)
	if err != nil {
		return mapDaytonaError(sandbox.StageMountResources, err)
	}
	if sandboxHandle.Process == nil {
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorUnavailable, true, 0, "daytona sandbox is missing process service", nil)
	}
	opts := []func(*options.ExecuteCommand){
		options.WithExecuteTimeout(e.commandTimeout),
	}
	if len(env) > 0 {
		opts = append(opts, options.WithCommandEnv(cloneCommandEnv(env)))
	}
	response, err := sandboxHandle.Process.ExecuteCommand(ctx, command, opts...)
	if err != nil {
		return mapDaytonaError(sandbox.StageMountResources, err)
	}
	if response == nil {
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorUnknown, true, 0, "daytona daytona command returned no response", nil)
	}
	if response.ExitCode != 0 {
		// Command output is deliberately NOT surfaced: tool errors (rclone
		// especially) can embed signed URLs or other capability material, and
		// the no-leak contract is pinned by test. Callers label the error
		// with the engine-authored command name instead.
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorUnknown, true, 0, "daytona daytona command failed", nil)
	}
	return nil
}

func (e *DaytonaHelperExecutor) StageDaytonaFile(ctx context.Context, target DaytonaCommandTarget, remotePath string, content io.Reader) error {
	if e == nil || e.client == nil {
		return errors.New("daytona sandbox client is unavailable")
	}
	if target.ProviderSandboxID == "" {
		return errors.New("provider sandbox id is required")
	}
	if !strings.HasPrefix(remotePath, "/tmp/tetral-runtime/") || path.Clean(remotePath) != remotePath {
		return errors.New("preparation file path must be under /tmp/tetral-runtime")
	}
	if content == nil {
		return errors.New("preparation file content is required")
	}
	sandboxHandle, err := e.client.Get(ctx, target.ProviderSandboxID)
	if err != nil {
		return mapDaytonaError(sandbox.StageMountResources, err)
	}
	if sandboxHandle.FileSystem == nil {
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorUnavailable, true, 0, "daytona sandbox is missing filesystem service", nil)
	}
	if err := sandboxHandle.FileSystem.CreateFolder(ctx, path.Dir(remotePath), options.WithMode("0700")); err != nil {
		return mapDaytonaError(sandbox.StageMountResources, err)
	}
	if err := sandboxHandle.FileSystem.UploadFileStream(ctx, content, remotePath); err != nil {
		return mapDaytonaError(sandbox.StageMountResources, err)
	}
	return nil
}

func cloneCommandEnv(env map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}
	out := make(map[string]string, len(env))
	for key, value := range env {
		out[key] = value
	}
	return out
}

func (e *DaytonaHelperExecutor) executeCommandHelper(ctx context.Context, reference CommandReference, helperCommand string, input map[string]any) (CommandResult, error) {
	if reference.Task.TaskID == "" {
		return CommandResult{}, errors.New("command task id is required")
	}
	toolUseEventID := reference.ToolUseEventID
	if toolUseEventID == "" {
		toolUseEventID = reference.Task.SourceToolUseEventID
	}
	if toolUseEventID == "" {
		toolUseEventID = reference.Task.TaskID
	}
	payload, err := newHelperPayload(reference.Target, helperCommand, toolUseEventID, input)
	if err != nil {
		return CommandResult{}, err
	}
	result, err := e.executeHelper(ctx, reference.Target, toolUseEventID, helperCommand, payload)
	if err != nil {
		return CommandResult{}, err
	}
	return CommandResult{ResultJSON: result.ResultJSON, TerminalStatus: terminalStatusFromResult(result.ResultJSON)}, nil
}

func (e *DaytonaHelperExecutor) executeHelper(ctx context.Context, target ToolTarget, payloadID string, helperCommand string, payload map[string]any) (helperResult, error) {
	payloadPath, process, err := e.stageHelperPayload(ctx, target, payloadID, helperCommand, payload)
	if err != nil {
		return helperResult{}, err
	}
	return e.executePreparedHelper(ctx, process, helperCommand, payloadPath)
}

func (e *DaytonaHelperExecutor) stageHelperPayload(ctx context.Context, target ToolTarget, payloadID string, helperCommand string, payload map[string]any) (string, daytonaProcess, error) {
	if e == nil || e.client == nil {
		return "", nil, daytonaProviderError(sandbox.StageExecuteTool, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona sandbox client is unavailable", nil)
	}
	if target.ProviderSandboxID == "" {
		return "", nil, daytonaProviderError(sandbox.StageExecuteTool, sandbox.ProviderErrorInvalidRequest, false, 0, "provider sandbox id is required", nil)
	}
	if !helperCommandNamePattern.MatchString(helperCommand) {
		return "", nil, daytonaProviderError(sandbox.StageExecuteTool, sandbox.ProviderErrorInvalidRequest, false, 0, "helper command has invalid shape", nil)
	}
	if !helperPayloadIDPattern.MatchString(payloadID) {
		return "", nil, daytonaProviderError(sandbox.StageExecuteTool, sandbox.ProviderErrorInvalidRequest, false, 0, "helper payload id has invalid shape", nil)
	}
	providerSandbox, err := retryDaytonaTransient(ctx, func() (daytonaSandboxHandle, error) {
		return e.client.Get(ctx, target.ProviderSandboxID)
	}, nil)
	if err != nil {
		return "", nil, MarkProviderOperationNotSubmitted(mapDaytonaError(sandbox.StageExecuteTool, err))
	}
	if providerSandbox.Process == nil || providerSandbox.FileSystem == nil {
		return "", nil, daytonaProviderError(sandbox.StageExecuteTool, sandbox.ProviderErrorMalformedResponse, false, 0, "daytona sandbox is missing process or filesystem service", nil)
	}
	stageDir := path.Join(payloadStageRootPath, payloadID)
	stagePath := path.Join(stageDir, payloadFileName)
	payloadDir := path.Join(payloadRootPath, payloadID)
	payloadPath := path.Join(payloadDir, payloadFileName)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", nil, daytonaProviderError(sandbox.StageExecuteTool, sandbox.ProviderErrorInvalidRequest, false, 0, "sandbox helper payload is invalid", err)
	}
	if err := retryDaytonaTransientError(ctx, func() error {
		return providerSandbox.FileSystem.CreateFolder(ctx, stageDir, options.WithMode("0700"))
	}); err != nil {
		return "", nil, err
	}
	if err := retryDaytonaTransientError(ctx, func() error {
		return providerSandbox.FileSystem.UploadFileStream(ctx, bytes.NewReader(encoded), stagePath)
	}); err != nil {
		_ = providerSandbox.FileSystem.DeleteFile(ctx, stageDir, true)
		return "", nil, err
	}
	// The freeze runs the privileged chain under one sudo sh -c so no
	// intermediate state is observable: the final root is root-owned before
	// the payload directory arrives, leftovers are cleared first, and
	// ownership/mode land before the helper reads. There is deliberately no
	// post-execution cleanup command: the helper unlinks the payload file
	// itself, the runtime-user filesystem API cannot delete below the
	// root-owned final root anyway, and the next call's sweep (rmdir of
	// empty leftover directories plus rm -rf of this event id) reclaims what
	// remains, so a session leaves at most one empty directory behind.
	freezeScript := "install -d -m 0700 -o " + shellQuote(helperUser) + " -g " + shellQuote(helperUser) + " -- " + shellQuote(payloadRootPath) +
		" && for leftover in " + shellQuote(payloadRootPath) + "/*/; do rmdir -- \"$leftover\" 2>/dev/null || true; done" +
		" && rm -rf -- " + shellQuote(payloadDir) +
		" && mv -T -- " + shellQuote(stageDir) + " " + shellQuote(payloadDir) +
		" && chown -R " + shellQuote(helperUser) + ":" + shellQuote(helperUser) + " -- " + shellQuote(payloadDir) +
		" && chmod 0700 -- " + shellQuote(payloadDir) +
		" && chmod 0600 -- " + shellQuote(payloadPath)
	permissionResponse, err := retryDaytonaTransient(ctx, func() (*types.ExecuteResponse, error) {
		return providerSandbox.Process.ExecuteCommand(ctx, "sudo -n sh -c "+shellQuote(freezeScript))
	}, nil)
	if err != nil {
		_ = providerSandbox.FileSystem.DeleteFile(ctx, stageDir, true)
		return "", nil, err
	}
	if permissionResponse == nil {
		_ = providerSandbox.FileSystem.DeleteFile(ctx, stageDir, true)
		return "", nil, daytonaProviderError(sandbox.StageExecuteTool, sandbox.ProviderErrorMalformedResponse, false, 0, "sandbox helper payload permission command returned no response", nil)
	}
	if permissionResponse.ExitCode != 0 {
		_ = providerSandbox.FileSystem.DeleteFile(ctx, stageDir, true)
		return "", nil, daytonaProviderError(sandbox.StageExecuteTool, sandbox.ProviderErrorUnknown, true, 0, "sandbox helper payload permission command failed", nil)
	}
	return payloadPath, providerSandbox.Process, nil
}

func (e *DaytonaHelperExecutor) executePreparedHelper(ctx context.Context, process daytonaProcess, helperCommand string, payloadPath string) (helperResult, error) {
	if e == nil || process == nil {
		return helperResult{}, errors.New("daytona sandbox is missing process service")
	}
	response, err := process.ExecuteCommand(ctx, "sudo -n -u "+shellQuote(helperUser)+" "+shellQuote(helperPath)+" "+shellQuote(helperCommand)+" --payload "+shellQuote(payloadPath))
	if err != nil {
		return helperResult{}, err
	}
	if response == nil {
		return helperResult{}, errors.New("sandbox helper returned no response")
	}
	if response.ExitCode != 0 {
		return helperResult{}, newHelperFailureError(fmt.Sprintf("sandbox helper exited with code %d", response.ExitCode), nil)
	}
	result, err := parseHelperResult(helperCommand, response.Result)
	if err != nil {
		return helperResult{}, newHelperFailureError("sandbox helper emitted an invalid envelope", err)
	}
	return result, nil
}

type helperResult struct {
	ResultJSON string
}

func parseHelperResult(expectedTool string, stdout string) (helperResult, error) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return helperResult{}, errors.New("sandbox helper produced empty stdout")
	}
	if !json.Valid([]byte(trimmed)) {
		return helperResult{}, errors.New("sandbox helper stdout is not JSON")
	}
	var topLevel map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &topLevel); err != nil {
		return helperResult{}, err
	}
	for _, forbidden := range []string{"error_kind", "result_json", "terminal_status", "background_task"} {
		if _, ok := topLevel[forbidden]; ok {
			return helperResult{}, fmt.Errorf("sandbox helper envelope contains forbidden field %q", forbidden)
		}
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal([]byte(trimmed), &envelope); err != nil {
		return helperResult{}, err
	}
	if envelope.SchemaVersion != protocol.SchemaVersion {
		return helperResult{}, errors.New("sandbox helper envelope has unsupported schema_version")
	}
	if envelope.Tool == "" {
		return helperResult{}, errors.New("sandbox helper envelope is missing tool")
	}
	if expectedTool != "" && envelope.Tool != expectedTool {
		return helperResult{}, fmt.Errorf("sandbox helper envelope tool %q does not match invoked tool %q", envelope.Tool, expectedTool)
	}
	switch envelope.Status {
	case protocol.ToolStatusSuccess, protocol.ToolStatusError, protocol.ToolStatusRunning:
	default:
		return helperResult{}, errors.New("sandbox helper envelope has invalid status")
	}
	if envelope.Truncated == nil {
		return helperResult{}, errors.New("sandbox helper envelope is missing truncated")
	}
	if envelope.Status == protocol.ToolStatusError && envelope.Error == nil {
		return helperResult{}, errors.New("sandbox helper error envelope is missing error")
	}
	if envelope.Status != protocol.ToolStatusError && envelope.Error != nil {
		return helperResult{}, errors.New("sandbox helper non-error envelope includes error")
	}
	if len(envelope.ResultBytes()) == 0 {
		return helperResult{}, errors.New("sandbox helper envelope is missing result")
	}
	return helperResult{
		ResultJSON: stripProviderMetadataFromResult(trimmed),
	}, nil
}

func synthesizeHelperBackgroundTask(target ToolTarget, result helperResult) *BackgroundTask {
	taskID := runningTaskIDFromResult(result.ResultJSON)
	if taskID == "" || target.ProviderSandboxID == "" {
		return nil
	}
	return &BackgroundTask{
		TaskID:                      taskID,
		ProviderSessionID:           target.ProviderSandboxID,
		ProviderCommandID:           taskID,
		ProviderCommandMetadataJSON: `{}`,
	}
}

func runningTaskIDFromResult(resultJSON string) string {
	var envelope struct {
		Status string `json:"status"`
		Result struct {
			TaskID string `json:"task_id"`
		} `json:"result"`
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(resultJSON), &envelope); err != nil {
		return ""
	}
	if envelope.Status != protocol.ToolStatusRunning {
		return ""
	}
	if envelope.Result.TaskID != "" {
		return envelope.Result.TaskID
	}
	return envelope.TaskID
}

func stripProviderMetadataFromResult(raw string) string {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return raw
	}
	encoded, err := json.Marshal(stripProviderMetadataValue(value))
	if err != nil {
		return raw
	}
	return string(encoded)
}

func stripProviderMetadataValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range []string{
			"background_task",
			"engine_sandbox_id",
			"provider_sandbox_id",
			"provider_session_id",
			"provider_command_id",
			"provider_command_metadata",
			"provider_command_metadata_json",
			"provider_metadata",
			"provider_metadata_json",
		} {
			delete(typed, key)
		}
		for key, child := range typed {
			typed[key] = stripProviderMetadataValue(child)
		}
		return typed
	case []any:
		for index, child := range typed {
			typed[index] = stripProviderMetadataValue(child)
		}
		return typed
	default:
		return value
	}
}

// terminalStatusFromResult reads a helper result envelope and returns the
// settled task status, or "" when the task has not settled. It parses status
// and the result object's exit_code, signal, timed_out, and cancelled fields
// and resolves them in fixed precedence:
//
//	status = running                                 ""          (still running)
//	result.cancelled = true                          "cancelled"
//	result.timed_out = true                          "expired"
//	result.exit_code = 0                            "completed"
//	result.exit_code != 0, result.signal set, or task_lost "failed"
//	otherwise, including unparseable JSON            ""
//
// cancelled outranks timed_out, which outranks the exit_code/signal split, so a
// task ended by cancel or timeout never reports as completed or failed.
func terminalStatusFromResult(resultJSON string) string {
	var payload struct {
		Status string `json:"status"`
		Error  *struct {
			Kind string `json:"kind"`
		} `json:"error"`
		Result struct {
			ExitCode  *int    `json:"exit_code"`
			Signal    *string `json:"signal"`
			TimedOut  bool    `json:"timed_out"`
			Cancelled bool    `json:"cancelled"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(resultJSON), &payload); err != nil {
		return ""
	}
	switch {
	case payload.Status == protocol.ToolStatusRunning:
		return ""
	case payload.Result.Cancelled:
		return "cancelled"
	case payload.Result.TimedOut:
		return "expired"
	case payload.Result.ExitCode != nil && *payload.Result.ExitCode == 0:
		return "completed"
	case payload.Result.ExitCode != nil || payload.Result.Signal != nil || payload.Error != nil && payload.Error.Kind == protocol.ErrorKindTaskLost:
		return "failed"
	default:
		return ""
	}
}

func safePayloadID(value string) string {
	if value == "" {
		return "unknown"
	}
	var builder strings.Builder
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' || character == '-' || character == '.' {
			builder.WriteRune(character)
		} else {
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func runtimeUserShellCommand(script string) string {
	return "sudo -H -n -u " + shellQuote(RuntimeUser) + " sh -lc " + shellQuote(script)
}
