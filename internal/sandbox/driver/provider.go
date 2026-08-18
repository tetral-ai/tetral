package driver

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"math"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const DaytonaProviderName = "daytona"

var ErrSandboxOwnershipMismatch = stderrors.New("sandbox provider ownership mismatch")
var daytonaDiskCapacityMessagePattern = regexp.MustCompile(`^Total disk limit exceeded\. Maximum allowed: ([1-9][0-9]*)(KiB|MiB|GiB|TiB)\.$`)

type DaytonaLifecycleProvider struct {
	client         daytonaLifecycleClient
	lifecycle      daytonaLifecyclePolicy
	commandTimeout time.Duration
	deleteSandbox  func(context.Context, *daytona.Sandbox, time.Duration) error
}

type daytonaLifecyclePolicy struct {
	stopTimeout        time.Duration
	autoStopMinutes    *int
	autoArchiveMinutes *int
	autoDeleteMinutes  *int
}

type daytonaLifecycleClient interface {
	Create(context.Context, any, ...func(*options.CreateSandbox)) (*daytona.Sandbox, error)
	Get(context.Context, string) (*daytona.Sandbox, error)
}

// NewDaytonaLifecycleProviderForSDKClient binds lifecycle operations to the
// process-wide Daytona client owned by the Sandbox Service adapter.
func NewDaytonaLifecycleProviderForSDKClient(client *daytona.Client, cfg Config) (*DaytonaLifecycleProvider, error) {
	if client == nil {
		return nil, stderrors.New("daytona client is required")
	}
	return newDaytonaLifecycleProvider(client, cfg.Lifecycle, cfg.CommandTimeout)
}

func NewDaytonaLifecycleProviderForClient(client daytonaLifecycleClient, commandTimeout time.Duration) *DaytonaLifecycleProvider {
	provider, _ := newDaytonaLifecycleProvider(client, LifecyclePolicy{}, commandTimeout)
	return provider
}

func newDaytonaLifecycleProvider(client daytonaLifecycleClient, policy LifecyclePolicy, commandTimeout time.Duration) (*DaytonaLifecycleProvider, error) {
	if commandTimeout <= 0 {
		return nil, stderrors.New("daytona command timeout is required")
	}
	resolved, err := resolveDaytonaLifecyclePolicy(policy)
	if err != nil {
		return nil, err
	}
	return &DaytonaLifecycleProvider{
		client:         client,
		lifecycle:      resolved,
		commandTimeout: commandTimeout,
		deleteSandbox: func(ctx context.Context, sandbox *daytona.Sandbox, timeout time.Duration) error {
			return sandbox.DeleteWithTimeout(ctx, timeout)
		},
	}, nil
}

func (p *DaytonaLifecycleProvider) CreateSandbox(ctx context.Context, request sandbox.CreateSandboxRequest) (sandbox.ProviderHandle, error) {
	if p == nil || p.client == nil {
		return sandbox.ProviderHandle{}, MarkProviderOperationNotSubmitted(daytonaProviderError(sandbox.StageCreateSandbox, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona lifecycle provider is unavailable", nil))
	}
	if request.Setup.SandboxID == "" {
		return sandbox.ProviderHandle{}, MarkProviderOperationNotSubmitted(daytonaProviderError(sandbox.StageCreateSandbox, sandbox.ProviderErrorInvalidRequest, false, 0, "sandbox id is required", nil))
	}
	if request.Setup.ProviderArtifactRef == "" {
		return sandbox.ProviderHandle{}, MarkProviderOperationNotSubmitted(daytonaProviderError(sandbox.StageCreateSandbox, sandbox.ProviderErrorInvalidRequest, false, 0, "provider_artifact_ref is required", nil))
	}
	networkBlockAll, networkAllowList, err := daytonaNetworkPolicy(request.Setup.Network)
	if err != nil {
		return sandbox.ProviderHandle{}, MarkProviderOperationNotSubmitted(err)
	}
	created, err := p.client.Create(ctx, types.SnapshotParams{
		SandboxBaseParams: types.SandboxBaseParams{
			Name:                request.Setup.SandboxID,
			User:                RuntimeUser,
			Labels:              daytonaLabels(request),
			NetworkBlockAll:     networkBlockAll,
			NetworkAllowList:    networkAllowList,
			AutoStopInterval:    p.lifecycle.autoStopMinutes,
			AutoArchiveInterval: p.lifecycle.autoArchiveMinutes,
			AutoDeleteInterval:  p.lifecycle.autoDeleteMinutes,
		},
		Snapshot: request.Setup.ProviderArtifactRef,
	}, options.WithWaitForStart(true))
	if err != nil {
		mapped := mapDaytonaError(sandbox.StageCreateSandbox, err)
		if daytonaRequestWasRejected(err) {
			mapped = MarkProviderOperationNotSubmitted(mapped)
		}
		return sandbox.ProviderHandle{}, mapped
	}
	if created == nil || created.ID == "" {
		return sandbox.ProviderHandle{}, daytonaProviderError(sandbox.StageCreateSandbox, sandbox.ProviderErrorUnknown, true, 0, "daytona create returned no sandbox", nil)
	}
	return sandbox.ProviderHandle{
		Provider:  DaytonaProviderName,
		SandboxID: created.ID,
		Metadata: map[string]string{
			"daytona_state": string(created.State),
		},
	}, nil
}

// ResolveSandbox returns the exact provider resource for a stable Tetral name
// only when every Tetral ownership label matches and the only additional label
// is Daytona's SDK-owned default-language label. It is the crash-recovery probe
// that prevents an uncertain Create response from authorizing another Create.
func (p *DaytonaLifecycleProvider) ResolveSandbox(ctx context.Context, name string, labels map[string]string) (sandbox.ProviderHandle, bool, error) {
	if p == nil || p.client == nil {
		return sandbox.ProviderHandle{}, false, daytonaProviderError(sandbox.StageStatus, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona lifecycle provider is unavailable", nil)
	}
	if name == "" || len(labels) == 0 {
		return sandbox.ProviderHandle{}, false, daytonaProviderError(sandbox.StageStatus, sandbox.ProviderErrorInvalidRequest, false, 0, "sandbox resolution identity is required", nil)
	}
	got, err := p.client.Get(ctx, name)
	if err != nil {
		mapped := mapDaytonaError(sandbox.StageStatus, err)
		var providerErr *sandbox.ProviderError
		if stderrors.As(mapped, &providerErr) && providerErr.Kind == sandbox.ProviderErrorNotFound {
			return sandbox.ProviderHandle{}, false, nil
		}
		return sandbox.ProviderHandle{}, false, mapped
	}
	if got == nil || got.ID == "" {
		return sandbox.ProviderHandle{}, false, daytonaProviderError(sandbox.StageStatus, sandbox.ProviderErrorUnknown, false, 0, "daytona returned an invalid sandbox identity", nil)
	}
	if got.Name != name {
		return sandbox.ProviderHandle{}, false, fmt.Errorf("%w: %w", ErrSandboxOwnershipMismatch,
			daytonaProviderError(sandbox.StageStatus, sandbox.ProviderErrorUnknown, false, 0, "daytona sandbox stable name does not match", nil))
	}
	if !daytonaOwnershipLabelsMatch(got.Labels, labels) {
		return sandbox.ProviderHandle{}, false, fmt.Errorf("%w: %w", ErrSandboxOwnershipMismatch,
			daytonaProviderError(sandbox.StageStatus, sandbox.ProviderErrorUnknown, false, 0, "daytona sandbox ownership labels do not match", nil))
	}
	return sandbox.ProviderHandle{
		Provider: DaytonaProviderName, SandboxID: got.ID,
		Metadata: map[string]string{"daytona_state": string(got.State)},
	}, true, nil
}

func daytonaOwnershipLabelsMatch(got map[string]string, ownership map[string]string) bool {
	if len(got) != len(ownership) && len(got) != len(ownership)+1 {
		return false
	}
	for key, value := range ownership {
		if got[key] != value {
			return false
		}
	}
	for key, value := range got {
		if _, owned := ownership[key]; owned {
			continue
		}
		if key != types.CodeToolboxLanguageLabel || value != string(types.CodeLanguagePython) {
			return false
		}
	}
	return true
}

func (p *DaytonaLifecycleProvider) StartSandbox(ctx context.Context, handle sandbox.ProviderHandle) error {
	if p == nil || p.client == nil {
		return MarkProviderOperationNotSubmitted(daytonaProviderError(sandbox.StageStartSandbox, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona lifecycle provider is unavailable", nil))
	}
	if handle.SandboxID == "" {
		return MarkProviderOperationNotSubmitted(daytonaProviderError(sandbox.StageStartSandbox, sandbox.ProviderErrorInvalidRequest, false, 0, "provider sandbox id is required", nil))
	}
	got, err := p.client.Get(ctx, handle.SandboxID)
	if err != nil {
		return MarkProviderOperationNotSubmitted(mapDaytonaError(sandbox.StageStartSandbox, err))
	}
	if got == nil {
		return MarkProviderOperationNotSubmitted(daytonaProviderError(sandbox.StageStartSandbox, sandbox.ProviderErrorNotFound, false, http.StatusNotFound, "daytona sandbox not found", nil))
	}
	if err := got.Start(ctx); err != nil {
		return mapDaytonaError(sandbox.StageStartSandbox, err)
	}
	return nil
}

func (p *DaytonaLifecycleProvider) CheckBaseTemplateHealth(ctx context.Context, handle sandbox.ProviderHandle) error {
	if p == nil || p.client == nil {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona lifecycle provider is unavailable", nil)
	}
	if handle.SandboxID == "" {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorInvalidRequest, false, 0, "provider sandbox id is required", nil)
	}
	got, err := p.client.Get(ctx, handle.SandboxID)
	if err != nil {
		return mapDaytonaError(sandbox.StageCheckBaseTemplate, err)
	}
	if got == nil || got.Process == nil {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnavailable, true, 0, "daytona sandbox is missing process service", nil)
	}
	response, err := got.Process.ExecuteCommand(ctx, runtimeUserShellCommand(shellQuote(helperPath)+" health"), options.WithExecuteTimeout(p.commandTimeout))
	if err != nil {
		return mapDaytonaError(sandbox.StageCheckBaseTemplate, err)
	}
	if response == nil {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnknown, true, 0, "sandbox helper health returned no response", nil)
	}
	return providerErrorForHealthResponse(response.Result, response.ExitCode)
}

func providerErrorForHealthResponse(stdout string, exitCode int) error {
	if exitCode != 0 {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnknown, true, 0, "sandbox helper health exited before emitting an authoritative envelope", nil)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &envelope); err != nil {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnknown, true, 0, "sandbox helper health returned an invalid envelope", err)
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "health" {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnknown, true, 0, "sandbox helper health returned a non-authoritative envelope", nil)
	}
	if envelope.Status == protocol.ToolStatusError || envelope.Error != nil {
		cause := stderrors.New("sandbox helper health failed")
		if result := strings.TrimSpace(string(envelope.ResultBytes())); result != "" {
			cause = stderrors.New(result)
		}
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnavailable, false, 0, "sandbox base template health check failed", cause)
	}
	if envelope.Status != protocol.ToolStatusSuccess {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnknown, true, 0, "sandbox helper health returned an invalid status", nil)
	}
	if len(envelope.ResultBytes()) == 0 {
		return daytonaProviderError(sandbox.StageCheckBaseTemplate, sandbox.ProviderErrorUnknown, true, 0, "sandbox helper health envelope is missing result", nil)
	}
	return nil
}

func (p *DaytonaLifecycleProvider) PrepareBaseDirectories(ctx context.Context, handle sandbox.ProviderHandle) error {
	if p == nil || p.client == nil {
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona lifecycle provider is unavailable", nil)
	}
	if handle.SandboxID == "" {
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorInvalidRequest, false, 0, "provider sandbox id is required", nil)
	}
	got, err := p.client.Get(ctx, handle.SandboxID)
	if err != nil {
		return mapDaytonaError(sandbox.StageMountResources, err)
	}
	if got == nil || got.Process == nil {
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorUnavailable, true, 0, "daytona sandbox is missing process service", nil)
	}
	response, err := got.Process.ExecuteCommand(ctx, canonicalSandboxBaseDirectoryCommand(), options.WithExecuteTimeout(p.commandTimeout))
	if err != nil {
		return mapDaytonaError(sandbox.StageMountResources, err)
	}
	if response == nil {
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorUnknown, true, 0, "sandbox base directory preparation returned no response", nil)
	}
	if response.ExitCode != 0 {
		return daytonaProviderError(sandbox.StageMountResources, sandbox.ProviderErrorUnknown, true, 0, "sandbox base directory preparation failed", nil)
	}
	return nil
}

func canonicalSandboxBaseDirectoryCommand() string {
	writableRoots := []string{
		"/workspace",
		helperSessionUploadsRoot,
		helperSessionOutputsRoot,
	}
	readOnlyProjectionRoots := []string{
		helperMemoryRoot,
		helperSkillsRoot,
	}
	runtimeRoots := []string{
		"/tmp/tetral-runtime",
		"/tmp/tetral-runtime/rclone-cache",
	}
	// Every install runs under sudo: Daytona executes commands as the sandbox
	// user (RuntimeUser, non-root), which cannot create directories under
	// root-owned /mnt or chown anything to root. The sandbox image guarantees
	// the runtime user passwordless sudo; the projection mount and teardown
	// scripts rely on the same contract.
	parts := make([]string, 0, len(writableRoots)+len(readOnlyProjectionRoots)+len(runtimeRoots))
	for _, root := range writableRoots {
		parts = append(parts, "sudo install -d -m 0755 -o "+shellQuote(RuntimeUser)+" -g "+shellQuote(RuntimeUser)+" "+shellQuote(root))
	}
	for _, root := range readOnlyProjectionRoots {
		parts = append(parts, "sudo install -d -m 0755 -o root -g root "+shellQuote(root))
	}
	for _, root := range runtimeRoots {
		parts = append(parts, "sudo install -d -m 0700 -o "+shellQuote(RuntimeUser)+" -g "+shellQuote(RuntimeUser)+" "+shellQuote(root))
	}
	return strings.Join(parts, " && ")
}

func (p *DaytonaLifecycleProvider) InspectState(ctx context.Context, providerResourceID string) (string, error) {
	if p == nil || p.client == nil {
		return "", daytonaProviderError(sandbox.StageStatus, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona lifecycle provider is unavailable", nil)
	}
	got, err := p.client.Get(ctx, providerResourceID)
	if err != nil {
		return "", mapDaytonaError(sandbox.StageStatus, err)
	}
	if got == nil || got.ID == "" || got.ID != providerResourceID {
		return "", daytonaProviderError(sandbox.StageStatus, sandbox.ProviderErrorMalformedResponse, false, 0, "daytona status response identity is invalid", nil)
	}
	return string(got.State), nil
}

func (p *DaytonaLifecycleProvider) ReleaseSandbox(ctx context.Context, handle sandbox.ProviderHandle) error {
	if p == nil || p.client == nil {
		return daytonaProviderError(sandbox.StageReleaseSandbox, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona lifecycle provider is unavailable", nil)
	}
	got, err := p.client.Get(ctx, handle.SandboxID)
	if err != nil {
		return MarkProviderOperationNotSubmitted(mapDaytonaError(sandbox.StageReleaseSandbox, err))
	}
	if got == nil {
		return daytonaProviderError(sandbox.StageReleaseSandbox, sandbox.ProviderErrorNotFound, false, http.StatusNotFound, "daytona sandbox not found", nil)
	}
	if err := p.deleteSandbox(ctx, got, p.lifecycle.stopTimeout); err != nil {
		mapped := mapDaytonaError(sandbox.StageReleaseSandbox, err)
		if daytonaRequestWasRejected(err) {
			mapped = MarkProviderOperationNotSubmitted(mapped)
		}
		return mapped
	}
	return nil
}

func resolveDaytonaLifecyclePolicy(policy LifecyclePolicy) (daytonaLifecyclePolicy, error) {
	autoStop, err := daytonaIntervalMinutes(policy.AutoStopInterval, "auto-stop")
	if err != nil {
		return daytonaLifecyclePolicy{}, err
	}
	autoArchive, err := daytonaIntervalMinutes(policy.AutoArchiveInterval, "auto-archive")
	if err != nil {
		return daytonaLifecyclePolicy{}, err
	}
	autoDelete, err := daytonaIntervalMinutes(policy.AutoDeleteInterval, "auto-delete")
	if err != nil {
		return daytonaLifecyclePolicy{}, err
	}
	return daytonaLifecyclePolicy{
		stopTimeout:        policy.StopTimeout,
		autoStopMinutes:    autoStop,
		autoArchiveMinutes: autoArchive,
		autoDeleteMinutes:  autoDelete,
	}, nil
}

func daytonaIntervalMinutes(interval time.Duration, name string) (*int, error) {
	if interval <= 0 {
		return nil, nil
	}
	minutes := interval / time.Minute
	if interval%time.Minute != 0 {
		minutes++
	}
	if minutes > time.Duration(math.MaxInt32) {
		return nil, fmt.Errorf("daytona %s interval exceeds provider bounds", name)
	}
	value := int(minutes)
	return &value, nil
}

func daytonaLabels(request sandbox.CreateSandboxRequest) map[string]string {
	return map[string]string{
		"tetral.workspace_id":    string(request.Setup.WorkspaceID),
		"tetral.session_id":      request.Setup.SessionID,
		"tetral.sandbox_id":      request.Setup.SandboxID,
		"tetral.environment_id":  request.Setup.EnvironmentID,
		"tetral.lifecycle_owner": "sandbox",
	}
}

func daytonaNetworkPolicy(network sandbox.NetworkSetup) (bool, *string, error) {
	switch network.Type {
	case "", "unrestricted":
		return false, nil, nil
	case "blocked":
		return true, nil, nil
	case "cidr_allow_list":
		if network.NetworkAllowList == "" {
			return false, nil, daytonaProviderError(sandbox.StageCreateSandbox, sandbox.ProviderErrorInvalidRequest, false, 0, "network_allow_list is required for cidr_allow_list networking", nil)
		}
		allowList, err := normalizeDaytonaCIDRAllowList(network.NetworkAllowList)
		if err != nil {
			return false, nil, err
		}
		return false, &allowList, nil
	default:
		return false, nil, daytonaProviderError(sandbox.StageCreateSandbox, sandbox.ProviderErrorInvalidRequest, false, 0, "unsupported network policy type", nil)
	}
}

func normalizeDaytonaCIDRAllowList(raw string) (string, error) {
	parts := strings.Split(raw, ",")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			return "", daytonaProviderError(sandbox.StageCreateSandbox, sandbox.ProviderErrorInvalidRequest, false, 0, "network_allow_list must be a comma-separated CIDR list", nil)
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return "", daytonaProviderError(sandbox.StageCreateSandbox, sandbox.ProviderErrorInvalidRequest, false, 0, "network_allow_list entries must be CIDR prefixes", err)
		}
		normalized = append(normalized, prefix.String())
	}
	return strings.Join(normalized, ","), nil
}

func mapDaytonaError(stage sandbox.ProviderStage, err error) error {
	if err == nil {
		return nil
	}
	var providerErr *sandbox.ProviderError
	if stderrors.As(err, &providerErr) {
		return err
	}
	var notFound *daytonaerrors.DaytonaNotFoundError
	if stderrors.As(err, &notFound) {
		return daytonaProviderError(stage, sandbox.ProviderErrorNotFound, false, http.StatusNotFound, "daytona sandbox not found", err)
	}
	var auth *daytonaerrors.DaytonaAuthenticationError
	if stderrors.As(err, &auth) {
		return daytonaProviderError(stage, sandbox.ProviderErrorAuthFailed, false, http.StatusUnauthorized, "daytona authentication failed", err)
	}
	var forbidden *daytonaerrors.DaytonaForbiddenError
	if stderrors.As(err, &forbidden) {
		return daytonaProviderError(stage, sandbox.ProviderErrorAuthFailed, false, http.StatusForbidden, "daytona authorization failed", err)
	}
	var validation *daytonaerrors.DaytonaValidationError
	if stderrors.As(err, &validation) {
		if stage == sandbox.StageCreateSandbox && daytonaCreateDiskCapacityExceeded(validation) {
			return daytonaProviderError(stage, sandbox.ProviderErrorQuotaExceeded, true, http.StatusBadRequest, "sandbox provider capacity is unavailable", err)
		}
		return daytonaProviderError(stage, sandbox.ProviderErrorInvalidRequest, false, http.StatusBadRequest, "daytona rejected sandbox request", err)
	}
	var conflict *daytonaerrors.DaytonaConflictError
	if stderrors.As(err, &conflict) {
		return daytonaProviderError(stage, sandbox.ProviderErrorConflict, true, http.StatusConflict, "daytona sandbox conflict", err)
	}
	var rateLimited *daytonaerrors.DaytonaRateLimitError
	if stderrors.As(err, &rateLimited) {
		return daytonaProviderError(stage, sandbox.ProviderErrorUnavailable, true, http.StatusTooManyRequests, "daytona rate limited sandbox request", err)
	}
	var timeout *daytonaerrors.DaytonaTimeoutError
	if stderrors.As(err, &timeout) {
		return daytonaProviderError(stage, sandbox.ProviderErrorTimeout, true, 0, "daytona sandbox request timed out", err)
	}
	var server *daytonaerrors.DaytonaServerError
	if stderrors.As(err, &server) {
		return daytonaProviderError(stage, sandbox.ProviderErrorUnavailable, true, server.StatusCode, "daytona sandbox service unavailable", err)
	}
	var daytonaErr *daytonaerrors.DaytonaError
	if stderrors.As(err, &daytonaErr) {
		retryable := daytonaErr.StatusCode >= 500 || daytonaErr.StatusCode == http.StatusTooManyRequests
		return daytonaProviderError(stage, sandbox.ProviderErrorUnknown, retryable, daytonaErr.StatusCode, "daytona sandbox request failed", err)
	}
	return daytonaProviderError(stage, sandbox.ProviderErrorUnknown, true, 0, "daytona sandbox request failed", err)
}

// Daytona Create capacity is recognized from the first logical line of the
// SDK's structured response. That line is the stable machine discriminator;
// later lines are mutable provider guidance. The anchored grammar deliberately
// rejects prefixes and wording drift, while Queue retry remains owned by the
// activation lifecycle after this adapter emits a proved-not-started outcome.
func daytonaCreateDiskCapacityExceeded(validation *daytonaerrors.DaytonaValidationError) bool {
	if validation == nil || validation.DaytonaError == nil {
		return false
	}
	message := strings.TrimSpace(validation.Message)
	firstLine, _, _ := strings.Cut(message, "\n")
	match := daytonaDiskCapacityMessagePattern.FindStringSubmatch(strings.TrimSpace(firstLine))
	if len(match) != 3 {
		return false
	}
	limit, err := strconv.ParseUint(match[1], 10, 64)
	return err == nil && limit > 0
}

func daytonaRequestWasRejected(err error) bool {
	var rateLimited *daytonaerrors.DaytonaRateLimitError
	if stderrors.As(err, &rateLimited) {
		return true
	}
	var auth *daytonaerrors.DaytonaAuthenticationError
	if stderrors.As(err, &auth) {
		return true
	}
	var forbidden *daytonaerrors.DaytonaForbiddenError
	if stderrors.As(err, &forbidden) {
		return true
	}
	var validation *daytonaerrors.DaytonaValidationError
	if stderrors.As(err, &validation) {
		return true
	}
	var conflict *daytonaerrors.DaytonaConflictError
	return stderrors.As(err, &conflict)
}

func daytonaProviderError(stage sandbox.ProviderStage, kind sandbox.ProviderErrorKind, retryable bool, statusCode int, message string, cause error) error {
	return &sandbox.ProviderError{
		Provider:    DaytonaProviderName,
		Stage:       stage,
		Kind:        kind,
		Retryable:   retryable,
		StatusCode:  statusCode,
		SafeMessage: message,
		Cause:       cause,
	}
}
