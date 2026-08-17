package queue

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	JobIDPrefix    = "qjob_"
	LeaseTokenSize = 16
)

const (
	KindRuntimeInput                = "runtime_input"
	KindRuntimeConfigUpdate         = "runtime_config_update"
	KindCleanupSession              = "cleanup_session"
	KindSessionDeleteCleanup        = "session_delete_cleanup"
	KindEnvironmentBuild            = "environment_build"
	KindEnvironmentReadyFanout      = "environment_ready_fanout"
	KindSandboxToolExecute          = "sandbox_tool_execute"
	KindSandboxActivate             = "sandbox_activate"
	KindSandboxMaterialize          = "sandbox_materialize"
	KindSandboxRelease              = "sandbox_release"
	KindSandboxToolCancel           = "sandbox_tool_cancel"
	KindSandboxOutputCapture        = "sandbox_output_capture"
	KindSandboxOutputCaptureCleanup = "sandbox_output_capture_cleanup"
	KindSandboxMemoryProjection     = "sandbox_memory_projection"
	KindSandboxBackgroundCommand    = "sandbox_background_command"
	KindSandboxBackgroundReconcile  = "sandbox_background_reconcile"

	// MaxMcpManifestBytes is owned by Bridge manifest acceptance. Queue jobs
	// carry only the manifest row identity; delivery rebuilds read the bounded
	// content from that durable row.
	MaxMcpManifestBytes = 256 * 1024
	// Runtime-input segmentation owns this bound: 512 fixed-format event ids
	// stay around 12 KiB before the JSON envelope.
	MaxRuntimeInputEventRefsPerJob = 512

	// Queue payloads carry references only; every variable QueueJob field is
	// bounded; the sum of bounds stays below the scoped Lease response fuse.
	// The all-kind accept fuse is over five times the largest current
	// refs-only payload (a 512-event runtime_input job).
	MaxQueueJobPayloadBytes = 64 * 1024
	// SandboxMemoryProjectionMaxAttempts bounds projection-only retries after
	// the authoritative memory mutation has committed.
	SandboxMemoryProjectionMaxAttempts = 5
	// Output capture and staged-blob cleanup use separate bounded generations.
	// Exhaustion closes the current generation before replay or cleanup creates
	// a successor with a new durable identity.
	SandboxOutputCaptureMaxAttempts        = 5
	SandboxOutputCaptureCleanupMaxAttempts = 5
	// Bridge/Sandbox startup knobs own lease_owner; Lease admission rechecks
	// it, and deployed service identifiers are below 32 bytes.
	MaxQueueLeaseOwnerBytes = 256
	// Queue-owned formatters compose bounded workspace/session/resource ids;
	// 512 bytes covers every current partition-key shape.
	MaxQueuePartitionKeyBytes = 512
	// The longest dedupe key embeds an MCP server name whose 255-code-point
	// bound is owned and enforced by internal/agent admission; at four UTF-8
	// bytes per code point it occupies at most 1,020 bytes, plus fixed identity
	// components.
	MaxQueueDedupeKeyBytes = 1280
	// Queue-minted job ids are the fixed prefix plus id.New's eight random
	// bytes encoded as 16 hexadecimal characters.
	MaxQueueJobIDBytes = len(JobIDPrefix) + 16
	// Kind and status are closed queue enums; these are their longest members.
	MaxQueueJobKindBytes   = len(KindSandboxOutputCaptureCleanup)
	MaxQueueJobStatusBytes = len(StatusDeadLettered)
	// RFC3339Nano plus PostgreSQL's six-digit maximum year bounds timestamps.
	MaxQueueTimestampBytes = 35
	// Queue-minted lease tokens are a fixed prefix plus 16 random bytes.
	MaxQueueLeaseTokenBytes = len("qlt_") + 2*LeaseTokenSize
	// Protobuf terms are worst-case key, length, and int32 varint widths for
	// the current QueueJob schema; the reflection census fails on a new field.
	maxProtoFieldTagBytes      = 2
	maxProtoStringLengthBytes  = 3 // Covers values through the 64 KiB payload fuse.
	maxProtoInt32Bytes         = 10
	queueLeaseRepeatedTagBytes = 1
	queueLeaseJobLengthBytes   = 3
	// Fixed response headroom covers the response envelope and unknown fields;
	// it is independent from the identically sized payload fuse.
	QueueLeaseResponseFixedOverhead = 64 * 1024
)

type QueueJobFieldBound struct {
	Name            string
	MaxValueBytes   int
	Numeric         bool
	SeparatePayload bool
}

func queueJobFieldBounds() []QueueJobFieldBound {
	return []QueueJobFieldBound{
		{Name: "id", MaxValueBytes: MaxQueueJobIDBytes},
		// Bootstrap owns workspace ids, while queue accept boundaries enforce
		// internal/workspace.MaxWorkspaceIDBytes before a row can be returned.
		{Name: "workspace_id", MaxValueBytes: workspace.MaxWorkspaceIDBytes},
		{Name: "kind", MaxValueBytes: MaxQueueJobKindBytes},
		{Name: "partition_key", MaxValueBytes: MaxQueuePartitionKeyBytes},
		{Name: "dedupe_key", MaxValueBytes: MaxQueueDedupeKeyBytes},
		{Name: "payload_version", MaxValueBytes: maxProtoInt32Bytes, Numeric: true},
		{Name: "payload_json", MaxValueBytes: MaxQueueJobPayloadBytes, SeparatePayload: true},
		{Name: "status", MaxValueBytes: MaxQueueJobStatusBytes},
		{Name: "priority", MaxValueBytes: maxProtoInt32Bytes, Numeric: true},
		{Name: "available_at", MaxValueBytes: MaxQueueTimestampBytes},
		{Name: "leased_by", MaxValueBytes: MaxQueueLeaseOwnerBytes},
		{Name: "lease_token", MaxValueBytes: MaxQueueLeaseTokenBytes},
		{Name: "leased_at", MaxValueBytes: MaxQueueTimestampBytes},
		{Name: "leased_until", MaxValueBytes: MaxQueueTimestampBytes},
		{Name: "attempt_count", MaxValueBytes: maxProtoInt32Bytes, Numeric: true},
		{Name: "max_attempts", MaxValueBytes: maxProtoInt32Bytes, Numeric: true},
	}
}

// QueueJobFieldBounds returns the registry that drives Lease envelope
// arithmetic. A new QueueJob proto field is incomplete until it appears here.
func QueueJobFieldBounds() []QueueJobFieldBound {
	return queueJobFieldBounds()
}

// QueueJobFieldEnvelopeBytes returns a field's conservative protobuf wire
// contribution excluding a separately charged payload value.
func QueueJobFieldEnvelopeBytes(bound QueueJobFieldBound) int {
	if bound.Numeric {
		return maxProtoFieldTagBytes + bound.MaxValueBytes
	}
	valueBytes := bound.MaxValueBytes
	if bound.SeparatePayload {
		valueBytes = 0
	}
	return maxProtoFieldTagBytes + maxProtoStringLengthBytes + valueBytes
}

func queueJobEnvelopeAllowance() int {
	total := queueLeaseRepeatedTagBytes + queueLeaseJobLengthBytes
	for _, bound := range queueJobFieldBounds() {
		total += QueueJobFieldEnvelopeBytes(bound)
	}
	return total
}

// QueueJobEnvelopeAllowance is every non-payload value plus conservative
// protobuf key/length/varint overhead, derived from QueueJobFieldBounds.
func QueueJobEnvelopeAllowance() int {
	return queueJobEnvelopeAllowance()
}

// Queue job lifecycle state machine. These are the closed status values of a
// queue_jobs row. Every transition is written by a PostgreSQLQueueStore method
// in postgresql_store.go. Every caller-driven write off "leased"
// (Ack/Retry/Defer/DeadLetter/Heartbeat) matches on the row's lease_token, so a stale
// token affects no row. The maintenance path ReclaimExpiredLeases is the
// exception: it reclaims an expired lease off "leased" without the now-stale
// token, matching only on workspace_id/id/status='leased'.
//
//	state          meaning                                       writers (postgresql_store.go)         transitions
//	-------------  --------------------------------------------  ------------------------------------  ----------------------------
//	pending        admitted, awaiting a lease; Retry/Defer       Enqueue (insert), Retry (not          -> leased, -> cancelled,
//	               re-admit with backoff-delayed available_at,   exhausted), Defer, ReclaimExpired-
//	               reclaim re-admits at available_at = now       Leases                                -> dead_lettered
//	leased         one consumer holds the row under a             Lease                                 -> pending, -> acknowledged,
//	               lease_token for the lease window                                                     -> dead_lettered
//	acknowledged   terminal; the leased work committed            Ack                                   (none)
//	cancelled      terminal; pending work was fenced out          Cancel, CancelTx                       (none)
//	dead_lettered  terminal; attempts exhausted or an explicit    Retry (exhausted), DeadLetter,         (none)
//	               dead-letter
//	                                                            DeadLetterExhaustedTx
//
// Readers of status: Lease candidate selection with its per-partition barrier,
// the active-dedupe lookup in EnqueueTx, Metrics, the Sandbox over-budget
// census, and terminal-retention maintenance.
//
// INVARIANTS:
//   - At most one active job per (workspace_id, dedupe_key) across pending and
//     leased. EnqueueTx's ON CONFLICT ... DO NOTHING plus the partial-unique
//     index reject a second in-flight duplicate and return the existing active
//     row. The scope excludes terminal rows, so a later job for the same durable
//     work item is admitted once the prior job is acknowledged, cancelled, or
//     dead_lettered.
//   - At most one leased job per partition_key: the same-session serial-execution
//     barrier. leaseCandidate's NOT EXISTS leased-in-partition guard and the
//     partial-unique index run a partition's jobs one at a time.
//   - Caller-driven transitions off leased (Ack/Retry/Defer/DeadLetter/Heartbeat) are
//     lease-token fenced; a stale one carrying an old lease_token matches no
//     row and is ignored. Heartbeat cannot revive an expired lease. Lease,
//     Heartbeat, and reclaim author durable lease times from PostgreSQL's
//     clock. ReclaimExpiredLeases is exempt by design: it reclaims an expired
//     lease off leased without the stale token.
//   - Lease scans candidates FOR UPDATE SKIP LOCKED; runtime-facing consumers
//     hold at most one leased job per session partition.
//
// UPDATE-WITH: internal/queue/postgresql_store.go (every transition writer plus
// the ON CONFLICT and NOT EXISTS guards that enforce the two invariants).
const (
	StatusPending      = "pending"
	StatusLeased       = "leased"
	StatusAcknowledged = "acknowledged"
	StatusCancelled    = "cancelled"
	StatusDeadLettered = "dead_lettered"
)

const DefaultMaxAttempts = 10

const (
	SandboxTerminalRetentionAge  = 24 * time.Hour
	SandboxMaintenanceBatchLimit = 100
)

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

type IntegrityError struct {
	Message string
}

func (e *IntegrityError) Error() string { return e.Message }

type Job struct {
	ID                     string
	WorkspaceID            workspace.ID
	Kind                   string
	PartitionKey           string
	QueuePartitionSequence int64
	DedupeKey              string
	PayloadVersion         int
	PayloadJSON            json.RawMessage
	Status                 string
	Priority               int
	AvailableAt            time.Time
	LeasedBy               string
	LeaseToken             string
	LeasedAt               *time.Time
	LeasedUntil            *time.Time
	AttemptCount           int
	MaxAttempts            int
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

type MetricsSnapshot struct {
	Kind             string
	PendingJobs      int
	LeasedJobs       int
	RetryPendingJobs int
	DeadLetteredJobs int
	ReadyLagSeconds  float64
}

type EnqueueRequest struct {
	ID             string
	WorkspaceID    workspace.ID
	Kind           string
	PartitionKey   string
	DedupeKey      string
	PayloadVersion int
	PayloadJSON    json.RawMessage
	Priority       int
	MaxAttempts    int
	AvailableAt    time.Time
	Now            time.Time
}

type LeaseRequest struct {
	WorkspaceID   workspace.ID
	Kinds         []string
	LeaseOwner    string
	MaxJobs       int
	LeaseDuration time.Duration
	Now           time.Time
}

type HeartbeatRequest struct {
	WorkspaceID   workspace.ID
	JobID         string
	LeaseToken    string
	LeaseDuration time.Duration
}

type HeartbeatResult struct {
	Updated     bool
	LeasedUntil time.Time
}

type ActiveLeaseRequest struct {
	WorkspaceID workspace.ID
	JobID       string
	LeaseToken  string
}

// ExactLeaseRequest is the complete durable identity of one leased Queue job.
// Business transactions use it after acquiring their owning locks so a stale
// worker cannot act through a reclaimed row that happens to retain the same
// business payload.
type ExactLeaseRequest struct {
	WorkspaceID  workspace.ID
	JobID        string
	LeaseToken   string
	Kind         string
	PartitionKey string
	DedupeKey    string
}

// InterruptFenceRequest binds cancellation of message notifications to the
// exact live interrupt lease that owns the fence.
type InterruptFenceRequest struct {
	Lease                  ExactLeaseRequest
	SessionID              string
	SessionThreadID        string
	InterruptFenceSequence int64
	Now                    time.Time
}

type AckRequest struct {
	WorkspaceID workspace.ID
	JobID       string
	LeaseToken  string
	Now         time.Time
}

type RetryRequest struct {
	WorkspaceID  workspace.ID
	JobID        string
	LeaseToken   string
	ErrorKind    string
	ErrorMessage string
	Now          time.Time
}

type DeferRequest struct {
	WorkspaceID workspace.ID
	JobID       string
	LeaseToken  string
	Now         time.Time
}

type DeadLetterRequest struct {
	WorkspaceID  workspace.ID
	JobID        string
	LeaseToken   string
	ErrorKind    string
	ErrorMessage string
	Now          time.Time
}

// ReplaceMalformedRuntimeInputCustodyRequest identifies one leased malformed
// runtime-input job and the canonical Inbox identity that may replace it.
type ReplaceMalformedRuntimeInputCustodyRequest struct {
	WorkspaceID    workspace.ID
	SessionID      string
	RuntimeInputID string
	JobID          string
	LeaseToken     string
	Now            time.Time
}

// ReplaceMalformedRuntimeInputCustodyResult reports the atomic Queue outcome.
type ReplaceMalformedRuntimeInputCustodyResult struct {
	DeadLettered bool
	Replaced     bool
}

type CancelRequest struct {
	WorkspaceID            workspace.ID
	SessionID              string
	SessionThreadID        string
	InterruptFenceSequence int64
	Now                    time.Time
}

type ReclaimExpiredLeasesRequest struct {
	WorkspaceID  workspace.ID
	Kind         string
	Limit        int
	ErrorKind    string
	ErrorMessage string
}

type TargetedCancelRequest struct {
	WorkspaceID  workspace.ID
	JobID        string
	Kind         string
	PartitionKey string
	DedupeKey    string
	Now          time.Time
}

type ListPendingAtOrOverBudgetRequest struct {
	Limit int
}

type PendingAtOrOverBudgetJob struct {
	WorkspaceID  workspace.ID
	JobID        string
	Kind         string
	PartitionKey string
	DedupeKey    string
	PayloadJSON  json.RawMessage
	AttemptCount int
	MaxAttempts  int
}

type DeadLetterExhaustedRequest struct {
	WorkspaceID          workspace.ID
	JobID                string
	ObservedAttemptCount int
	ErrorKind            string
	ErrorMessage         string
	Now                  time.Time
}

type SandboxTerminalSweepRequest struct {
	Now   time.Time
	Limit int
}

type EmptyPartitionCounterSweepRequest struct {
	Limit int
}

func NewJobID() string {
	return id.New(JobIDPrefix)
}

func NewLeaseToken() (string, error) {
	bytes := make([]byte, LeaseTokenSize)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "qlt_" + hex.EncodeToString(bytes), nil
}

func NormalizeEnqueueRequest(request EnqueueRequest) (EnqueueRequest, error) {
	if request.WorkspaceID == "" {
		return EnqueueRequest{}, &ValidationError{Message: "workspace_id is required"}
	}
	if len(request.WorkspaceID) > workspace.MaxWorkspaceIDBytes {
		return EnqueueRequest{}, &ValidationError{Message: "workspace_id exceeds the maximum queue transport size"}
	}
	if request.Kind == "" {
		return EnqueueRequest{}, &ValidationError{Message: "kind is required"}
	}
	if request.PartitionKey == "" {
		return EnqueueRequest{}, &ValidationError{Message: "partition_key is required"}
	}
	if len(request.PartitionKey) > MaxQueuePartitionKeyBytes {
		return EnqueueRequest{}, &ValidationError{Message: "partition_key exceeds the maximum queue transport size"}
	}
	if len(request.DedupeKey) > MaxQueueDedupeKeyBytes {
		return EnqueueRequest{}, &ValidationError{Message: "dedupe_key exceeds the maximum queue transport size"}
	}
	if len(request.PayloadJSON) == 0 {
		return EnqueueRequest{}, &ValidationError{Message: "payload_json is required"}
	}
	if len(request.PayloadJSON) > MaxQueueJobPayloadBytes {
		return EnqueueRequest{}, &ValidationError{Message: "payload_json exceeds the maximum queue job payload size"}
	}
	if request.ID == "" {
		request.ID = NewJobID()
	} else if len(request.ID) > MaxQueueJobIDBytes {
		return EnqueueRequest{}, &ValidationError{Message: "id exceeds the maximum queue transport size"}
	}
	if request.PayloadVersion == 0 {
		request.PayloadVersion = 1
	}
	if request.PayloadVersion < 0 {
		return EnqueueRequest{}, &ValidationError{Message: "payload_version must be positive"}
	}
	if request.MaxAttempts < 0 {
		return EnqueueRequest{}, &ValidationError{Message: "max_attempts must not be negative"}
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	request.Now = request.Now.UTC()
	if request.AvailableAt.IsZero() {
		request.AvailableAt = request.Now
	}
	request.AvailableAt = request.AvailableAt.UTC()
	if err := validateCanonicalQueueShape(request); err != nil {
		return EnqueueRequest{}, err
	}
	return request, nil
}

func ValidateLeaseRequest(request LeaseRequest) error {
	if request.WorkspaceID == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if len(request.WorkspaceID) > workspace.MaxWorkspaceIDBytes {
		return &ValidationError{Message: "workspace_id exceeds the maximum queue transport size"}
	}
	if len(request.Kinds) == 0 {
		return &ValidationError{Message: "kinds is required"}
	}
	seen := map[string]bool{}
	for _, kind := range request.Kinds {
		if kind == "" {
			return &ValidationError{Message: "kinds must not contain empty values"}
		}
		if !isKnownKind(kind) {
			return &ValidationError{Message: "unknown queue job kind: " + kind}
		}
		if seen[kind] {
			return &ValidationError{Message: "kinds must not contain duplicates"}
		}
		seen[kind] = true
	}
	if err := ValidateLeaseOwner(request.LeaseOwner); err != nil {
		return err
	}
	if request.MaxJobs <= 0 {
		return &ValidationError{Message: "max_jobs must be positive"}
	}
	if err := ValidateLeaseBatchSize(request.MaxJobs); err != nil {
		return err
	}
	if request.LeaseDuration <= 0 {
		return &ValidationError{Message: "lease_duration must be positive"}
	}
	return nil
}

func MaxQueueLeaseJobs() int {
	return (sessionrpc.MaxQueueLeaseGRPCMessageBytes - QueueLeaseResponseFixedOverhead) /
		(MaxQueueJobPayloadBytes + QueueJobEnvelopeAllowance())
}

func ValidateLeaseOwner(leaseOwner string) error {
	if leaseOwner == "" {
		return &ValidationError{Message: "lease_owner is required"}
	}
	if len(leaseOwner) > MaxQueueLeaseOwnerBytes {
		return &ValidationError{Message: "lease_owner exceeds the maximum queue transport size"}
	}
	return nil
}

func ValidateLeaseBatchSize(maxJobs int) error {
	if maxJobs <= 0 {
		return &ValidationError{Message: "max_jobs must be positive"}
	}
	maximum := MaxQueueLeaseJobs()
	if maxJobs > maximum {
		return &ValidationError{Message: fmt.Sprintf("max_jobs must not exceed %d under the queue Lease message capacity", maximum)}
	}
	return nil
}

func validateFencedRequest(workspaceID workspace.ID, jobID string, leaseToken string) error {
	if workspaceID == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if jobID == "" {
		return &ValidationError{Message: "job_id is required"}
	}
	if leaseToken == "" {
		return &ValidationError{Message: "lease_token is required"}
	}
	return nil
}

func validateCancelRequest(request CancelRequest) error {
	if request.WorkspaceID == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if request.SessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if request.SessionThreadID == "" {
		return &ValidationError{Message: "session_thread_id is required"}
	}
	if request.InterruptFenceSequence <= 0 {
		return &ValidationError{Message: "interrupt_fence_sequence must be positive"}
	}
	return nil
}

func validateReclaimExpiredLeasesRequest(request ReclaimExpiredLeasesRequest) error {
	if request.Limit < 0 {
		return &ValidationError{Message: "limit must not be negative"}
	}
	return nil
}

func FormatPartitionKey(prefix string, workspaceID workspace.ID, resourceID string) string {
	if prefix == "" || workspaceID == "" || resourceID == "" {
		return ""
	}
	return prefix + ":" + string(workspaceID) + ":" + resourceID
}

func FormatSessionPartitionKey(workspaceID workspace.ID, sessionID string) string {
	return FormatPartitionKey("session", workspaceID, sessionID)
}

func FormatEnvironmentPartitionKey(workspaceID workspace.ID, environmentID string) string {
	return FormatPartitionKey("environment", workspaceID, environmentID)
}

func FormatRuntimeInputDedupeKey(workspaceID workspace.ID, sessionID string, runtimeInputID string) string {
	if workspaceID == "" || sessionID == "" || runtimeInputID == "" {
		return ""
	}
	return "runtime_input:" + string(workspaceID) + ":" + sessionID + ":" + runtimeInputID
}

func FormatTaskNotificationRuntimeInputID(taskID string) string {
	if taskID == "" {
		return ""
	}
	return "task_notification:" + taskID
}

func NewTaskNotificationRuntimeInputEnqueueRequest(workspaceID workspace.ID, sessionID string, sessionThreadID string, taskID string, now time.Time) (EnqueueRequest, error) {
	runtimeInputID := FormatTaskNotificationRuntimeInputID(taskID)
	payload, err := json.Marshal(struct {
		WorkspaceID     string   `json:"workspace_id"`
		SessionID       string   `json:"session_id"`
		SessionThreadID string   `json:"session_thread_id"`
		RuntimeInputID  string   `json:"runtime_input_id"`
		EventIDs        []string `json:"event_ids"`
		SequenceFrom    int64    `json:"sequence_from"`
		SequenceTo      int64    `json:"sequence_to"`
		InputKind       string   `json:"input_kind"`
	}{string(workspaceID), sessionID, sessionThreadID, runtimeInputID, []string{}, 0, 0, "task_notification"})
	if err != nil {
		return EnqueueRequest{}, err
	}
	return EnqueueRequest{
		ID: NewJobID(), WorkspaceID: workspaceID, Kind: KindRuntimeInput,
		PartitionKey:   FormatSessionPartitionKey(workspaceID, sessionID),
		DedupeKey:      FormatRuntimeInputDedupeKey(workspaceID, sessionID, runtimeInputID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: DefaultMaxAttempts, Now: now,
	}, nil
}

func FormatRuntimeConfigUpdateDedupeKey(workspaceID workspace.ID, sessionID string, configGeneration string) string {
	return formatQueueDedupeKey(KindRuntimeConfigUpdate, workspaceID, sessionID, configGeneration)
}

func FormatRuntimeMCPManifestUpdateDedupeKey(workspaceID workspace.ID, sessionID string, mcpServerName string, manifestGeneration string) string {
	return formatQueueDedupeKey(KindRuntimeConfigUpdate, workspaceID, sessionID, "mcp_manifest:"+mcpServerName+":"+manifestGeneration)
}

func FormatCleanupSessionDedupeKey(workspaceID workspace.ID, sessionID string, cleanupJobID string) string {
	return formatQueueDedupeKey(KindCleanupSession, workspaceID, sessionID, cleanupJobID)
}

func FormatSessionDeleteCleanupDedupeKey(workspaceID workspace.ID, sessionID string, deleteCleanupID string) string {
	return formatQueueDedupeKey(KindSessionDeleteCleanup, workspaceID, sessionID, deleteCleanupID)
}

func FormatEnvironmentBuildDedupeKey(workspaceID workspace.ID, environmentID string, generation string) string {
	return formatQueueDedupeKey(KindEnvironmentBuild, workspaceID, environmentID, generation)
}

func FormatEnvironmentReadyFanoutDedupeKey(workspaceID workspace.ID, environmentID string, generation string) string {
	return formatQueueDedupeKey(KindEnvironmentReadyFanout, workspaceID, environmentID, generation)
}

func FormatSandboxExecutionPartitionKey(workspaceID workspace.ID, sessionID string, sessionThreadID string, toolUseEventID string) string {
	if workspaceID == "" || sessionID == "" || sessionThreadID == "" || toolUseEventID == "" {
		return ""
	}
	return "sandbox-execution:" + string(workspaceID) + ":" + sessionID + ":" + sessionThreadID + ":" + toolUseEventID
}

func FormatSandboxToolExecuteDedupeKey(workspaceID workspace.ID, sessionID string, sessionThreadID string, toolUseEventID string, generation int64) string {
	if generation <= 0 {
		return ""
	}
	return formatSandboxExecutionDedupeKey(KindSandboxToolExecute, workspaceID, sessionID, sessionThreadID, toolUseEventID, strconv.FormatInt(generation, 10))
}

func FormatSandboxLifecyclePartitionKey(workspaceID workspace.ID, logicalSandboxID string) string {
	return FormatPartitionKey("sandbox-lifecycle", workspaceID, logicalSandboxID)
}

func FormatSandboxLifecycleDedupeKey(kind string, workspaceID workspace.ID, logicalSandboxID string, operationID string) string {
	if !isSandboxLifecycleKind(kind) {
		return ""
	}
	return formatQueueDedupeKey(kind, workspaceID, logicalSandboxID, operationID)
}

func FormatSandboxCancelPartitionKey(workspaceID workspace.ID, sessionID string, sessionThreadID string, toolUseEventID string) string {
	if workspaceID == "" || sessionID == "" || sessionThreadID == "" || toolUseEventID == "" {
		return ""
	}
	return "sandbox-cancel:" + string(workspaceID) + ":" + sessionID + ":" + sessionThreadID + ":" + toolUseEventID
}

func FormatSandboxToolCancelDedupeKey(workspaceID workspace.ID, sessionID string, sessionThreadID string, toolUseEventID string) string {
	return formatSandboxExecutionDedupeKey(KindSandboxToolCancel, workspaceID, sessionID, sessionThreadID, toolUseEventID, "cancel")
}

func FormatSandboxCapturePartitionKey(workspaceID workspace.ID, sessionID string, finishIdleWriteID string) string {
	if workspaceID == "" || sessionID == "" || finishIdleWriteID == "" {
		return ""
	}
	return "sandbox-capture:" + string(workspaceID) + ":" + sessionID + ":" + finishIdleWriteID
}

func FormatSandboxOutputCaptureDedupeKey(workspaceID workspace.ID, sessionID string, finishIdleWriteID string, captureGeneration int64) string {
	if workspaceID == "" || sessionID == "" || finishIdleWriteID == "" || captureGeneration <= 0 {
		return ""
	}
	return KindSandboxOutputCapture + ":" + string(workspaceID) + ":" + sessionID + ":" + finishIdleWriteID + ":" + strconv.FormatInt(captureGeneration, 10)
}

func FormatSandboxOutputCaptureCleanupDedupeKey(workspaceID workspace.ID, sessionID string, finishIdleWriteID string, captureGeneration int64, cleanupGeneration int64) string {
	if workspaceID == "" || sessionID == "" || finishIdleWriteID == "" || captureGeneration <= 0 || cleanupGeneration <= 0 {
		return ""
	}
	return KindSandboxOutputCaptureCleanup + ":" + string(workspaceID) + ":" + sessionID + ":" + finishIdleWriteID + ":" + strconv.FormatInt(captureGeneration, 10) + ":" + strconv.FormatInt(cleanupGeneration, 10)
}

func FormatSandboxMemoryPartitionKey(workspaceID workspace.ID, memoryStoreID string) string {
	return FormatPartitionKey("sandbox-memory", workspaceID, memoryStoreID)
}

func FormatSandboxMemoryProjectionDedupeKey(workspaceID workspace.ID, memoryStoreID string, memoryWriteID string) string {
	return formatQueueDedupeKey(KindSandboxMemoryProjection, workspaceID, memoryStoreID, memoryWriteID)
}

func FormatSandboxBackgroundPartitionKey(workspaceID workspace.ID, sessionID string, taskID string) string {
	if workspaceID == "" || sessionID == "" || taskID == "" {
		return ""
	}
	return "sandbox-background:" + string(workspaceID) + ":" + sessionID + ":" + taskID
}

func FormatSandboxBackgroundCommandDedupeKey(workspaceID workspace.ID, sessionID string, taskID string, requestID string) string {
	if workspaceID == "" || sessionID == "" || taskID == "" || requestID == "" {
		return ""
	}
	return KindSandboxBackgroundCommand + ":" + string(workspaceID) + ":" + sessionID + ":" + taskID + ":" + requestID
}

func FormatSandboxBackgroundReconcileDedupeKey(workspaceID workspace.ID, sessionID string, taskID string, generation int64) string {
	if workspaceID == "" || sessionID == "" || taskID == "" || generation <= 0 {
		return ""
	}
	return KindSandboxBackgroundReconcile + ":" + string(workspaceID) + ":" + sessionID + ":" + taskID + ":" + strconv.FormatInt(generation, 10)
}

func formatSandboxExecutionDedupeKey(kind string, workspaceID workspace.ID, sessionID string, sessionThreadID string, toolUseEventID string, suffix string) string {
	if kind == "" || workspaceID == "" || sessionID == "" || sessionThreadID == "" || toolUseEventID == "" || suffix == "" {
		return ""
	}
	return kind + ":" + string(workspaceID) + ":" + sessionID + ":" + sessionThreadID + ":" + toolUseEventID + ":" + suffix
}

func formatQueueDedupeKey(kind string, workspaceID workspace.ID, ownerID string, itemID string) string {
	if kind == "" || workspaceID == "" || ownerID == "" || itemID == "" {
		return ""
	}
	return kind + ":" + string(workspaceID) + ":" + ownerID + ":" + itemID
}

func validateCanonicalQueueShape(request EnqueueRequest) error {
	if !isKnownKind(request.Kind) {
		return &ValidationError{Message: "unknown queue job kind: " + request.Kind}
	}
	if request.DedupeKey == "" {
		return &ValidationError{Message: "dedupe_key is required for " + request.Kind}
	}
	rawPayload, err := decodePayloadObject(request.PayloadJSON)
	if err != nil {
		return err
	}
	payload := payloadTokens(rawPayload)
	workspaceID, ok := payload["workspace_id"]
	if !ok || workspaceID == "" {
		return &ValidationError{Message: "payload_json missing workspace_id"}
	}
	if workspaceID != string(request.WorkspaceID) {
		return &ValidationError{Message: "payload workspace_id must match queue workspace_id"}
	}
	if IsSandboxJobKind(request.Kind) && request.MaxAttempts <= 0 {
		return &ValidationError{Message: "max_attempts must be positive for " + request.Kind}
	}
	if IsSandboxJobKind(request.Kind) {
		if err := requirePayloadStrings(rawPayload, "workspace_id"); err != nil {
			return err
		}
	}
	switch request.Kind {
	case KindRuntimeInput:
		runtimeInputKeys := []string{"workspace_id", "session_id", "session_thread_id", "runtime_input_id", "event_ids", "sequence_from", "sequence_to", "input_kind"}
		if err := validatePayloadKeys(rawPayload, runtimeInputKeys...); err != nil {
			return err
		}
		sessionID, runtimeInputID, err := requiredPayloadTokens(payload, "session_id", "runtime_input_id")
		if err != nil {
			return err
		}
		if _, _, err := requiredPayloadTokens(payload, "session_thread_id", "input_kind"); err != nil {
			return err
		}
		inputKind := payload["input_kind"]
		if !isRuntimeInputKind(inputKind) {
			return &ValidationError{Message: "runtime_input input_kind is invalid"}
		}
		var eventIDs []string
		if inputKind == "task_notification" || inputKind == "agent_mail" {
			eventIDs, err = payloadStringArray(rawPayload, "event_ids", true)
			if err != nil {
				return err
			}
			if value := payload["sequence_from"]; value != "" {
				if _, err := parsePayloadInt64(value, "sequence_from"); err != nil {
					return err
				}
			}
			if value := payload["sequence_to"]; value != "" {
				if _, err := parsePayloadInt64(value, "sequence_to"); err != nil {
					return err
				}
			}
		} else {
			eventIDs, err = requiredPayloadStringArray(rawPayload, "event_ids")
			if err != nil {
				return err
			}
			if _, err := requiredPayloadInt64(payload, "sequence_from"); err != nil {
				return err
			}
			if _, err := requiredPayloadInt64(payload, "sequence_to"); err != nil {
				return err
			}
		}
		if len(eventIDs) > MaxRuntimeInputEventRefsPerJob {
			return &ValidationError{Message: "runtime_input event_ids exceeds the maximum event reference count"}
		}
		if inputKind == "agent_mail" && (len(eventIDs) != 0 || payload["sequence_from"] != "0" || payload["sequence_to"] != "0") {
			return &ValidationError{Message: "agent_mail must be a bare runtime-input poke"}
		}
		return requireCanonicalKeys(request, FormatSessionPartitionKey(request.WorkspaceID, sessionID), FormatRuntimeInputDedupeKey(request.WorkspaceID, sessionID, runtimeInputID))
	case KindRuntimeConfigUpdate:
		if _, ok := rawPayload["config_generation"]; ok {
			if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "config_generation"); err != nil {
				return err
			}
			sessionID, err := requiredPayloadToken(payload, "session_id")
			if err != nil {
				return err
			}
			configGeneration, err := requiredPayloadInt64(payload, "config_generation")
			if err != nil || configGeneration <= 0 {
				return &ValidationError{Message: "payload_json missing config_generation"}
			}
			return requireCanonicalKeys(request, FormatSessionPartitionKey(request.WorkspaceID, sessionID), FormatRuntimeConfigUpdateDedupeKey(request.WorkspaceID, sessionID, strconv.FormatInt(configGeneration, 10)))
		}
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "mcp_server_name", "manifest_generation"); err != nil {
			return err
		}
		sessionID, err := requiredPayloadToken(payload, "session_id")
		if err != nil {
			return err
		}
		mcpServerName, manifestGeneration, err := mcpManifestPayloadIdentity(rawPayload)
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request, FormatSessionPartitionKey(request.WorkspaceID, sessionID), FormatRuntimeMCPManifestUpdateDedupeKey(request.WorkspaceID, sessionID, mcpServerName, manifestGeneration))
	case KindCleanupSession:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "cleanup_job_id"); err != nil {
			return err
		}
		sessionID, cleanupJobID, err := requiredPayloadTokens(payload, "session_id", "cleanup_job_id")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request, FormatSessionPartitionKey(request.WorkspaceID, sessionID), FormatCleanupSessionDedupeKey(request.WorkspaceID, sessionID, cleanupJobID))
	case KindSessionDeleteCleanup:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "delete_cleanup_id"); err != nil {
			return err
		}
		sessionID, deleteCleanupID, err := requiredPayloadTokens(payload, "session_id", "delete_cleanup_id")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request, FormatSessionPartitionKey(request.WorkspaceID, sessionID), FormatSessionDeleteCleanupDedupeKey(request.WorkspaceID, sessionID, deleteCleanupID))
	case KindEnvironmentBuild:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "environment_id", "generation"); err != nil {
			return err
		}
		environmentID, generation, err := requiredPayloadTokens(payload, "environment_id", "generation")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request, FormatEnvironmentPartitionKey(request.WorkspaceID, environmentID), FormatEnvironmentBuildDedupeKey(request.WorkspaceID, environmentID, generation))
	case KindEnvironmentReadyFanout:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "environment_id", "generation"); err != nil {
			return err
		}
		environmentID, generation, err := requiredPayloadTokens(payload, "environment_id", "generation")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request, FormatEnvironmentPartitionKey(request.WorkspaceID, environmentID), FormatEnvironmentReadyFanoutDedupeKey(request.WorkspaceID, environmentID, generation))
	case KindSandboxToolExecute:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "session_thread_id", "tool_use_event_id"); err != nil {
			return err
		}
		if err := requirePayloadStrings(rawPayload, "session_id", "session_thread_id", "tool_use_event_id"); err != nil {
			return err
		}
		sessionID, sessionThreadID, toolUseEventID, err := sandboxExecutionPayloadIdentity(payload)
		if err != nil {
			return err
		}
		if request.PartitionKey != FormatSandboxExecutionPartitionKey(request.WorkspaceID, sessionID, sessionThreadID, toolUseEventID) {
			return &ValidationError{Message: fmt.Sprintf("%s partition_key must be canonical", request.Kind)}
		}
		return validateSandboxToolExecuteDedupeKey(request, sessionID, sessionThreadID, toolUseEventID)
	case KindSandboxActivate, KindSandboxMaterialize, KindSandboxRelease:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "logical_sandbox_id", "operation_id"); err != nil {
			return err
		}
		if err := requirePayloadStrings(rawPayload, "session_id", "logical_sandbox_id", "operation_id"); err != nil {
			return err
		}
		if _, err := requiredPayloadToken(payload, "session_id"); err != nil {
			return err
		}
		logicalSandboxID, operationID, err := requiredPayloadTokens(payload, "logical_sandbox_id", "operation_id")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request,
			FormatSandboxLifecyclePartitionKey(request.WorkspaceID, logicalSandboxID),
			FormatSandboxLifecycleDedupeKey(request.Kind, request.WorkspaceID, logicalSandboxID, operationID),
		)
	case KindSandboxToolCancel:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "session_thread_id", "tool_use_event_id"); err != nil {
			return err
		}
		if err := requirePayloadStrings(rawPayload, "session_id", "session_thread_id", "tool_use_event_id"); err != nil {
			return err
		}
		sessionID, sessionThreadID, toolUseEventID, err := sandboxExecutionPayloadIdentity(payload)
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request,
			FormatSandboxCancelPartitionKey(request.WorkspaceID, sessionID, sessionThreadID, toolUseEventID),
			FormatSandboxToolCancelDedupeKey(request.WorkspaceID, sessionID, sessionThreadID, toolUseEventID),
		)
	case KindSandboxOutputCapture:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "finish_idle_write_id", "capture_generation"); err != nil {
			return err
		}
		if err := requirePayloadStrings(rawPayload, "session_id", "finish_idle_write_id"); err != nil {
			return err
		}
		sessionID, finishIdleWriteID, err := requiredPayloadTokens(payload, "session_id", "finish_idle_write_id")
		if err != nil {
			return err
		}
		captureGeneration, err := requiredPositivePayloadInt64(payload, "capture_generation")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request,
			FormatSandboxCapturePartitionKey(request.WorkspaceID, sessionID, finishIdleWriteID),
			FormatSandboxOutputCaptureDedupeKey(request.WorkspaceID, sessionID, finishIdleWriteID, captureGeneration),
		)
	case KindSandboxOutputCaptureCleanup:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "finish_idle_write_id", "capture_generation", "cleanup_generation"); err != nil {
			return err
		}
		if err := requirePayloadStrings(rawPayload, "session_id", "finish_idle_write_id"); err != nil {
			return err
		}
		sessionID, finishIdleWriteID, err := requiredPayloadTokens(payload, "session_id", "finish_idle_write_id")
		if err != nil {
			return err
		}
		captureGeneration, err := requiredPositivePayloadInt64(payload, "capture_generation")
		if err != nil {
			return err
		}
		cleanupGeneration, err := requiredPositivePayloadInt64(payload, "cleanup_generation")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request,
			FormatSandboxCapturePartitionKey(request.WorkspaceID, sessionID, finishIdleWriteID),
			FormatSandboxOutputCaptureCleanupDedupeKey(request.WorkspaceID, sessionID, finishIdleWriteID, captureGeneration, cleanupGeneration),
		)
	case KindSandboxMemoryProjection:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "memory_store_id", "memory_write_id"); err != nil {
			return err
		}
		if err := requirePayloadStrings(rawPayload, "session_id", "memory_store_id", "memory_write_id"); err != nil {
			return err
		}
		if _, err := requiredPayloadToken(payload, "session_id"); err != nil {
			return err
		}
		memoryStoreID, memoryWriteID, err := requiredPayloadTokens(payload, "memory_store_id", "memory_write_id")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request,
			FormatSandboxMemoryPartitionKey(request.WorkspaceID, memoryStoreID),
			FormatSandboxMemoryProjectionDedupeKey(request.WorkspaceID, memoryStoreID, memoryWriteID),
		)
	case KindSandboxBackgroundCommand:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "task_id", "request_id"); err != nil {
			return err
		}
		if err := requirePayloadStrings(rawPayload, "session_id", "task_id", "request_id"); err != nil {
			return err
		}
		sessionID, taskID, err := requiredPayloadTokens(payload, "session_id", "task_id")
		if err != nil {
			return err
		}
		requestID, err := requiredPayloadToken(payload, "request_id")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request,
			FormatSandboxBackgroundPartitionKey(request.WorkspaceID, sessionID, taskID),
			FormatSandboxBackgroundCommandDedupeKey(request.WorkspaceID, sessionID, taskID, requestID),
		)
	case KindSandboxBackgroundReconcile:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "task_id", "reconcile_generation"); err != nil {
			return err
		}
		if err := requirePayloadStrings(rawPayload, "session_id", "task_id"); err != nil {
			return err
		}
		sessionID, taskID, err := requiredPayloadTokens(payload, "session_id", "task_id")
		if err != nil {
			return err
		}
		generation, err := requiredPositivePayloadInt64(payload, "reconcile_generation")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request,
			FormatSandboxBackgroundPartitionKey(request.WorkspaceID, sessionID, taskID),
			FormatSandboxBackgroundReconcileDedupeKey(request.WorkspaceID, sessionID, taskID, generation),
		)
	default:
		return &ValidationError{Message: "unknown queue job kind: " + request.Kind}
	}
}

func validateSandboxToolExecuteDedupeKey(request EnqueueRequest, sessionID string, sessionThreadID string, toolUseEventID string) error {
	separator := strings.LastIndexByte(request.DedupeKey, ':')
	if separator < 0 || separator == len(request.DedupeKey)-1 {
		return &ValidationError{Message: KindSandboxToolExecute + " dedupe_key must carry a positive execution generation"}
	}
	generation, err := strconv.ParseInt(request.DedupeKey[separator+1:], 10, 64)
	if err != nil || generation <= 0 || request.DedupeKey != FormatSandboxToolExecuteDedupeKey(request.WorkspaceID, sessionID, sessionThreadID, toolUseEventID, generation) {
		return &ValidationError{Message: KindSandboxToolExecute + " dedupe_key must be canonical"}
	}
	return nil
}

func sandboxExecutionPayloadIdentity(payload map[string]string) (string, string, string, error) {
	sessionID, err := requiredPayloadToken(payload, "session_id")
	if err != nil {
		return "", "", "", err
	}
	sessionThreadID, err := requiredPayloadToken(payload, "session_thread_id")
	if err != nil {
		return "", "", "", err
	}
	toolUseEventID, err := requiredPayloadToken(payload, "tool_use_event_id")
	if err != nil {
		return "", "", "", err
	}
	return sessionID, sessionThreadID, toolUseEventID, nil
}

func requireCanonicalKeys(request EnqueueRequest, wantPartitionKey string, wantDedupeKey string) error {
	if request.PartitionKey != wantPartitionKey {
		return &ValidationError{Message: fmt.Sprintf("%s partition_key must be %q", request.Kind, wantPartitionKey)}
	}
	if request.DedupeKey != wantDedupeKey {
		return &ValidationError{Message: fmt.Sprintf("%s dedupe_key must be %q", request.Kind, wantDedupeKey)}
	}
	return nil
}

func decodePayloadObject(payloadJSON json.RawMessage) (map[string]json.RawMessage, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payloadJSON, &raw); err != nil {
		return nil, &ValidationError{Message: "payload_json must be a JSON object"}
	}
	if raw == nil {
		return nil, &ValidationError{Message: "payload_json must be a JSON object"}
	}
	return raw, nil
}

func payloadTokens(raw map[string]json.RawMessage) map[string]string {
	tokens := make(map[string]string, len(raw))
	for key, value := range raw {
		token, ok := payloadToken(value)
		if ok {
			tokens[key] = token
		}
	}
	return tokens
}

func validatePayloadKeys(raw map[string]json.RawMessage, allowed ...string) error {
	allowedSet := map[string]bool{}
	for _, key := range allowed {
		allowedSet[key] = true
	}
	for key := range raw {
		if !allowedSet[key] {
			return &ValidationError{Message: "payload_json contains unsupported field " + key}
		}
	}
	return nil
}

func requirePayloadStrings(raw map[string]json.RawMessage, keys ...string) error {
	for _, key := range keys {
		value, ok := raw[key]
		if !ok {
			return &ValidationError{Message: "payload_json missing " + key}
		}
		var text string
		if err := json.Unmarshal(value, &text); err != nil || text == "" {
			return &ValidationError{Message: "payload_json field " + key + " must be a non-empty string"}
		}
	}
	return nil
}

func payloadToken(raw json.RawMessage) (string, bool) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, text != ""
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err == nil {
		token := number.String()
		return token, token != ""
	}
	return "", false
}

func requiredPayloadTokens(payload map[string]string, first string, second string) (string, string, error) {
	firstValue, err := requiredPayloadToken(payload, first)
	if err != nil {
		return "", "", err
	}
	secondValue, err := requiredPayloadToken(payload, second)
	if err != nil {
		return "", "", err
	}
	return firstValue, secondValue, nil
}

func requiredPayloadToken(payload map[string]string, key string) (string, error) {
	value, ok := payload[key]
	if !ok || value == "" {
		return "", &ValidationError{Message: "payload_json missing " + key}
	}
	return value, nil
}

func mcpManifestPayloadIdentity(raw map[string]json.RawMessage) (string, string, error) {
	tokens := payloadTokens(raw)
	mcpServerName, err := requiredPayloadToken(tokens, "mcp_server_name")
	if err != nil {
		return "", "", &ValidationError{Message: "payload_json missing mcp_server_name"}
	}
	manifestGeneration, err := requiredPayloadInt64(tokens, "manifest_generation")
	if err != nil || manifestGeneration <= 0 {
		return "", "", &ValidationError{Message: "payload_json missing manifest_generation"}
	}
	return mcpServerName, strconv.FormatInt(manifestGeneration, 10), nil
}

func requiredPayloadInt64(payload map[string]string, key string) (int64, error) {
	value, ok := payload[key]
	if !ok || value == "" {
		return 0, &ValidationError{Message: "payload_json missing " + key}
	}
	return parsePayloadInt64(value, key)
}

func requiredPositivePayloadInt64(payload map[string]string, key string) (int64, error) {
	value, err := requiredPayloadInt64(payload, key)
	if err != nil || value <= 0 {
		return 0, &ValidationError{Message: "payload_json field " + key + " must be positive"}
	}
	return value, nil
}

func parsePayloadInt64(value string, key string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, &ValidationError{Message: "payload_json field " + key + " must be an integer"}
	}
	return parsed, nil
}

func requiredPayloadStringArray(raw map[string]json.RawMessage, key string) ([]string, error) {
	return payloadStringArray(raw, key, false)
}

func payloadStringArray(raw map[string]json.RawMessage, key string, allowEmpty bool) ([]string, error) {
	value, ok := raw[key]
	if !ok {
		return nil, &ValidationError{Message: "payload_json missing " + key}
	}
	var values []string
	if err := json.Unmarshal(value, &values); err != nil {
		return nil, &ValidationError{Message: "payload_json field " + key + " must be a string array"}
	}
	if len(values) == 0 && !allowEmpty {
		return nil, &ValidationError{Message: "payload_json field " + key + " must not be empty"}
	}
	for _, item := range values {
		if item == "" {
			return nil, &ValidationError{Message: "payload_json field " + key + " must not contain empty values"}
		}
	}
	return values, nil
}

func isKnownKind(kind string) bool {
	switch kind {
	case KindRuntimeInput, KindRuntimeConfigUpdate, KindCleanupSession, KindSessionDeleteCleanup, KindEnvironmentBuild, KindEnvironmentReadyFanout,
		KindSandboxToolExecute, KindSandboxActivate, KindSandboxMaterialize, KindSandboxRelease, KindSandboxToolCancel,
		KindSandboxOutputCapture, KindSandboxOutputCaptureCleanup, KindSandboxMemoryProjection, KindSandboxBackgroundCommand, KindSandboxBackgroundReconcile:
		return true
	default:
		return false
	}
}

func IsSandboxJobKind(kind string) bool {
	switch kind {
	case KindSandboxToolExecute, KindSandboxActivate, KindSandboxMaterialize, KindSandboxRelease, KindSandboxToolCancel,
		KindSandboxOutputCapture, KindSandboxOutputCaptureCleanup, KindSandboxMemoryProjection, KindSandboxBackgroundCommand, KindSandboxBackgroundReconcile:
		return true
	default:
		return false
	}
}

func isSandboxLifecycleKind(kind string) bool {
	switch kind {
	case KindSandboxActivate, KindSandboxMaterialize, KindSandboxRelease:
		return true
	default:
		return false
	}
}

func isRuntimeInputKind(inputKind string) bool {
	switch inputKind {
	case "messages", "interrupt_control", "tool_confirmation", "task_notification", "agent_mail", "approval_review", "rejection":
		return true
	default:
		return false
	}
}

func IsValidationError(err error) bool {
	var validation *ValidationError
	return errors.As(err, &validation)
}

func IsIntegrityError(err error) bool {
	var integrity *IntegrityError
	return errors.As(err, &integrity)
}
