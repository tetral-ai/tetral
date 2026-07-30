package queue

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	KindRuntimeInput            = "runtime_input"
	KindRuntimeConfigUpdate     = "runtime_config_update"
	KindCleanupSession          = "cleanup_session"
	KindSessionDeleteCleanup    = "session_delete_cleanup"
	KindSessionPrepare          = "session_prepare"
	KindEnvironmentBuild        = "environment_build"
	KindEnvironmentReadyFanout  = "environment_ready_fanout"
	KindEnvironmentFailedFanout = "environment_failed_fanout"

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
	MaxQueueJobKindBytes   = len(KindEnvironmentFailedFanout)
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
// (Ack/Retry/Defer/DeadLetter) matches on the row's lease_token, so a stale
// token affects no row. The maintenance path ReclaimExpiredLeases is the
// exception: it reclaims an expired lease off "leased" without the now-stale
// token, matching only on workspace_id/id/status='leased'.
//
//	state          meaning                                       writers (postgresql_store.go)         transitions
//	-------------  --------------------------------------------  ------------------------------------  ----------------------------
//	pending        admitted, awaiting a lease; Retry/Defer       Enqueue (insert), Retry (not          -> leased, -> cancelled
//	               re-admit with backoff-delayed available_at,   exhausted), Defer, ReclaimExpired-
//	               reclaim re-admits at available_at = now       Leases
//	leased         one consumer holds the row under a             Lease                                 -> pending, -> acknowledged,
//	               lease_token for the lease window                                                     -> dead_lettered
//	acknowledged   terminal; the leased work committed            Ack                                   (none)
//	cancelled      terminal; a superseded pending runtime_input   Cancel (runtime_input rows only)      (none)
//	               row was fenced out
//	dead_lettered  terminal; attempts exhausted or an explicit    Retry (exhausted), DeadLetter         (none)
//	               dead-letter
//
// Readers of status: Lease candidate selection with its per-partition barrier,
// the active-dedupe lookup in EnqueueTx, and Metrics.
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
//   - Caller-driven transitions off leased (Ack/Retry/Defer/DeadLetter) are
//     lease-token fenced; a stale one carrying an old lease_token matches no
//     row and is ignored. ReclaimExpiredLeases is exempt by design: it
//     reclaims an expired lease off leased without the stale token.
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

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

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
	Now           time.Time
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
	Now          time.Time
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

func FormatSessionPrepareDedupeKey(workspaceID workspace.ID, sessionID string, preparationAttemptID string) string {
	return formatQueueDedupeKey(KindSessionPrepare, workspaceID, sessionID, preparationAttemptID)
}

func FormatEnvironmentBuildDedupeKey(workspaceID workspace.ID, environmentID string, generation string) string {
	return formatQueueDedupeKey(KindEnvironmentBuild, workspaceID, environmentID, generation)
}

func FormatEnvironmentReadyFanoutDedupeKey(workspaceID workspace.ID, environmentID string, generation string) string {
	return formatQueueDedupeKey(KindEnvironmentReadyFanout, workspaceID, environmentID, generation)
}

func FormatEnvironmentFailedFanoutDedupeKey(workspaceID workspace.ID, environmentID string, generation string) string {
	return formatQueueDedupeKey(KindEnvironmentFailedFanout, workspaceID, environmentID, generation)
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
	switch request.Kind {
	case KindRuntimeInput:
		runtimeInputKeys := []string{"workspace_id", "session_id", "session_thread_id", "runtime_input_id", "event_ids", "sequence_from", "sequence_to", "input_kind", "preparation_attempt_id"}
		if value, ok := payloadToken(rawPayload["preparation_attempt_id"]); !ok || value == "" {
			return &ValidationError{Message: "runtime_input preparation_attempt_id is invalid"}
		}
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
	case KindSessionPrepare:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "session_id", "preparation_attempt_id"); err != nil {
			return err
		}
		sessionID, preparationAttemptID, err := requiredPayloadTokens(payload, "session_id", "preparation_attempt_id")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request, FormatSessionPartitionKey(request.WorkspaceID, sessionID), FormatSessionPrepareDedupeKey(request.WorkspaceID, sessionID, preparationAttemptID))
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
	case KindEnvironmentFailedFanout:
		if err := validatePayloadKeys(rawPayload, "workspace_id", "environment_id", "generation"); err != nil {
			return err
		}
		environmentID, generation, err := requiredPayloadTokens(payload, "environment_id", "generation")
		if err != nil {
			return err
		}
		return requireCanonicalKeys(request, FormatEnvironmentPartitionKey(request.WorkspaceID, environmentID), FormatEnvironmentFailedFanoutDedupeKey(request.WorkspaceID, environmentID, generation))
	default:
		return &ValidationError{Message: "unknown queue job kind: " + request.Kind}
	}
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
	case KindRuntimeInput, KindRuntimeConfigUpdate, KindCleanupSession, KindSessionDeleteCleanup, KindSessionPrepare, KindEnvironmentBuild, KindEnvironmentReadyFanout, KindEnvironmentFailedFanout:
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
