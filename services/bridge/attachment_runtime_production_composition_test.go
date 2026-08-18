package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginefiles "github.com/tetral-ai/tetral/internal/files"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

type attachmentStartLostACKStore struct {
	BridgeAPIStore
	mu       sync.Mutex
	dropped  bool
	writeIDs []string
}

func (s *attachmentStartLostACKStore) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	response, err := s.BridgeAPIStore.WriteEvent(ctx, request)
	if err != nil || request.GetEventType() != "span.model_request_start" || len(request.GetConsumedFileAttachments()) == 0 {
		return response, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeIDs = append(s.writeIDs, request.GetRuntimeWriteId())
	if !s.dropped && response.GetCommitted() != nil {
		s.dropped = true
		return nil, status.Error(codes.Unavailable, "Request Start acknowledgement unavailable")
	}
	return response, nil
}

func (s *attachmentStartLostACKStore) facts() (bool, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped, append([]string(nil), s.writeIDs...)
}

func TestPostgreSQLAttachmentRequestStartLostACKReplaysBeforeSingleProviderInvocation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_attachment_start_lost_ack"
		threadID  = "thr_attachment_start_lost_ack"
		bindingID = "bind_attachment_start_lost_ack"
		podUID    = "pod_attachment_start_lost_ack"
		fileID    = "file_attachment_start_lost_ack"
	)
	seedBridgeAPISession(t, admin, string(workspace.DefaultID), sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, string(workspace.DefaultID), sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	client := dbconnect.NewClientForTesting(runtimeDB)
	attachmentStore := blob.NewFakeBlobStore()
	sourceEventID, runtimeInputID := birthAttachmentRuntimeInput(t, admin, client, attachmentStore, sessionID, fileID)
	baseStore := NewPostgreSQLBridgeAPIStore(client)
	baseStore.AttachmentBlobStore = attachmentStore
	baseStore.FileBlobStore = attachmentStore
	baseStore.RuntimeBindingTokenHMACKey = []byte("attachment-lost-ack-composition-key")
	lostACKStore := &attachmentStartLostACKStore{BridgeAPIStore: baseStore}
	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, lostACKStore)
	defer stopBridge()

	process := startAttachmentRecoveryRuntime(t, bridgeAddress, "complete", sessionID, threadID, bindingID, 1, podUID)
	deliverAttachmentRuntimeInput(t, runtimeDB, admin, process.port, sessionID, podUID)
	started := process.providerStart(t)
	waitForAttachmentRequestEnd(t, admin, process, sessionID, "")
	process.kill(t)

	dropped, writeIDs := lostACKStore.facts()
	if !dropped || len(writeIDs) != 2 || writeIDs[0] == "" || writeIDs[0] != writeIDs[1] {
		t.Fatalf("Request Start lost-ACK replay = dropped:%t writes:%v; want two identical declarations", dropped, writeIDs)
	}
	if started.ProviderInvocations != 1 || started.GatewayRequests != 1 || started.AttachmentCount != 1 || started.LoweredAttachmentBytes != int64(len("attachment")) {
		t.Fatalf("Runtime Gateway/provider invocation = %+v; want one lowered request with one attachment", started)
	}
	assertAttachmentRecoveryFacts(t, admin, sessionID, sourceEventID, fileID, runtimeInputID, 1, 1, 1)
}

func TestPostgreSQLAttachmentPostStartPodLossColdLoadsWithoutSecondProviderInvocation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_attachment_post_start_pod_loss"
		threadID  = "thr_attachment_post_start_pod_loss"
		bindingID = "bind_attachment_post_start_pod_loss"
		podUID    = "pod_attachment_post_start_pod_loss"
		fileID    = "file_attachment_post_start_pod_loss"
	)
	seedBridgeAPISession(t, admin, string(workspace.DefaultID), sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, string(workspace.DefaultID), sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	client := dbconnect.NewClientForTesting(runtimeDB)
	attachmentStore := blob.NewFakeBlobStore()
	sourceEventID, runtimeInputID := birthAttachmentRuntimeInput(t, admin, client, attachmentStore, sessionID, fileID)
	bridgeStore := NewPostgreSQLBridgeAPIStore(client)
	bridgeStore.AttachmentBlobStore = attachmentStore
	bridgeStore.FileBlobStore = attachmentStore
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("attachment-pod-loss-composition-key")
	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, bridgeStore)
	defer stopBridge()

	process := startAttachmentRecoveryRuntime(t, bridgeAddress, "hang", sessionID, threadID, bindingID, 1, podUID)
	deliverAttachmentRuntimeInput(t, runtimeDB, admin, process.port, sessionID, podUID)
	started := process.providerStart(t)
	if started.ProviderInvocations != 1 || started.GatewayRequests != 1 || started.AttachmentCount != 1 || started.LoweredAttachmentBytes != int64(len("attachment")) {
		t.Fatalf("started Gateway/provider invocation = %+v; want one lowered request with one attachment", started)
	}
	process.kill(t)

	repairStore := runtimePodLossSweepStore(runtimeDB, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := repairStore.RepairLostRuntimeBindings(context.Background(), string(workspace.DefaultID)); err != nil || repaired != 1 {
		t.Fatalf("repair post-Start Runtime pod loss = %d/%v; want 1/nil", repaired, err)
	}
	waitForAttachmentRequestEnd(t, admin, process, sessionID, "runtime_pod_lost")
	seedBridgeAPIRuntimeBinding(t, admin, string(workspace.DefaultID), sessionID, bindingID, 2, "pod_attachment_post_start_replacement")
	cold := runAttachmentColdReplacement(t, bridgeAddress, sessionID, threadID, bindingID, 2, "pod_attachment_post_start_replacement", fileID)
	if cold.PendingAttachments != 0 || cold.ProviderInvocations != 0 || cold.AttachmentInMessageOrCheckpoint || !cold.PreloadResult.OK {
		t.Fatalf("replacement cold load = %+v; want no attachment and no Provider invocation", cold)
	}

	second, err := bridgeStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope(sessionID, threadID, bindingID, 2, "pod_attachment_post_start_replacement"),
		RuntimeWriteId: "rwrite_attachment_post_start_second", ModelRequestId: "mreq_attachment_post_start_second",
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: requestKindAgentProviderRequest,
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{SourceEventId: sourceEventID, FileId: fileID}},
	})
	if status.Code(err) != codes.AlreadyExists || second != nil {
		t.Fatalf("post-repair distinct Start = %#v/%v; want AlreadyExists", second, err)
	}
	assertAttachmentRecoveryFacts(t, admin, sessionID, sourceEventID, fileID, runtimeInputID, 1, 1, 1)
}

func birthAttachmentRuntimeInput(t *testing.T, admin *sql.DB, client *dbconnect.Client, attachmentStore blob.BlobStore, sessionID, fileID string) (string, string) {
	t.Helper()
	seedBridgeAPIFileAttachment(t, admin, attachmentStore, fileID, "attachment.png", "image/png", "attachment")
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(
		client,
		sessionevent.WithFileAttachmentValidator(enginefiles.NewPostgreSQLStore(client, attachmentStore)),
	))
	appended, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID,
		"idem_"+sessionID, sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserMessage,
			Content: []sessionevent.ContentBlock{{
				Type:   sessionevent.ContentBlockTypeImage,
				Source: &sessionevent.ContentSource{Type: sessionevent.ContentSourceTypeFile, FileID: fileID},
			}},
		}}})
	if err != nil || len(appended.Data) != 1 {
		t.Fatalf("birth attachment Runtime input = %#v/%v", appended, err)
	}
	var runtimeInputID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id
		FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='messages'`, sessionID).Scan(&runtimeInputID); err != nil {
		t.Fatalf("read attachment Runtime input identity: %v", err)
	}
	return appended.Data[0].ID, runtimeInputID
}

func serveAttachmentCompositionBridge(t *testing.T, store BridgeAPIStore) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for attachment composition Bridge: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}

type attachmentRuntimeTokenSource struct{}

func (attachmentRuntimeTokenSource) Token(context.Context) (string, error) {
	return "attachment-runtime-composition-token", nil
}

func deliverAttachmentRuntimeInput(t *testing.T, runtimeDB, admin *sql.DB, port int, sessionID, podUID string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("align attachment Runtime binding: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "attachment-runtime-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver attachment Runtime input = active:%t err:%v", active, err)
	}
}

type attachmentRecoveryProcess struct {
	command             *exec.Cmd
	output              bytes.Buffer
	port                int
	providerStartedPath string
	acceptResultPath    string
	inspectResultPath   string
	closePath           string
}

func startAttachmentRecoveryRuntime(t *testing.T, bridgeAddress, mode, sessionID, threadID, bindingID string, generation int64, podUID string) *attachmentRecoveryProcess {
	t.Helper()
	tempDir := t.TempDir()
	readyPath := filepath.Join(tempDir, "ready.json")
	process := &attachmentRecoveryProcess{
		providerStartedPath: filepath.Join(tempDir, "provider-started.json"),
		acceptResultPath:    filepath.Join(tempDir, "accept-result.json"),
		inspectResultPath:   filepath.Join(tempDir, "inspect-result.json"),
		closePath:           filepath.Join(tempDir, "close"),
	}
	inputPath := filepath.Join(tempDir, "input.json")
	encoded, err := json.Marshal(map[string]any{
		"mode": mode, "bridgeAddress": bridgeAddress, "workspaceId": workspace.DefaultID,
		"sessionId": sessionID, "sessionThreadId": threadID, "bindingId": bindingID,
		"bindingGeneration": generation, "targetPodUid": podUID, "readyPath": readyPath,
		"acceptResultPath":    process.acceptResultPath,
		"inspectResultPath":   process.inspectResultPath,
		"providerStartedPath": process.providerStartedPath, "closePath": process.closePath,
	})
	if err != nil {
		t.Fatalf("encode attachment Runtime composition: %v", err)
	}
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write attachment Runtime composition: %v", err)
	}
	process.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/attachment-recovery-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	process.command.Dir = "../agent-runtime"
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start attachment Runtime composition: %v", err)
	}
	t.Cleanup(func() {
		if process.command.ProcessState == nil {
			_ = process.command.Process.Kill()
			_ = process.command.Wait()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(readyPath)
		if readErr == nil {
			var ready struct {
				Port int `json:"port"`
			}
			if json.Unmarshal(raw, &ready) == nil && ready.Port > 0 {
				process.port = ready.Port
				return process
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("attachment Runtime composition did not become ready: %s", process.output.String())
	return nil
}

func (p *attachmentRecoveryProcess) providerStart(t *testing.T) struct {
	ProviderInvocations    int   `json:"providerInvocations"`
	GatewayRequests        int   `json:"gatewayRequests"`
	AttachmentCount        int   `json:"attachmentCount"`
	LoweredAttachmentBytes int64 `json:"loweredAttachmentBytes"`
} {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(p.providerStartedPath)
		if err == nil {
			var started struct {
				ProviderInvocations    int   `json:"providerInvocations"`
				GatewayRequests        int   `json:"gatewayRequests"`
				AttachmentCount        int   `json:"attachmentCount"`
				LoweredAttachmentBytes int64 `json:"loweredAttachmentBytes"`
			}
			if json.Unmarshal(raw, &started) == nil {
				return started
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	accept, _ := os.ReadFile(p.acceptResultPath)
	inspected, _ := os.ReadFile(p.inspectResultPath)
	t.Fatalf("Provider invocation did not start: accept=%s inspect=%s output=%s", accept, inspected, p.output.String())
	return struct {
		ProviderInvocations    int   `json:"providerInvocations"`
		GatewayRequests        int   `json:"gatewayRequests"`
		AttachmentCount        int   `json:"attachmentCount"`
		LoweredAttachmentBytes int64 `json:"loweredAttachmentBytes"`
	}{}
}

func (p *attachmentRecoveryProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Kill(); err != nil {
		t.Fatalf("dispose attachment Runtime pod process: %v", err)
	}
	_ = p.command.Wait()
}

type attachmentColdReplacementOutput struct {
	PendingAttachments              int  `json:"pendingAttachments"`
	ProviderInvocations             int  `json:"providerInvocations"`
	AttachmentInMessageOrCheckpoint bool `json:"attachmentInMessageOrCheckpoint"`
	PreloadResult                   struct {
		OK bool `json:"ok"`
	} `json:"preloadResult"`
}

func runAttachmentColdReplacement(t *testing.T, bridgeAddress, sessionID, threadID, bindingID string, generation int64, podUID, fileID string) attachmentColdReplacementOutput {
	t.Helper()
	inputPath := filepath.Join(t.TempDir(), "cold-input.json")
	encoded, err := json.Marshal(map[string]any{
		"mode": "cold", "bridgeAddress": bridgeAddress, "workspaceId": workspace.DefaultID,
		"sessionId": sessionID, "sessionThreadId": threadID, "bindingId": bindingID,
		"bindingGeneration": generation, "targetPodUid": podUID, "fileId": fileID,
	})
	if err != nil {
		t.Fatalf("encode attachment cold replacement: %v", err)
	}
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write attachment cold replacement: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/attachment-recovery-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run attachment cold replacement: %v: %s", err, output)
	}
	var cold attachmentColdReplacementOutput
	if err := json.Unmarshal(output, &cold); err != nil {
		t.Fatalf("decode attachment cold replacement: %v: %s", err, output)
	}
	return cold
}

func waitForAttachmentRequestEnd(t *testing.T, admin *sql.DB, process *attachmentRecoveryProcess, sessionID, errorKind string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'
			  AND ($2='' OR payload_json::jsonb ->> 'error_kind'=$2)`, sessionID, errorKind).Scan(&count)
		if err == nil && count == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	var inboxStatus string
	var starts, ends, messages int
	var eventTypes string
	_ = admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='messages'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),
		(SELECT coalesce(string_agg(type, ',' ORDER BY sequence), '') FROM session_events WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&inboxStatus, &starts, &ends, &messages, &eventTypes)
	accept, _ := os.ReadFile(process.acceptResultPath)
	inspected, _ := os.ReadFile(process.inspectResultPath)
	t.Fatalf("Request End %q did not commit for %s: Inbox=%s starts=%d ends=%d messages=%d events=%s accept=%s inspect=%s output=%s",
		errorKind, sessionID, inboxStatus, starts, ends, messages, eventTypes, accept, inspected, process.output.String())
}

func assertAttachmentRecoveryFacts(t *testing.T, admin *sql.DB, sessionID, sourceEventID, fileID, runtimeInputID string, wantStarts, wantConsumptions, wantEnds int) {
	t.Helper()
	var starts, consumptions, ends int
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_file_attachment_consumptions WHERE workspace_id='default' AND source_event_id=$2 AND file_id=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end'),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$4),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb ->> 'runtime_input_id'=$4)`,
		sessionID, sourceEventID, fileID, runtimeInputID).Scan(&starts, &consumptions, &ends, &inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read attachment recovery facts: %v", err)
	}
	if starts != wantStarts || consumptions != wantConsumptions || ends != wantEnds || inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("attachment recovery facts = starts:%d consumptions:%d ends:%d Inbox:%s Queue:%s", starts, consumptions, ends, inboxStatus, queueStatus)
	}
}
