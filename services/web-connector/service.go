package webconnector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

type Service struct {
	providergatewayv1.UnimplementedProviderGatewayServiceServer
	store    *SnapshotStore
	backend  Backend
	verifier *BindingVerifier
	metrics  *Metrics
	now      func() time.Time
	logger   *slog.Logger
}

func (s *Service) WithLogger(logger *slog.Logger) *Service { s.logger = logger; return s }

type jobRecord struct {
	InputHash string          `json:"input_hash"`
	Response  json.RawMessage `json:"response"`
	SettledAt string          `json:"settled_at"`
}

var (
	errIdempotencyConflict = errors.New("idempotency conflict")
	errInvalidJobRecord    = errors.New("invalid job record")
)

type executionResult struct {
	response    *providergatewayv1.RunWebResponse
	createdKeys []string
}

func NewService(blobs blob.BlobStore, backend Backend, verifier *BindingVerifier, metrics *Metrics, now func() time.Time, random RandomRead) *Service {
	if now == nil {
		now = time.Now
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	return &Service{store: NewSnapshotStore(blobs, random, now), backend: backend, verifier: verifier, metrics: metrics, now: now}
}

func (s *Service) RunWeb(ctx context.Context, request *providergatewayv1.RunWebRequest) (response *providergatewayv1.RunWebResponse, err error) {
	started := s.now()
	operation := operationLabel(nil)
	statusLabel := "runtime_error"
	defer func() {
		if response != nil {
			operation = response.GetUsage().GetOperation()
			statusLabel = statusName(response.GetStatus())
		}
		s.metrics.ObserveRequest(operation, statusLabel, s.now().Sub(started))
	}()
	identity, ok := grpcauth.IdentityFromContext(ctx)
	if !ok || identity.KubernetesPodUID == "" {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid internal request")
	}
	operation = operationLabel(request.GetInput())
	if validationText := validateSemanticEnvelope(request); validationText != "" {
		return s.errorResponse(providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR, validationText, operation, started), nil
	}
	if !validStructuralRequest(request) {
		return nil, status.Error(codes.InvalidArgument, "invalid internal request")
	}
	if s.verifier == nil || !s.verifier.Verify(request, identity.KubernetesPodUID) {
		return nil, status.Error(codes.PermissionDenied, "runtime binding token rejected")
	}
	scope := Scope{request.GetWorkspaceId(), request.GetSessionId(), request.GetSessionThreadId()}
	inputHash := CanonicalInputHash(canonicalInput(request.GetInput()))
	if replay, replayErr := s.loadJob(ctx, scope, request.GetToolUseEventId(), inputHash); replayErr == nil {
		return replay, nil
	} else if errors.Is(replayErr, errIdempotencyConflict) {
		if s.logger != nil {
			s.logger.Warn("web.idempotency.conflict", slog.String("operation", "web.idempotency.lookup"), slog.String("error.code", "idempotency_conflict"))
		}
		return s.errorResponse(providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, "tool delivery conflict", operation, started), nil
	} else if !isNotFound(replayErr) {
		if s.logger != nil {
			s.logger.Warn("web.idempotency.lookup_failed", slog.String("operation", "web.idempotency.lookup"), slog.String("error.code", "cache_unavailable"))
		}
		return s.errorResponse(providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, "web backend temporarily unavailable", operation, started), nil
	}
	executed := s.execute(ctx, scope, request.GetInput(), operation, started)
	response = executed.response
	response.Usage.DurationMs = s.now().Sub(started).Milliseconds()
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
		s.store.DeleteKeys(ctx, executed.createdKeys)
		executed.createdKeys = nil
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED &&
		response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR {
		return response, nil
	}
	if persistErr := s.persistJob(ctx, scope, request.GetToolUseEventId(), inputHash, response); persistErr != nil {
		if isDuplicate(persistErr) {
			winner, winnerErr := s.loadJob(ctx, scope, request.GetToolUseEventId(), inputHash)
			if winnerErr == nil {
				s.store.DeleteKeys(ctx, executed.createdKeys)
				return winner, nil
			}
			if errors.Is(winnerErr, errIdempotencyConflict) {
				s.store.DeleteKeys(ctx, executed.createdKeys)
				return s.errorResponseWithUsage(providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, "tool delivery conflict", response.GetUsage(), started), nil
			}
		}
		s.store.DeleteKeys(ctx, executed.createdKeys)
		return s.errorResponseWithUsage(providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR, "web backend temporarily unavailable", response.GetUsage(), started), nil
	}
	return response, nil
}

func (s *Service) execute(ctx context.Context, scope Scope, input *providergatewayv1.WebToolInput, operation string, started time.Time) executionResult {
	usage := &providergatewayv1.WebUsage{Operation: operation}
	blocks := make([]string, 0)
	refs := make([]*providergatewayv1.WebRef, 0)
	seen := map[string]bool{}
	createdKeys := make([]string, 0)
	var nextLine *int32
	windowTruncated := false
	sourceIncomplete := false
	var targetHTTPStatus *int32
	targetHTTPStatusCount := 0
	recordTargetHTTPStatus := func(value *int32) {
		if value == nil {
			return
		}
		targetHTTPStatusCount++
		targetHTTPStatus = int32ptr(*value)
	}
	finalizeTargetHTTPStatus := func() {
		if targetHTTPStatusCount == 1 {
			usage.TargetHttpStatus = targetHTTPStatus
		} else {
			usage.TargetHttpStatus = nil
		}
	}
	finishError := func(kind BackendOutcomeKind, text string) executionResult {
		statusValue := providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR
		if kind == BackendRuntimeError || kind == BackendUnavailable {
			statusValue = providergatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR
			text = "web backend temporarily unavailable"
		}
		finalizeTargetHTTPStatus()
		usage.DurationMs = s.now().Sub(started).Milliseconds()
		return executionResult{response: &providergatewayv1.RunWebResponse{Status: statusValue, ResultText: text, Refs: refs, Usage: usage}, createdKeys: createdKeys}
	}
	for _, query := range input.GetSearchQuery() {
		if len(query.GetDomains()) > maxSearchDomains {
			return finishError(BackendToolError, fmt.Sprintf("too many domains (max %d)", maxSearchDomains))
		}
		hits, outcome := s.backend.Search(ctx, query.GetQ(), query.GetDomains())
		s.observeOutcome("search", outcome)
		usage.BackendTokens += outcome.Tokens
		usage.WebSearchRequests += outcome.Requests
		if outcome.Kind != BackendSuccess {
			return finishError(outcome.Kind, backendToolText(outcome))
		}
		successful := outcome.Requests
		if successful == 0 {
			successful = 1
			if len(query.GetDomains()) > 1 {
				successful = int64(len(query.GetDomains()))
			}
		}
		if outcome.Requests == 0 {
			usage.WebSearchRequests += successful
		}
		itemRefs := make([]Ref, 0, len(hits))
		for _, hit := range hits {
			ref, written, storeErr := s.store.StoreStub(ctx, scope, hit)
			if storeErr != nil {
				return finishError(BackendRuntimeError, "")
			}
			usage.StoredBytes += written
			s.metrics.ObserveCacheBytes(written)
			createdKeys = append(createdKeys, metaKey(scope, ref.ID))
			itemRefs = append(itemRefs, ref)
			appendProtoRef(&refs, seen, ref)
		}
		blocks = append(blocks, formatSearch(query.GetQ(), query.GetDomains(), hits, itemRefs))
	}
	for _, open := range input.GetOpen() {
		var page Page
		var ref Ref
		var openErr string
		if open.Url != nil && open.GetUrl() != "" {
			classification := ClassifyURL(open.GetUrl())
			if !classification.Allowed {
				return finishError(BackendToolError, "URL not allowed: "+classification.Reason)
			}
			outcomePage, outcome := s.backend.Fetch(ctx, open.GetUrl())
			s.observeOutcome("reader", outcome)
			usage.BackendTokens += outcome.Tokens
			usage.WebFetchRequests += outcome.Requests
			recordTargetHTTPStatus(outcome.TargetHTTPStatus)
			if outcome.Kind != BackendSuccess {
				return finishError(outcome.Kind, backendToolText(outcome))
			}
			if outcome.Requests == 0 {
				usage.WebFetchRequests++
			}
			page = outcomePage
			storedRef, written, storeErr := s.store.StorePage(ctx, scope, page)
			if storeErr != nil {
				return finishError(BackendRuntimeError, "")
			}
			ref = storedRef
			usage.StoredBytes += written
			s.metrics.ObserveCacheBytes(written)
			createdKeys = append(createdKeys, docKey(scope, ref.ID))
			storedPage, loadErr := s.store.LoadPage(ctx, scope, ref.ID)
			if loadErr != nil {
				return finishError(BackendRuntimeError, "")
			}
			page = storedPage
		} else {
			var openKind BackendOutcomeKind
			var fetchedTargetHTTPStatus *int32
			page, ref, openKind, openErr, fetchedTargetHTTPStatus = s.pageForRef(ctx, scope, open.GetRefId(), usage)
			recordTargetHTTPStatus(fetchedTargetHTTPStatus)
			if openErr != "" {
				return finishError(openKind, openErr)
			}
		}
		lineno := int32(1)
		if open.Lineno != nil {
			lineno = open.GetLineno()
		}
		window, windowErr := formatWindow(ref.ID, page, lineno)
		if windowErr != nil {
			return finishError(BackendToolError, windowErr.Error())
		}
		blocks = append(blocks, window.text)
		appendProtoRef(&refs, seen, window.ref)
		if len(input.GetOpen()) == 1 {
			nextLine = window.next
			windowTruncated = window.truncated
		}
		sourceIncomplete = sourceIncomplete || page.SourceIncomplete
	}
	for _, find := range input.GetFind() {
		page, ref, findKind, textErr, fetchedTargetHTTPStatus := s.pageForRef(ctx, scope, find.GetRefId(), usage)
		recordTargetHTTPStatus(fetchedTargetHTTPStatus)
		if textErr != "" {
			return finishError(findKind, textErr)
		}
		text, findErr := formatFind(ref.ID, find.GetPattern(), page)
		if findErr != nil {
			return finishError(BackendToolError, "invalid pattern: "+findErr.Error())
		}
		blocks = append(blocks, text)
		appendProtoRef(&refs, seen, Ref{ID: ref.ID, URL: page.URL, Title: page.Title, TotalLines: int32(len(page.Lines))})
		sourceIncomplete = sourceIncomplete || page.SourceIncomplete
	}
	finalizeTargetHTTPStatus()
	usage.DurationMs = s.now().Sub(started).Milliseconds()
	return executionResult{response: &providergatewayv1.RunWebResponse{Status: providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED, ResultText: strings.Join(blocks, "\n\n"), Refs: refs, NextLineno: nextLine, WindowTruncated: windowTruncated, SourceIncomplete: sourceIncomplete, Usage: usage}, createdKeys: createdKeys}
}

func (s *Service) pageForRef(ctx context.Context, scope Scope, id string, usage *providergatewayv1.WebUsage) (Page, Ref, BackendOutcomeKind, string, *int32) {
	if !validRefID(id) {
		return Page{}, Ref{}, BackendToolError, fmt.Sprintf("invalid or expired ref: %s; re-open by URL if needed", id), nil
	}
	page, err := s.store.LoadPage(ctx, scope, id)
	if err == nil {
		return page, Ref{ID: id, URL: page.URL, Title: page.Title, TotalLines: int32(len(page.Lines))}, BackendSuccess, "", nil
	}
	if !isNotFound(err) {
		return Page{}, Ref{}, BackendRuntimeError, "web backend temporarily unavailable", nil
	}
	meta, metaErr := s.store.LoadMeta(ctx, scope, id)
	if metaErr != nil {
		if isNotFound(metaErr) {
			return Page{}, Ref{}, BackendToolError, fmt.Sprintf("invalid or expired ref: %s; re-open by URL if needed", id), nil
		}
		return Page{}, Ref{}, BackendRuntimeError, "web backend temporarily unavailable", nil
	}
	classification := ClassifyURL(meta.URL)
	if !classification.Allowed {
		return Page{}, Ref{}, BackendToolError, "URL not allowed: " + classification.Reason, nil
	}
	page, outcome := s.backend.Fetch(ctx, meta.URL)
	s.observeOutcome("reader", outcome)
	usage.BackendTokens += outcome.Tokens
	usage.WebFetchRequests += outcome.Requests
	if outcome.Kind != BackendSuccess {
		return Page{}, Ref{}, outcome.Kind, backendToolText(outcome), outcome.TargetHTTPStatus
	}
	if outcome.Requests == 0 {
		usage.WebFetchRequests++
	}
	ref, written, storeErr := s.store.StorePageForRef(ctx, scope, id, page)
	if storeErr != nil {
		return Page{}, Ref{}, BackendRuntimeError, "web backend temporarily unavailable", outcome.TargetHTTPStatus
	}
	usage.StoredBytes += written
	s.metrics.ObserveCacheBytes(written)
	loaded, loadErr := s.store.LoadPage(ctx, scope, id)
	if loadErr == nil {
		page = loaded
	}
	return page, ref, BackendSuccess, "", outcome.TargetHTTPStatus
}

func validRefID(value string) bool {
	if len(value) != 28 || !strings.HasPrefix(value, "r_") {
		return false
	}
	for _, r := range value[2:] {
		if (r < 'a' || r > 'z') && (r < '2' || r > '7') {
			return false
		}
	}
	return true
}

func (s *Service) persistJob(ctx context.Context, scope Scope, eventID, inputHash string, response *providergatewayv1.RunWebResponse) error {
	baseStoredBytes := response.GetUsage().GetStoredBytes()
	target := baseStoredBytes
	for i := 0; i < 8; i++ {
		candidate := proto.Clone(response).(*providergatewayv1.RunWebResponse)
		candidate.Usage.StoredBytes = target
		responseRaw, _ := protojson.MarshalOptions{UseProtoNames: true}.Marshal(candidate)
		recordRaw, _ := json.Marshal(jobRecord{InputHash: inputHash, Response: responseRaw, SettledAt: s.now().UTC().Format(time.RFC3339Nano)})
		next := baseStoredBytes + int64(len(recordRaw))
		if next == target {
			err := s.store.PutJob(ctx, scope, eventID, recordRaw)
			if err == nil {
				response.Usage.StoredBytes = target
				s.metrics.ObserveCacheBytes(int64(len(recordRaw)))
			}
			return err
		}
		target = next
	}
	candidate := proto.Clone(response).(*providergatewayv1.RunWebResponse)
	candidate.Usage.StoredBytes = target
	responseRaw, _ := protojson.MarshalOptions{UseProtoNames: true}.Marshal(candidate)
	recordRaw, _ := json.Marshal(jobRecord{InputHash: inputHash, Response: responseRaw, SettledAt: s.now().UTC().Format(time.RFC3339Nano)})
	err := s.store.PutJob(ctx, scope, eventID, recordRaw)
	if err == nil {
		response.Usage.StoredBytes = target
		s.metrics.ObserveCacheBytes(int64(len(recordRaw)))
	}
	return err
}
func (s *Service) loadJob(ctx context.Context, scope Scope, eventID, inputHash string) (*providergatewayv1.RunWebResponse, error) {
	raw, err := s.store.GetJob(ctx, scope, eventID)
	if err != nil {
		return nil, err
	}
	var record jobRecord
	if json.Unmarshal(raw, &record) != nil {
		return nil, errInvalidJobRecord
	}
	if !validInputHash(record.InputHash) || len(record.Response) == 0 {
		return nil, errInvalidJobRecord
	}
	if _, parseErr := time.Parse(time.RFC3339Nano, record.SettledAt); parseErr != nil {
		return nil, errInvalidJobRecord
	}
	if record.InputHash != inputHash {
		return nil, errIdempotencyConflict
	}
	response := &providergatewayv1.RunWebResponse{}
	if protojson.Unmarshal(record.Response, response) != nil {
		return nil, errInvalidJobRecord
	}
	if response.GetStatus() == providergatewayv1.RunWebStatus_RUN_WEB_STATUS_UNSPECIFIED || response.GetUsage() == nil {
		return nil, errInvalidJobRecord
	}
	return response, nil
}

func validInputHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func (s *Service) errorResponse(value providergatewayv1.RunWebStatus, textValue, operation string, started time.Time) *providergatewayv1.RunWebResponse {
	return &providergatewayv1.RunWebResponse{Status: value, ResultText: textValue, Usage: &providergatewayv1.WebUsage{Operation: operation, DurationMs: s.now().Sub(started).Milliseconds()}}
}

func (s *Service) errorResponseWithUsage(value providergatewayv1.RunWebStatus, textValue string, usage *providergatewayv1.WebUsage, started time.Time) *providergatewayv1.RunWebResponse {
	if usage == nil {
		usage = &providergatewayv1.WebUsage{}
	} else {
		usage = proto.Clone(usage).(*providergatewayv1.WebUsage)
	}
	usage.DurationMs = s.now().Sub(started).Milliseconds()
	return &providergatewayv1.RunWebResponse{Status: value, ResultText: textValue, Usage: usage}
}
func (s *Service) observeOutcome(api string, outcome BackendOutcome) {
	if managed, ok := s.backend.(interface{ ManagesMetrics() bool }); ok && managed.ManagesMetrics() {
		return
	}
	name := "success"
	if outcome.Kind != BackendSuccess {
		name = "error"
	}
	s.metrics.ObserveBackend(api, name, outcome.Tokens)
}

func validateSemanticEnvelope(r *providergatewayv1.RunWebRequest) string {
	if r.GetWorkspaceId() == "" || r.GetSessionId() == "" || r.GetSessionThreadId() == "" || r.GetToolUseEventId() == "" {
		return "invalid web request: missing scope or event identity"
	}
	input := r.GetInput()
	if input == nil {
		return "invalid web request: empty input"
	}
	count := len(input.GetSearchQuery()) + len(input.GetOpen()) + len(input.GetFind())
	if count == 0 {
		return "invalid web request: empty input"
	}
	if count > maxOperations {
		return "invalid web request: input item limit exceeded"
	}
	return ""
}
func validStructuralRequest(r *providergatewayv1.RunWebRequest) bool {
	for _, v := range []string{r.GetWorkspaceId(), r.GetSessionId(), r.GetSessionThreadId(), r.GetToolUseEventId(), r.GetBindingId()} {
		if len([]byte(v)) > 128 || !utf8.ValidString(v) {
			return false
		}
	}
	if r.GetBindingGeneration() <= 0 || r.GetBindingGeneration() > 0xffffffff || len([]byte(r.GetRuntimeBindingToken())) == 0 || len([]byte(r.GetRuntimeBindingToken())) > 1024 {
		return false
	}
	for _, q := range r.GetInput().GetSearchQuery() {
		if q.GetQ() == "" || len([]byte(q.GetQ())) > 64*1024 {
			return false
		}
		for _, d := range q.GetDomains() {
			if d == "" || len([]byte(d)) > 253 {
				return false
			}
		}
	}
	for _, o := range r.GetInput().GetOpen() {
		hasURL := o.Url != nil && o.GetUrl() != ""
		hasRef := o.RefId != nil && o.GetRefId() != ""
		if hasURL == hasRef {
			return false
		}
		if o.Url != nil && len([]byte(o.GetUrl())) > 64*1024 {
			return false
		}
		if o.RefId != nil && len([]byte(o.GetRefId())) > 128 {
			return false
		}
	}
	for _, f := range r.GetInput().GetFind() {
		if f.GetRefId() == "" || len([]byte(f.GetRefId())) > 128 {
			return false
		}
	}
	return true
}
func operationLabel(input *providergatewayv1.WebToolInput) string {
	if input == nil {
		return "mixed"
	}
	kinds := 0
	label := ""
	if len(input.GetSearchQuery()) > 0 {
		kinds++
		label = "search"
	}
	if len(input.GetOpen()) > 0 {
		kinds++
		label = "open"
	}
	if len(input.GetFind()) > 0 {
		kinds++
		label = "find"
	}
	if kinds != 1 {
		return "mixed"
	}
	return label
}
func canonicalInput(input *providergatewayv1.WebToolInput) map[string]any {
	result := map[string]any{}
	if len(input.GetSearchQuery()) > 0 {
		items := make([]any, 0, len(input.GetSearchQuery()))
		for _, q := range input.GetSearchQuery() {
			item := map[string]any{"q": q.GetQ()}
			if len(q.GetDomains()) > 0 {
				item["domains"] = q.GetDomains()
			}
			items = append(items, item)
		}
		result["search_query"] = items
	}
	if len(input.GetOpen()) > 0 {
		items := make([]any, 0, len(input.GetOpen()))
		for _, o := range input.GetOpen() {
			item := map[string]any{}
			if o.Url != nil {
				item["url"] = o.GetUrl()
			}
			if o.RefId != nil {
				item["ref_id"] = o.GetRefId()
			}
			if o.Lineno != nil {
				item["lineno"] = o.GetLineno()
			}
			items = append(items, item)
		}
		result["open"] = items
	}
	if len(input.GetFind()) > 0 {
		items := make([]any, 0, len(input.GetFind()))
		for _, f := range input.GetFind() {
			items = append(items, map[string]any{"ref_id": f.GetRefId(), "pattern": f.GetPattern()})
		}
		result["find"] = items
	}
	return result
}
func appendProtoRef(refs *[]*providergatewayv1.WebRef, seen map[string]bool, ref Ref) {
	if seen[ref.ID] {
		return
	}
	seen[ref.ID] = true
	value := &providergatewayv1.WebRef{RefId: ref.ID, Url: ref.URL}
	if ref.Title != "" {
		value.Title = stringptr(ref.Title)
	}
	if ref.LineStart > 0 {
		value.LineStart = int32ptr(ref.LineStart)
	}
	if ref.LineEnd > 0 {
		value.LineEnd = int32ptr(ref.LineEnd)
	}
	if ref.TotalLines > 0 {
		value.TotalLines = int32ptr(ref.TotalLines)
	}
	*refs = append(*refs, value)
}
func backendToolText(outcome BackendOutcome) string {
	if outcome.TargetHTTPStatus != nil {
		return fmt.Sprintf("target returned HTTP %d", *outcome.TargetHTTPStatus)
	}
	return "URL could not be fetched"
}
func statusName(value providergatewayv1.RunWebStatus) string {
	switch value {
	case providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED:
		return "completed"
	case providergatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR:
		return "tool_error"
	default:
		return "runtime_error"
	}
}
func int32ptr(value int32) *int32    { return &value }
func stringptr(value string) *string { return &value }
