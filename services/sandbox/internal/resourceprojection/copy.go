package resourceprojection

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/tetral-ai/tetral/internal/blob"
)

// CopyStatus reports how a session-copy destination was reconciled. The
// underlying CopyObject is create-only and cannot overwrite (blob layer sends
// If-None-Match), so a size/ETag-mismatched destination is repaired
// delete-then-copy (recopied_mismatched) rather than overwritten, and a present
// matching destination is skipped (skipped_matching) — copy-if-absent. That
// copy-if-absent behavior is what lets a surviving session copy shield the
// session when its canonical object is later deleted: only a recopy of a
// now-missing canonical fails, and it fails as the copy error kind
// canonical_missing.
type CopyStatus string

const (
	CopyStatusCopied           CopyStatus = "copied"
	CopyStatusSkippedMatching  CopyStatus = "skipped_matching"
	CopyStatusRecopiedMismatch CopyStatus = "recopied_mismatched"
	CopyStatusSkippedRace      CopyStatus = "skipped_after_race"
)

type CopyExecutor struct {
	Blob blob.BlobStore
}

type CopyResult struct {
	ResourceID     string
	SourceKey      string
	DestinationKey string
	Status         CopyStatus
}

type CopyError struct {
	Kind       string
	Operation  string
	ResourceID string
	Cause      error
}

func (e *CopyError) Error() string {
	if e == nil {
		return ""
	}
	if e.Kind == "" {
		return "resource projection copy failed"
	}
	return "resource projection copy failed: " + e.Kind
}

func (e *CopyError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *CopyError) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, e.Error())
}

func (e CopyExecutor) CopyIfNeeded(ctx context.Context, action Action) (CopyResult, error) {
	result := CopyResult{
		ResourceID:     action.ResourceID,
		SourceKey:      action.SourceKey,
		DestinationKey: action.DestinationKey,
	}
	if e.Blob == nil {
		return result, &CopyError{Kind: "blob_store_required", Operation: "validate", ResourceID: action.ResourceID}
	}
	if action.Type != ActionCopyObject {
		return result, &CopyError{Kind: "invalid_action", Operation: "validate", ResourceID: action.ResourceID}
	}
	if action.SourceKey == "" || action.DestinationKey == "" {
		return result, &CopyError{Kind: "invalid_key", Operation: "validate", ResourceID: action.ResourceID}
	}
	destination, err := e.Blob.HeadObject(ctx, action.DestinationKey)
	if err == nil {
		status, err := e.handleExistingDestination(ctx, action, destination)
		result.Status = status
		return result, err
	}
	if !isBlobNotFound(err) {
		return result, copyErr("head_destination_failed", "head_destination", action, err)
	}
	status, err := e.copyMissingDestination(ctx, action)
	result.Status = status
	return result, err
}

func (e CopyExecutor) handleExistingDestination(ctx context.Context, action Action, destination blob.ObjectMetadata) (CopyStatus, error) {
	source, err := e.Blob.HeadObject(ctx, action.SourceKey)
	if err != nil {
		if isBlobNotFound(err) {
			return CopyStatusSkippedMatching, nil
		}
		return "", copyErr("head_source_failed", "head_source", action, err)
	}
	if objectMetadataMatches(source, destination) {
		return CopyStatusSkippedMatching, nil
	}
	if err := e.Blob.Delete(ctx, action.DestinationKey); err != nil && !isBlobNotFound(err) {
		return "", copyErr("delete_mismatched_failed", "delete_destination", action, err)
	}
	_, err = e.copyMissingDestination(ctx, action)
	if err != nil {
		return "", err
	}
	return CopyStatusRecopiedMismatch, nil
}

func (e CopyExecutor) copyMissingDestination(ctx context.Context, action Action) (CopyStatus, error) {
	err := e.Blob.CopyObject(ctx, action.SourceKey, action.DestinationKey)
	if err == nil {
		return CopyStatusCopied, nil
	}
	if isBlobNotFound(err) {
		return "", copyErr("canonical_missing", "copy_object", action, err)
	}
	if isBlobDuplicate(err) {
		return e.handleDuplicateDestination(ctx, action, err)
	}
	return "", copyErr("copy_failed", "copy_object", action, err)
}

func (e CopyExecutor) handleDuplicateDestination(ctx context.Context, action Action, cause error) (CopyStatus, error) {
	destination, err := e.Blob.HeadObject(ctx, action.DestinationKey)
	if err != nil {
		return "", copyErr("copy_race_unresolved", "head_destination_after_duplicate", action, cause)
	}
	status, err := e.handleExistingDestination(ctx, action, destination)
	if err != nil {
		return "", err
	}
	if status == CopyStatusSkippedMatching {
		return CopyStatusSkippedRace, nil
	}
	return status, nil
}

func objectMetadataMatches(source blob.ObjectMetadata, destination blob.ObjectMetadata) bool {
	if source.SizeBytes != destination.SizeBytes {
		return false
	}
	if source.ETag != "" || destination.ETag != "" {
		return source.ETag != "" && source.ETag == destination.ETag
	}
	return true
}

func copyErr(kind string, operation string, action Action, cause error) *CopyError {
	return &CopyError{
		Kind:       kind,
		Operation:  operation,
		ResourceID: action.ResourceID,
		Cause:      cause,
	}
}

func isBlobNotFound(err error) bool {
	var notFound *blob.NotFoundError
	return errors.As(err, &notFound)
}

func isBlobDuplicate(err error) bool {
	var duplicate *blob.DuplicateKeyError
	return errors.As(err, &duplicate)
}
