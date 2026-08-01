package agentruntimebridge

import (
	"context"
	"errors"

	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/services/bridge/internal/outputcapture"
)

var ErrSandboxOutputCaptureUnavailable = errors.New("sandbox output capture is unavailable")
var ErrSandboxMemoryProjectionUnavailable = errors.New("sandbox memory projection is unavailable")

func sandboxHelperFailureError(err error) bool {
	var helperErr *sandboxdriver.HelperFailureError
	return errors.As(err, &helperErr)
}

type SandboxDriverToolExecutor struct {
	Driver sandboxdriver.ToolExecutor
}

func NewSandboxDriverToolExecutor(driver sandboxdriver.ToolExecutor) *SandboxDriverToolExecutor {
	return &SandboxDriverToolExecutor{Driver: driver}
}

func (e *SandboxDriverToolExecutor) ScanOutputs(ctx context.Context, target outputcapture.SandboxOutputTarget) (outputcapture.SandboxOutputScan, error) {
	capturer, ok := e.Driver.(sandboxdriver.OutputCapturer)
	if !ok {
		return outputcapture.SandboxOutputScan{}, ErrSandboxOutputCaptureUnavailable
	}
	result, err := capturer.CaptureOutputs(ctx, sandboxdriver.OutputCaptureTarget{
		WorkspaceID:       target.WorkspaceID,
		SessionID:         target.SessionID,
		SessionThreadID:   target.SessionThreadID,
		BindingID:         target.BindingID,
		BindingGeneration: target.BindingGeneration,
		SandboxID:         target.SandboxID,
		ProviderSandboxID: target.ProviderSandboxID,
		MaxFiles:          target.MaxFiles,
		MaxFileBytes:      target.MaxFileBytes,
		MaxTotalBytes:     target.MaxTotalBytes,
	})
	if err != nil {
		return outputcapture.SandboxOutputScan{}, err
	}
	files := make([]outputcapture.SandboxOutputFile, 0, len(result.Files))
	for _, file := range result.Files {
		open := file.Open
		files = append(files, outputcapture.SandboxOutputFile{
			SourcePath: file.SourcePath,
			Kind:       file.Kind,
			LinkCount:  file.LinkCount,
			SizeBytes:  file.SizeBytes,
			SHA256:     file.SHA256,
			MIMEType:   file.MIMEType,
			Skipped:    file.Skipped,
			SkipReason: file.SkipReason,
			Open:       open,
		})
	}
	records := make([]outputcapture.ScanRecord, 0, len(result.Records))
	for _, record := range result.Records {
		records = append(records, outputcapture.ScanRecord{
			ParentPath: record.ParentPath,
			Reason:     record.Reason,
			Count:      record.Count,
		})
	}
	return outputcapture.SandboxOutputScan{
		Files:                files,
		Truncated:            result.Truncated,
		UnrepresentableNames: result.UnrepresentableNames,
		Records:              records,
	}, nil
}

func (e *SandboxDriverToolExecutor) RefreshMemoryProjection(ctx context.Context, refresh MemoryProjectionRefresh) error {
	refresher, ok := e.Driver.(sandboxdriver.MemoryProjectionRefresher)
	if !ok {
		return ErrSandboxMemoryProjectionUnavailable
	}
	ops := make([]sandboxdriver.MemoryProjectionOp, 0, len(refresh.Ops))
	for _, op := range refresh.Ops {
		ops = append(ops, sandboxdriver.MemoryProjectionOp{
			Kind:          op.Kind,
			RelativePath:  op.RelativePath,
			Content:       op.Content,
			ContentSHA256: op.ContentSHA256,
		})
	}
	return refresher.RefreshMemoryProjection(ctx, sandboxdriver.MemoryProjectionRefresh{
		Target:     toDriverTarget(refresh.Target),
		MountPaths: append([]string(nil), refresh.MountPaths...),
		Ops:        ops,
	})
}

func toDriverTarget(target SandboxToolTarget) sandboxdriver.ToolTarget {
	return sandboxdriver.ToolTarget{
		WorkspaceID:       target.WorkspaceID,
		SessionID:         target.SessionID,
		SessionThreadID:   target.SessionThreadID,
		BindingID:         target.BindingID,
		BindingGeneration: target.BindingGeneration,
		SandboxID:         target.SandboxID,
		ProviderSandboxID: target.ProviderSandboxID,
		ResourceRootsJSON: target.ResourceRootsJSON,
	}
}
