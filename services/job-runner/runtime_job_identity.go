package jobrunner

import "github.com/tetral-ai/tetral/internal/runtimecontrol"

func (job RuntimeJob) inputIdentity() runtimecontrol.InputIdentity {
	return runtimecontrol.InputIdentity{
		WorkspaceID:     job.WorkspaceID,
		SessionID:       job.SessionID,
		SessionThreadID: job.SessionThreadID,
		RuntimeInputID:  job.RuntimeInputID,
		InputKind:       job.InputKind,
		EventIDs:        job.EventIDs,
		SequenceFrom:    job.SequenceFrom,
		SequenceTo:      job.SequenceTo,
	}
}
