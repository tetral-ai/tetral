package runtimecontrol

import (
	"database/sql"
)

const RuntimeTaskNotificationPayloadMaxBytes = 16 * 1024

const RequestKindAgentProviderRequest = "agent_provider_request"

const RequestKindCompactionSummary = "compaction_summary"

const RequestKindApprovalReviewer = "approval_reviewer"

func RowsAffected(result sql.Result) bool {
	if result == nil {
		return false
	}
	count, err := result.RowsAffected()
	return err == nil && count > 0
}

func DefaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func NullableJSONString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}
