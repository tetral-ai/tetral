package mcpmanifest

import (
	"strconv"
)

func InputID(sessionID string, mcpServerName string, manifestGeneration int64) string {
	return "runtime_config_update:mcp_manifest:" + sessionID + ":" + mcpServerName + ":" + strconv.FormatInt(manifestGeneration, 10)
}
