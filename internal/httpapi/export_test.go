package httpapi

import "github.com/tetral-ai/tetral/internal/workspace"

// DriveChangeSubscriberCountForTest observes handler-owned subscriptions while
// the listener is still running, before its shutdown clears them globally.
func DriveChangeSubscriberCountForTest(c *DriveChanges, ws workspace.ID, driveID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.subscribers[driveKey{ws: ws, driveID: driveID}])
}
