package joint

import (
	"crypto/rand"
	"encoding/hex"
)

// RunIDVariable passes a run's ID from the runner to its test processes; a
// test process started without it makes a run of its own. A run's
// resources lie under its roots or carry its ID in their names, so that a
// cleanup can take exactly its own run's.
const RunIDVariable = "TETRAL_JOINT_RUN_ID"

// NewRunID returns a fresh run ID of 12 hexadecimal digits.
func NewRunID() string {
	id := make([]byte, 6)
	_, _ = rand.Read(id)
	return hex.EncodeToString(id)
}

// MemoryRoot is the Linux directory holding the run's in-memory machine
// homes.
func MemoryRoot(runID string) string {
	return "/dev/shm/tetral-joint-" + runID
}

// MacRoot is the Mac directory, relative to the home directory, holding the
// run's machine directories, disk images and lane directory.
func MacRoot(runID string) string {
	return "tetral-test-machines/run-" + runID
}

// RAMVolume starts the name of each RAM disk volume the run erases on the
// Mac; the disk's device name follows it.
func RAMVolume(runID string) string {
	return "tetral-joint-" + runID + "-"
}
