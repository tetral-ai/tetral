// Package driver owns provider-specific sandbox tool transport.
package driver

import "time"

type Config struct {
	DaytonaAPIURL string
	DaytonaTarget string
	DaytonaAPIKey string
	// ArtifactBaseImage is the image every environment artifact build starts
	// FROM. It must name the deployed sandbox image: a drifting or absent tag
	// fails every non-empty-packages environment build at its first
	// Dockerfile line.
	ArtifactBaseImage string
	CommandTimeout    time.Duration
	Lifecycle         LifecyclePolicy
}

type LifecyclePolicy struct {
	StopTimeout         time.Duration
	AutoStopInterval    time.Duration
	AutoArchiveInterval time.Duration
	AutoDeleteInterval  time.Duration
}
