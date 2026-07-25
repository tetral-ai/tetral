// Package driver owns provider-specific sandbox tool transport.
package driver

import "time"

type Config struct {
	DaytonaAPIURL             string
	DaytonaTarget             string
	DaytonaAPIKey             string
	PreparationCommandTimeout time.Duration
	Lifecycle                 LifecyclePolicy
}

type LifecyclePolicy struct {
	StopTimeout         time.Duration
	StopForceAfter      time.Duration
	AutoStopInterval    time.Duration
	AutoArchiveInterval time.Duration
	AutoDeleteInterval  time.Duration
}
