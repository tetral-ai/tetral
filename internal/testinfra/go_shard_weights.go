package testinfra

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"time"
)

//go:embed go_shard_weights.json
var goShardWeightsJSON []byte

type goShardCalibration struct {
	Version      int                             `json:"version"`
	CalibratedAt string                          `json:"calibrated_at"`
	Packages     map[string]goPackageCalibration `json:"packages"`
}

type goPackageCalibration struct {
	DefaultMS int            `json:"default_ms"`
	TestsMS   map[string]int `json:"tests_ms"`
}

func loadGoShardCalibration() (goShardCalibration, error) {
	var value goShardCalibration
	if err := json.Unmarshal(goShardWeightsJSON, &value); err != nil {
		return value, fmt.Errorf("decode Go shard calibration: %w", err)
	}
	if value.Version != 1 || len(value.Packages) == 0 {
		return value, fmt.Errorf("unsupported or empty Go shard calibration")
	}
	if _, err := time.Parse(time.DateOnly, value.CalibratedAt); err != nil {
		return value, fmt.Errorf("go shard calibration date is malformed")
	}
	for name, item := range value.Packages {
		if name == "" || item.DefaultMS <= 0 {
			return value, fmt.Errorf("invalid Go shard calibration package %q", name)
		}
		for test, weight := range item.TestsMS {
			if test == "" || weight <= 0 {
				return value, fmt.Errorf("invalid Go shard calibration test %q", test)
			}
		}
	}
	return value, nil
}
