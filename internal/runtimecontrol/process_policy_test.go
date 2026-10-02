package runtimecontrol

import (
	"testing"
	"time"
)

func TestProcessPolicyConfiguredRelationships(t *testing.T) {
	configured := map[string]string{EnvProcessRegistrationTimeout: "90", EnvProcessReportTimeout: "20", EnvProcessReportInterval: "50", EnvProcessFreshness: "150"}
	policy, err := ProcessPolicyFromEnv(func(key string) string { return configured[key] })
	if err != nil {
		t.Fatal(err)
	}
	if policy.RegistrationTimeout != 90*time.Millisecond || policy.ReportTimeout != 20*time.Millisecond || policy.ReportInterval != 50*time.Millisecond || policy.Freshness != 150*time.Millisecond {
		t.Fatal("configured process policy did not reach typed owner")
	}
	configured[EnvProcessReportTimeout] = "50"
	if _, err := ProcessPolicyFromEnv(func(key string) string { return configured[key] }); err == nil {
		t.Fatal("report timeout is not shorter than interval")
	}
	configured[EnvProcessReportTimeout] = "20"
	configured[EnvProcessFreshness] = "50"
	if _, err := ProcessPolicyFromEnv(func(key string) string { return configured[key] }); err == nil {
		t.Fatal("freshness is not greater than interval")
	}
	configured[EnvProcessFreshness] = "150"
	configured[EnvProcessRegistrationTimeout] = "9223372036854775807"
	if _, err := ProcessPolicyFromEnv(func(key string) string { return configured[key] }); err == nil {
		t.Fatal("overflowed process timeout was accepted")
	}
}
