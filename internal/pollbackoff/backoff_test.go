package pollbackoff

import (
	"reflect"
	"testing"
	"time"
)

func TestBackoffGrowsOnlyAcrossEmptyPollsAndResetsAfterWork(t *testing.T) {
	backoff := New(time.Second, 8*time.Second)
	got := []time.Duration{
		backoff.Next(false),
		backoff.Next(false),
		backoff.Next(false),
		backoff.Next(false),
		backoff.Next(false),
		backoff.Next(true),
		backoff.Next(false),
	}
	want := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		8 * time.Second,
		time.Second,
		time.Second,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("backoff delays = %v; want %v", got, want)
	}
}
