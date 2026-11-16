package ciflow

import (
	"encoding/json"
	"fmt"
	"time"
)

// JobTimes are a job's timestamps as GitHub lists them for a run attempt.
type JobTimes struct {
	Name      string
	Created   time.Time
	Started   time.Time
	Completed time.Time
}

// RanEarlier says the attempt did not run the job again: GitHub lists such a
// job with the attempt's creation time and the start of the run it kept.
func (job JobTimes) RanEarlier() bool {
	return !job.Started.IsZero() && job.Started.Before(job.Created)
}

// ParseJobs reads GitHub's list of a run attempt's jobs.
func ParseJobs(body []byte) ([]JobTimes, error) {
	var listed struct {
		Jobs []struct {
			Name      string     `json:"name"`
			Created   time.Time  `json:"created_at"`
			Started   *time.Time `json:"started_at"`
			Completed *time.Time `json:"completed_at"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		return nil, fmt.Errorf("decode the run's jobs: %w", err)
	}
	var jobs []JobTimes
	for _, job := range listed.Jobs {
		if job.Created.IsZero() {
			return nil, fmt.Errorf("the job %s has no creation time", job.Name)
		}
		times := JobTimes{Name: job.Name, Created: job.Created}
		if job.Started != nil {
			times.Started = *job.Started
		}
		if job.Completed != nil {
			times.Completed = *job.Completed
		}
		jobs = append(jobs, times)
	}
	if len(jobs) == 0 {
		return nil, fmt.Errorf("the run lists no job")
	}
	return jobs, nil
}

// WithFirstCreations gives each job of the current attempt that was not run
// again the creation time of the attempt that ran it, so its runner wait and
// the time since count as they did then. earlier holds the lists of the
// attempts before.
func WithFirstCreations(current []JobTimes, earlier ...[]JobTimes) ([]JobTimes, error) {
	jobs := make([]JobTimes, 0, len(current))
	for _, job := range current {
		if job.RanEarlier() {
			found := false
			for _, attempt := range earlier {
				for _, candidate := range attempt {
					if candidate.Name == job.Name && candidate.Started.Equal(job.Started) && !candidate.RanEarlier() {
						job.Created, found = candidate.Created, true
					}
				}
			}
			if !found {
				return nil, fmt.Errorf("no earlier attempt lists the run of %s that started at %s", job.Name, job.Started.Format(time.RFC3339))
			}
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// DurationReport gives one line for each job but the gate with its runner
// wait and its run time, and one for the run, counted from the earliest
// creation among those jobs to the moment the gate finished.
func DurationReport(jobs []JobTimes, gate string) []string {
	var lines []string
	var earliest, decided time.Time
	for _, job := range jobs {
		if job.Name == gate {
			decided = job.Completed
			continue
		}
		if earliest.IsZero() || job.Created.Before(earliest) {
			earliest = job.Created
		}
		wait, run := "not started", "not finished"
		if !job.Started.IsZero() {
			wait = seconds(job.Started.Sub(job.Created))
			if !job.Completed.IsZero() {
				run = seconds(job.Completed.Sub(job.Started))
			}
		}
		lines = append(lines, fmt.Sprintf("%s: runner wait %s, run %s", job.Name, wait, run))
	}
	if decided.IsZero() || earliest.IsZero() {
		return append(lines, "run: the gate "+gate+" has not decided")
	}
	return append(lines, "run: "+seconds(decided.Sub(earliest)))
}

func seconds(duration time.Duration) string {
	return fmt.Sprintf("%d s", int64(duration/time.Second))
}
