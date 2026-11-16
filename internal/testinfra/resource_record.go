package testinfra

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A run records the host's available memory and load average and each
// container's memory in resources.jsonl of its evidence, at its start, every
// resourceRecordInterval and at its end, so a test that failed while the
// runner was short of memory or overloaded shows it. It is a record only:
// what it cannot read is written into the sample, and nothing fails the run.
const (
	resourceRecordInterval = 15 * time.Second
	resourceRecordFile     = "resources.jsonl"
)

type resourceSample struct {
	At                time.Time `json:"at"`
	MemAvailableBytes int64     `json:"mem_available_bytes,omitempty"`
	MemTotalBytes     int64     `json:"mem_total_bytes,omitempty"`
	Load1             float64   `json:"load1"`
	Load5             float64   `json:"load5"`
	Load15            float64   `json:"load15"`
	// ContainerMemory is Docker's "used / limit" for each running container.
	ContainerMemory map[string]string `json:"container_memory,omitempty"`
	Errors          []string          `json:"errors,omitempty"`
}

// recordResources writes a sample to path now and every interval until the
// returned stop, which writes a last one.
func recordResources(path string, interval time.Duration) (stop func()) {
	// The path is beneath the runner's evidence directory.
	//nolint:gosec
	file, err := os.Create(path)
	if err != nil {
		return func() {}
	}
	_, dockerErr := exec.LookPath("docker")
	write := func() {
		sample := sampleResources(dockerErr == nil)
		body, _ := json.Marshal(sample)
		_, _ = file.Write(append(body, '\n'))
	}
	done := make(chan struct{})
	var finished sync.WaitGroup
	finished.Add(1)
	go func() {
		defer finished.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		write()
		for {
			select {
			case <-done:
				write()
				return
			case <-ticker.C:
				write()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			finished.Wait()
			_ = file.Close()
		})
	}
}

func sampleResources(docker bool) resourceSample {
	sample := resourceSample{At: time.Now().UTC()}
	if meminfo, err := os.ReadFile("/proc/meminfo"); err != nil {
		sample.Errors = append(sample.Errors, err.Error())
	} else {
		sample.MemAvailableBytes = meminfoBytes(meminfo, "MemAvailable")
		sample.MemTotalBytes = meminfoBytes(meminfo, "MemTotal")
	}
	if loadavg, err := os.ReadFile("/proc/loadavg"); err != nil {
		sample.Errors = append(sample.Errors, err.Error())
	} else if fields := strings.Fields(string(loadavg)); len(fields) >= 3 {
		sample.Load1, _ = strconv.ParseFloat(fields[0], 64)
		sample.Load5, _ = strconv.ParseFloat(fields[1], 64)
		sample.Load15, _ = strconv.ParseFloat(fields[2], 64)
	}
	if docker {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		output, err := commandStdout(exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"))
		if err != nil {
			sample.Errors = append(sample.Errors, err.Error())
		}
		for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
			if name, usage, found := strings.Cut(line, "\t"); found {
				if sample.ContainerMemory == nil {
					sample.ContainerMemory = map[string]string{}
				}
				sample.ContainerMemory[name] = usage
			}
		}
	}
	return sample
}

// meminfoBytes reads one "Name: value kB" line of /proc/meminfo.
func meminfoBytes(meminfo []byte, name string) int64 {
	scanner := bufio.NewScanner(bytes.NewReader(meminfo))
	for scanner.Scan() {
		field, value, found := strings.Cut(scanner.Text(), ":")
		if !found || field != name {
			continue
		}
		kilobytes, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(value), " kB"), 10, 64)
		if err != nil {
			return 0
		}
		return kilobytes << 10
	}
	return 0
}
