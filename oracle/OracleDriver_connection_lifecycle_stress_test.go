/*
** Copyright (c) 2026 Oracle and/or its affiliates.
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	oracleTest "github.com/oracle/go-oracledb/v26/internal/tests"
)

// Samples can catch cycles between stages. Final counts are checked only after
// all workers join. Client closes alone do not assert Oracle session cleanup.
type lifecycleCounts struct {
	CyclesStarted          uint64 `json:"cycles_started"`
	ConnectorsCreated      uint64 `json:"connectors_created"`
	ConnectionAttempts     uint64 `json:"connection_attempts"`
	SuccessfulConnections  uint64 `json:"successful_connections"`
	TCPSVerifications      uint64 `json:"tcps_verifications"`
	SessionIdentifications uint64 `json:"session_identifications"`
	QueryCompletions       uint64 `json:"query_completions"`
	RowCloses              uint64 `json:"row_closes"`
	ConnectionCloses       uint64 `json:"connection_closes"`
	CompletedCycles        uint64 `json:"completed_cycles"`
	FailedCycles           uint64 `json:"failed_cycles"`
	WorkersFinished        uint64 `json:"workers_finished"`
}

type lifecycleProgress struct {
	sync.Mutex
	counts lifecycleCounts
}

func (p *lifecycleProgress) record(stage string) {
	p.Lock()
	defer p.Unlock()
	switch stage {
	case "cycles_started":
		p.counts.CyclesStarted++
	case "connectors_created":
		p.counts.ConnectorsCreated++
	case "connection_attempts":
		p.counts.ConnectionAttempts++
	case "successful_connections":
		p.counts.SuccessfulConnections++
	case "tcps_verifications":
		p.counts.TCPSVerifications++
	case "session_identifications":
		p.counts.SessionIdentifications++
	case "query_completions":
		p.counts.QueryCompletions++
	case "row_closes":
		p.counts.RowCloses++
	case "connection_closes":
		p.counts.ConnectionCloses++
	case "completed_cycles":
		p.counts.CompletedCycles++
	case "failed_cycles":
		p.counts.FailedCycles++
	case "workers_finished":
		p.counts.WorkersFinished++
	}
}

func (p *lifecycleProgress) snapshot() lifecycleCounts {
	p.Lock()
	defer p.Unlock()
	return p.counts
}

func (c lifecycleCounts) validate(workers int, expected uint64, initialized bool) error {
	if c.WorkersFinished != uint64(workers) || c.FailedCycles != 0 || c.CompletedCycles == 0 || c.CompletedCycles != c.CyclesStarted {
		return fmt.Errorf("incomplete workload: workers_finished=%d cycles_started=%d completed=%d failed=%d", c.WorkersFinished, c.CyclesStarted, c.CompletedCycles, c.FailedCycles)
	}
	for stage, count := range map[string]uint64{
		"connectors": c.ConnectorsCreated, "attempts": c.ConnectionAttempts, "connections": c.SuccessfulConnections,
		"queries": c.QueryCompletions, "rows closed": c.RowCloses, "connections closed": c.ConnectionCloses,
	} {
		if count != c.CompletedCycles {
			return fmt.Errorf("%s=%d completed_cycles=%d", stage, count, c.CompletedCycles)
		}
	}
	if initialized && (c.TCPSVerifications != c.CompletedCycles || c.SessionIdentifications != c.CompletedCycles) {
		return errors.New("incomplete TCPS verification or session identification")
	}
	if expected > 0 && c.CompletedCycles != expected {
		return fmt.Errorf("completed=%d expected=%d", c.CompletedCycles, expected)
	}
	return nil
}

type connectionLifecycleRunResult struct {
	Counts   lifecycleCounts `json:"counts"`
	Duration time.Duration   `json:"elapsed_ns"`
	Active   time.Duration   `json:"active_ns"`
	Drain    time.Duration   `json:"drain_ns"`
}

// Only the exclusive stress category invokes this manual, potentially multi-day
// workload. Ordinary Go test discovery cannot start it.
func runDriverConnectionLifecycleStress(t *testing.T) {
	if TestingConfig == nil || TestingConfig.Stress == nil || TestingConfig.Stress.ConnectionLifecycle == nil {
		t.Skip("no stress.connection_lifecycle profile; stress workload did not run")
	}
	config, err := resolveLifecycleConfig(*TestingConfig.Stress.ConnectionLifecycle)
	if err != nil {
		t.Fatalf("stress.connection_lifecycle: %v", err)
	}
	if err := validateRetentionTLS(TestingConfig); err != nil {
		t.Fatal(err)
	}
	timeout, _ := time.ParseDuration(config.Timeout)
	if deadline, ok := t.Deadline(); ok && time.Until(deadline) <= timeout {
		t.Fatalf("Go test timeout must exceed JSON timeout %s plus final diagnostics", config.Timeout)
	}
	runID := newLifecycleRunID()
	artifact := filepath.Join(config.OutputDirectory, runID, "samples.jsonl")
	t.Logf("connection_lifecycle run_id=%s pid=%d samples=%s workers=%d cycles_per_worker=%d duration=%s; external evidence required for leak sign-off", runID, os.Getpid(), artifact, config.Workers, config.CyclesPerWorker, config.Duration)
	result, err := executeConnectionLifecycleStress(context.Background(), config, runID, lifecycleStressOptions{
		TestConfig: TestingConfig,
		Factory:    func() (driver.Connector, error) { return openTestConnectorWithConfig(TestingConfig) },
		Initialize: initializeLifecycleSession(runID),
		ObserveFor: 2 * time.Minute,
	})
	runStatus := "passed"
	if err != nil {
		runStatus = "failed"
	}
	t.Logf("connection_lifecycle run_status=%s workload_status=%s artifact_status=%s measured=%+v warmup=%+v active=%s drain=%s observation=%s wall=%s samples=%s leak_assessment=requires_external_evidence_and_review", runStatus, result.WorkloadStatus, result.ArtifactStatus, result.Workload.Counts, result.Warmup.Counts, result.Workload.Active, result.Workload.Drain, result.Observation, result.Wall, artifact)
	if err != nil {
		t.Fatal(lifecycleSafeError(err, TestingConfig))
	}
}

// Synchronize once, then repeat independently. Duration expiry stops admission
// without canceling in-flight cycles. Failures cancel peers; all workers join.
func runConnectionLifecycleWorkers(parent context.Context, config oracleTest.ConnectionLifecycleConfig, factory lifecycleConnectorFactory, initialize lifecycleSessionInitializer, progress *lifecycleProgress) (result connectionLifecycleRunResult, err error) {
	if err = config.Validate(); err != nil {
		return
	}
	config = config.Normalized()
	timeout, _ := time.ParseDuration(config.Timeout)
	deadlineCtx, stopDeadline := context.WithTimeout(parent, timeout)
	defer stopDeadline()
	ctx, cancel := context.WithCancelCause(deadlineCtx)
	defer cancel(nil)
	operationTimeout, _ := time.ParseDuration(config.OperationTimeout)
	duration, _ := time.ParseDuration(config.Duration)
	start := make(chan struct{})
	var ready, finished sync.WaitGroup
	ready.Add(config.Workers)
	finished.Add(config.Workers)
	workerErrors := make([]error, config.Workers)
	var admitUntil time.Time // Closing start publishes this value to all workers.
	event := lifecycleEvent(progress.record)
	for worker := 0; worker < config.Workers; worker++ {
		go func(worker int) {
			defer finished.Done()
			defer event.emit("workers_finished")
			defer func() {
				if p := recover(); p != nil {
					workerErrors[worker] = fmt.Errorf("worker %d panic: %v", worker+1, p)
					cancel(workerErrors[worker])
				}
			}()
			ready.Done()
			<-start
			for cycle := uint64(0); ; cycle++ {
				if ctx.Err() != nil {
					return
				}
				if duration > 0 {
					if !time.Now().Before(admitUntil) {
						return
					}
				} else if cycle >= uint64(config.CyclesPerWorker) {
					return
				}
				opCtx, opCancel := context.WithTimeout(ctx, operationTimeout)
				_, e := executeDirectConnectionCycle(opCtx, factory, initialize, event)
				opCancel()
				if e != nil {
					workerErrors[worker] = fmt.Errorf("worker %d cycle %d: %w", worker+1, cycle+1, e)
					cancel(workerErrors[worker])
					return
				}
			}
		}(worker)
	}
	ready.Wait()
	began := time.Now()
	if duration > 0 {
		admitUntil = began.Add(duration)
	}
	close(start)
	// Deadlines are cooperative. Go's overall -timeout remains the final bound if
	// a driver call or Close ignores cancellation; do not abandon live workers.
	finished.Wait()
	result.Duration = time.Since(began)
	result.Active = result.Duration
	if duration > 0 && result.Duration > duration {
		result.Active = duration
		result.Drain = result.Duration - duration
	}
	result.Counts = progress.snapshot()
	err = context.Cause(ctx)
	first := err
	for _, e := range workerErrors {
		if e != nil && e != first {
			err = errors.Join(err, e)
		}
	}
	if err == nil {
		expected := uint64(0)
		if duration == 0 {
			expected = uint64(config.Workers * config.CyclesPerWorker)
		}
		err = result.Counts.validate(config.Workers, expected, initialize != nil)
	}
	return
}

// Only current counters and one previous sample are retained in memory.
// Runtime snapshots are process observations, not driver-owned allocation totals.
type lifecycleMemory struct {
	HeapAlloc     uint64  `json:"heap_alloc"`
	HeapObjects   uint64  `json:"heap_objects"`
	HeapInuse     uint64  `json:"heap_inuse"`
	HeapSys       uint64  `json:"heap_sys"`
	HeapReleased  uint64  `json:"heap_released"`
	StackInuse    uint64  `json:"stack_inuse"`
	TotalAlloc    uint64  `json:"total_alloc"`
	Mallocs       uint64  `json:"mallocs"`
	Frees         uint64  `json:"frees"`
	NumGC         uint32  `json:"num_gc"`
	PauseTotalNs  uint64  `json:"pause_total_ns"`
	GCCPUFraction float64 `json:"gc_cpu_fraction"`
}

type lifecycleSample struct {
	Phase           string          `json:"phase"`
	RunID           string          `json:"run_id"`
	PID             int             `json:"pid"`
	Timestamp       time.Time       `json:"timestamp_utc"`
	Elapsed         time.Duration   `json:"elapsed_ns"`
	Counts          lifecycleCounts `json:"counts"`
	Memory          lifecycleMemory `json:"memory"`
	Goroutines      int             `json:"goroutines"`
	AllocationDelta uint64          `json:"total_alloc_delta"`
	MallocDelta     uint64          `json:"mallocs_delta"`
	FreeDelta       uint64          `json:"frees_delta"`
	GCDelta         uint32          `json:"gc_delta"`
	PauseDelta      uint64          `json:"gc_pause_ns_delta"`
}

type lifecycleRecorder struct {
	writer   io.Writer
	runID    string
	started  time.Time
	previous runtime.MemStats
}

func (r *lifecycleRecorder) event(v any) error { return json.NewEncoder(r.writer).Encode(v) }

func (r *lifecycleRecorder) sample(phase string, p *lifecycleProgress) (lifecycleMemory, error) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s := lifecycleSample{Phase: phase, RunID: r.runID, PID: os.Getpid(), Timestamp: time.Now().UTC(), Elapsed: time.Since(r.started), Counts: p.snapshot(), Goroutines: runtime.NumGoroutine(), Memory: lifecycleMemory{
		HeapAlloc: mem.HeapAlloc, HeapObjects: mem.HeapObjects, HeapInuse: mem.HeapInuse, HeapSys: mem.HeapSys, HeapReleased: mem.HeapReleased, StackInuse: mem.StackInuse,
		TotalAlloc: mem.TotalAlloc, Mallocs: mem.Mallocs, Frees: mem.Frees, NumGC: mem.NumGC, PauseTotalNs: mem.PauseTotalNs, GCCPUFraction: mem.GCCPUFraction,
	}}
	if r.previous.TotalAlloc != 0 {
		s.AllocationDelta = mem.TotalAlloc - r.previous.TotalAlloc
		s.MallocDelta = mem.Mallocs - r.previous.Mallocs
		s.FreeDelta = mem.Frees - r.previous.Frees
		s.GCDelta = mem.NumGC - r.previous.NumGC
		s.PauseDelta = mem.PauseTotalNs - r.previous.PauseTotalNs
	}
	r.previous = mem
	return s.Memory, r.event(s)
}

// The sampler is the only writer during active work. Stop joins it before the
// coordinator writes endpoints. The closure is idempotent, including on failure.
func startLifecycleSampler(ctx context.Context, interval time.Duration, r *lifecycleRecorder, p *lifecycleProgress, fail context.CancelCauseFunc) func() error {
	samplingCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var sampleErr error
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-samplingCtx.Done():
				return
			case <-ticker.C:
				if _, sampleErr = r.sample("natural", p); sampleErr != nil {
					fail(sampleErr)
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() error { once.Do(func() { cancel(); <-done }); return sampleErr }
}

func (r *lifecycleRecorder) endpoint(phase string, p *lifecycleProgress, profiles bool, dir string) (lifecycleMemory, error) {
	runtime.GC() // Only drained endpoints use controlled GC.
	mem, err := r.sample(phase+"_post_gc", p)
	if err == nil && profiles {
		err = writeRetentionProfile(filepath.Join(dir, phase+"-heap.pprof"), "heap", 0)
	}
	return mem, err
}

type lifecycleStressOptions struct {
	TestConfig *TestConfig
	Factory    lifecycleConnectorFactory
	Initialize lifecycleSessionInitializer
	ObserveFor time.Duration // Production entry point supplies two minutes.
}

type lifecycleStressResult struct {
	Warmup         connectionLifecycleRunResult `json:"warmup"`
	Workload       connectionLifecycleRunResult `json:"workload"`
	Baseline       lifecycleMemory              `json:"baseline"`
	Final          lifecycleMemory              `json:"final"`
	Settled        lifecycleMemory              `json:"settled"`
	Observation    time.Duration                `json:"observation_ns"`
	Wall           time.Duration                `json:"wall_ns"`
	Artifact       string                       `json:"artifact"`
	WorkloadStatus string                       `json:"workload_status"`
	ArtifactStatus string                       `json:"artifact_status"`
}

// Resolve only a lifecycle value copy. Relative paths retain their existing
// process-working-directory meaning; logs and collectors receive absolute paths.
func resolveLifecycleConfig(config oracleTest.ConnectionLifecycleConfig) (oracleTest.ConnectionLifecycleConfig, error) {
	if err := config.Validate(); err != nil {
		return config, err
	}
	config = config.Normalized()
	dir, err := filepath.Abs(config.OutputDirectory)
	if err != nil {
		return config, err
	}
	config.OutputDirectory = dir
	return config, nil
}

type lifecycleArtifactFile interface {
	Sync() error
	Close() error
}

// The stream cannot certify its own future Sync/Close. Its final record reports
// workload status with pending artifact finalization. Only the returned result
// and terminal summary, after both calls succeed, declare finalized artifacts.
func finalizeLifecycleArtifact(r *lifecycleRecorder, file lifecycleArtifactFile, result *lifecycleStressResult, config *TestConfig, runErr error) error {
	result.ArtifactStatus = "pending_finalization"
	err := errors.Join(runErr, r.event(map[string]any{
		"phase": "result", "run_id": r.runID, "timestamp_utc": time.Now().UTC(),
		"workload_status": result.WorkloadStatus, "artifact_status": result.ArtifactStatus,
		"result": *result, "error": lifecycleSafeError(runErr, config),
		"leak_assessment": "incomplete_evidence", "external_evidence_review_required": true,
	}), lifecycleClose("samples sync", file.Sync), lifecycleClose("samples close", file.Close))
	result.ArtifactStatus = "finalized"
	if err != nil {
		result.ArtifactStatus = "incomplete"
	}
	return err
}

func executeConnectionLifecycleStress(parent context.Context, config oracleTest.ConnectionLifecycleConfig, runID string, options lifecycleStressOptions) (result lifecycleStressResult, err error) {
	result.WorkloadStatus = "not_started"
	result.ArtifactStatus = "incomplete"
	if config, err = resolveLifecycleConfig(config); err != nil {
		return
	}
	began := time.Now()
	timeout, _ := time.ParseDuration(config.Timeout)
	deadlineCtx, stopDeadline := context.WithTimeout(parent, timeout)
	defer stopDeadline()
	ctx, cancel := context.WithCancelCause(deadlineCtx)
	defer cancel(nil)
	dir := filepath.Join(config.OutputDirectory, runID)
	if err = os.MkdirAll(config.OutputDirectory, 0700); err != nil {
		return
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return
	} // Never overwrite an earlier run.
	result.Artifact = filepath.Join(dir, "samples.jsonl")
	f, e := os.OpenFile(result.Artifact, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return result, e
	}
	r := &lifecycleRecorder{writer: f, runID: runID, started: began}
	defer func() {
		result.Wall = time.Since(began)
		hadFailure := err != nil
		if err != nil && config.ProfilesEnabled() {
			err = errors.Join(err, writeRetentionProfile(filepath.Join(dir, "failure-goroutines.txt"), "goroutine", 2))
		}
		err = finalizeLifecycleArtifact(r, f, &result, options.TestConfig, err)
		if !hadFailure && err != nil && config.ProfilesEnabled() {
			// Final Sync/Close can be the first failure, after endpoints completed.
			err = errors.Join(err, writeRetentionProfile(filepath.Join(dir, "failure-goroutines.txt"), "goroutine", 2))
		}
	}()
	if err = r.event(lifecycleMetadata(config, runID, began, options.TestConfig)); err != nil {
		return
	}
	// Warm-up uses the same operation, but its totals remain separate.
	warmupConfig := config
	warmupConfig.Duration = ""
	warmupConfig.CyclesPerWorker = 1
	result.Warmup, err = runConnectionLifecycleWorkers(ctx, warmupConfig, options.Factory, options.Initialize, &lifecycleProgress{})
	if e = r.event(map[string]any{"phase": "warmup", "run_id": runID, "result": result.Warmup}); e != nil {
		err = errors.Join(err, e)
	}
	if err != nil {
		return
	}
	if result.Warmup.Counts.TCPSVerifications == uint64(config.Workers) {
		if err = r.event(map[string]any{"phase": "tcps_verified", "run_id": runID, "verified_connections": result.Warmup.Counts.TCPSVerifications}); err != nil {
			return
		}
	}
	p := &lifecycleProgress{}
	if _, err = r.sample("baseline_natural", p); err != nil {
		return
	}
	if result.Baseline, err = r.endpoint("baseline", p, config.ProfilesEnabled(), dir); err != nil {
		return
	}
	interval, _ := time.ParseDuration(config.SampleInterval)
	stopSamples := startLifecycleSampler(ctx, interval, r, p, cancel)
	defer stopSamples()
	result.Workload, err = runConnectionLifecycleWorkers(ctx, config, options.Factory, options.Initialize, p)
	result.WorkloadStatus = "complete"
	if err != nil {
		result.WorkloadStatus = "failed"
	}
	err = errors.Join(err, stopSamples())
	if _, e = r.sample("final_natural", p); e != nil {
		err = errors.Join(err, e)
	}
	result.Final, e = r.endpoint("final", p, config.ProfilesEnabled(), dir)
	err = errors.Join(err, e)
	// Admission has stopped and workers are gone. Leave the process visible to
	// external collectors without creating new connections during observation.
	observeStart := time.Now()
	if e = r.event(map[string]any{"phase": "observation_start", "run_id": runID, "timestamp_utc": observeStart.UTC(), "planned_ns": options.ObserveFor.Nanoseconds(), "workload_status": "drained"}); e != nil {
		err = errors.Join(err, e)
	}
	if err == nil && options.ObserveFor > 0 {
		timer := time.NewTimer(options.ObserveFor)
		select {
		case <-timer.C:
		case <-ctx.Done():
			err = errors.Join(err, context.Cause(ctx))
		}
		timer.Stop()
	}
	result.Observation = time.Since(observeStart)
	result.Settled, e = r.endpoint("settled", p, config.ProfilesEnabled(), dir)
	err = errors.Join(err, e, context.Cause(ctx))
	return
}

func lifecycleSafeError(err error, config *TestConfig) string {
	if err == nil {
		return ""
	}
	return redactRetentionText(err.Error(), config)
}

func lifecycleMetadata(config oracleTest.ConnectionLifecycleConfig, runID string, began time.Time, testConfig *TestConfig) map[string]any {
	m := map[string]any{"phase": "configuration", "run_id": runID, "pid": os.Getpid(), "run_start_utc": began.UTC(), "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "cpu": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0), "gogc": os.Getenv("GOGC"), "gomemlimit": os.Getenv("GOMEMLIMIT"), "effective_memory_limit_bytes": debug.SetMemoryLimit(-1), "memory_profile_rate": runtime.MemProfileRate, "profile": config, "diagnostic_profiles_enabled": config.ProfilesEnabled(), "module": lifecycleModule, "protocol_required": "tcps", "driver_commit": "unavailable", "source_dirty": "unavailable", "process_start_utc": "unavailable", "external_monitoring": "required_for_signoff", "declared_network": os.Getenv("ORACLE_STRESS_NETWORK_DESCRIPTION"), "declared_vpn": os.Getenv("ORACLE_STRESS_VPN_STATUS")}
	// JSON version declarations are not evidence of the live server's version.
	// External collectors run separately and their availability is reviewed from
	// their status fields; the workload must not claim they are attached.
	m["configured_database_version"] = "unavailable"
	m["server_reported_database_version"] = "unavailable; record from DBA monitoring evidence"
	m["monitoring_availability"] = map[string]string{"os": "unavailable_to_workload_process", "oracle": "unavailable_to_workload_process"}
	if testConfig != nil {
		m["config_name"] = testConfig.ConfigName
		m["configured_database_version"] = testConfig.DatabaseVersion
		if testConfig.DatabaseVersion.Major == 0 {
			m["configured_database_version"] = "unavailable"
		}
	}
	for _, key := range []string{"declared_network", "declared_vpn"} {
		if m[key] == "" {
			m[key] = "unavailable"
		}
	}
	// Metadata commands finish before heap endpoints and active measurement.
	command := func(name string, args ...string) (string, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Env = append(os.Environ(), "LC_ALL=C", "TZ=UTC")
		output, err := cmd.Output()
		return strings.TrimSpace(string(output)), err == nil
	}
	if value, ok := command("git", "rev-parse", "HEAD"); ok {
		m["driver_commit"] = value
	}
	if value, ok := command("git", "status", "--porcelain"); ok {
		m["source_dirty"] = value != ""
	}
	if runtime.GOOS == "windows" {
		if value, ok := command("powershell", "-NoProfile", "-NonInteractive", "-Command", "(Get-Process -Id "+strconv.Itoa(os.Getpid())+").StartTime.ToUniversalTime().ToString('o')"); ok {
			m["process_start_utc"] = value
		}
	} else if value, ok := command("ps", "-p", strconv.Itoa(os.Getpid()), "-o", "lstart="); ok {
		if parsed, e := time.ParseInLocation("Mon Jan _2 15:04:05 2006", value, time.UTC); e == nil {
			m["process_start_utc"] = parsed
		}
	}
	return m
}
