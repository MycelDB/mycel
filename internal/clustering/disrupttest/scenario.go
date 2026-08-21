package disrupttest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ScenarioConfig struct {
	Name         string `json:"name,omitempty"`
	RestartNode  string `json:"restartNode,omitempty"`
	Workload     string `json:"workload,omitempty"`
	Profile      string `json:"profile,omitempty"`
	NoDisruption bool   `json:"noDisruption,omitempty"`
}

type WriteEvent struct {
	Time      time.Time `json:"time"`
	RunID     string    `json:"runId"`
	Worker    string    `json:"worker"`
	Seq       int64     `json:"seq"`
	Attempt   int       `json:"attempt"`
	Success   bool      `json:"success"`
	Transient bool      `json:"transient"`
	Error     string    `json:"error,omitempty"`
}

type ReadEvent struct {
	Time      time.Time `json:"time"`
	RunID     string    `json:"runId"`
	Worker    string    `json:"worker"`
	Seq       int64     `json:"seq"`
	Success   bool      `json:"success"`
	Transient bool      `json:"transient"`
	Error     string    `json:"error,omitempty"`
}

type ScenarioSummary struct {
	RunID                 string                    `json:"runId"`
	Profile               string                    `json:"profile"`
	Workload              string                    `json:"workload"`
	RestartNodes          []string                  `json:"restartNodes,omitempty"`
	AttemptedWrites       int64                     `json:"attemptedWrites"`
	SuccessfulWrites      int64                     `json:"successfulWrites"`
	AmbiguousWrites       int64                     `json:"ambiguousWrites,omitempty"`
	TransientFailures     int64                     `json:"transientFailures"`
	PermanentFailures     int64                     `json:"permanentFailures"`
	FinalCounts           map[string]WorkloadCounts `json:"finalCounts"`
	ReadChecks            int64                     `json:"readChecks,omitempty"`
	ReadFailures          int64                     `json:"readFailures,omitempty"`
	ReadTransientFailures int64                     `json:"readTransientFailures,omitempty"`
	ReadPermanentFailures int64                     `json:"readPermanentFailures,omitempty"`
	RecoveryDurationMS    int64                     `json:"recoveryDurationMs,omitempty"`
	Scopes                []TestScope               `json:"scopes"`
	Diagnostics           []Diagnostics             `json:"diagnostics,omitempty"`
	Warnings              []string                  `json:"warnings,omitempty"`
}

type serviceClient struct {
	driver   ClusterDriver
	username string
	password string
	mu       sync.Mutex
	endpoint Endpoint
	cleanup  func()
	client   *MycelClient
	pinned   *NodeRef
}

func newServiceClient(driver ClusterDriver, username, password string) *serviceClient {
	return &serviceClient{driver: driver, username: username, password: password}
}

func (s *serviceClient) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connectLocked(ctx)
}

func (s *serviceClient) Reconnect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	endpoint, cleanup, client, err := s.openConnectionLocked(ctx)
	if err != nil {
		return err
	}
	oldClient := s.client
	oldCleanup := s.cleanup
	s.endpoint = endpoint
	s.cleanup = cleanup
	s.client = client
	closeConnection(oldClient, oldCleanup)
	return nil
}

func (s *serviceClient) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
}

func (s *serviceClient) PinToNode(ctx context.Context, node NodeRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
	s.pinned = &node
	return s.connectLocked(ctx)
}

func (s *serviceClient) connectLocked(ctx context.Context) error {
	endpoint, cleanup, client, err := s.openConnectionLocked(ctx)
	if err != nil {
		return err
	}
	s.closeLocked()
	s.endpoint = endpoint
	s.cleanup = cleanup
	s.client = client
	return nil
}

func (s *serviceClient) openConnectionLocked(ctx context.Context) (Endpoint, func(), *MycelClient, error) {
	var endpoint Endpoint
	var cleanup func()
	var err error
	if s.pinned != nil {
		endpoint, cleanup, err = s.driver.PortForward(ctx, *s.pinned, 9091)
	} else {
		endpoint, cleanup, err = s.driver.ServiceEndpoint(ctx)
	}
	if err != nil {
		return Endpoint{}, nil, nil, err
	}
	client, err := DialMycel(ctx, endpoint, s.username, s.password)
	if err != nil {
		cleanup()
		return Endpoint{}, nil, nil, err
	}
	return endpoint, cleanup, client, nil
}

func (s *serviceClient) closeLocked() {
	closeConnection(s.client, s.cleanup)
	s.client = nil
	s.cleanup = nil
}

func closeConnection(client *MycelClient, cleanup func()) {
	if client != nil {
		_ = client.Close()
	}
	if cleanup != nil {
		cleanup()
	}
}

func (s *serviceClient) withClient(fn func(*MycelClient) error) error {
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()
	if client == nil {
		return fmt.Errorf("service client is not connected")
	}
	return fn(client)
}

func (s *serviceClient) CreateScope(ctx context.Context, runID string) (scope TestScope, err error) {
	err = s.withClient(func(c *MycelClient) error {
		var createErr error
		scope, createErr = c.CreateScope(ctx, runID)
		return createErr
	})
	return scope, err
}

func (s *serviceClient) WriteChaos(ctx context.Context, scope TestScope, workerID string, seq int64) error {
	err := s.withClient(func(c *MycelClient) error { return c.WriteChaos(ctx, scope, workerID, seq) })
	if err != nil && IsTransientError(err) {
		if reconnectErr := s.Reconnect(ctx); reconnectErr != nil {
			return reconnectErr
		}
		return s.withClient(func(c *MycelClient) error { return c.WriteChaos(ctx, scope, workerID, seq) })
	}
	return err
}

func (s *serviceClient) ExecuteGQLScript(ctx context.Context, scope TestScope, script string) error {
	err := s.withClient(func(c *MycelClient) error { return c.ExecuteGQLScript(ctx, scope, script) })
	if err != nil && IsTransientError(err) {
		if reconnectErr := s.Reconnect(ctx); reconnectErr != nil {
			return reconnectErr
		}
		return s.withClient(func(c *MycelClient) error { return c.ExecuteGQLScript(ctx, scope, script) })
	}
	return err
}

func (s *serviceClient) CountGQL(ctx context.Context, scope TestScope, gql string) (count int64, err error) {
	err = s.withClient(func(c *MycelClient) error {
		var countErr error
		count, countErr = c.CountGQL(ctx, scope, gql)
		return countErr
	})
	if err != nil && IsTransientError(err) {
		if reconnectErr := s.Reconnect(ctx); reconnectErr != nil {
			return 0, reconnectErr
		}
		err = s.withClient(func(c *MycelClient) error {
			var countErr error
			count, countErr = c.CountGQL(ctx, scope, gql)
			return countErr
		})
	}
	return count, err
}

func (s *serviceClient) CountChaos(ctx context.Context, scope TestScope) (count int64, err error) {
	err = s.withClient(func(c *MycelClient) error {
		var countErr error
		count, countErr = c.CountChaos(ctx, scope)
		return countErr
	})
	if err != nil && IsTransientError(err) {
		if reconnectErr := s.Reconnect(ctx); reconnectErr != nil {
			return 0, reconnectErr
		}
		err = s.withClient(func(c *MycelClient) error {
			var countErr error
			count, countErr = c.CountChaos(ctx, scope)
			return countErr
		})
	}
	return count, err
}

func (s *serviceClient) LocalConsistencyCounts(ctx context.Context, scopes []TestScope) (counts WorkloadCounts, err error) {
	err = s.withClient(func(c *MycelClient) error {
		var countErr error
		counts, countErr = c.LocalConsistencyCounts(ctx, scopes)
		return countErr
	})
	if err != nil && IsTransientError(err) {
		if reconnectErr := s.Reconnect(ctx); reconnectErr != nil {
			return WorkloadCounts{}, reconnectErr
		}
		err = s.withClient(func(c *MycelClient) error {
			var countErr error
			counts, countErr = c.LocalConsistencyCounts(ctx, scopes)
			return countErr
		})
	}
	return counts, err
}

func (s *serviceClient) Diagnostics(ctx context.Context) Diagnostics {
	s.mu.Lock()
	client := s.client
	endpoint := s.endpoint
	s.mu.Unlock()
	if client == nil {
		return Diagnostics{Endpoint: endpoint.Addr, Warning: "service client is not connected"}
	}
	return client.Diagnostics(ctx, endpoint.Addr)
}

type scenarioRuntime struct {
	cfg           Config
	profile       Profile
	driver        ClusterDriver
	adminPassword string
	artifactDir   string
	eventFile     *os.File
	readEventFile *os.File
	mu            sync.Mutex
	attempted     atomic.Int64
	succeeded     atomic.Int64
	transient     atomic.Int64
	permanent     atomic.Int64
	readChecks    atomic.Int64
	readFailures  atomic.Int64
	readTransient atomic.Int64
	readPermanent atomic.Int64
	expectedMu    sync.Mutex
	expectedScope map[string]WorkloadCounts
}

func (r *scenarioRuntime) progressf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[raft-disrupt] "+format+"\n", args...)
}

func LoadScenarioFile(path string) (ScenarioConfig, error) {
	if strings.TrimSpace(path) == "" {
		return ScenarioConfig{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ScenarioConfig{}, err
	}
	var cfg ScenarioConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return ScenarioConfig{}, err
	}
	return cfg, nil
}

func applyScenarioFile(cfg Config, sc ScenarioConfig) Config {
	if sc.Profile != "" {
		cfg.Profile = sc.Profile
	}
	if sc.RestartNode != "" {
		cfg.RestartNode = sc.RestartNode
	}
	if sc.Workload != "" {
		cfg.Workload = sc.Workload
	}
	if sc.NoDisruption {
		cfg.NoDisruption = true
	}
	return cfg
}

func RunScenario(ctx context.Context, cfg Config, profile Profile, driver ClusterDriver, adminPassword string, artifactDir string) (ScenarioSummary, error) {
	if sc, err := LoadScenarioFile(cfg.ScenarioFile); err != nil {
		return ScenarioSummary{}, err
	} else {
		cfg = applyScenarioFile(cfg, sc)
	}
	resolvedProfile, err := ResolveProfile(cfg.Profile)
	if err != nil {
		return ScenarioSummary{}, err
	}
	workload, err := ResolveWorkload(strings.TrimSpace(cfg.Workload))
	if err != nil {
		return ScenarioSummary{}, err
	}
	cfg.Workload = workload.Name()
	if profile.Name != resolvedProfile.Name {
		profile = resolvedProfile
	}
	r := &scenarioRuntime{cfg: cfg, profile: profile, driver: driver, adminPassword: adminPassword, artifactDir: artifactDir, expectedScope: map[string]WorkloadCounts{}}
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return ScenarioSummary{}, err
	}
	eventPath := filepath.Join(artifactDir, "write-events.jsonl")
	file, err := os.Create(eventPath)
	if err != nil {
		return ScenarioSummary{}, err
	}
	r.eventFile = file
	defer file.Close()
	readEventPath := filepath.Join(artifactDir, "read-events.jsonl")
	readFile, err := os.Create(readEventPath)
	if err != nil {
		return ScenarioSummary{}, err
	}
	r.readEventFile = readFile
	defer readFile.Close()
	return r.run(ctx)
}

func (r *scenarioRuntime) run(ctx context.Context) (ScenarioSummary, error) {
	runID := time.Now().UTC().Format("20060102-150405")
	r.progressf("connecting service endpoint")
	client := newServiceClient(r.driver, r.cfg.AdminUsername, r.adminPassword)
	if err := client.Connect(ctx); err != nil {
		return ScenarioSummary{}, err
	}
	defer client.Close()
	workload, err := ResolveWorkload(r.cfg.Workload)
	if err != nil {
		return ScenarioSummary{}, err
	}
	r.progressf("creating workload scope(s) workload=%s", workload.Name())
	scopes, err := workload.Setup(ctx, client, runID)
	if err != nil {
		return ScenarioSummary{}, err
	}
	r.progressf("selecting pod that can open workload sessions")
	pinned, err := r.selectWorkloadSessionNode(ctx, workload, scopes)
	if err != nil {
		return ScenarioSummary{}, err
	}
	r.progressf("pinning workload client to pod %s", pinned.Name)
	if err := client.PinToNode(ctx, pinned); err != nil {
		return ScenarioSummary{}, err
	}
	r.progressf("waiting for workload scope readiness through pinned client and pods")
	if err := r.waitWorkloadReady(ctx, client, workload, scopes); err != nil {
		return ScenarioSummary{}, err
	}
	restartNodes, err := ResolveRestartNodes(ctx, r.driver, r.cfg.RestartNode)
	if err != nil {
		return ScenarioSummary{}, err
	}
	if r.cfg.NoDisruption {
		restartNodes = nil
	}
	r.progressf("starting workload profile=%s duration=%s writers=%d rate=%d/s", r.profile.Name, r.profile.Duration, r.profile.Writers, r.profile.Rate)
	stopAt := time.Now().Add(r.profile.Duration)
	workCtx, cancel := context.WithDeadline(ctx, stopAt)
	defer cancel()
	var progressWG sync.WaitGroup
	progressWG.Add(1)
	go func() {
		defer progressWG.Done()
		r.workloadProgress(workCtx, stopAt)
	}()
	var wg sync.WaitGroup
	for i := 0; i < r.profile.Writers; i++ {
		worker := fmt.Sprintf("worker-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.writer(workCtx, stopAt, client, workload, scopes, worker)
		}()
	}
	var recoveryDuration time.Duration
	if len(restartNodes) > 0 {
		warmup := minDuration(5*time.Second, r.profile.Duration/4)
		select {
		case <-ctx.Done():
			cancel()
			wg.Wait()
			return ScenarioSummary{}, ctx.Err()
		case <-time.After(warmup):
		}
		start := time.Now()
		for _, node := range restartNodes {
			r.progressf("restarting pod %s", node)
			if err := r.driver.RestartNode(ctx, NodeRef{Name: node}); err != nil {
				cancel()
				wg.Wait()
				return ScenarioSummary{}, err
			}
			if err := r.driver.WaitAllReady(ctx); err != nil {
				cancel()
				wg.Wait()
				return ScenarioSummary{}, err
			}
			if err := r.waitWorkloadReady(ctx, client, workload, scopes); err != nil {
				cancel()
				wg.Wait()
				return ScenarioSummary{}, err
			}
		}
		recoveryDuration = time.Since(start)
	}
	<-workCtx.Done()
	progressWG.Wait()
	r.progressf("workload duration complete; waiting for writers")
	wg.Wait()
	var scenarioErr error
	if err := r.driver.WaitAllReady(ctx); err != nil {
		scenarioErr = appendError(scenarioErr, err)
	}
	if r.permanent.Load() > 0 {
		scenarioErr = appendError(scenarioErr, fmt.Errorf("write attempts had permanent failures: %d", r.permanent.Load()))
	}
	if r.readFailures.Load() > 0 {
		scenarioErr = appendError(scenarioErr, fmt.Errorf("mixed committed read checks failed: %d", r.readFailures.Load()))
	}
	r.progressf("waiting for final count convergence")
	counts, diags, warnings, err := r.waitFinalConvergence(ctx, client, workload, scopes)
	if err != nil {
		scenarioErr = appendError(scenarioErr, err)
	}
	summary := r.currentSummary(runID, restartNodes, recoveryDuration, scopes, counts, diags, warnings)
	if err := r.writeJSON("scenario-summary.json", summary); err != nil {
		scenarioErr = appendError(scenarioErr, err)
	}
	return summary, scenarioErr
}

func (r *scenarioRuntime) currentSummary(runID string, restartNodes []string, recoveryDuration time.Duration, scopes []TestScope, counts map[string]WorkloadCounts, diags []Diagnostics, warnings []string) ScenarioSummary {
	return ScenarioSummary{
		RunID:                 runID,
		Profile:               r.profile.Name,
		Workload:              r.cfg.Workload,
		RestartNodes:          restartNodes,
		AttemptedWrites:       r.attempted.Load(),
		SuccessfulWrites:      r.succeeded.Load(),
		AmbiguousWrites:       EstimateAmbiguousWrites(r.cfg.Workload, counts, r.succeeded.Load()),
		TransientFailures:     r.transient.Load(),
		PermanentFailures:     r.permanent.Load(),
		FinalCounts:           counts,
		ReadChecks:            r.readChecks.Load(),
		ReadFailures:          r.readFailures.Load(),
		ReadTransientFailures: r.readTransient.Load(),
		ReadPermanentFailures: r.readPermanent.Load(),
		RecoveryDurationMS:    recoveryDuration.Milliseconds(),
		Scopes:                scopes,
		Diagnostics:           diags,
		Warnings:              warnings,
	}
}

func appendError(existing error, next error) error {
	if next == nil {
		return existing
	}
	if existing == nil {
		return next
	}
	return fmt.Errorf("%w; %v", existing, next)
}

func (r *scenarioRuntime) selectWorkloadSessionNode(ctx context.Context, workload Workload, scopes []TestScope) (NodeRef, error) {
	nodes, err := r.driver.Nodes(ctx)
	if err != nil {
		return NodeRef{}, err
	}
	var lastErr error
	for _, node := range nodes {
		endpoint, cleanup, err := r.driver.PortForward(ctx, node, 9091)
		if err != nil {
			lastErr = fmt.Errorf("pod %s port-forward: %w", node.Name, err)
			continue
		}
		podClient, err := DialMycel(ctx, endpoint, r.cfg.AdminUsername, r.adminPassword)
		if err != nil {
			cleanup()
			lastErr = fmt.Errorf("pod %s login: %w", node.Name, err)
			continue
		}
		_, countErr := workload.Count(ctx, podClient, scopes)
		_ = podClient.Close()
		cleanup()
		if countErr == nil {
			return node, nil
		}
		lastErr = fmt.Errorf("pod %s workload session count: %w", node.Name, countErr)
	}
	return NodeRef{}, fmt.Errorf("no pod can open workload sessions: %w", lastErr)
}

func (r *scenarioRuntime) waitWorkloadReady(ctx context.Context, client *serviceClient, workload Workload, scopes []TestScope) error {
	deadline := time.Now().Add(90 * time.Second)
	nextLog := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := workload.Count(ctx, client, scopes); err != nil {
			lastErr = err
		} else if err := r.countWorkloadThroughPods(ctx, workload, scopes); err != nil {
			lastErr = err
		} else {
			return nil
		}
		if time.Now().After(nextLog) {
			r.progressf("still waiting for workload scope readiness; last error: %v", lastErr)
			nextLog = time.Now().Add(10 * time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("workload scope did not become ready on all endpoints: %w", lastErr)
}

func (r *scenarioRuntime) countWorkloadThroughPods(ctx context.Context, workload Workload, scopes []TestScope) error {
	nodes, err := r.driver.Nodes(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		endpoint, cleanup, err := r.driver.PortForward(ctx, node, 9091)
		if err != nil {
			return fmt.Errorf("pod %s port-forward: %w", node.Name, err)
		}
		podClient, err := DialMycel(ctx, endpoint, r.cfg.AdminUsername, r.adminPassword)
		if err != nil {
			cleanup()
			return fmt.Errorf("pod %s login: %w", node.Name, err)
		}
		_, countErr := podClient.LocalConsistencyCounts(ctx, scopes)
		_ = podClient.Close()
		cleanup()
		if countErr != nil {
			return fmt.Errorf("pod %s count: %w", node.Name, countErr)
		}
	}
	return nil
}

func (r *scenarioRuntime) workloadProgress(ctx context.Context, stopAt time.Time) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			remaining := time.Until(stopAt).Round(time.Second)
			if remaining < 0 {
				remaining = 0
			}
			r.progressf("workload progress remaining=%s attempted=%d successful=%d transient=%d permanent=%d readChecks=%d", remaining, r.attempted.Load(), r.succeeded.Load(), r.transient.Load(), r.permanent.Load(), r.readChecks.Load())
		}
	}
}

func (r *scenarioRuntime) writer(ctx context.Context, stopAt time.Time, client *serviceClient, workload Workload, scopes []TestScope, worker string) {
	interval := time.Second / time.Duration(maxInt(1, r.profile.Rate))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !stopAt.IsZero() && !time.Now().Before(stopAt) {
				return
			}
			seq++
			r.writeWithRetry(ctx, stopAt, client, workload, scopes, worker, seq)
		}
	}
}

func (r *scenarioRuntime) writeWithRetry(ctx context.Context, stopAt time.Time, client *serviceClient, workload Workload, scopes []TestScope, worker string, seq int64) {
	backoff := 100 * time.Millisecond
	for attempt := 1; attempt <= 4; attempt++ {
		if !stopAt.IsZero() && !time.Now().Before(stopAt) {
			return
		}
		r.attempted.Add(1)
		err := workload.Write(ctx, client, scopes, worker, seq)
		if err == nil {
			r.succeeded.Add(1)
			r.recordEvent(WriteEvent{Time: time.Now().UTC(), RunID: scopes[0].RunID, Worker: worker, Seq: seq, Attempt: attempt, Success: true})
			r.addExpected(workload.ExpectedWriteCounts(scopes, worker, seq))
			if seq%10 == 0 {
				if _, readErr := workload.Count(ctx, client, scopes); readErr != nil {
					if ctx.Err() != nil || (!stopAt.IsZero() && !time.Now().Before(stopAt)) {
						return
					}
					transientRead := IsTransientError(readErr)
					r.readChecks.Add(1)
					r.readFailures.Add(1)
					if transientRead {
						r.readTransient.Add(1)
					} else {
						r.readPermanent.Add(1)
					}
					r.recordReadEvent(ReadEvent{Time: time.Now().UTC(), RunID: scopes[0].RunID, Worker: worker, Seq: seq, Success: false, Transient: transientRead, Error: readErr.Error()})
				} else {
					r.readChecks.Add(1)
					r.recordReadEvent(ReadEvent{Time: time.Now().UTC(), RunID: scopes[0].RunID, Worker: worker, Seq: seq, Success: true})
				}
			}
			return
		}
		if ctx.Err() != nil || (!stopAt.IsZero() && !time.Now().Before(stopAt)) {
			return
		}
		transient := IsTransientError(err)
		if transient {
			r.transient.Add(1)
		} else {
			r.permanent.Add(1)
		}
		r.recordEvent(WriteEvent{Time: time.Now().UTC(), RunID: scopes[0].RunID, Worker: worker, Seq: seq, Attempt: attempt, Success: false, Transient: transient, Error: err.Error()})
		if !transient || attempt == 4 {
			return
		}
		_ = client.Reconnect(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
		}
	}
}

func (r *scenarioRuntime) addExpected(counts map[string]WorkloadCounts) {
	r.expectedMu.Lock()
	defer r.expectedMu.Unlock()
	for scope, count := range counts {
		r.expectedScope[scope] = r.expectedScope[scope].Add(count)
	}
}

func (r *scenarioRuntime) expectedScopeSnapshot() map[string]WorkloadCounts {
	r.expectedMu.Lock()
	defer r.expectedMu.Unlock()
	out := make(map[string]WorkloadCounts, len(r.expectedScope))
	for scope, count := range r.expectedScope {
		out[scope] = count
	}
	return out
}

func (r *scenarioRuntime) recordEvent(ev WriteEvent) {
	if r.eventFile == nil {
		return
	}
	data, _ := json.Marshal(ev)
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.eventFile.Write(append(data, '\n'))
}

func (r *scenarioRuntime) recordReadEvent(ev ReadEvent) {
	if r.readEventFile == nil {
		return
	}
	data, _ := json.Marshal(ev)
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.readEventFile.Write(append(data, '\n'))
}

func (r *scenarioRuntime) waitFinalConvergence(ctx context.Context, client *serviceClient, workload Workload, scopes []TestScope) (map[string]WorkloadCounts, []Diagnostics, []string, error) {
	deadline := time.Now().Add(2 * time.Minute)
	nextLog := time.Now().Add(10 * time.Second)
	minimum := workload.ExpectedMinimum(r.succeeded.Load())
	expected := r.expectedScopeSnapshot()
	var lastCounts map[string]WorkloadCounts
	var lastDiags []Diagnostics
	var lastWarnings []string
	var lastErr error
	for time.Now().Before(deadline) {
		counts, diags, warnings, err := r.collectCounts(ctx, client, workload, scopes)
		if err != nil {
			lastErr = err
		} else if err := AssertCountsConverged(counts, minimum, expected); err != nil {
			lastCounts = counts
			lastDiags = diags
			lastWarnings = warnings
			lastErr = err
		} else if err := AssertClusterIdentityConverged(diags); err != nil {
			lastCounts = counts
			lastDiags = diags
			lastWarnings = warnings
			lastErr = err
		} else {
			return counts, diags, warnings, nil
		}
		if time.Now().After(nextLog) {
			r.progressf("still waiting for final count convergence; last error: %v", lastErr)
			nextLog = time.Now().Add(10 * time.Second)
		}
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if lastCounts != nil || lastDiags != nil || lastWarnings != nil {
		return lastCounts, lastDiags, lastWarnings, fmt.Errorf("final counts did not converge: %w", lastErr)
	}
	return nil, nil, nil, fmt.Errorf("final counts did not converge: %w", lastErr)
}

func (r *scenarioRuntime) collectCounts(ctx context.Context, client *serviceClient, workload Workload, scopes []TestScope) (map[string]WorkloadCounts, []Diagnostics, []string, error) {
	clientCount, err := client.LocalConsistencyCounts(ctx, scopes)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("client local count: %w", err)
	}
	counts := map[string]WorkloadCounts{"client": clientCount}
	diags := []Diagnostics{client.Diagnostics(ctx)}
	podCounts, podDiags, warnings, err := r.perPodCounts(ctx, workload, scopes)
	if err != nil {
		return nil, nil, nil, err
	}
	for name, count := range podCounts {
		counts[name] = count
	}
	diags = append(diags, podDiags...)
	return counts, diags, warnings, nil
}

func (r *scenarioRuntime) perPodCounts(ctx context.Context, workload Workload, scopes []TestScope) (map[string]WorkloadCounts, []Diagnostics, []string, error) {
	nodes, err := r.driver.Nodes(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	counts := make(map[string]WorkloadCounts, len(nodes))
	var diagnostics []Diagnostics
	var warnings []string
	for _, node := range nodes {
		endpoint, cleanup, err := r.driver.PortForward(ctx, node, 9091)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("pod %s port-forward: %w", node.Name, err)
		}
		podClient, err := DialMycel(ctx, endpoint, r.cfg.AdminUsername, r.adminPassword)
		if err != nil {
			cleanup()
			return nil, nil, nil, fmt.Errorf("pod %s login: %w", node.Name, err)
		}
		count, countErr := podClient.LocalConsistencyCounts(ctx, scopes)
		diagnostics = append(diagnostics, podClient.Diagnostics(ctx, endpoint.Addr))
		_ = podClient.Close()
		cleanup()
		if countErr != nil {
			return nil, nil, nil, fmt.Errorf("pod %s count: %w", node.Name, countErr)
		}
		counts[node.Name] = count
	}
	return counts, diagnostics, warnings, nil
}

func (r *scenarioRuntime) writeJSON(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.artifactDir, name), data, 0o644)
}

func ResolveRestartNodes(ctx context.Context, driver ClusterDriver, ref string) ([]string, error) {
	nodes, err := driver.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n.Name != "" {
			names = append(names, n.Name)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("no restart candidate pods found")
	}
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return []string{names[0]}, nil
	}
	if strings.EqualFold(ref, "all") {
		return names, nil
	}
	if ordinal, err := strconv.Atoi(ref); err == nil {
		wantSuffix := fmt.Sprintf("-%d", ordinal)
		for _, name := range names {
			if strings.HasSuffix(name, wantSuffix) {
				return []string{name}, nil
			}
		}
		return nil, fmt.Errorf("no pod with ordinal %d", ordinal)
	}
	for _, name := range names {
		if name == ref {
			return []string{name}, nil
		}
	}
	return nil, fmt.Errorf("restart node %q not found", ref)
}

func AssertClusterIdentityConverged(diagnostics []Diagnostics) error {
	var baseline string
	var baselineEndpoint string
	mismatches := make([]string, 0)
	seen := 0
	for _, d := range diagnostics {
		if d.ClusterID == "" {
			continue
		}
		seen++
		if baseline == "" {
			baseline = d.ClusterID
			baselineEndpoint = d.Endpoint
			continue
		}
		if d.ClusterID != baseline {
			mismatches = append(mismatches, fmt.Sprintf("%s cluster %s differs from %s cluster %s", d.Endpoint, d.ClusterID, baselineEndpoint, baseline))
		}
	}
	if len(mismatches) > 0 {
		return fmt.Errorf("cluster identity convergence failed: %s", strings.Join(mismatches, "; "))
	}
	if seen == 0 {
		return nil
	}
	return nil
}

func EstimateAmbiguousWrites(workloadName string, counts map[string]WorkloadCounts, successfulWrites int64) int64 {
	if len(counts) == 0 || successfulWrites < 0 {
		return 0
	}
	count, ok := counts["client"]
	if !ok {
		names := make([]string, 0, len(counts))
		for name := range counts {
			names = append(names, name)
		}
		sort.Strings(names)
		count = counts[names[0]]
	}
	var observedWrites int64
	switch strings.TrimSpace(workloadName) {
	case WorkloadEdges:
		observedWrites = count.Edges
	case WorkloadNodes, WorkloadMultiSpace, "":
		observedWrites = count.Nodes
	default:
		return 0
	}
	if observedWrites <= successfulWrites {
		return 0
	}
	return observedWrites - successfulWrites
}

func AssertCountsConverged(counts map[string]WorkloadCounts, expectedMinimum WorkloadCounts, expectedScopes map[string]WorkloadCounts) error {
	if len(counts) == 0 {
		return fmt.Errorf("no final counts recorded")
	}
	var baselineName string
	var baseline WorkloadCounts
	mismatches := make([]string, 0)
	for name, count := range counts {
		if count.Below(expectedMinimum) {
			mismatches = append(mismatches, fmt.Sprintf("%s count %+v below expected minimum %+v", name, count, expectedMinimum))
		}
		for scope, expected := range expectedScopes {
			actual, ok := count.Scopes[scope]
			if !ok {
				mismatches = append(mismatches, fmt.Sprintf("%s missing scope %s count", name, scope))
				continue
			}
			if actual.Below(expected) {
				mismatches = append(mismatches, fmt.Sprintf("%s scope %s count %+v below expected %+v", name, scope, actual, expected))
			}
		}
		if baselineName == "" {
			baselineName, baseline = name, count
			continue
		}
		if !count.Equal(baseline) {
			mismatches = append(mismatches, fmt.Sprintf("%s=%+v differs from %s=%+v", name, count, baselineName, baseline))
		}
	}
	if len(mismatches) > 0 {
		return fmt.Errorf("count convergence failed: %s", strings.Join(mismatches, "; "))
	}
	return nil
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
