package connectors

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

func TestFakeConnectorDefaultEmbeddingRemainsCompatible(t *testing.T) {
	connector := &FakeConnector{}
	response, err := connector.Embed(context.Background(), EmbeddingRequest{Input: "unchanged default"})
	if err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	if response.ProviderRequestID != "fake" || len(response.Vector) != 3 || response.Vector[0] != 1 || response.Vector[1] != 0 || response.Vector[2] != 0 {
		t.Fatalf("default response = %#v", response)
	}
}

func TestTokenBasisV1ExactAxesAndNormalization(t *testing.T) {
	connector := &FakeConnector{}
	request := tokenBasisRequest(" AXIS-0\naxis-1 ", 4, nil)
	response, err := connector.Embed(context.Background(), request)
	if err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	want := 1 / math.Sqrt(2)
	if len(response.Vector) != 4 || response.Vector[0] != want || response.Vector[1] != want || response.Vector[2] != 0 || response.Vector[3] != 0 {
		t.Fatalf("vector = %#v, want [%v %v 0 0]", response.Vector, want, want)
	}
	if response.ProviderRequestID != "fake-tbv1-359f7f82955c-b4a090904c11" {
		t.Fatalf("request id = %q", response.ProviderRequestID)
	}
	again, err := connector.Embed(context.Background(), tokenBasisRequest("axis-0 axis-1", 4, nil))
	if err != nil {
		t.Fatalf("Embed(normalized) error = %v", err)
	}
	for i := range response.Vector {
		if response.Vector[i] != again.Vector[i] {
			t.Fatalf("normalized vector differs at %d: %v != %v", i, response.Vector[i], again.Vector[i])
		}
	}
}

func TestTokenBasisV1UnknownInputGolden(t *testing.T) {
	vector, err := tokenBasisV1("unknown fixture phrase", 4)
	if err != nil {
		t.Fatalf("tokenBasisV1() error = %v", err)
	}
	want := []float64{-0.7501057230609837, 0.11382475110883553, 0.2263382429306356, -0.6108652306796019}
	if len(vector) != len(want) {
		t.Fatalf("vector length = %d", len(vector))
	}
	for i := range want {
		if vector[i] != want[i] {
			t.Fatalf("vector[%d] = %.17g, want %.17g (full=%#v)", i, vector[i], want[i], vector)
		}
	}
}

func TestFakeFixtureScriptFiniteSequenceAndReset(t *testing.T) {
	input := "axis-2"
	script := fixtureScriptForInputs(map[string][]map[string]any{
		input: {
			{"kind": "retryable-error", "code": "fixture_busy"},
			{"kind": "success"},
		},
	})
	connector := &FakeConnector{}
	request := tokenBasisRequest(input, 4, script)

	first, err := connector.Embed(context.Background(), request)
	if err == nil || ErrorCode(err) != "fixture_busy" {
		t.Fatalf("first error = %v, response=%#v", err, first)
	}
	var connectorErr ConnectorError
	if !errors.As(err, &connectorErr) || !connectorErr.Retryable {
		t.Fatalf("first error is not retryable: %v", err)
	}
	second, err := connector.Embed(context.Background(), request)
	if err != nil || len(second.Vector) != 4 || second.Vector[2] != 1 {
		t.Fatalf("second response=%#v error=%v", second, err)
	}
	if _, err := connector.Embed(context.Background(), request); ErrorCode(err) != "fake_fixture_script_exhausted" {
		t.Fatalf("exhausted error = %v", err)
	}
	stats := connector.FixtureStats()
	if len(stats) != 1 || stats[0].Attempts != 2 || stats[0].Failed != 1 || stats[0].Completed != 1 || stats[0].Canceled != 0 {
		t.Fatalf("stats = %#v", stats)
	}

	connector.ResetFixtureState()
	if _, err := connector.Embed(context.Background(), request); ErrorCode(err) != "fixture_busy" {
		t.Fatalf("error after explicit reset = %v", err)
	}
	freshProcess := &FakeConnector{}
	if _, err := freshProcess.Embed(context.Background(), request); ErrorCode(err) != "fixture_busy" {
		t.Fatalf("error after connector restart = %v", err)
	}
}

func TestFakeFixtureResetIsolatedFromDelayedInFlightRequest(t *testing.T) {
	input := "axis-0"
	script := fixtureScriptForInputs(map[string][]map[string]any{input: {
		{"kind": "success", "delay_ms": 200},
	}})
	connector := &FakeConnector{}
	result := make(chan error, 1)
	go func() {
		_, err := connector.Embed(context.Background(), tokenBasisRequest(input, 3, script))
		result <- err
	}()
	deadline := time.Now().Add(time.Second)
	for len(connector.FixtureStats()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(connector.FixtureStats()) != 1 {
		t.Fatal("delayed request did not select a scripted attempt")
	}
	connector.ResetFixtureState()
	if err := <-result; ErrorCode(err) != "fake_fixture_reset" {
		t.Fatalf("in-flight result error = %v", err)
	}
	if stats := connector.FixtureStats(); len(stats) != 0 {
		t.Fatalf("pre-reset completion contaminated reset accounting: %#v", stats)
	}
}

func TestFakeFixtureDelayHonorsCancellation(t *testing.T) {
	input := "axis-0"
	script := fixtureScriptForInputs(map[string][]map[string]any{
		input: {{"kind": "success", "delay_ms": 500}},
	})
	connector := &FakeConnector{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := connector.Embed(ctx, tokenBasisRequest(input, 3, script))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Embed() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 300*time.Millisecond {
		t.Fatalf("cancellation took %s", elapsed)
	}
	stats := connector.FixtureStats()
	if len(stats) != 1 || stats[0].Attempts != 1 || stats[0].Canceled != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestFakeFixtureAttemptStateIsScopedByResolvedProfile(t *testing.T) {
	input := "axis-0"
	script := fixtureScriptForInputs(map[string][]map[string]any{input: {
		{"kind": "retryable-error", "code": "fixture_first"},
		{"kind": "success"},
	}})
	connector := &FakeConnector{}
	firstProfile := tokenBasisRequest(input, 3, script)
	firstProfile.Model.Key = "fixture/model-a"
	secondProfile := tokenBasisRequest(input, 3, script)
	secondProfile.Model.Key = "fixture/model-b"
	for _, request := range []EmbeddingRequest{firstProfile, secondProfile} {
		if _, err := connector.Embed(context.Background(), request); ErrorCode(err) != "fixture_first" {
			t.Fatalf("profile %q first outcome error = %v", request.Model.Key, err)
		}
	}
	stats := connector.FixtureStats()
	if len(stats) != 2 || stats[0].ScopeSHA256 == stats[1].ScopeSHA256 {
		t.Fatalf("fixture stats do not distinguish resolved profiles: %#v", stats)
	}
}

func TestFakeFixtureConcurrentAttemptSelectionIsBounded(t *testing.T) {
	input := "axis-1"
	outcomes := make([]map[string]any, 16)
	for i := range outcomes {
		outcomes[i] = map[string]any{"kind": "error", "code": "fixture_" + string(rune('a'+i))}
	}
	script := fixtureScriptForInputs(map[string][]map[string]any{input: outcomes})
	connector := &FakeConnector{}
	request := tokenBasisRequest(input, 3, script)
	codes := make(chan string, len(outcomes))
	var wg sync.WaitGroup
	for range outcomes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := connector.Embed(context.Background(), request)
			codes <- ErrorCode(err)
		}()
	}
	wg.Wait()
	close(codes)
	got := make([]string, 0, len(outcomes))
	for code := range codes {
		got = append(got, code)
	}
	sort.Strings(got)
	for i, code := range got {
		want := "fixture_" + string(rune('a'+i))
		if code != want {
			t.Fatalf("codes[%d] = %q, want %q; all=%v", i, code, want, got)
		}
	}
	if _, err := connector.Embed(context.Background(), request); ErrorCode(err) != "fake_fixture_script_exhausted" {
		t.Fatalf("post-concurrency exhausted error = %v", err)
	}
	stats := connector.FixtureStats()
	if len(stats) != 1 || stats[0].Attempts != len(outcomes) || stats[0].Failed != len(outcomes) {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestFakeFixtureDelayAllowsOutOfOrderCompletion(t *testing.T) {
	slow := "axis-0"
	fast := "axis-1"
	script := fixtureScriptForInputs(map[string][]map[string]any{
		slow: {{"kind": "success", "delay_ms": 100}},
		fast: {{"kind": "success"}},
	})
	connector := &FakeConnector{}
	completed := make(chan string, 2)
	go func() {
		_, _ = connector.Embed(context.Background(), tokenBasisRequest(slow, 3, script))
		completed <- slow
	}()
	time.Sleep(10 * time.Millisecond)
	go func() {
		_, _ = connector.Embed(context.Background(), tokenBasisRequest(fast, 3, script))
		completed <- fast
	}()
	if first := <-completed; first != fast {
		t.Fatalf("first completion = %q, want %q", first, fast)
	}
	if second := <-completed; second != slow {
		t.Fatalf("second completion = %q, want %q", second, slow)
	}
}

func TestFakeFixtureRejectsUnknownAndUnboundedConfiguration(t *testing.T) {
	cases := []struct {
		name    string
		fixture map[string]any
	}{
		{name: "unknown strategy", fixture: map[string]any{"strategy": "mystery"}},
		{name: "unknown field", fixture: map[string]any{"strategy": FakeFixtureTokenBasisV1, "extra": true}},
		{name: "unknown outcome", fixture: fixtureScriptForInputs(map[string][]map[string]any{"axis-0": {{"kind": "forever"}}})},
		{name: "unbounded delay", fixture: fixtureScriptForInputs(map[string][]map[string]any{"axis-0": {{"kind": "success", "delay_ms": 30001}}})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			connector := &FakeConnector{}
			_, err := connector.Embed(context.Background(), tokenBasisRequest("axis-0", 3, tc.fixture))
			if err == nil {
				t.Fatal("Embed() error = nil")
			}
		})
	}
	connector := &FakeConnector{}
	_, err := connector.Embed(context.Background(), tokenBasisRequest("axis-0", 0, nil))
	if err == nil || !strings.Contains(err.Error(), "dimensions") {
		t.Fatalf("zero-dimension error = %v", err)
	}
}

func TestFakeFixtureMetadataAndErrorsAreByteBounded(t *testing.T) {
	connector := &FakeConnector{}
	fixture := map[string]any{"strategy": strings.Repeat("attacker-controlled", 100000)}
	_, err := connector.Embed(context.Background(), tokenBasisRequest("axis-0", 3, fixture))
	if !errors.Is(err, errFakeFixtureMetadataTooLarge) {
		t.Fatalf("oversized metadata error = %v", err)
	}
	fixture = map[string]any{"strategy": strings.Repeat("x", fakeFixtureMaxTextBytes+1)}
	_, err = connector.Embed(context.Background(), tokenBasisRequest("axis-0", 3, fixture))
	if err == nil || len(err.Error()) > 256 || strings.Contains(err.Error(), strings.Repeat("x", fakeFixtureMaxTextBytes)) {
		t.Fatalf("text-bound error is unsafe: %v", err)
	}
}

func TestFakeFixtureBlockedAndRefusedOutcomes(t *testing.T) {
	for _, tc := range []struct {
		kind string
		code string
	}{
		{kind: "blocked", code: "fake_blocked"},
		{kind: "refused", code: "fake_refused"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			input := "axis-0"
			script := fixtureScriptForInputs(map[string][]map[string]any{input: {{"kind": tc.kind}}})
			response, err := (&FakeConnector{}).Embed(context.Background(), tokenBasisRequest(input, 3, script))
			if ErrorCode(err) != tc.code || !strings.HasPrefix(response.ProviderRequestID, "fake-") {
				t.Fatalf("response=%#v error=%v", response, err)
			}
		})
	}
}

func TestFakeFixtureWrongDimensionsOutcome(t *testing.T) {
	input := "axis-0"
	script := fixtureScriptForInputs(map[string][]map[string]any{
		input: {{"kind": "wrong-dimensions", "dimensions": 2}},
	})
	request := tokenBasisRequest(input, 3, script)
	request.Secret = "credential-must-not-affect-fixture"
	response, err := (&FakeConnector{}).Embed(context.Background(), request)
	if err != nil || len(response.Vector) != 2 || strings.Contains(response.ProviderRequestID, request.Secret) {
		t.Fatalf("response=%#v error=%v", response, err)
	}
}

func tokenBasisRequest(input string, dimensions int, fixture map[string]any) EmbeddingRequest {
	if fixture == nil {
		fixture = map[string]any{"strategy": FakeFixtureTokenBasisV1}
	}
	return EmbeddingRequest{
		Input: input,
		Endpoint: domaininference.Endpoint{
			ConnectorType: domaininference.ConnectorFake,
		},
		Model: domaininference.Model{
			EmbeddingDims: dimensions,
			Metadata:      map[string]any{FakeFixtureMetadataKey: fixture},
		},
	}
}

func fixtureScriptForInputs(inputs map[string][]map[string]any) map[string]any {
	keys := make([]string, 0, len(inputs))
	for input := range inputs {
		keys = append(keys, input)
	}
	sort.Strings(keys)
	entries := make([]any, 0, len(keys))
	for _, input := range keys {
		outcomes := make([]any, 0, len(inputs[input]))
		for _, outcome := range inputs[input] {
			outcomes = append(outcomes, outcome)
		}
		entries = append(entries, map[string]any{
			"input_sha256": fakeFixtureInputDigest(normalizeFakeFixtureInput(input)),
			"outcomes":     outcomes,
		})
	}
	return map[string]any{
		"strategy": FakeFixtureTokenBasisV1,
		"script": map[string]any{
			"version":       fakeFixtureScriptVersion,
			"reset":         fakeFixtureResetProcess,
			"require_match": true,
			"entries":       entries,
		},
	}
}
