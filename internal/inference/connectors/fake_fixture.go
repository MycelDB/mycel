package connectors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

const (
	FakeFixtureMetadataKey      = "mycel_fixture"
	FakeFixtureTokenBasisV1     = "token-basis-v1"
	fakeFixtureScriptVersion    = "v1"
	fakeFixtureResetProcess     = "process"
	fakeFixtureMaxDimensions    = 4096
	fakeFixtureMaxEntries       = 256
	fakeFixtureMaxOutcomes      = 32
	fakeFixtureMaxDelay         = 30 * time.Second
	fakeFixtureMaxStateEntries  = 1024
	fakeFixtureMaxMetadataBytes = 1 << 20
	fakeFixtureMaxTextBytes     = 128
)

var (
	fakeFixtureDigestPattern       = regexp.MustCompile(`^[a-f0-9]{64}$`)
	fakeFixtureCodePattern         = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	errFakeFixtureMetadataTooLarge = errors.New("fake fixture metadata exceeds byte limit")
)

type fakeFixtureConfig struct {
	Strategy string             `json:"strategy"`
	Script   *fakeFixtureScript `json:"script,omitempty"`
}

type fakeFixtureScript struct {
	Version      string                   `json:"version"`
	Reset        string                   `json:"reset"`
	RequireMatch bool                     `json:"require_match,omitempty"`
	Entries      []fakeFixtureScriptEntry `json:"entries"`
}

type fakeFixtureScriptEntry struct {
	InputSHA256 string               `json:"input_sha256"`
	Outcomes    []fakeFixtureOutcome `json:"outcomes"`
}

type fakeFixtureOutcome struct {
	Kind       string `json:"kind"`
	Code       string `json:"code,omitempty"`
	DelayMS    int    `json:"delay_ms,omitempty"`
	Dimensions int    `json:"dimensions,omitempty"`
}

type FakeFixtureStat struct {
	ScriptSHA256 string `json:"script_sha256"`
	ScopeSHA256  string `json:"scope_sha256"`
	InputSHA256  string `json:"input_sha256"`
	Attempts     int    `json:"attempts"`
	Completed    int    `json:"completed"`
	Canceled     int    `json:"canceled"`
	Failed       int    `json:"failed"`
}

type fakeFixtureSelection struct {
	config      fakeFixtureConfig
	configHash  string
	scopeHash   string
	generation  uint64
	inputDigest string
	normalized  string
	outcome     *fakeFixtureOutcome
	attempt     int
	stateKey    string
}

type limitedFixtureBuffer struct {
	bytes.Buffer
	remaining int
}

func (w *limitedFixtureBuffer) Write(value []byte) (int, error) {
	if len(value) > w.remaining {
		return 0, errFakeFixtureMetadataTooLarge
	}
	written, err := w.Buffer.Write(value)
	w.remaining -= written
	return written, err
}

func encodeFakeFixtureMetadata(raw any) ([]byte, error) {
	buffer := &limitedFixtureBuffer{remaining: fakeFixtureMaxMetadataBytes}
	if err := json.NewEncoder(buffer).Encode(raw); err != nil {
		if errors.Is(err, errFakeFixtureMetadataTooLarge) || errors.Is(err, io.ErrShortWrite) {
			return nil, errFakeFixtureMetadataTooLarge
		}
		return nil, errors.New("fake fixture metadata cannot be encoded")
	}
	return buffer.Bytes(), nil
}

func fakeFixtureFromRequest(req EmbeddingRequest) (fakeFixtureConfig, string, bool, error) {
	for _, metadata := range []map[string]any{req.Capability.Metadata, req.Model.Metadata, req.Endpoint.Metadata} {
		raw, ok := metadata[FakeFixtureMetadataKey]
		if !ok {
			continue
		}
		encoded, err := encodeFakeFixtureMetadata(raw)
		if err != nil {
			return fakeFixtureConfig{}, "", true, err
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		var config fakeFixtureConfig
		if err := decoder.Decode(&config); err != nil {
			return fakeFixtureConfig{}, "", true, errors.New("fake fixture metadata is invalid")
		}
		if err := validateFakeFixtureConfig(config, req.Model.EmbeddingDims); err != nil {
			return fakeFixtureConfig{}, "", true, err
		}
		canonical, err := json.Marshal(config)
		if err != nil || len(canonical) > fakeFixtureMaxMetadataBytes {
			return fakeFixtureConfig{}, "", true, errors.New("fake fixture metadata canonicalization failed")
		}
		sum := sha256.Sum256(canonical)
		return config, hex.EncodeToString(sum[:]), true, nil
	}
	return fakeFixtureConfig{}, "", false, nil
}

func validateFakeFixtureConfig(config fakeFixtureConfig, dimensions int) error {
	if len(config.Strategy) == 0 || len(config.Strategy) > fakeFixtureMaxTextBytes || config.Strategy != FakeFixtureTokenBasisV1 {
		return errors.New("fake fixture strategy is invalid")
	}
	if dimensions <= 0 || dimensions > fakeFixtureMaxDimensions {
		return fmt.Errorf("fake fixture dimensions must be between 1 and %d", fakeFixtureMaxDimensions)
	}
	if config.Script == nil {
		return nil
	}
	script := config.Script
	if len(script.Version) == 0 || len(script.Version) > fakeFixtureMaxTextBytes || script.Version != fakeFixtureScriptVersion {
		return errors.New("fake fixture script version is invalid")
	}
	if len(script.Reset) == 0 || len(script.Reset) > fakeFixtureMaxTextBytes || script.Reset != fakeFixtureResetProcess {
		return errors.New("fake fixture script reset mode is invalid")
	}
	if len(script.Entries) == 0 || len(script.Entries) > fakeFixtureMaxEntries {
		return fmt.Errorf("fake fixture script entries must contain between 1 and %d items", fakeFixtureMaxEntries)
	}
	seen := make(map[string]struct{}, len(script.Entries))
	for i, entry := range script.Entries {
		if !fakeFixtureDigestPattern.MatchString(entry.InputSHA256) {
			return fmt.Errorf("fake fixture script entry %d input_sha256 must be lowercase SHA-256", i)
		}
		if _, ok := seen[entry.InputSHA256]; ok {
			return fmt.Errorf("fake fixture script input_sha256 %q is duplicated", entry.InputSHA256)
		}
		seen[entry.InputSHA256] = struct{}{}
		if len(entry.Outcomes) == 0 || len(entry.Outcomes) > fakeFixtureMaxOutcomes {
			return fmt.Errorf("fake fixture script entry %d outcomes must contain between 1 and %d items", i, fakeFixtureMaxOutcomes)
		}
		for j, outcome := range entry.Outcomes {
			if err := validateFakeFixtureOutcome(outcome, dimensions); err != nil {
				return fmt.Errorf("fake fixture script entry %d outcome %d: %w", i, j, err)
			}
		}
	}
	return nil
}

func validateFakeFixtureOutcome(outcome fakeFixtureOutcome, dimensions int) error {
	if len(outcome.Kind) == 0 || len(outcome.Kind) > fakeFixtureMaxTextBytes {
		return errors.New("outcome kind is invalid")
	}
	switch outcome.Kind {
	case "success", "blocked", "refused", "retryable-error", "error", "wrong-dimensions":
	default:
		return errors.New("outcome kind is invalid")
	}
	if outcome.DelayMS < 0 || outcome.DelayMS > int(fakeFixtureMaxDelay/time.Millisecond) {
		return fmt.Errorf("delay_ms must be between 0 and %d", fakeFixtureMaxDelay.Milliseconds())
	}
	if len(outcome.Code) > fakeFixtureMaxTextBytes || (outcome.Code != "" && !fakeFixtureCodePattern.MatchString(outcome.Code)) {
		return errors.New("outcome code is invalid")
	}
	if outcome.Kind == "wrong-dimensions" {
		if outcome.Dimensions < 0 || outcome.Dimensions > fakeFixtureMaxDimensions || outcome.Dimensions == dimensions {
			return fmt.Errorf("wrong-dimensions must declare zero or a value between 1 and %d other than model dimensions", fakeFixtureMaxDimensions)
		}
	} else if outcome.Dimensions != 0 {
		return errors.New("dimensions is valid only for wrong-dimensions")
	}
	return nil
}

func normalizeFakeFixtureInput(input string) string {
	var out strings.Builder
	out.Grow(len(input))
	space := true
	for i := 0; i < len(input); i++ {
		c := input[i]
		switch c {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			if !space {
				out.WriteByte(' ')
				space = true
			}
		default:
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			out.WriteByte(c)
			space = false
		}
	}
	return strings.TrimSpace(out.String())
}

func fakeFixtureInputDigest(normalized string) string {
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])
}

func fakeFixtureScopeDigest(req EmbeddingRequest, configHash string) string {
	identity := struct {
		ConfigHash       string `json:"config_hash"`
		EndpointKey      string `json:"endpoint_key"`
		ModelKey         string `json:"model_key"`
		ProviderModel    string `json:"provider_model"`
		Dimensions       int    `json:"dimensions"`
		VectorSpace      string `json:"vector_space"`
		CapabilityKey    string `json:"capability_key"`
		ProviderOverride string `json:"provider_model_override"`
	}{
		ConfigHash:       configHash,
		EndpointKey:      req.Endpoint.Key,
		ModelKey:         req.Model.Key,
		ProviderModel:    req.Model.ProviderModelName,
		Dimensions:       req.Model.EmbeddingDims,
		VectorSpace:      req.Model.VectorSpace,
		CapabilityKey:    req.Capability.Key,
		ProviderOverride: req.Capability.ProviderModelOverride,
	}
	canonical, _ := json.Marshal(identity)
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func tokenBasisV1(input string, dimensions int) ([]float64, error) {
	if dimensions <= 0 || dimensions > fakeFixtureMaxDimensions {
		return nil, fmt.Errorf("token-basis-v1 dimensions must be between 1 and %d", fakeFixtureMaxDimensions)
	}
	normalized := normalizeFakeFixtureInput(input)
	vector := make([]float64, dimensions)
	recognized := false
	for _, token := range fakeFixtureTokens(normalized) {
		if !strings.HasPrefix(token, "axis-") {
			continue
		}
		index, ok := parseFixtureAxis(token[len("axis-"):])
		if !ok || index >= dimensions {
			continue
		}
		vector[index]++
		recognized = true
	}
	if !recognized {
		fillStableHashVector(vector, normalized)
	}
	return normalizeFixtureVector(vector)
}

func fakeFixtureTokens(normalized string) []string {
	return strings.FieldsFunc(normalized, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-')
	})
}

func parseFixtureAxis(value string) (int, bool) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, false
	}
	index := 0
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, false
		}
		if index > (fakeFixtureMaxDimensions-1)/10 {
			return 0, false
		}
		index = index*10 + int(value[i]-'0')
	}
	return index, true
}

func fillStableHashVector(vector []float64, normalized string) {
	for offset, block := 0, uint32(0); offset < len(vector); block++ {
		hasher := sha256.New()
		_, _ = hasher.Write([]byte(normalized))
		_, _ = hasher.Write([]byte{0})
		var suffix [4]byte
		binary.BigEndian.PutUint32(suffix[:], block)
		_, _ = hasher.Write(suffix[:])
		digest := hasher.Sum(nil)
		for i := 0; i+4 <= len(digest) && offset < len(vector); i, offset = i+4, offset+1 {
			value := int32(binary.BigEndian.Uint32(digest[i : i+4]))
			vector[offset] = float64(value) / 2147483648.0
		}
	}
}

func normalizeFixtureVector(vector []float64) ([]float64, error) {
	sum := 0.0
	for _, value := range vector {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, errors.New("token-basis-v1 generated a non-finite component")
		}
		sum += value * value
	}
	if sum == 0 {
		vector[0] = 1
		return vector, nil
	}
	norm := math.Sqrt(sum)
	for i := range vector {
		vector[i] /= norm
	}
	return vector, nil
}

func (f *FakeConnector) selectFakeFixture(req EmbeddingRequest) (fakeFixtureSelection, bool, error) {
	config, configHash, configured, err := fakeFixtureFromRequest(req)
	if err != nil || !configured {
		return fakeFixtureSelection{}, configured, err
	}
	normalized := normalizeFakeFixtureInput(req.Input)
	digest := fakeFixtureInputDigest(normalized)
	scopeHash := fakeFixtureScopeDigest(req, configHash)
	selection := fakeFixtureSelection{config: config, configHash: configHash, scopeHash: scopeHash, inputDigest: digest, normalized: normalized}
	if config.Script == nil {
		return selection, true, nil
	}
	var outcomes []fakeFixtureOutcome
	for _, entry := range config.Script.Entries {
		if entry.InputSHA256 == digest {
			outcomes = entry.Outcomes
			break
		}
	}
	if len(outcomes) == 0 {
		if config.Script.RequireMatch {
			return fakeFixtureSelection{}, true, ConnectorError{Code: "fake_fixture_input_unknown", Err: errors.New("fake fixture script has no outcome for normalized input digest")}
		}
		return selection, true, nil
	}
	selection.stateKey = scopeHash + ":" + digest
	f.mu.Lock()
	defer f.mu.Unlock()
	selection.generation = f.fixtureGeneration
	if f.fixtureAttempts == nil {
		f.fixtureAttempts = map[string]int{}
		f.fixtureStats = map[string]FakeFixtureStat{}
	}
	attempt := f.fixtureAttempts[selection.stateKey]
	if attempt >= len(outcomes) {
		return fakeFixtureSelection{}, true, ConnectorError{Code: "fake_fixture_script_exhausted", Err: errors.New("fake fixture outcome sequence exhausted")}
	}
	if _, exists := f.fixtureAttempts[selection.stateKey]; !exists && len(f.fixtureAttempts) >= fakeFixtureMaxStateEntries {
		return fakeFixtureSelection{}, true, ConnectorError{Code: "fake_fixture_state_capacity", Err: errors.New("fake fixture state capacity reached")}
	}
	f.fixtureAttempts[selection.stateKey] = attempt + 1
	stat := f.fixtureStats[selection.stateKey]
	stat.ScriptSHA256 = configHash
	stat.ScopeSHA256 = scopeHash
	stat.InputSHA256 = digest
	stat.Attempts++
	f.fixtureStats[selection.stateKey] = stat
	outcome := outcomes[attempt]
	selection.outcome = &outcome
	selection.attempt = attempt + 1
	return selection, true, nil
}

func (f *FakeConnector) executeFakeFixture(ctx context.Context, req EmbeddingRequest, selection fakeFixtureSelection) (EmbeddingResponse, error) {
	requestID := fmt.Sprintf("fake-tbv1-%s-%s", selection.scopeHash[:12], selection.inputDigest[:12])
	if selection.outcome != nil {
		requestID = fmt.Sprintf("fake-%s-%s-a%d", selection.scopeHash[:12], selection.inputDigest[:12], selection.attempt)
		if selection.outcome.DelayMS > 0 {
			timer := time.NewTimer(time.Duration(selection.outcome.DelayMS) * time.Millisecond)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				f.recordFakeFixtureCompletion(selection.stateKey, selection.generation, "canceled")
				return EmbeddingResponse{ProviderRequestID: requestID}, ctx.Err()
			case <-timer.C:
			}
		}
	}
	if err := ctx.Err(); err != nil {
		f.recordFakeFixtureCompletion(selection.stateKey, selection.generation, "canceled")
		return EmbeddingResponse{ProviderRequestID: requestID}, err
	}
	if selection.stateKey != "" && !f.fixtureGenerationCurrent(selection.generation) {
		return EmbeddingResponse{ProviderRequestID: requestID}, ConnectorError{Code: "fake_fixture_reset", Err: errors.New("fake fixture state reset during request")}
	}
	kind := "success"
	if selection.outcome != nil {
		kind = selection.outcome.Kind
	}
	switch kind {
	case "blocked":
		f.recordFakeFixtureCompletion(selection.stateKey, selection.generation, "failed")
		return EmbeddingResponse{ProviderRequestID: requestID}, ConnectorError{Code: "fake_blocked", Err: errors.New("fake fixture blocked response")}
	case "refused":
		f.recordFakeFixtureCompletion(selection.stateKey, selection.generation, "failed")
		return EmbeddingResponse{ProviderRequestID: requestID}, ConnectorError{Code: "fake_refused", Err: errors.New("fake fixture refused response")}
	case "retryable-error", "error":
		code := selection.outcome.Code
		if code == "" {
			if kind == "retryable-error" {
				code = "fake_retryable"
			} else {
				code = "fake_failure"
			}
		}
		f.recordFakeFixtureCompletion(selection.stateKey, selection.generation, "failed")
		return EmbeddingResponse{ProviderRequestID: requestID}, ConnectorError{Code: code, Retryable: kind == "retryable-error", Err: errors.New("fake fixture scripted error")}
	}
	dimensions := req.Model.EmbeddingDims
	if kind == "wrong-dimensions" {
		dimensions = selection.outcome.Dimensions
		if dimensions == 0 {
			dimensions = req.Model.EmbeddingDims + 1
			if dimensions > fakeFixtureMaxDimensions {
				dimensions = req.Model.EmbeddingDims - 1
			}
		}
	}
	vector, err := tokenBasisV1(selection.normalized, dimensions)
	if err != nil {
		f.recordFakeFixtureCompletion(selection.stateKey, selection.generation, "failed")
		return EmbeddingResponse{ProviderRequestID: requestID}, ConnectorError{Code: "fake_fixture_invalid", Err: err}
	}
	inputTokens := EstimateTokens(req.Input)
	f.recordFakeFixtureCompletion(selection.stateKey, selection.generation, "completed")
	return EmbeddingResponse{Vector: vector, ProviderRequestID: requestID, Usage: domaininference.Usage{InputTokens: inputTokens, TotalTokens: inputTokens, TokenCountSource: "estimated"}}, nil
}

func (f *FakeConnector) fixtureGenerationCurrent(generation uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return generation == f.fixtureGeneration
}

func (f *FakeConnector) recordFakeFixtureCompletion(stateKey string, generation uint64, outcome string) {
	if stateKey == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if generation != f.fixtureGeneration {
		return
	}
	if f.fixtureStats == nil {
		f.fixtureStats = map[string]FakeFixtureStat{}
	}
	stat := f.fixtureStats[stateKey]
	switch outcome {
	case "completed":
		stat.Completed++
	case "canceled":
		stat.Canceled++
	case "failed":
		stat.Failed++
	}
	f.fixtureStats[stateKey] = stat
}

func (f *FakeConnector) FixtureStats() []FakeFixtureStat {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]FakeFixtureStat, 0, len(f.fixtureStats))
	for _, stat := range f.fixtureStats {
		out = append(out, stat)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ScriptSHA256 != out[j].ScriptSHA256 {
			return out[i].ScriptSHA256 < out[j].ScriptSHA256
		}
		if out[i].ScopeSHA256 != out[j].ScopeSHA256 {
			return out[i].ScopeSHA256 < out[j].ScopeSHA256
		}
		return out[i].InputSHA256 < out[j].InputSHA256
	})
	return out
}

// ResetFixtureState resets process-local scripted attempt/accounting state.
// A daemon restart has the same effect because the fake connector is recreated.
func (f *FakeConnector) ResetFixtureState() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fixtureGeneration++
	f.fixtureAttempts = map[string]int{}
	f.fixtureStats = map[string]FakeFixtureStat{}
}
