package hybrid

import (
	"errors"
	"math"
	"testing"
)

func TestNormalizeWeightsDefaultsAndNormalizes(t *testing.T) {
	defaults, err := NormalizeWeights(WeightOptions{})
	if err != nil {
		t.Fatalf("NormalizeWeights defaults returned error: %v", err)
	}
	assertClose(t, defaults.Lexical, 0.5)
	assertClose(t, defaults.Semantic, 0.5)

	weighted, err := NormalizeWeights(WeightOptions{Lexical: 2, Semantic: 1})
	if err != nil {
		t.Fatalf("NormalizeWeights weighted returned error: %v", err)
	}
	assertClose(t, weighted.Lexical, 2.0/3.0)
	assertClose(t, weighted.Semantic, 1.0/3.0)
}

func TestNormalizeWeightsRejectsInvalidWeights(t *testing.T) {
	if _, err := NormalizeWeights(WeightOptions{Lexical: -1, Semantic: 1}); !errors.Is(err, ErrInvalidWeights) {
		t.Fatalf("negative weight error = %v, want ErrInvalidWeights", err)
	}
}

func TestCandidateCountDefaultAndCap(t *testing.T) {
	if got := CandidateCount(0, 0, 1000); got != DefaultPageSize*DefaultCandidateRatio {
		t.Fatalf("default candidate count = %d, want %d", got, DefaultPageSize*DefaultCandidateRatio)
	}
	if got := CandidateCount(20, 0, 1000); got != 100 {
		t.Fatalf("page-size derived candidate count = %d, want 100", got)
	}
	if got := CandidateCount(20, 250, 1000); got != 250 {
		t.Fatalf("requested candidate count = %d, want 250", got)
	}
	if got := CandidateCount(500, 0, 1000); got != 1000 {
		t.Fatalf("capped candidate count = %d, want 1000", got)
	}
}

func TestFuseWeightedReciprocalRank(t *testing.T) {
	results, err := Fuse([]Candidate{
		{NodeID: "a", Lexical: &Source{RawScore: 12, Rank: 1}, Semantic: &Source{RawScore: 0.70, Rank: 10}},
		{NodeID: "b", Lexical: &Source{RawScore: 5, Rank: 3}, Semantic: &Source{RawScore: 0.99, Rank: 1}},
		{NodeID: "c", Lexical: &Source{RawScore: 8, Rank: 2}},
	}, Options{Weights: WeightOptions{Lexical: 0.6, Semantic: 0.4}, RankConstant: 0})
	if err != nil {
		t.Fatalf("Fuse returned error: %v", err)
	}
	assertIDs(t, results, []string{"b", "a", "c"})
	assertClose(t, results[0].Score, 0.6*(1.0/63.0)+0.4*(1.0/61.0))
	assertClose(t, results[1].Score, 0.6*(1.0/61.0)+0.4*(1.0/70.0))
	assertClose(t, results[2].Score, 0.6*(1.0/62.0))
	if !results[0].SourceMask.Has(SourceLexical) || !results[0].SourceMask.Has(SourceSemantic) {
		t.Fatalf("result source mask = %v, want both sources", results[0].SourceMask)
	}
}

func TestFuseKeepsSemanticOnlyAndLexicalOnlyCandidates(t *testing.T) {
	results, err := Fuse([]Candidate{
		{NodeID: "lexical", Lexical: &Source{RawScore: 10, Rank: 1}},
		{NodeID: "semantic", Semantic: &Source{RawScore: 0.95, Rank: 1}},
	}, Options{})
	if err != nil {
		t.Fatalf("Fuse returned error: %v", err)
	}
	assertIDs(t, results, []string{"lexical", "semantic"})
	if results[0].Semantic != nil || results[1].Lexical != nil {
		t.Fatalf("unexpected sources: %#v", results)
	}
}

func TestFuseRequireBoth(t *testing.T) {
	results, err := Fuse([]Candidate{
		{NodeID: "both", Lexical: &Source{RawScore: 10, Rank: 2}, Semantic: &Source{RawScore: 0.9, Rank: 1}},
		{NodeID: "lexical", Lexical: &Source{RawScore: 12, Rank: 1}},
		{NodeID: "semantic", Semantic: &Source{RawScore: 0.8, Rank: 2}},
	}, Options{RequireBoth: true})
	if err != nil {
		t.Fatalf("Fuse returned error: %v", err)
	}
	assertIDs(t, results, []string{"both"})
}

func TestFuseZeroWeightDisablesScoreContribution(t *testing.T) {
	results, err := Fuse([]Candidate{
		{NodeID: "lexical", Lexical: &Source{RawScore: 100, Rank: 1}},
		{NodeID: "semantic", Semantic: &Source{RawScore: 1, Rank: 1}},
	}, Options{Weights: WeightOptions{Lexical: 0, Semantic: 1}})
	if err != nil {
		t.Fatalf("Fuse returned error: %v", err)
	}
	assertIDs(t, results, []string{"semantic", "lexical"})
	assertClose(t, results[0].Score, 1.0/61.0)
	assertClose(t, results[1].Score, 0)
}

func TestFuseDeduplicatesByBestSourceRank(t *testing.T) {
	results, err := Fuse([]Candidate{
		{NodeID: "a", Lexical: &Source{RawScore: 1, Rank: 5}},
		{NodeID: "a", Lexical: &Source{RawScore: 2, Rank: 3}},
		{NodeID: "a", Semantic: &Source{RawScore: 0.5, Rank: 4}},
	}, Options{})
	if err != nil {
		t.Fatalf("Fuse returned error: %v", err)
	}
	assertIDs(t, results, []string{"a"})
	if results[0].Lexical.Rank != 3 || results[0].Semantic.Rank != 4 {
		t.Fatalf("deduped sources = %#v", results[0])
	}
}

func TestFuseTieBreaksPreferBothSourcesThenBestRankThenNodeID(t *testing.T) {
	results, err := Fuse([]Candidate{
		{NodeID: "z", Lexical: &Source{RawScore: 1, Rank: 1}},
		{NodeID: "b", Lexical: &Source{RawScore: 1, Rank: 2}, Semantic: &Source{RawScore: 1, Rank: 2}},
		{NodeID: "a", Lexical: &Source{RawScore: 1, Rank: 2}, Semantic: &Source{RawScore: 1, Rank: 2}},
	}, Options{Weights: WeightOptions{Lexical: 1, Semantic: 0}, RankConstant: 0})
	if err != nil {
		t.Fatalf("Fuse returned error: %v", err)
	}
	assertIDs(t, results, []string{"z", "a", "b"})

	results, err = Fuse([]Candidate{
		{NodeID: "one-source", Lexical: &Source{RawScore: 1, Rank: 1}},
		{NodeID: "both", Lexical: &Source{RawScore: 1, Rank: 1}, Semantic: &Source{RawScore: 0.1, Rank: 99}},
	}, Options{Weights: WeightOptions{Lexical: 1, Semantic: 0}, RankConstant: 0})
	if err != nil {
		t.Fatalf("Fuse returned error: %v", err)
	}
	assertIDs(t, results, []string{"both", "one-source"})
}

func TestFuseRejectsInvalidCandidatesAndOptions(t *testing.T) {
	cases := []struct {
		name string
		in   []Candidate
		opts Options
		want error
	}{
		{name: "missing node id", in: []Candidate{{Lexical: &Source{Rank: 1}}}, want: ErrInvalidCandidate},
		{name: "missing source", in: []Candidate{{NodeID: "a"}}, want: ErrInvalidCandidate},
		{name: "bad lexical rank", in: []Candidate{{NodeID: "a", Lexical: &Source{Rank: 0}}}, want: ErrInvalidCandidate},
		{name: "bad rank constant", in: []Candidate{{NodeID: "a", Lexical: &Source{Rank: 1}}}, opts: Options{RankConstant: -1}, want: ErrInvalidRankConstant},
		{name: "unsupported strategy", in: []Candidate{{NodeID: "a", Lexical: &Source{Rank: 1}}}, opts: Options{Strategy: "raw_sum"}, want: ErrUnsupportedFusionMode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Fuse(tc.in, tc.opts)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Fuse error = %v, want %v", err, tc.want)
			}
		})
	}
}

func assertIDs(t *testing.T, results []Result, want []string) {
	t.Helper()
	if len(results) != len(want) {
		t.Fatalf("results len = %d, want %d: %#v", len(results), len(want), results)
	}
	for i := range want {
		if results[i].NodeID != want[i] {
			t.Fatalf("result ids = %#v, want %#v", resultIDs(results), want)
		}
	}
}

func resultIDs(results []Result) []string {
	ids := make([]string, len(results))
	for i, result := range results {
		ids[i] = result.NodeID
	}
	return ids
}

func assertClose(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 0.0000001 {
		t.Fatalf("value = %.12f, want %.12f", got, want)
	}
}
