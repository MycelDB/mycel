package hybrid

import (
	"errors"
	"fmt"
	"sort"
)

const (
	DefaultLexicalWeight  = 0.5
	DefaultSemanticWeight = 0.5
	DefaultRankConstant   = 60.0
	DefaultPageSize       = 20
	DefaultMinCandidates  = 50
	DefaultCandidateRatio = 5
)

var (
	ErrInvalidWeights        = errors.New("invalid hybrid search weights")
	ErrInvalidRankConstant   = errors.New("invalid hybrid rank constant")
	ErrInvalidCandidate      = errors.New("invalid hybrid candidate")
	ErrUnsupportedFusionMode = errors.New("unsupported hybrid fusion strategy")
)

type SourceKind string

const (
	SourceLexical  SourceKind = "lexical"
	SourceSemantic SourceKind = "semantic"
)

type FusionStrategy string

const (
	FusionWeightedReciprocalRank FusionStrategy = "weighted_reciprocal_rank"
)

type WeightOptions struct {
	Lexical  float64
	Semantic float64
}

type Weights struct {
	Lexical  float64
	Semantic float64
}

type Options struct {
	Weights      WeightOptions
	Strategy     FusionStrategy
	RequireBoth  bool
	RankConstant float64
}

type Candidate struct {
	NodeID   string
	Lexical  *Source
	Semantic *Source
}

type Source struct {
	RawScore float64
	Rank     int
}

type Result struct {
	NodeID     string
	Score      float64
	Lexical    *ResultSource
	Semantic   *ResultSource
	SourceMask SourceMask
}

type ResultSource struct {
	RawScore        float64
	Rank            int
	NormalizedScore float64
}

type SourceMask uint8

const (
	MaskLexical SourceMask = 1 << iota
	MaskSemantic
)

func (m SourceMask) Has(kind SourceKind) bool {
	switch kind {
	case SourceLexical:
		return m&MaskLexical != 0
	case SourceSemantic:
		return m&MaskSemantic != 0
	default:
		return false
	}
}

func NormalizeWeights(opts WeightOptions) (Weights, error) {
	lexical := opts.Lexical
	semantic := opts.Semantic
	if lexical == 0 && semantic == 0 {
		lexical = DefaultLexicalWeight
		semantic = DefaultSemanticWeight
	}
	if lexical < 0 || semantic < 0 {
		return Weights{}, fmt.Errorf("%w: weights must be non-negative", ErrInvalidWeights)
	}
	total := lexical + semantic
	if total <= 0 {
		return Weights{}, fmt.Errorf("%w: at least one weight must be greater than zero", ErrInvalidWeights)
	}
	return Weights{Lexical: lexical / total, Semantic: semantic / total}, nil
}

func CandidateCount(pageSize, requested, cap int) int {
	if requested > 0 {
		return clampPositive(requested, cap)
	}
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	count := pageSize * DefaultCandidateRatio
	if count < DefaultMinCandidates {
		count = DefaultMinCandidates
	}
	return clampPositive(count, cap)
}

func Fuse(candidates []Candidate, opts Options) ([]Result, error) {
	if opts.Strategy == "" {
		opts.Strategy = FusionWeightedReciprocalRank
	}
	if opts.Strategy != FusionWeightedReciprocalRank {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedFusionMode, opts.Strategy)
	}
	rankConstant := opts.RankConstant
	if rankConstant == 0 {
		rankConstant = DefaultRankConstant
	}
	if rankConstant < 0 {
		return nil, fmt.Errorf("%w: rank constant must be non-negative", ErrInvalidRankConstant)
	}
	weights, err := NormalizeWeights(opts.Weights)
	if err != nil {
		return nil, err
	}
	byNode := map[string]Candidate{}
	for _, candidate := range candidates {
		if candidate.NodeID == "" {
			return nil, fmt.Errorf("%w: node_id is required", ErrInvalidCandidate)
		}
		if candidate.Lexical == nil && candidate.Semantic == nil {
			return nil, fmt.Errorf("%w: at least one source is required for %s", ErrInvalidCandidate, candidate.NodeID)
		}
		if err := validateSource(candidate.Lexical); err != nil {
			return nil, fmt.Errorf("%w: lexical source for %s: %v", ErrInvalidCandidate, candidate.NodeID, err)
		}
		if err := validateSource(candidate.Semantic); err != nil {
			return nil, fmt.Errorf("%w: semantic source for %s: %v", ErrInvalidCandidate, candidate.NodeID, err)
		}
		existing := byNode[candidate.NodeID]
		existing.NodeID = candidate.NodeID
		existing.Lexical = bestSource(existing.Lexical, candidate.Lexical)
		existing.Semantic = bestSource(existing.Semantic, candidate.Semantic)
		byNode[candidate.NodeID] = existing
	}
	results := make([]Result, 0, len(byNode))
	for _, candidate := range byNode {
		if opts.RequireBoth && (candidate.Lexical == nil || candidate.Semantic == nil) {
			continue
		}
		result := Result{NodeID: candidate.NodeID}
		if candidate.Lexical != nil {
			normalized := reciprocalRank(rankConstant, candidate.Lexical.Rank)
			result.Lexical = &ResultSource{RawScore: candidate.Lexical.RawScore, Rank: candidate.Lexical.Rank, NormalizedScore: normalized}
			result.Score += weights.Lexical * normalized
			result.SourceMask |= MaskLexical
		}
		if candidate.Semantic != nil {
			normalized := reciprocalRank(rankConstant, candidate.Semantic.Rank)
			result.Semantic = &ResultSource{RawScore: candidate.Semantic.RawScore, Rank: candidate.Semantic.Rank, NormalizedScore: normalized}
			result.Score += weights.Semantic * normalized
			result.SourceMask |= MaskSemantic
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return lessResult(results[i], results[j]) })
	return results, nil
}

func validateSource(source *Source) error {
	if source == nil {
		return nil
	}
	if source.Rank <= 0 {
		return fmt.Errorf("rank must be greater than zero")
	}
	return nil
}

func bestSource(a, b *Source) *Source {
	if a == nil {
		return cloneSource(b)
	}
	if b == nil {
		return cloneSource(a)
	}
	if b.Rank < a.Rank || (b.Rank == a.Rank && b.RawScore > a.RawScore) {
		return cloneSource(b)
	}
	return cloneSource(a)
}

func cloneSource(source *Source) *Source {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func reciprocalRank(rankConstant float64, rank int) float64 {
	return 1 / (rankConstant + float64(rank))
}

func lessResult(a, b Result) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	aBoth := a.SourceMask&MaskLexical != 0 && a.SourceMask&MaskSemantic != 0
	bBoth := b.SourceMask&MaskLexical != 0 && b.SourceMask&MaskSemantic != 0
	if aBoth != bBoth {
		return aBoth
	}
	if bestRank(a) != bestRank(b) {
		return bestRank(a) < bestRank(b)
	}
	return a.NodeID < b.NodeID
}

func bestRank(result Result) int {
	best := int(^uint(0) >> 1)
	if result.Lexical != nil && result.Lexical.Rank < best {
		best = result.Lexical.Rank
	}
	if result.Semantic != nil && result.Semantic.Rank < best {
		best = result.Semantic.Rank
	}
	return best
}

func clampPositive(value, cap int) int {
	if cap > 0 && value > cap {
		return cap
	}
	return value
}
