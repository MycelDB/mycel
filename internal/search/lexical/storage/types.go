package storage

const (
	FormatVersion      = 1
	IndexFormatVersion = "lexical-index-v1"
)

type Manifest struct {
	FormatVersion   int      `json:"format_version"`
	AnalyzerVersion string   `json:"analyzer_version"`
	SpaceID         string   `json:"space_id"`
	DomainID        string   `json:"domain_id"`
	Segments        []string `json:"segments"`
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
}

type Cursor struct {
	IndexedGraphRevision uint64 `json:"indexed_graph_revision"`
	UpdatedAt            string `json:"updated_at"`
	LastError            string `json:"last_error"`
}

type SegmentMetadata struct {
	SegmentID        string  `json:"segment_id"`
	FormatVersion    int     `json:"format_version"`
	DocCount         int     `json:"doc_count"`
	DeletedCount     int     `json:"deleted_count"`
	TermCount        int     `json:"term_count"`
	AvgDocLength     float64 `json:"avg_doc_length"`
	MinGraphRevision uint64  `json:"min_graph_revision"`
	MaxGraphRevision uint64  `json:"max_graph_revision"`
}

type Document struct {
	DocID         uint64
	NodeID        string
	GraphRevision uint64
	TokenCount    uint64
	Fields        []DocumentField
	Deleted       bool
}

type DocumentField struct {
	Path       string
	TokenCount uint64
}

type Posting struct {
	DocID         uint64
	TermFrequency uint64
	Positions     []uint64
	FieldMask     uint64
}

type SegmentData struct {
	Metadata SegmentMetadata
	Docs     []Document
	Terms    map[string][]Posting
}

type TermInfo struct {
	Term              string
	DocumentFrequency uint64
	PostingsOffset    uint64
	PostingsLength    uint64
}
