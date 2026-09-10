package storage

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/fsperm"
)

var ErrNotFound = errors.New("lexical index storage not found")

type Store struct {
	root     string
	spaceID  string
	domainID string
}

func NewStore(root, spaceID, domainID string) Store {
	return Store{root: root, spaceID: spaceID, domainID: domainID}
}

func (s Store) ScopeDir() string {
	return filepath.Join(s.root, "search", "lexical", s.spaceID, s.domainID)
}

func (s Store) ReadManifest() (Manifest, error) {
	var manifest Manifest
	if err := readJSON(filepath.Join(s.ScopeDir(), "manifest.json"), &manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.FormatVersion != FormatVersion {
		return Manifest{}, fmt.Errorf("unsupported lexical manifest format version %d", manifest.FormatVersion)
	}
	return manifest, nil
}

func (s Store) WriteManifest(manifest Manifest) error {
	if manifest.FormatVersion == 0 {
		manifest.FormatVersion = FormatVersion
	}
	if manifest.SpaceID == "" {
		manifest.SpaceID = s.spaceID
	}
	if manifest.DomainID == "" {
		manifest.DomainID = s.domainID
	}
	now := utcNow()
	if manifest.CreatedAt == "" {
		manifest.CreatedAt = now
	}
	manifest.UpdatedAt = now
	return writeJSONAtomic(filepath.Join(s.ScopeDir(), "manifest.json"), manifest)
}

func (s Store) ReadCursor() (Cursor, error) {
	var cursor Cursor
	if err := readJSON(filepath.Join(s.ScopeDir(), "cursor.json"), &cursor); err != nil {
		return Cursor{}, err
	}
	return cursor, nil
}

func (s Store) WriteCursor(cursor Cursor) error {
	cursor.UpdatedAt = utcNow()
	return writeJSONAtomic(filepath.Join(s.ScopeDir(), "cursor.json"), cursor)
}

func (s Store) PublishSegment(data SegmentData) error {
	if strings.TrimSpace(data.Metadata.SegmentID) == "" {
		return fmt.Errorf("segment id is required")
	}
	if err := validateSegmentID(data.Metadata.SegmentID); err != nil {
		return err
	}
	segmentsDir := filepath.Join(s.ScopeDir(), "segments")
	if err := os.MkdirAll(segmentsDir, fsperm.SharedDir); err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp(segmentsDir, ".tmp-"+data.Metadata.SegmentID+"-")
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tmpDir)
		}
	}()
	if err := WriteSegment(tmpDir, data); err != nil {
		return err
	}
	if err := fsyncDir(tmpDir); err != nil {
		return err
	}
	finalDir := filepath.Join(segmentsDir, data.Metadata.SegmentID)
	if _, err := os.Stat(finalDir); err == nil {
		return fmt.Errorf("segment %q already exists", data.Metadata.SegmentID)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(tmpDir, finalDir); err != nil {
		return err
	}
	committed = true
	if err := fsyncDir(segmentsDir); err != nil {
		return err
	}
	manifest, err := s.ReadManifest()
	if errors.Is(err, ErrNotFound) {
		manifest = Manifest{FormatVersion: FormatVersion, SpaceID: s.spaceID, DomainID: s.domainID}
	} else if err != nil {
		return err
	}
	manifest.Segments = append(manifest.Segments, data.Metadata.SegmentID)
	return s.WriteManifest(manifest)
}

func (s Store) ReadSegment(segmentID string) (SegmentData, error) {
	if err := validateSegmentID(segmentID); err != nil {
		return SegmentData{}, err
	}
	return ReadSegment(filepath.Join(s.ScopeDir(), "segments", segmentID))
}

func WriteSegment(dir string, data SegmentData) error {
	if err := validateSegmentID(data.Metadata.SegmentID); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, fsperm.SharedDir); err != nil {
		return err
	}
	data = normalizeSegmentData(data)
	postingsBytes, termInfos, err := encodeTerms(data.Terms)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "postings.bin"), postingsBytes, fsperm.SharedFile); err != nil {
		return err
	}
	termsBytes, err := encodeTermIndex(termInfos)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "terms.idx"), termsBytes, fsperm.SharedFile); err != nil {
		return err
	}
	docsBytes, err := encodeDocs(data.Docs)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "docs.bin"), docsBytes, fsperm.SharedFile); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "segment.json"), data.Metadata); err != nil {
		return err
	}
	checksum, err := checksumFiles(dir, []string{"segment.json", "terms.idx", "postings.bin", "docs.bin"})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "checksum.sha256"), []byte(checksum+"\n"), fsperm.SharedFile); err != nil {
		return err
	}
	return fsyncFilesAndDir(dir, []string{"segment.json", "terms.idx", "postings.bin", "docs.bin", "checksum.sha256"})
}

func ReadSegment(dir string) (SegmentData, error) {
	storedChecksum, err := os.ReadFile(filepath.Join(dir, "checksum.sha256"))
	if err != nil {
		return SegmentData{}, mapNotExist(err)
	}
	actualChecksum, err := checksumFiles(dir, []string{"segment.json", "terms.idx", "postings.bin", "docs.bin"})
	if err != nil {
		return SegmentData{}, err
	}
	if strings.TrimSpace(string(storedChecksum)) != actualChecksum {
		return SegmentData{}, fmt.Errorf("lexical segment checksum mismatch")
	}
	var meta SegmentMetadata
	if err := readJSON(filepath.Join(dir, "segment.json"), &meta); err != nil {
		return SegmentData{}, err
	}
	if meta.FormatVersion != FormatVersion {
		return SegmentData{}, fmt.Errorf("unsupported lexical segment format version %d", meta.FormatVersion)
	}
	docsBytes, err := os.ReadFile(filepath.Join(dir, "docs.bin"))
	if err != nil {
		return SegmentData{}, err
	}
	docs, err := decodeDocs(docsBytes)
	if err != nil {
		return SegmentData{}, err
	}
	termsBytes, err := os.ReadFile(filepath.Join(dir, "terms.idx"))
	if err != nil {
		return SegmentData{}, err
	}
	termInfos, err := decodeTermIndex(termsBytes)
	if err != nil {
		return SegmentData{}, err
	}
	postingsBytes, err := os.ReadFile(filepath.Join(dir, "postings.bin"))
	if err != nil {
		return SegmentData{}, err
	}
	terms, err := decodeTerms(termInfos, postingsBytes)
	if err != nil {
		return SegmentData{}, err
	}
	return SegmentData{Metadata: meta, Docs: docs, Terms: terms}, nil
}

func normalizeSegmentData(data SegmentData) SegmentData {
	data.Metadata.FormatVersion = FormatVersion
	data.Metadata.DocCount = len(data.Docs)
	data.Metadata.DeletedCount = 0
	data.Metadata.TermCount = len(data.Terms)
	data.Metadata.AvgDocLength = 0
	data.Metadata.MinGraphRevision = 0
	data.Metadata.MaxGraphRevision = 0
	var total uint64
	var minRev, maxRev uint64
	for i := range data.Docs {
		if data.Docs[i].DocID == 0 {
			data.Docs[i].DocID = uint64(i + 1)
		}
		total += data.Docs[i].TokenCount
		if data.Docs[i].Deleted {
			data.Metadata.DeletedCount++
		}
		if data.Docs[i].GraphRevision != 0 && (minRev == 0 || data.Docs[i].GraphRevision < minRev) {
			minRev = data.Docs[i].GraphRevision
		}
		if data.Docs[i].GraphRevision > maxRev {
			maxRev = data.Docs[i].GraphRevision
		}
	}
	if len(data.Docs) > 0 {
		data.Metadata.AvgDocLength = float64(total) / float64(len(data.Docs))
	}
	data.Metadata.MinGraphRevision = minRev
	data.Metadata.MaxGraphRevision = maxRev
	sort.Slice(data.Docs, func(i, j int) bool { return data.Docs[i].DocID < data.Docs[j].DocID })
	return data
}

func encodeDocs(docs []Document) ([]byte, error) {
	var b strings.Builder
	w := bufio.NewWriter(&b)
	writeUvarint(w, uint64(len(docs)))
	for _, doc := range docs {
		writeUvarint(w, doc.DocID)
		writeString(w, doc.NodeID)
		writeUvarint(w, doc.GraphRevision)
		writeUvarint(w, doc.TokenCount)
		if doc.Deleted {
			writeUvarint(w, 1)
		} else {
			writeUvarint(w, 0)
		}
		writeUvarint(w, uint64(len(doc.Fields)))
		for _, field := range doc.Fields {
			writeString(w, field.Path)
			writeUvarint(w, field.TokenCount)
		}
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

func decodeDocs(data []byte) ([]Document, error) {
	r := bytesReader(data)
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	docs := make([]Document, 0, count)
	for i := uint64(0); i < count; i++ {
		docID, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		nodeID, err := readString(r)
		if err != nil {
			return nil, err
		}
		rev, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		tokenCount, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		deleted, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		fieldCount, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		fields := make([]DocumentField, 0, fieldCount)
		for j := uint64(0); j < fieldCount; j++ {
			path, err := readString(r)
			if err != nil {
				return nil, err
			}
			fieldTokens, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, err
			}
			fields = append(fields, DocumentField{Path: path, TokenCount: fieldTokens})
		}
		docs = append(docs, Document{DocID: docID, NodeID: nodeID, GraphRevision: rev, TokenCount: tokenCount, Deleted: deleted == 1, Fields: fields})
	}
	return docs, nil
}

func encodeTerms(terms map[string][]Posting) ([]byte, []TermInfo, error) {
	keys := make([]string, 0, len(terms))
	for term := range terms {
		keys = append(keys, term)
	}
	sort.Strings(keys)
	var out []byte
	infos := make([]TermInfo, 0, len(keys))
	for _, term := range keys {
		postings := append([]Posting(nil), terms[term]...)
		sort.Slice(postings, func(i, j int) bool { return postings[i].DocID < postings[j].DocID })
		start := len(out)
		encoded, err := encodePostings(postings)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, encoded...)
		infos = append(infos, TermInfo{Term: term, DocumentFrequency: uint64(len(postings)), PostingsOffset: uint64(start), PostingsLength: uint64(len(encoded))})
	}
	return out, infos, nil
}

func decodeTerms(infos []TermInfo, postingsBytes []byte) (map[string][]Posting, error) {
	terms := make(map[string][]Posting, len(infos))
	for _, info := range infos {
		end := info.PostingsOffset + info.PostingsLength
		if end > uint64(len(postingsBytes)) {
			return nil, fmt.Errorf("postings range for term %q exceeds postings file", info.Term)
		}
		postings, err := decodePostings(postingsBytes[info.PostingsOffset:end], info.DocumentFrequency)
		if err != nil {
			return nil, fmt.Errorf("decode postings for %q: %w", info.Term, err)
		}
		terms[info.Term] = postings
	}
	return terms, nil
}

func encodePostings(postings []Posting) ([]byte, error) {
	var b strings.Builder
	w := bufio.NewWriter(&b)
	var previousDoc uint64
	for _, posting := range postings {
		if posting.DocID < previousDoc {
			return nil, fmt.Errorf("postings must be sorted by doc id")
		}
		writeUvarint(w, posting.DocID-previousDoc)
		writeUvarint(w, posting.TermFrequency)
		writeUvarint(w, posting.FieldMask)
		writeUvarint(w, uint64(len(posting.Positions)))
		var previousPosition uint64
		for _, position := range posting.Positions {
			if position < previousPosition {
				return nil, fmt.Errorf("positions must be sorted")
			}
			writeUvarint(w, position-previousPosition)
			previousPosition = position
		}
		previousDoc = posting.DocID
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

func decodePostings(data []byte, count uint64) ([]Posting, error) {
	r := bytesReader(data)
	postings := make([]Posting, 0, count)
	var previousDoc uint64
	for i := uint64(0); i < count; i++ {
		delta, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		tf, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		fieldMask, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		positionCount, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		positions := make([]uint64, 0, positionCount)
		var previousPosition uint64
		for j := uint64(0); j < positionCount; j++ {
			positionDelta, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, err
			}
			previousPosition += positionDelta
			positions = append(positions, previousPosition)
		}
		previousDoc += delta
		postings = append(postings, Posting{DocID: previousDoc, TermFrequency: tf, FieldMask: fieldMask, Positions: positions})
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("trailing postings bytes")
	}
	return postings, nil
}

func encodeTermIndex(infos []TermInfo) ([]byte, error) {
	var b strings.Builder
	w := bufio.NewWriter(&b)
	writeUvarint(w, uint64(len(infos)))
	for _, info := range infos {
		writeString(w, info.Term)
		writeUvarint(w, info.DocumentFrequency)
		writeUvarint(w, info.PostingsOffset)
		writeUvarint(w, info.PostingsLength)
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

func decodeTermIndex(data []byte) ([]TermInfo, error) {
	r := bytesReader(data)
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	infos := make([]TermInfo, 0, count)
	for i := uint64(0); i < count; i++ {
		term, err := readString(r)
		if err != nil {
			return nil, err
		}
		df, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		offset, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		length, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		infos = append(infos, TermInfo{Term: term, DocumentFrequency: df, PostingsOffset: offset, PostingsLength: length})
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("trailing term index bytes")
	}
	return infos, nil
}

func writeUvarint(w io.ByteWriter, value uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	_, _ = w.(io.Writer).Write(buf[:n])
}

func writeString(w io.ByteWriter, value string) {
	writeUvarint(w, uint64(len(value)))
	_, _ = w.(io.Writer).Write([]byte(value))
}

func readString(r *byteReader) (string, error) {
	length, err := binary.ReadUvarint(r)
	if err != nil {
		return "", err
	}
	if length > uint64(r.Len()) {
		return "", io.ErrUnexpectedEOF
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readJSON(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return mapNotExist(err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, fsperm.SharedFile)
}

func writeJSONAtomic(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), fsperm.SharedDir); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-"+filepath.Base(path)+"-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	committed = true
	return fsyncDir(filepath.Dir(path))
}

func checksumFiles(dir string, names []string) (string, error) {
	h := sha256.New()
	for _, name := range names {
		if _, err := h.Write([]byte(name)); err != nil {
			return "", err
		}
		if _, err := h.Write([]byte{0}); err != nil {
			return "", err
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return "", err
		}
		if _, err := h.Write(data); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func fsyncFilesAndDir(dir string, names []string) error {
	for _, name := range names {
		file, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	return fsyncDir(dir)
}

func fsyncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func validateSegmentID(segmentID string) error {
	if strings.TrimSpace(segmentID) == "" {
		return fmt.Errorf("segment id is required")
	}
	if strings.Contains(segmentID, "/") || strings.Contains(segmentID, "\\") || strings.Contains(segmentID, "..") {
		return fmt.Errorf("invalid segment id %q", segmentID)
	}
	return nil
}

func utcNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func mapNotExist(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

type byteReader struct {
	data []byte
	pos  int
}

func bytesReader(data []byte) *byteReader { return &byteReader{data: data} }

func (r *byteReader) ReadByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *byteReader) Len() int { return len(r.data) - r.pos }
