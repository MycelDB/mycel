package client

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/fsperm"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	spaceExportFormatVersion = "mycel-space-export-v1"
	spaceExportTTL           = 24 * time.Hour
	spaceExportChunkBytes    = 64 * 1024
)

type spaceExportJobRecord struct {
	ExportID     string
	PrincipalID  string
	Username     string
	Kind         string
	Status       clientv1.SpaceExportStatus
	Options      *clientv1.SpaceExportOptions
	Counts       *clientv1.SpaceExportCounts
	Filename     string
	Path         string
	SizeBytes    int64
	Progress     int32
	ErrorMessage string
	CreateTime   time.Time
	UpdateTime   time.Time
	CompleteTime time.Time
	ExpireTime   time.Time
}

type spaceManifest struct {
	FormatVersion string               `json:"format_version"`
	GeneratedAt   time.Time            `json:"generated_at"`
	RequestedBy   spaceSubject         `json:"requested_by"`
	Options       spaceOptions         `json:"options"`
	Counts        spaceCounts          `json:"counts"`
	Spaces        []spaceManifestSpace `json:"spaces"`
	Files         []spaceManifestFile  `json:"files"`
	Limitations   []string             `json:"limitations,omitempty"`
}

type spaceSubject struct {
	PrincipalID string `json:"principal_id"`
	Username    string `json:"username"`
	Type        string `json:"type,omitempty"`
}

type spaceOptions struct {
	SpaceID              string   `json:"space_id"`
	DomainIDs            []string `json:"domain_ids,omitempty"`
	IncludeBlobs         bool     `json:"include_blobs"`
	IncludeSystemDomains bool     `json:"include_system_domains"`
}

type spaceCounts struct {
	Spaces  int `json:"spaces"`
	Domains int `json:"domains"`
	Nodes   int `json:"nodes"`
	Edges   int `json:"edges"`
	Blobs   int `json:"blobs"`
	Files   int `json:"files"`
}

type spaceManifestSpace struct {
	SpaceID          string                `json:"space_id"`
	Name             string                `json:"name"`
	OwnerPrincipalID string                `json:"owner_principal_id,omitempty"`
	Path             string                `json:"path"`
	Domains          []spaceManifestDomain `json:"domains"`
}

type spaceManifestDomain struct {
	DomainID   string `json:"domain_id"`
	Key        string `json:"key,omitempty"`
	Name       string `json:"name,omitempty"`
	Default    bool   `json:"default,omitempty"`
	Path       string `json:"path"`
	NodesPath  string `json:"nodes_path,omitempty"`
	EdgesPath  string `json:"edges_path,omitempty"`
	SchemaPath string `json:"schema_path,omitempty"`
	BlobPath   string `json:"blob_manifest_path,omitempty"`
	NodeCount  int    `json:"node_count"`
	EdgeCount  int    `json:"edge_count"`
	BlobCount  int    `json:"blob_count"`
}

type spaceManifestFile struct {
	Path        string `json:"path"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256Hex   string `json:"sha256_hex"`
}

type spaceBlobRow struct {
	BlobID           string `json:"blob_id"`
	Digest           string `json:"digest,omitempty"`
	SizeBytes        int64  `json:"size_bytes"`
	DeclaredMimeType string `json:"declared_mime_type,omitempty"`
	OriginalFilename string `json:"original_filename,omitempty"`
	File             string `json:"file,omitempty"`
	SHA256Hex        string `json:"sha256_hex,omitempty"`
}

func (s *ImportExportService) WithSchemaManager(schemas schemaservice.Manager) *ImportExportService {
	s.schemas = schemas
	return s
}

func (s *ImportExportService) WithSpaceExportDir(dir string) *ImportExportService {
	s.spaceExportDir = strings.TrimSpace(dir)
	return s
}

func normalizeSpaceExportOptions(in *clientv1.SpaceExportOptions) *clientv1.SpaceExportOptions {
	if in == nil {
		return &clientv1.SpaceExportOptions{IncludeBlobs: true}
	}
	out := proto.Clone(in).(*clientv1.SpaceExportOptions)
	out.SpaceId = strings.TrimSpace(out.GetSpaceId())
	cleaned := make([]string, 0, len(out.GetDomainIds()))
	seen := map[string]bool{}
	for _, domainID := range out.GetDomainIds() {
		domainID = strings.TrimSpace(domainID)
		if domainID == "" || seen[domainID] {
			continue
		}
		seen[domainID] = true
		cleaned = append(cleaned, domainID)
	}
	out.DomainIds = cleaned
	return out
}

func (s *ImportExportService) resolveSpaceExportDomains(ctx context.Context, principalID, spaceID string, opts *clientv1.SpaceExportOptions) ([]graphmodel.Domain, error) {
	if len(opts.GetDomainIds()) == 0 {
		domains, err := s.spaces.ListVisibleDomains(ctx, principalID, spaceID, opts.GetIncludeSystemDomains())
		if err != nil {
			return nil, mapDomainError(err, "space export list domains")
		}
		return domains, nil
	}
	domains := make([]graphmodel.Domain, 0, len(opts.GetDomainIds()))
	for _, domainID := range opts.GetDomainIds() {
		domain, err := s.spaces.GetVisibleDomain(ctx, principalID, spaceID, domainID, "")
		if err != nil {
			return nil, mapDomainError(err, "space export get domain")
		}
		domains = append(domains, domain)
	}
	return domains, nil
}

func (s *ImportExportService) CreateSpaceExport(ctx context.Context, req *clientv1.CreateSpaceExportRequest) (*clientv1.CreateSpaceExportResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(s.spaceExportDir) == "" {
		return nil, status.Error(codes.FailedPrecondition, "space export storage is not configured")
	}
	opts := normalizeSpaceExportOptions(req.GetOptions())
	if opts.GetSpaceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "space_id is required")
	}
	if _, err := s.spaces.GetVisibleSpace(ctx, principal.PrincipalID, opts.GetSpaceId()); err != nil {
		return nil, mapSpaceError(err, "space export authorize space")
	}
	if len(opts.GetDomainIds()) > 0 {
		for _, domainID := range opts.GetDomainIds() {
			if strings.TrimSpace(domainID) == "" {
				return nil, status.Error(codes.InvalidArgument, "domain_ids must not contain empty values")
			}
			if _, err := s.spaces.GetVisibleDomain(ctx, principal.PrincipalID, opts.GetSpaceId(), domainID, ""); err != nil {
				return nil, mapDomainError(err, "space export authorize domain")
			}
		}
	}
	now := time.Now().UTC()
	job := &spaceExportJobRecord{ExportID: uuid.NewString(), PrincipalID: principal.PrincipalID, Username: principal.Username, Kind: string(principal.Kind), Status: clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_QUEUED, Options: proto.Clone(opts).(*clientv1.SpaceExportOptions), Counts: &clientv1.SpaceExportCounts{}, Filename: "mycel-space-export-" + now.Format("20060102T150405Z") + ".zip", Progress: 0, CreateTime: now, UpdateTime: now, ExpireTime: now.Add(spaceExportTTL)}
	job.Path = filepath.Join(s.spaceExportDir, principal.PrincipalID, job.ExportID+".zip")
	s.spaceMu.Lock()
	if s.spaceJobs == nil {
		s.spaceJobs = map[string]*spaceExportJobRecord{}
	}
	s.spaceJobs[job.ExportID] = job
	initial := mapSpaceExportJob(cloneSpaceJob(job))
	s.spaceMu.Unlock()
	go s.runSpaceExportJob(job)
	return &clientv1.CreateSpaceExportResponse{Job: initial}, nil
}

func (s *ImportExportService) GetSpaceExport(ctx context.Context, req *clientv1.GetSpaceExportRequest) (*clientv1.GetSpaceExportResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	job, err := s.spaceJobForPrincipal(req.GetExportId(), principal.PrincipalID)
	if err != nil {
		return nil, err
	}
	return &clientv1.GetSpaceExportResponse{Job: mapSpaceExportJob(job)}, nil
}

func (s *ImportExportService) ListSpaceExports(ctx context.Context, req *clientv1.ListSpaceExportsRequest) (*clientv1.ListSpaceExportsResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := normalizePageSize(req.GetPageSize())
	s.spaceMu.Lock()
	s.expireSpaceJobsLocked(time.Now().UTC())
	jobs := make([]*spaceExportJobRecord, 0, len(s.spaceJobs))
	for _, job := range s.spaceJobs {
		if job.PrincipalID == principal.PrincipalID && (strings.TrimSpace(req.GetSpaceId()) == "" || job.Options.GetSpaceId() == strings.TrimSpace(req.GetSpaceId())) {
			jobs = append(jobs, cloneSpaceJob(job))
		}
	}
	s.spaceMu.Unlock()
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreateTime.After(jobs[j].CreateTime) })
	if offset > len(jobs) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the export list")
	}
	end := offset + pageSize
	if end > len(jobs) {
		end = len(jobs)
	}
	out := make([]*clientv1.SpaceExportJob, 0, end-offset)
	for _, job := range jobs[offset:end] {
		out = append(out, mapSpaceExportJob(job))
	}
	var next string
	if end < len(jobs) {
		next = fmt.Sprintf("%d", end)
	}
	return &clientv1.ListSpaceExportsResponse{Jobs: out, NextPageToken: next}, nil
}

func (s *ImportExportService) DownloadSpaceExport(req *clientv1.DownloadSpaceExportRequest, stream clientv1.ImportExportService_DownloadSpaceExportServer) error {
	principal, err := spaceUserPrincipalFromContext(stream.Context())
	if err != nil {
		return err
	}
	job, err := s.spaceJobForPrincipal(req.GetExportId(), principal.PrincipalID)
	if err != nil {
		return err
	}
	if job.Status != clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_SUCCEEDED {
		return status.Error(codes.FailedPrecondition, "space export is not ready for download")
	}
	f, err := os.Open(job.Path)
	if err != nil {
		return status.Errorf(codes.NotFound, "space export artifact not found")
	}
	defer f.Close()
	buf := make([]byte, spaceExportChunkBytes)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&clientv1.DownloadSpaceExportResponse{Chunk: append([]byte(nil), buf[:n]...)}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (s *ImportExportService) DeleteSpaceExport(ctx context.Context, req *clientv1.DeleteSpaceExportRequest) (*clientv1.DeleteSpaceExportResponse, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	s.spaceMu.Lock()
	defer s.spaceMu.Unlock()
	s.expireSpaceJobsLocked(time.Now().UTC())
	job := s.spaceJobs[req.GetExportId()]
	if job == nil || job.PrincipalID != principal.PrincipalID {
		return nil, status.Error(codes.NotFound, "space export not found")
	}
	_ = os.Remove(job.Path)
	job.Status = clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_DELETED
	job.UpdateTime = time.Now().UTC()
	job.Progress = 100
	return &clientv1.DeleteSpaceExportResponse{ExportId: job.ExportID, Deleted: true}, nil
}

func (s *ImportExportService) spaceJobForPrincipal(exportID, principalID string) (*spaceExportJobRecord, error) {
	s.spaceMu.Lock()
	defer s.spaceMu.Unlock()
	s.expireSpaceJobsLocked(time.Now().UTC())
	job := s.spaceJobs[strings.TrimSpace(exportID)]
	if job == nil || job.PrincipalID != principalID {
		return nil, status.Error(codes.NotFound, "space export not found")
	}
	return cloneSpaceJob(job), nil
}

func (s *ImportExportService) expireSpaceJobsLocked(now time.Time) {
	for _, job := range s.spaceJobs {
		if !job.ExpireTime.IsZero() && now.After(job.ExpireTime) && job.Status != clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_DELETED && job.Status != clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_EXPIRED {
			_ = os.Remove(job.Path)
			job.Status = clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_EXPIRED
			job.UpdateTime = now
			job.Progress = 100
		}
	}
}

func (s *ImportExportService) runSpaceExportJob(job *spaceExportJobRecord) {
	s.updateSpaceJob(job.ExportID, func(current *spaceExportJobRecord) {
		current.Status = clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_RUNNING
		current.Progress = 5
		current.UpdateTime = time.Now().UTC()
	})
	err := os.MkdirAll(filepath.Dir(job.Path), fsperm.PrivateDir)
	if err == nil {
		var f *os.File
		f, err = os.OpenFile(job.Path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fsperm.PrivateFile)
		if err == nil {
			manifest, writeErr := s.writeSpaceExportArchive(context.Background(), job, f)
			closeErr := f.Close()
			if writeErr != nil {
				err = writeErr
			} else if closeErr != nil {
				err = closeErr
			} else if stat, statErr := os.Stat(job.Path); statErr == nil {
				s.updateSpaceJob(job.ExportID, func(current *spaceExportJobRecord) {
					current.Counts = &clientv1.SpaceExportCounts{Spaces: int32(manifest.Counts.Spaces), Domains: int32(manifest.Counts.Domains), Nodes: int32(manifest.Counts.Nodes), Edges: int32(manifest.Counts.Edges), Blobs: int32(manifest.Counts.Blobs), Files: int32(manifest.Counts.Files)}
					current.SizeBytes = stat.Size()
				})
			}
		}
	}
	if err != nil {
		_ = os.Remove(job.Path)
		s.updateSpaceJob(job.ExportID, func(current *spaceExportJobRecord) {
			if current.Status == clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_DELETED || current.Status == clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_EXPIRED {
				return
			}
			current.Status = clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_FAILED
			current.ErrorMessage = err.Error()
			current.Progress = 100
			current.UpdateTime = time.Now().UTC()
		})
		return
	}
	now := time.Now().UTC()
	s.updateSpaceJob(job.ExportID, func(current *spaceExportJobRecord) {
		if current.Status == clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_DELETED || current.Status == clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_EXPIRED {
			_ = os.Remove(job.Path)
			return
		}
		current.Status = clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_SUCCEEDED
		current.Progress = 100
		current.UpdateTime = now
		current.CompleteTime = now
	})
}

func (s *ImportExportService) updateSpaceJob(exportID string, update func(*spaceExportJobRecord)) {
	s.spaceMu.Lock()
	defer s.spaceMu.Unlock()
	if current := s.spaceJobs[exportID]; current != nil {
		update(current)
	}
}

func (s *ImportExportService) writeSpaceExportArchive(ctx context.Context, job *spaceExportJobRecord, out io.Writer) (spaceManifest, error) {
	opts := job.Options
	manifest := spaceManifest{FormatVersion: spaceExportFormatVersion, GeneratedAt: time.Now().UTC(), RequestedBy: spaceSubject{PrincipalID: job.PrincipalID, Username: job.Username, Type: job.Kind}, Options: spaceOptions{SpaceID: opts.GetSpaceId(), DomainIDs: opts.GetDomainIds(), IncludeBlobs: opts.GetIncludeBlobs(), IncludeSystemDomains: opts.GetIncludeSystemDomains()}, Limitations: []string{"Secrets, daemon-internal credentials, WAL/raft logs, local indexes, checkpoints, and runtime cache files are intentionally omitted.", "Semantic/vector index payloads are derived data and are not included in v1; graph semantic fields, domain schema/source metadata, and blob references are included where visible."}}
	zw := zip.NewWriter(out)
	add := func(name, contentType string, data []byte) error {
		entry, err := writeSpaceArchiveEntry(zw, name, contentType, data)
		if err != nil {
			return err
		}
		manifest.Files = append(manifest.Files, entry)
		return nil
	}
	if err := add("README.md", "text/markdown", []byte(spaceArchiveREADME())); err != nil {
		_ = zw.Close()
		return spaceManifest{}, err
	}
	requestRaw, err := json.MarshalIndent(map[string]any{"format": "mycel-space-export-request-v1", "space_id": opts.GetSpaceId(), "domain_ids": opts.GetDomainIds(), "include_blobs": opts.GetIncludeBlobs(), "include_system_domains": opts.GetIncludeSystemDomains(), "requested_by_principal_id": job.PrincipalID, "requested_by_username": job.Username, "requested_by_type": job.Kind, "generated_at": manifest.GeneratedAt}, "", "  ")
	if err != nil {
		_ = zw.Close()
		return spaceManifest{}, err
	}
	if err := add("export.json", "application/json", append(requestRaw, '\n')); err != nil {
		_ = zw.Close()
		return spaceManifest{}, err
	}
	sp, err := s.spaces.GetVisibleSpace(ctx, job.PrincipalID, opts.GetSpaceId())
	if err != nil {
		_ = zw.Close()
		return spaceManifest{}, mapSpaceError(err, "space export get space")
	}
	access, err := s.spaces.EffectiveAccess(ctx, job.PrincipalID, sp)
	if err != nil {
		_ = zw.Close()
		return spaceManifest{}, mapSpaceError(err, "space export space access")
	}
	mappedSpace := MapSpace(sp, access)
	spacePath := fmt.Sprintf("spaces/%s/space.json", mappedSpace.GetSpaceId())
	spaceRaw, err := spaceProtoJSON(mappedSpace)
	if err != nil {
		_ = zw.Close()
		return spaceManifest{}, err
	}
	if err := add(spacePath, "application/json", spaceRaw); err != nil {
		_ = zw.Close()
		return spaceManifest{}, err
	}
	spaceEntry := spaceManifestSpace{SpaceID: mappedSpace.GetSpaceId(), Name: mappedSpace.GetName(), OwnerPrincipalID: mappedSpace.GetOwner().GetId(), Path: spacePath}
	domains, err := s.resolveSpaceExportDomains(ctx, job.PrincipalID, mappedSpace.GetSpaceId(), opts)
	if err != nil {
		_ = zw.Close()
		return spaceManifest{}, err
	}
	domainAccess, err := s.spaces.DomainEffectiveAccess(ctx, job.PrincipalID, mappedSpace.GetSpaceId())
	if err != nil {
		_ = zw.Close()
		return spaceManifest{}, mapDomainError(err, "space export domain access")
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].ID.String() < domains[j].ID.String() })
	for i, domain := range domains {
		domainEntry, files, err := s.writeSpaceDomainArchive(ctx, zw, job.PrincipalID, mappedSpace.GetSpaceId(), domain, domainAccess, opts.GetIncludeBlobs())
		if err != nil {
			_ = zw.Close()
			return spaceManifest{}, err
		}
		manifest.Files = append(manifest.Files, files...)
		spaceEntry.Domains = append(spaceEntry.Domains, domainEntry)
		manifest.Counts.Domains++
		manifest.Counts.Nodes += domainEntry.NodeCount
		manifest.Counts.Edges += domainEntry.EdgeCount
		manifest.Counts.Blobs += domainEntry.BlobCount
		s.updateSpaceJob(job.ExportID, func(current *spaceExportJobRecord) {
			current.Progress = int32(10 + (80*(i+1))/max(1, len(domains)))
			current.UpdateTime = time.Now().UTC()
		})
	}
	manifest.Spaces = append(manifest.Spaces, spaceEntry)
	manifest.Counts.Spaces = len(manifest.Spaces)
	manifest.Counts.Files = len(manifest.Files)
	manifestRaw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		_ = zw.Close()
		return spaceManifest{}, err
	}
	if _, err := writeSpaceArchiveEntry(zw, "manifest.json", "application/json", append(manifestRaw, '\n')); err != nil {
		_ = zw.Close()
		return spaceManifest{}, err
	}
	if err := zw.Close(); err != nil {
		return spaceManifest{}, err
	}
	return manifest, nil
}

func (s *ImportExportService) writeSpaceDomainArchive(ctx context.Context, zw *zip.Writer, principalID, spaceID string, domain graphmodel.Domain, access daemonspace.EffectiveAccess, includeBlobs bool) (spaceManifestDomain, []spaceManifestFile, error) {
	base := fmt.Sprintf("spaces/%s/domains/%s", spaceID, domain.ID.String())
	var files []spaceManifestFile
	add := func(name, contentType string, data []byte) error {
		entry, err := writeSpaceArchiveEntry(zw, name, contentType, data)
		if err != nil {
			return err
		}
		files = append(files, entry)
		return nil
	}
	domainRaw, err := spaceProtoJSON(MapDomain(domain, access))
	if err != nil {
		return spaceManifestDomain{}, nil, err
	}
	domainPath := base + "/domain.json"
	if err := add(domainPath, "application/json", domainRaw); err != nil {
		return spaceManifestDomain{}, nil, err
	}
	schemaPath := ""
	if s.schemas != nil {
		if schema, err := s.schemas.GetDomainSchema(ctx, domain.ID); err == nil && strings.TrimSpace(schema.SourceGWL) != "" {
			schemaPath = base + "/schema.gwl"
			if err := add(schemaPath, "text/plain", []byte(schema.SourceGWL+"\n")); err != nil {
				return spaceManifestDomain{}, nil, err
			}
		} else if err != nil && !errors.Is(err, schemaservice.ErrSchemaNotFound) {
			return spaceManifestDomain{}, nil, mapDomainError(err, "space export schema")
		}
	}
	session, err := s.sessions.OpenSession(ctx, daemonsession.OpenSessionInput{PrincipalID: principalID, SpaceID: spaceID, DomainID: domain.ID.String()})
	if err != nil {
		return spaceManifestDomain{}, nil, mapSessionError(err, "space export session")
	}
	defer func() { _, _ = s.sessions.CloseSession(context.Background(), principalID, session.ID) }()
	tx, err := s.sessions.BeginTransaction(ctx, daemonsession.BeginTransactionInput{PrincipalID: principalID, SessionID: session.ID, Mode: daemonsession.TransactionModeReadOnly})
	if err != nil {
		return spaceManifestDomain{}, nil, mapSessionError(err, "space export transaction")
	}
	defer func() { _, _ = s.sessions.CloseTransaction(context.Background(), principalID, tx.ID) }()
	nodes, err := allExportNodes(ctx, s.graphs, tx)
	if err != nil {
		return spaceManifestDomain{}, nil, mapGraphError(err, "space export nodes")
	}
	var blobManifest bytes.Buffer
	blobCount := 0
	if includeBlobs {
		seen := map[string]bool{}
		ids := make([]string, 0)
		for _, node := range nodes {
			if node.BlobRef == nil || strings.TrimSpace(string(*node.BlobRef)) == "" {
				continue
			}
			id := string(*node.BlobRef)
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		for _, id := range ids {
			meta, reader, err := s.blobs.OpenBlobInDomain(ctx, spaceID, domain.ID.String(), id)
			if err != nil {
				return spaceManifestDomain{}, nil, mapBlobError(err, "space export blob")
			}
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, reader); err != nil {
				_ = reader.Close()
				return spaceManifestDomain{}, nil, err
			}
			if err := reader.Close(); err != nil {
				return spaceManifestDomain{}, nil, err
			}
			filePath := fmt.Sprintf("spaces/%s/blobs/files/%s", spaceID, id)
			entry, err := writeSpaceArchiveEntry(zw, filePath, contentTypeOrOctet(meta.DeclaredMimeType), buf.Bytes())
			if err != nil {
				return spaceManifestDomain{}, nil, err
			}
			files = append(files, entry)
			row, err := json.Marshal(spaceBlobRow{BlobID: id, Digest: meta.Digest, SizeBytes: meta.SizeBytes, DeclaredMimeType: meta.DeclaredMimeType, OriginalFilename: meta.OriginalFilename, File: filePath, SHA256Hex: entry.SHA256Hex})
			if err != nil {
				return spaceManifestDomain{}, nil, err
			}
			blobManifest.Write(row)
			blobManifest.WriteByte('\n')
			blobCount++
		}
	}
	var nodeLines bytes.Buffer
	for _, node := range nodes {
		line, err := spaceProtoJSON(mapProtoNode(node))
		if err != nil {
			return spaceManifestDomain{}, nil, err
		}
		nodeLines.Write(line)
	}
	edges, err := allExportEdges(ctx, s.graphs, tx)
	if err != nil {
		return spaceManifestDomain{}, nil, mapGraphError(err, "space export edges")
	}
	var edgeLines bytes.Buffer
	for _, edge := range edges {
		line, err := spaceProtoJSON(mapProtoEdge(edge))
		if err != nil {
			return spaceManifestDomain{}, nil, err
		}
		edgeLines.Write(line)
	}
	nodesPath := base + "/nodes.jsonl"
	edgesPath := base + "/edges.jsonl"
	blobPath := base + "/blobs/manifest.jsonl"
	if err := add(nodesPath, "application/x-ndjson", nodeLines.Bytes()); err != nil {
		return spaceManifestDomain{}, nil, err
	}
	if err := add(edgesPath, "application/x-ndjson", edgeLines.Bytes()); err != nil {
		return spaceManifestDomain{}, nil, err
	}
	if err := add(blobPath, "application/x-ndjson", blobManifest.Bytes()); err != nil {
		return spaceManifestDomain{}, nil, err
	}
	return spaceManifestDomain{DomainID: domain.ID.String(), Key: domain.Key, Name: domain.Name, Default: domain.Default, Path: domainPath, NodesPath: nodesPath, EdgesPath: edgesPath, SchemaPath: schemaPath, BlobPath: blobPath, NodeCount: len(nodes), EdgeCount: len(edges), BlobCount: blobCount}, files, nil
}

func writeSpaceArchiveEntry(zw *zip.Writer, name, contentType string, data []byte) (spaceManifestFile, error) {
	name = cleanSpaceZipPath(name)
	if name == "" {
		return spaceManifestFile{}, fmt.Errorf("invalid ZIP entry path")
	}
	header := &zip.FileHeader{Name: name, Method: zip.Deflate}
	header.SetModTime(time.Now().UTC())
	w, err := zw.CreateHeader(header)
	if err != nil {
		return spaceManifestFile{}, err
	}
	if _, err := w.Write(data); err != nil {
		return spaceManifestFile{}, err
	}
	sum := sha256.Sum256(data)
	return spaceManifestFile{Path: name, ContentType: contentType, SizeBytes: int64(len(data)), SHA256Hex: hex.EncodeToString(sum[:])}, nil
}

func cleanSpaceZipPath(name string) string {
	name = strings.TrimSpace(strings.ReplaceAll(name, "\\", "/"))
	name = strings.TrimPrefix(name, "/")
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return ""
	}
	return clean
}

func spaceProtoJSON(msg proto.Message) ([]byte, error) {
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func contentTypeOrOctet(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "application/octet-stream"
	}
	return value
}

func cloneSpaceJob(job *spaceExportJobRecord) *spaceExportJobRecord {
	out := *job
	if job.Options != nil {
		out.Options = proto.Clone(job.Options).(*clientv1.SpaceExportOptions)
	}
	if job.Counts != nil {
		out.Counts = proto.Clone(job.Counts).(*clientv1.SpaceExportCounts)
	}
	return &out
}

func mapSpaceExportJob(job *spaceExportJobRecord) *clientv1.SpaceExportJob {
	return &clientv1.SpaceExportJob{ExportId: job.ExportID, Status: job.Status, FormatVersion: spaceExportFormatVersion, Filename: job.Filename, SizeBytes: job.SizeBytes, ProgressPercent: job.Progress, Counts: job.Counts, ErrorMessage: job.ErrorMessage, CreateTime: timestampOrNil(job.CreateTime), UpdateTime: timestampOrNil(job.UpdateTime), CompleteTime: timestampOrNil(job.CompleteTime), ExpireTime: timestampOrNil(job.ExpireTime), Options: job.Options, RequestedByPrincipalId: job.PrincipalID}
}

func spaceArchiveREADME() string {
	return `# Mycel space export v1

This ZIP is a daemon-created space data export for the authenticated Mycel principal that requested it.

Important files:

- manifest.json: format version, generated time, requester principal, included space/domains, counts, file checksums, and known limitations.
- export.json: non-secret request metadata, including requested space/domain selection.
- spaces/<space-id>/space.json: exported space metadata visible to the requesting user.
- spaces/<space-id>/domains/<domain-id>/domain.json: exported domain metadata.
- spaces/<space-id>/domains/<domain-id>/nodes.jsonl: one JSON node per line.
- spaces/<space-id>/domains/<domain-id>/edges.jsonl: one JSON edge per line.
- spaces/<space-id>/domains/<domain-id>/schema.gwl: domain schema, when configured and visible.
- spaces/<space-id>/domains/<domain-id>/blobs/manifest.jsonl: blob metadata rows for included payload files.
- spaces/<space-id>/blobs/files/<blob-id>: raw blob payload bytes, when include_blobs is enabled.

The format is designed to be inspected with standard ZIP, JSON, and text tools. JSONL files can be streamed line-by-line for large exports.

Intentionally omitted in v1: daemon secrets, credentials, WAL/Raft logs, checkpoints, local indexes, runtime caches, and derived semantic/vector index payloads.
`
}
