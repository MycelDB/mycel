package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const repositoryObjectChecksumMetadataKey = "mycel-sha256"

type repositoryObjectInfo struct {
	Key            string
	URI            string
	SizeBytes      int64
	ChecksumSHA256 string
}

type backupRepositoryObjectStore interface {
	PutFile(ctx context.Context, key, path string) (repositoryObjectInfo, error)
	PutBytes(ctx context.Context, key string, data []byte, contentType string) (repositoryObjectInfo, error)
	GetBytes(ctx context.Context, key string, limit int64) ([]byte, repositoryObjectInfo, error)
	GetFile(ctx context.Context, key, path string) (repositoryObjectInfo, error)
	Head(ctx context.Context, key string) (repositoryObjectInfo, error)
	ListKeys(ctx context.Context, prefix string) ([]string, error)
}

type backupRepositoryObjectStoreFactory func(context.Context, BackupRepository) (backupRepositoryObjectStore, error)

type s3RepositoryObjectStore struct {
	repo   BackupRepository
	client *s3.Client
}

func (m *Module) repositoryObjectStore(ctx context.Context, repo BackupRepository) (backupRepositoryObjectStore, error) {
	factory := m.repositoryObjectStoreFactory
	if factory == nil {
		factory = newS3RepositoryObjectStore
	}
	return factory(ctx, repo)
}

func newS3RepositoryObjectStore(ctx context.Context, repo BackupRepository) (backupRepositoryObjectStore, error) {
	client, err := newS3RepositoryClient(ctx, repo)
	if err != nil {
		return nil, err
	}
	return &s3RepositoryObjectStore{repo: repo, client: client}, nil
}

func (s *s3RepositoryObjectStore) PutFile(ctx context.Context, key, path string) (repositoryObjectInfo, error) {
	key = cleanRepositoryObjectKey(key)
	if key == "" {
		return repositoryObjectInfo{}, fmt.Errorf("repository object key is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return repositoryObjectInfo{}, err
	}
	defer file.Close()
	h := sha256.New()
	size, err := copyRepositoryObjectWithContext(ctx, h, file)
	if err != nil {
		return repositoryObjectInfo{}, err
	}
	checksum := hex.EncodeToString(h.Sum(nil))
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return repositoryObjectInfo{}, err
	}
	put := &s3.PutObjectInput{Bucket: aws.String(s.repo.S3.Bucket), Key: aws.String(key), Body: file, ContentLength: aws.Int64(size), Metadata: map[string]string{repositoryObjectChecksumMetadataKey: checksum}}
	if contentType := contentTypeForPath(path); contentType != "" {
		put.ContentType = aws.String(contentType)
	}
	applyRepositoryEncryption(s.repo, put)
	if _, err := s.client.PutObject(ctx, put); err != nil {
		return repositoryObjectInfo{}, err
	}
	return repositoryObjectInfo{Key: key, URI: repositoryObjectURI(s.repo, key), SizeBytes: size, ChecksumSHA256: checksum}, nil
}

func (s *s3RepositoryObjectStore) PutBytes(ctx context.Context, key string, data []byte, contentType string) (repositoryObjectInfo, error) {
	key = cleanRepositoryObjectKey(key)
	if key == "" {
		return repositoryObjectInfo{}, fmt.Errorf("repository object key is required")
	}
	digest := sha256.Sum256(data)
	checksum := hex.EncodeToString(digest[:])
	put := &s3.PutObjectInput{Bucket: aws.String(s.repo.S3.Bucket), Key: aws.String(key), Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))), Metadata: map[string]string{repositoryObjectChecksumMetadataKey: checksum}}
	if strings.TrimSpace(contentType) != "" {
		put.ContentType = aws.String(strings.TrimSpace(contentType))
	}
	applyRepositoryEncryption(s.repo, put)
	if _, err := s.client.PutObject(ctx, put); err != nil {
		return repositoryObjectInfo{}, err
	}
	return repositoryObjectInfo{Key: key, URI: repositoryObjectURI(s.repo, key), SizeBytes: int64(len(data)), ChecksumSHA256: checksum}, nil
}

func (s *s3RepositoryObjectStore) GetBytes(ctx context.Context, key string, limit int64) ([]byte, repositoryObjectInfo, error) {
	key = cleanRepositoryObjectKey(key)
	if key == "" {
		return nil, repositoryObjectInfo{}, fmt.Errorf("repository object key is required")
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.repo.S3.Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, repositoryObjectInfo{}, err
	}
	defer out.Body.Close()
	var reader io.Reader = out.Body
	if limit > 0 {
		reader = io.LimitReader(out.Body, limit+1)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil, repositoryObjectInfo{}, err
	}
	if limit > 0 && int64(len(raw)) > limit {
		return nil, repositoryObjectInfo{}, fmt.Errorf("repository object %s exceeds read limit", key)
	}
	info := repositoryObjectInfo{Key: key, URI: repositoryObjectURI(s.repo, key), SizeBytes: int64(len(raw))}
	if out.Metadata != nil {
		info.ChecksumSHA256 = strings.TrimSpace(out.Metadata[repositoryObjectChecksumMetadataKey])
	}
	return raw, info, nil
}

func (s *s3RepositoryObjectStore) GetFile(ctx context.Context, key, path string) (repositoryObjectInfo, error) {
	key = cleanRepositoryObjectKey(key)
	if key == "" {
		return repositoryObjectInfo{}, fmt.Errorf("repository object key is required")
	}
	if strings.TrimSpace(path) == "" {
		return repositoryObjectInfo{}, fmt.Errorf("repository object destination path is required")
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.repo.S3.Bucket), Key: aws.String(key)})
	if err != nil {
		return repositoryObjectInfo{}, err
	}
	defer out.Body.Close()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return repositoryObjectInfo{}, err
	}
	tmp := path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return repositoryObjectInfo{}, err
	}
	h := sha256.New()
	size, copyErr := copyRepositoryObjectWithContext(ctx, io.MultiWriter(file, h), out.Body)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return repositoryObjectInfo{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return repositoryObjectInfo{}, closeErr
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return repositoryObjectInfo{}, err
	}
	info := repositoryObjectInfo{Key: key, URI: repositoryObjectURI(s.repo, key), SizeBytes: size, ChecksumSHA256: hex.EncodeToString(h.Sum(nil))}
	if out.Metadata != nil {
		if checksum := strings.TrimSpace(out.Metadata[repositoryObjectChecksumMetadataKey]); checksum != "" {
			info.ChecksumSHA256 = checksum
		}
	}
	return info, nil
}

func (s *s3RepositoryObjectStore) Head(ctx context.Context, key string) (repositoryObjectInfo, error) {
	key = cleanRepositoryObjectKey(key)
	if key == "" {
		return repositoryObjectInfo{}, fmt.Errorf("repository object key is required")
	}
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.repo.S3.Bucket), Key: aws.String(key)})
	if err != nil {
		return repositoryObjectInfo{}, err
	}
	info := repositoryObjectInfo{Key: key, URI: repositoryObjectURI(s.repo, key)}
	if out.ContentLength != nil {
		info.SizeBytes = *out.ContentLength
	}
	if out.Metadata != nil {
		info.ChecksumSHA256 = strings.TrimSpace(out.Metadata[repositoryObjectChecksumMetadataKey])
	}
	return info, nil
}

func (s *s3RepositoryObjectStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	prefix = cleanRepositoryObjectKey(prefix)
	keys := []string{}
	var token *string
	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.repo.S3.Bucket), Prefix: aws.String(prefix), ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	sort.Strings(keys)
	return keys, nil
}

func copyRepositoryObjectWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 128*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			m, writeErr := dst.Write(buf[:n])
			written += int64(m)
			if writeErr != nil {
				return written, writeErr
			}
			if m != n {
				return written, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}

func repositoryBackupSetPrefix(repo BackupRepository, backupSetID string) string {
	return joinRepositoryKey(repo.S3.Prefix, "backup-sets", strings.TrimSpace(backupSetID))
}

func repositoryBackupSetPrefixFromInput(input StartClusterBackupInput, backupSetID string) string {
	if strings.TrimSpace(input.RepositoryID) == "" {
		return ""
	}
	if strings.TrimSpace(input.RepositoryPrefix) != "" {
		return cleanRepositoryObjectKey(input.RepositoryPrefix)
	}
	return joinRepositoryKey("backup-sets", strings.TrimSpace(backupSetID))
}

func repositorySafePathComponent(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", " ", "_", ":", "_", "..", "_")
	return replacer.Replace(value)
}

func repositoryObjectURI(repo BackupRepository, key string) string {
	return "s3://" + strings.TrimSpace(repo.S3.Bucket) + "/" + cleanRepositoryObjectKey(key)
}

func contentTypeForPath(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".json" {
		return "application/json"
	}
	return mime.TypeByExtension(ext)
}

func cleanRepositoryObjectKey(key string) string {
	key = strings.TrimSpace(key)
	key = strings.Trim(key, "/")
	if key == "." {
		return ""
	}
	parts := strings.Split(key, "/")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			continue
		}
		clean = append(clean, part)
	}
	return strings.Join(clean, "/")
}

func joinRepositoryKey(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = cleanRepositoryObjectKey(part)
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, "/")
}
