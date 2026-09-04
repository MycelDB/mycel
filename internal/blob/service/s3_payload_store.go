package service

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
)

type s3PayloadAPI interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type s3PayloadStore struct {
	cfg    Config
	client s3PayloadAPI
	tmpDir string
}

func newS3PayloadStore(ctx context.Context, cfg Config, tmpDir string) (*s3PayloadStore, error) {
	cfg = effectiveBlobConfig(cfg)
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("S3 blob backend requires bucket")
	}
	loadOptions := []func(*awsconfig.LoadOptions) error{}
	if cfg.S3Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(cfg.S3Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.S3EndpointURL)
		}
		o.UsePathStyle = cfg.S3ForcePathStyle
	})
	return &s3PayloadStore{cfg: cfg, client: client, tmpDir: tmpDir}, nil
}

func newS3PayloadStoreWithClient(cfg Config, tmpDir string, client s3PayloadAPI) (*s3PayloadStore, error) {
	cfg = effectiveBlobConfig(cfg)
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("S3 blob backend requires bucket")
	}
	if client == nil {
		return nil, fmt.Errorf("S3 client is required")
	}
	return &s3PayloadStore{cfg: cfg, client: client, tmpDir: tmpDir}, nil
}

func (s *s3PayloadStore) Put(ctx context.Context, spaceID string, mimeType string, r io.Reader) (graphmodel.BlobID, int64, PayloadDescriptor, error) {
	if strings.TrimSpace(spaceID) == "" || r == nil {
		return "", 0, PayloadDescriptor{}, fmt.Errorf("%w: space_id and reader are required", ErrInvalidInput)
	}
	if err := os.MkdirAll(s.tmpDir, 0o700); err != nil {
		return "", 0, PayloadDescriptor{}, err
	}
	tmp, err := os.CreateTemp(s.tmpDir, "s3-put-*.blob")
	if err != nil {
		return "", 0, PayloadDescriptor{}, err
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, hasher), r)
	if err != nil {
		cleanup()
		return "", 0, PayloadDescriptor{}, err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", 0, PayloadDescriptor{}, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, PayloadDescriptor{}, err
	}

	digestBytes := hasher.Sum(nil)
	id, err := graphmodel.BlobIDFromBytes(digestBytes)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, PayloadDescriptor{}, err
	}
	key := s.objectKey(spaceID, string(id))
	body, err := os.Open(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, PayloadDescriptor{}, err
	}
	defer func() {
		_ = body.Close()
		_ = os.Remove(tmpPath)
	}()

	put := &s3.PutObjectInput{Bucket: aws.String(s.cfg.S3Bucket), Key: aws.String(key), Body: body, ContentLength: aws.Int64(size), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digestBytes))}
	if strings.TrimSpace(mimeType) != "" {
		put.ContentType = aws.String(strings.TrimSpace(mimeType))
	}
	if s.cfg.S3KMSKeyID != "" {
		put.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		put.SSEKMSKeyId = aws.String(s.cfg.S3KMSKeyID)
	}
	out, err := s.client.PutObject(ctx, put)
	if err != nil {
		return "", 0, PayloadDescriptor{}, err
	}
	desc := PayloadDescriptor{Backend: blobBackendS3, SpaceID: spaceID, BlobID: string(id), SizeBytes: size, ChecksumAlgorithm: "sha256", ChecksumHex: string(id), S3Bucket: s.cfg.S3Bucket, S3Key: key, S3Region: s.cfg.S3Region}
	if out != nil && out.ETag != nil {
		desc.S3ETag = strings.Trim(*out.ETag, "\"")
	}
	if ok, err := s.Exists(ctx, desc); err != nil {
		return "", 0, PayloadDescriptor{}, err
	} else if !ok {
		return "", 0, PayloadDescriptor{}, fmt.Errorf("S3 object %s/%s was not visible after upload", s.cfg.S3Bucket, key)
	}
	return id, size, desc, nil
}

func (s *s3PayloadStore) Exists(ctx context.Context, desc PayloadDescriptor) (bool, error) {
	bucket, key := s.bucketKey(desc)
	if bucket == "" || key == "" {
		return false, fmt.Errorf("%w: S3 bucket and key are required", ErrInvalidInput)
	}
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		if isS3NotFound(err) {
			return false, nil
		}
		return false, err
	}
	if desc.SizeBytes >= 0 && out != nil && out.ContentLength != nil && *out.ContentLength != desc.SizeBytes {
		return false, fmt.Errorf("S3 object %s/%s size mismatch: got %d want %d", bucket, key, *out.ContentLength, desc.SizeBytes)
	}
	return true, nil
}

func (s *s3PayloadStore) Open(ctx context.Context, desc PayloadDescriptor) (io.ReadCloser, error) {
	bucket, key := s.bucketKey(desc)
	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: S3 bucket and key are required", ErrInvalidInput)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		if isS3NotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if out == nil || out.Body == nil {
		return nil, ErrNotFound
	}
	return out.Body, nil
}

func (s *s3PayloadStore) Delete(ctx context.Context, desc PayloadDescriptor) error {
	bucket, key := s.bucketKey(desc)
	if bucket == "" || key == "" {
		return fmt.Errorf("%w: S3 bucket and key are required", ErrInvalidInput)
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func (s *s3PayloadStore) objectKey(spaceID string, blobID string) string {
	fanout := blobID
	if len(fanout) > 2 {
		fanout = fanout[:2]
	}
	parts := []string{s.cfg.S3Prefix, "spaces", strings.TrimSpace(spaceID), "objects", fanout, strings.TrimSpace(blobID)}
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, "/")
}

func (s *s3PayloadStore) bucketKey(desc PayloadDescriptor) (string, string) {
	bucket := firstNonEmpty(desc.S3Bucket, s.cfg.S3Bucket)
	key := strings.TrimSpace(desc.S3Key)
	if key == "" && desc.SpaceID != "" && desc.BlobID != "" {
		key = s.objectKey(desc.SpaceID, desc.BlobID)
	}
	return bucket, key
}

func isS3NotFound(err error) bool {
	var headNotFound *types.NotFound
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &headNotFound) || errors.As(err, &noSuchKey) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "notfound") || strings.Contains(msg, "not found") || strings.Contains(msg, "status code: 404") || strings.Contains(msg, "nosuchkey") || strings.Contains(msg, "no such key")
}
