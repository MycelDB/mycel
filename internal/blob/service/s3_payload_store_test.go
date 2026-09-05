package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
)

type fakeS3PayloadClient struct {
	mu        sync.Mutex
	objects   map[string][]byte
	putInputs []*s3.PutObjectInput
	deleteErr error
	deletes   []string
}

func newFakeS3PayloadClient() *fakeS3PayloadClient {
	return &fakeS3PayloadClient{objects: map[string][]byte{}}
}

func (f *fakeS3PayloadClient) PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putInputs = append(f.putInputs, in)
	f.objects[f.objectKey(in.Bucket, in.Key)] = raw
	return &s3.PutObjectOutput{ETag: aws.String("fake-etag")}, nil
}

func (f *fakeS3PayloadClient) HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, ok := f.objects[f.objectKey(in.Bucket, in.Key)]
	if !ok {
		return nil, &types.NotFound{}
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(raw)))}, nil
}

func (f *fakeS3PayloadClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, ok := f.objects[f.objectKey(in.Bucket, in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(raw))}, nil
}

func (f *fakeS3PayloadClient) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := f.objectKey(in.Bucket, in.Key)
	f.deletes = append(f.deletes, key)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	delete(f.objects, key)
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeS3PayloadClient) objectKey(bucket *string, key *string) string {
	return aws.ToString(bucket) + "/" + aws.ToString(key)
}

func TestS3PayloadStorePutOpenDelete(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3PayloadClient()
	store, err := newS3PayloadStoreWithClient(Config{Backend: "s3", S3Bucket: "my-bucket", S3Prefix: "tenant-a", S3Region: "us-east-1", S3KMSKeyID: "alias/mycel"}, t.TempDir(), fake)
	if err != nil {
		t.Fatalf("newS3PayloadStoreWithClient() error = %v", err)
	}
	id, size, desc, err := store.Put(ctx, "space-1", "text/plain", strings.NewReader("hello s3"))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if id == "" || size != int64(len("hello s3")) || desc.Backend != "s3" || desc.S3Bucket != "my-bucket" || !strings.HasPrefix(desc.S3Key, "tenant-a/spaces/space-1/objects/") || desc.S3ETag != "fake-etag" {
		t.Fatalf("unexpected descriptor: id=%s size=%d desc=%+v", id, size, desc)
	}
	fake.mu.Lock()
	if len(fake.putInputs) != 1 || aws.ToString(fake.putInputs[0].ChecksumSHA256) == "" || fake.putInputs[0].ServerSideEncryption != types.ServerSideEncryptionAwsKms || aws.ToString(fake.putInputs[0].SSEKMSKeyId) != "alias/mycel" {
		fake.mu.Unlock()
		t.Fatalf("unexpected put inputs: %+v", fake.putInputs)
	}
	fake.mu.Unlock()
	ok, err := store.Exists(ctx, desc)
	if err != nil || !ok {
		t.Fatalf("Exists() = %v, %v; want true, nil", ok, err)
	}
	r, err := store.Open(ctx, desc)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	raw, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || string(raw) != "hello s3" {
		t.Fatalf("Open() bytes=%q err=%v", raw, err)
	}
	if err := store.Delete(ctx, desc); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	ok, err = store.Exists(ctx, desc)
	if err != nil || ok {
		t.Fatalf("Exists() after delete = %v, %v; want false, nil", ok, err)
	}
}

func TestModuleS3DeleteIsBestEffortAfterMetadataDelete(t *testing.T) {
	ctx := context.Background()
	fake := newFakeS3PayloadClient()
	store, err := newS3PayloadStoreWithClient(Config{Backend: "s3", S3Bucket: "my-bucket"}, t.TempDir(), fake)
	if err != nil {
		t.Fatal(err)
	}
	m := NewModule(fakeRefCounter{}, Config{Backend: "s3", S3Bucket: "my-bucket"})
	m.s3Store = store
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: strings.NewReader("delete me")})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	fake.deleteErr = errors.New("temporary delete failure")
	deleted, err := m.DeleteBlob(ctx, "space-1", meta.BlobID)
	if err != nil || deleted != meta.BlobID {
		t.Fatalf("DeleteBlob() = %q, %v", deleted, err)
	}
	if _, err := m.meta("space-1", meta.BlobID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("metadata after best-effort delete err = %v, want ErrNotFound", err)
	}
	fake.mu.Lock()
	deletes := len(fake.deletes)
	fake.mu.Unlock()
	if deletes != 1 {
		t.Fatalf("DeleteObject calls = %d, want 1", deletes)
	}
}
