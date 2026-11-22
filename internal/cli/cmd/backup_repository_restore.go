package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

type repositoryRestoreLocalInput struct {
	Bucket           string
	RepositoryPrefix string
	BackupSetPrefix  string
	BackupSetID      string
	EndpointURI      string
	Region           string
	AccessKeyEnv     string
	SecretKeyEnv     string
	SessionTokenEnv  string
	ForcePathStyle   bool
	Ordinal          int
	DataDir          string
	TargetPVCName    string
}

type repositoryRestoreLocalResult struct {
	BackupSetID       string `json:"backup_set_id"`
	RepositoryBucket  string `json:"repository_bucket"`
	BackupSetPrefix   string `json:"backup_set_prefix"`
	Ordinal           int    `json:"ordinal"`
	PodName           string `json:"pod_name"`
	TargetPVCName     string `json:"target_pvc_name,omitempty"`
	DataDir           string `json:"data_dir"`
	ArchiveURI        string `json:"archive_uri"`
	ArchiveChecksum   string `json:"archive_checksum_sha256"`
	ArchiveSizeBytes  int64  `json:"archive_size_bytes"`
	CompleteMarkerURI string `json:"complete_marker_uri"`
}

func NewAdminBackupClusterRestoreRepositoryLocalCommand(a *app.App) *cobra.Command {
	var in repositoryRestoreLocalInput
	cmd := &cobra.Command{Use: "restore-repository-local", Short: "Restore one repository-backed cluster backup ordinal into an offline local data dir", RunE: func(cmd *cobra.Command, args []string) error {
		res, err := restoreRepositoryBackupOrdinalLocal(cmd.Context(), in)
		if err != nil {
			return backupCLIError("restore repository cluster backup ordinal", err)
		}
		text := fmt.Sprintf("repository cluster backup ordinal restored: %s\nordinal: %d\npod: %s\ndata_dir: %s\narchive: %s\n", res.BackupSetID, res.Ordinal, res.PodName, res.DataDir, res.ArchiveURI)
		return a.Print(res, text)
	}}
	cmd.Flags().StringVar(&in.Bucket, "bucket", "", "S3-compatible repository bucket")
	cmd.Flags().StringVar(&in.RepositoryPrefix, "repository-prefix", "", "repository root prefix; used with --backup-set-id to locate backup-sets/<id>")
	cmd.Flags().StringVar(&in.BackupSetPrefix, "backup-set-prefix", "", "full backup-set object prefix containing backup-set.json and COMPLETE")
	cmd.Flags().StringVar(&in.BackupSetID, "backup-set-id", "", "backup set ID; required when --backup-set-prefix is omitted")
	cmd.Flags().StringVar(&in.EndpointURI, "endpoint-uri", "", "S3-compatible endpoint URI")
	cmd.Flags().StringVar(&in.Region, "region", "us-east-1", "S3 region")
	cmd.Flags().StringVar(&in.AccessKeyEnv, "access-key-env", "AWS_ACCESS_KEY_ID", "environment variable name containing access key ID")
	cmd.Flags().StringVar(&in.SecretKeyEnv, "secret-key-env", "AWS_SECRET_ACCESS_KEY", "environment variable name containing secret access key")
	cmd.Flags().StringVar(&in.SessionTokenEnv, "session-token-env", "", "optional environment variable name containing session token")
	cmd.Flags().BoolVar(&in.ForcePathStyle, "force-path-style", true, "use path-style S3 addressing")
	cmd.Flags().IntVar(&in.Ordinal, "ordinal", -1, "cluster node ordinal to restore")
	cmd.Flags().StringVar(&in.DataDir, restoreLocalDataDirFlag, "", "empty local data directory or mounted PVC root to restore into")
	cmd.Flags().StringVar(&in.TargetPVCName, "target-pvc-name", "", "target PVC name when restoring into a mounted PVC root")
	return cmd
}

func restoreRepositoryBackupOrdinalLocal(ctx context.Context, in repositoryRestoreLocalInput) (repositoryRestoreLocalResult, error) {
	bucket := strings.TrimSpace(in.Bucket)
	if bucket == "" {
		return repositoryRestoreLocalResult{}, fmt.Errorf("--bucket is required")
	}
	prefix := cleanRepositoryRestoreKey(in.BackupSetPrefix)
	backupSetID := strings.TrimSpace(in.BackupSetID)
	if prefix == "" {
		if backupSetID == "" {
			return repositoryRestoreLocalResult{}, fmt.Errorf("--backup-set-prefix or --backup-set-id is required")
		}
		prefix = joinRepositoryRestoreKey(in.RepositoryPrefix, "backup-sets", backupSetID)
	}
	if in.Ordinal < 0 {
		return repositoryRestoreLocalResult{}, fmt.Errorf("--ordinal must be non-negative")
	}
	dataDir := strings.TrimSpace(in.DataDir)
	if dataDir == "" {
		return repositoryRestoreLocalResult{}, fmt.Errorf("--%s is required", restoreLocalDataDirFlag)
	}
	if !filepath.IsAbs(dataDir) {
		return repositoryRestoreLocalResult{}, fmt.Errorf("--%s must be an absolute path", restoreLocalDataDirFlag)
	}
	if filepath.Clean(dataDir) == string(filepath.Separator) {
		return repositoryRestoreLocalResult{}, fmt.Errorf("refusing to restore into filesystem root")
	}
	client, err := newRepositoryRestoreS3Client(ctx, in)
	if err != nil {
		return repositoryRestoreLocalResult{}, err
	}
	manifestKey := joinRepositoryRestoreKey(prefix, "backup-set.json")
	manifestRaw, err := getRepositoryRestoreObjectBytes(ctx, client, bucket, manifestKey, 64<<20)
	if err != nil {
		return repositoryRestoreLocalResult{}, fmt.Errorf("download backup-set manifest: %w", err)
	}
	markerKey := joinRepositoryRestoreKey(prefix, "COMPLETE")
	markerRaw, err := getRepositoryRestoreObjectBytes(ctx, client, bucket, markerKey, 4<<20)
	if err != nil {
		return repositoryRestoreLocalResult{}, fmt.Errorf("download COMPLETE marker: %w", err)
	}
	doc, err := clusterbackup.ParseBackupSetDocument(manifestRaw)
	if err != nil {
		return repositoryRestoreLocalResult{}, fmt.Errorf("parse backup-set manifest: %w", err)
	}
	marker, err := clusterbackup.ParseCompleteMarker(markerRaw)
	if err != nil {
		return repositoryRestoreLocalResult{}, fmt.Errorf("parse COMPLETE marker: %w", err)
	}
	report := clusterbackup.ValidateBackupSetDocument(ctx, doc, &marker, clusterbackup.BackupSetValidationOptions{RequireCompleteMarker: true, AllowS3ArtifactURIs: true})
	if !report.Valid() {
		return repositoryRestoreLocalResult{}, report.Error()
	}
	manifestDigest := sha256.Sum256(manifestRaw)
	manifestChecksum := hex.EncodeToString(manifestDigest[:])
	if marker.ManifestChecksumSHA256 != manifestChecksum {
		return repositoryRestoreLocalResult{}, fmt.Errorf("COMPLETE manifest checksum mismatch: got %s want %s", marker.ManifestChecksumSHA256, manifestChecksum)
	}
	if marker.ManifestSizeBytes != "" {
		if want, err := strconv.ParseInt(marker.ManifestSizeBytes, 10, 64); err == nil && want != int64(len(manifestRaw)) {
			return repositoryRestoreLocalResult{}, fmt.Errorf("COMPLETE manifest size mismatch: got %d want %d", len(manifestRaw), want)
		}
	}
	manifest := doc.Manifest
	if backupSetID != "" && manifest.BackupSetID != backupSetID {
		return repositoryRestoreLocalResult{}, fmt.Errorf("backup-set manifest id %s does not match requested %s", manifest.BackupSetID, backupSetID)
	}
	artifact, ok := productionArtifactForOrdinal(manifest, in.Ordinal)
	if !ok {
		return repositoryRestoreLocalResult{}, fmt.Errorf("backup set %s has no artifact for ordinal %d", manifest.BackupSetID, in.Ordinal)
	}
	archiveKey, err := repositoryRestoreKeyFromS3URI(bucket, artifact.ArchiveURI)
	if err != nil {
		return repositoryRestoreLocalResult{}, err
	}
	workDir, err := os.MkdirTemp("", "mycel-repository-restore-*")
	if err != nil {
		return repositoryRestoreLocalResult{}, err
	}
	defer os.RemoveAll(workDir)
	archivePath := filepath.Join(workDir, artifact.ArchiveName)
	size, checksum, err := getRepositoryRestoreObjectFile(ctx, client, bucket, archiveKey, archivePath)
	if err != nil {
		return repositoryRestoreLocalResult{}, fmt.Errorf("download ordinal %d archive: %w", in.Ordinal, err)
	}
	if artifact.ChecksumSHA256 != "" && checksum != artifact.ChecksumSHA256 {
		return repositoryRestoreLocalResult{}, fmt.Errorf("archive checksum mismatch for ordinal %d: got %s want %s", in.Ordinal, checksum, artifact.ChecksumSHA256)
	}
	if artifact.SizeBytes != "" {
		if want, err := strconv.ParseInt(artifact.SizeBytes, 10, 64); err == nil && want != size {
			return repositoryRestoreLocalResult{}, fmt.Errorf("archive size mismatch for ordinal %d: got %d want %d", in.Ordinal, size, want)
		}
	}
	format := repositoryRestoreArchiveFormat(manifest.Artifacts.ArchiveFormat)
	if !backupcore.IsSupportedArchiveFormat(format) {
		return repositoryRestoreLocalResult{}, fmt.Errorf("unsupported archive format %q", manifest.Artifacts.ArchiveFormat)
	}
	if err := ensureRepositoryRestoreEmptyDataDir(dataDir); err != nil {
		return repositoryRestoreLocalResult{}, err
	}
	if err := clusterbackup.ExtractArchive(ctx, format, archivePath, dataDir); err != nil {
		return repositoryRestoreLocalResult{}, fmt.Errorf("extract archive: %w", err)
	}
	if err := clusterbackup.VerifyRestoredDataDir(dataDir); err != nil {
		return repositoryRestoreLocalResult{}, fmt.Errorf("verify restored data dir: %w", err)
	}
	markerDoc := clusterbackup.BuildRestoredDerivedReadiness(manifest, clusterbackup.RestoredDerivedReadinessOptions{Ordinal: artifact.Ordinal, NodeID: artifact.NodeID, PodName: artifact.PodName, TargetPVCName: strings.TrimSpace(in.TargetPVCName), RestoredAt: time.Now().UTC(), Evidence: map[string]string{"restore_helper": "restore-repository-local"}})
	if err := clusterbackup.WriteRestoredDerivedReadiness(dataDir, markerDoc); err != nil {
		return repositoryRestoreLocalResult{}, fmt.Errorf("write restored derived readiness marker: %w", err)
	}
	return repositoryRestoreLocalResult{BackupSetID: manifest.BackupSetID, RepositoryBucket: bucket, BackupSetPrefix: prefix, Ordinal: artifact.Ordinal, PodName: artifact.PodName, TargetPVCName: strings.TrimSpace(in.TargetPVCName), DataDir: dataDir, ArchiveURI: artifact.ArchiveURI, ArchiveChecksum: checksum, ArchiveSizeBytes: size, CompleteMarkerURI: "s3://" + bucket + "/" + markerKey}, nil
}

func newRepositoryRestoreS3Client(ctx context.Context, in repositoryRestoreLocalInput) (*s3.Client, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{}
	accessKeyEnv := strings.TrimSpace(in.AccessKeyEnv)
	secretKeyEnv := strings.TrimSpace(in.SecretKeyEnv)
	accessKey := ""
	secretKey := ""
	if accessKeyEnv != "" {
		accessKey = os.Getenv(accessKeyEnv)
	}
	if secretKeyEnv != "" {
		secretKey = os.Getenv(secretKeyEnv)
	}
	if accessKey != "" || secretKey != "" {
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("repository credential environment variables are incomplete")
		}
		sessionToken := ""
		if name := strings.TrimSpace(in.SessionTokenEnv); name != "" {
			sessionToken = os.Getenv(name)
		}
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)))
	}
	if strings.TrimSpace(in.Region) != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(strings.TrimSpace(in.Region)))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint := strings.TrimSpace(in.EndpointURI); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = in.ForcePathStyle
	}), nil
}

func getRepositoryRestoreObjectBytes(ctx context.Context, client *s3.Client, bucket, key string, limit int64) ([]byte, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	limited := io.LimitReader(out.Body, limit+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("repository object %s exceeds %d bytes", key, limit)
	}
	return raw, nil
}

func getRepositoryRestoreObjectFile(ctx context.Context, client *s3.Client, bucket, key, path string) (int64, string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return 0, "", err
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return 0, "", err
	}
	defer out.Body.Close()
	tmp := path + ".tmp-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, "", err
	}
	h := sha256.New()
	size, copyErr := io.Copy(file, io.TeeReader(out.Body, h))
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return 0, "", copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return 0, "", closeErr
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, "", err
	}
	return size, hex.EncodeToString(h.Sum(nil)), nil
}

func productionArtifactForOrdinal(manifest clusterbackup.BackupSetManifestV1, ordinal int) (clusterbackup.BackupSetNodeArtifact, bool) {
	for _, artifact := range manifest.Artifacts.Nodes {
		if artifact.Ordinal == ordinal {
			return artifact, true
		}
	}
	return clusterbackup.BackupSetNodeArtifact{}, false
}

func repositoryRestoreKeyFromS3URI(bucket, raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "s3" || u.Host == "" || strings.Trim(u.Path, "/") == "" {
		return "", fmt.Errorf("artifact URI %q is not a valid s3:// URI", raw)
	}
	if u.Host != bucket {
		return "", fmt.Errorf("artifact URI bucket %q does not match repository bucket %q", u.Host, bucket)
	}
	return cleanRepositoryRestoreKey(u.Path), nil
}

func repositoryRestoreArchiveFormat(value string) backupcore.ArchiveFormat {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "TAR":
		return backupcore.ArchiveFormatTar
	case "TAR_GZ", "TGZ":
		return backupcore.ArchiveFormatTarGz
	case "TAR_ZST", "TZST":
		return backupcore.ArchiveFormatTarZst
	case "ZIP":
		return backupcore.ArchiveFormatZip
	default:
		return backupcore.ArchiveFormat(strings.ToLower(strings.TrimSpace(value)))
	}
}

func ensureRepositoryRestoreEmptyDataDir(path string) error {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return fmt.Errorf("target data directory is required")
	}
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0o700)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("target data directory %s is not a directory", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("target data directory %s is not empty", path)
	}
	return nil
}

func joinRepositoryRestoreKey(parts ...string) string {
	out := []string{}
	for _, part := range parts {
		part = cleanRepositoryRestoreKey(part)
		if part != "" && part != "." {
			out = append(out, part)
		}
	}
	return strings.Join(out, "/")
}

func cleanRepositoryRestoreKey(value string) string {
	return strings.Trim(strings.TrimSpace(value), "/")
}
