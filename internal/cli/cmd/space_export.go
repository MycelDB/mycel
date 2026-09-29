package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/myceldb/mycel/internal/fsperm"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const spaceExportFormatVersion = "mycel-space-export-v1"

type spaceExportManifest struct {
	FormatVersion string             `json:"format_version"`
	RequestedBy   spaceExportSubject `json:"requested_by"`
	Counts        spaceExportCounts  `json:"counts"`
}

type spaceExportSubject struct {
	PrincipalID string `json:"principal_id"`
	Username    string `json:"username"`
}

type spaceExportCounts struct {
	Spaces  int `json:"spaces"`
	Domains int `json:"domains"`
	Nodes   int `json:"nodes"`
	Edges   int `json:"edges"`
	Blobs   int `json:"blobs"`
	Files   int `json:"files"`
}

func NewExportSpaceCommand(a *app.App) *cobra.Command {
	var outputPath, spaceID string
	var domainIDs []string
	var includeBlobs, includeSystemDomains bool
	var waitTimeout time.Duration
	cmd := &cobra.Command{Use: "space", Short: "Create a daemon-managed ZIP export for one space", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(outputPath) == "" || outputPath == "-" {
			return fmt.Errorf("--file is required for space export download")
		}
		if strings.TrimSpace(spaceID) == "" {
			return fmt.Errorf("--space-id is required")
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		client := clientv1.NewImportExportServiceClient(conn)
		res, err := client.CreateSpaceExport(authCtx, &clientv1.CreateSpaceExportRequest{Options: spaceExportOptions(spaceID, domainIDs, includeBlobs, includeSystemDomains)})
		if err != nil {
			return err
		}
		job, err := waitForSpaceExport(cmd.Context(), authCtx, client, res.GetJob().GetExportId(), waitTimeout)
		if err != nil {
			return err
		}
		if err := downloadSpaceExport(authCtx, client, job.GetExportId(), outputPath); err != nil {
			return err
		}
		return a.Print(job, fmt.Sprintf("space export downloaded: %s\n", outputPath))
	}}
	addSpaceExportSelectionFlags(cmd, &spaceID, &domainIDs, &includeBlobs, &includeSystemDomains)
	cmd.Flags().StringVarP(&outputPath, "file", "f", "", "output ZIP path")
	cmd.Flags().DurationVar(&waitTimeout, "wait-timeout", 10*time.Minute, "maximum time to wait for daemon export completion")
	cmd.AddCommand(newSpaceExportCreateCommand(a), newSpaceExportStatusCommand(a), newSpaceExportListCommand(a), newSpaceExportDownloadCommand(a), newSpaceExportDeleteCommand(a))
	return cmd
}

func newSpaceExportCreateCommand(a *app.App) *cobra.Command {
	var spaceID string
	var domainIDs []string
	var includeBlobs, includeSystemDomains bool
	cmd := &cobra.Command{Use: "create", Short: "Create a space export job", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(spaceID) == "" {
			return fmt.Errorf("--space-id is required")
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewImportExportServiceClient(conn).CreateSpaceExport(authCtx, &clientv1.CreateSpaceExportRequest{Options: spaceExportOptions(spaceID, domainIDs, includeBlobs, includeSystemDomains)})
		if err != nil {
			return err
		}
		return a.Print(res.GetJob(), "space export job created: "+res.GetJob().GetExportId()+"\n")
	}}
	addSpaceExportSelectionFlags(cmd, &spaceID, &domainIDs, &includeBlobs, &includeSystemDomains)
	return cmd
}

func newSpaceExportStatusCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "status EXPORT_ID", Args: cobra.ExactArgs(1), Short: "Show space export job status", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewImportExportServiceClient(conn).GetSpaceExport(authCtx, &clientv1.GetSpaceExportRequest{ExportId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetJob(), fmt.Sprintf("%s %s %d%%\n", res.GetJob().GetExportId(), res.GetJob().GetStatus().String(), res.GetJob().GetProgressPercent()))
	}}
}

func newSpaceExportListCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "list", Short: "List recent space export jobs", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewImportExportServiceClient(conn).ListSpaceExports(authCtx, &clientv1.ListSpaceExportsRequest{SpaceId: spaceID})
		if err != nil {
			return err
		}
		return a.Print(res.GetJobs(), fmt.Sprintf("%d space export jobs\n", len(res.GetJobs())))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "optional space id filter")
	return cmd
}

func newSpaceExportDownloadCommand(a *app.App) *cobra.Command {
	var outputPath string
	cmd := &cobra.Command{Use: "download EXPORT_ID", Args: cobra.ExactArgs(1), Short: "Download a completed space export artifact", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(outputPath) == "" || outputPath == "-" {
			return fmt.Errorf("--file is required")
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		if err := downloadSpaceExport(authCtx, clientv1.NewImportExportServiceClient(conn), args[0], outputPath); err != nil {
			return err
		}
		return a.Print(map[string]any{"export_id": args[0], "file": outputPath}, "space export downloaded: "+outputPath+"\n")
	}}
	cmd.Flags().StringVarP(&outputPath, "file", "f", "", "output ZIP path")
	return cmd
}

func newSpaceExportDeleteCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "delete EXPORT_ID", Args: cobra.ExactArgs(1), Short: "Delete a space export artifact", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewImportExportServiceClient(conn).DeleteSpaceExport(authCtx, &clientv1.DeleteSpaceExportRequest{ExportId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, "space export deleted: "+args[0]+"\n")
	}}
}

func addSpaceExportSelectionFlags(cmd *cobra.Command, spaceID *string, domainIDs *[]string, includeBlobs *bool, includeSystemDomains *bool) {
	cmd.Flags().StringVar(spaceID, "space-id", "", "space id to export")
	cmd.Flags().StringArrayVar(domainIDs, "domain-id", nil, "domain id to include; repeat to export multiple domains; defaults to all visible domains in the space")
	cmd.Flags().BoolVar(includeBlobs, "include-blobs", true, "include blob payload files referenced by exported graph nodes")
	cmd.Flags().BoolVar(includeSystemDomains, "include-system-domains", false, "include system/internal domains when --domain-id is not supplied")
}

func spaceExportOptions(spaceID string, domainIDs []string, includeBlobs, includeSystemDomains bool) *clientv1.SpaceExportOptions {
	return &clientv1.SpaceExportOptions{SpaceId: strings.TrimSpace(spaceID), DomainIds: domainIDs, IncludeBlobs: includeBlobs, IncludeSystemDomains: includeSystemDomains}
}

func waitForSpaceExport(parent context.Context, authCtx context.Context, client clientv1.ImportExportServiceClient, exportID string, timeout time.Duration) (*clientv1.SpaceExportJob, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		res, err := client.GetSpaceExport(authCtx, &clientv1.GetSpaceExportRequest{ExportId: exportID})
		if err != nil {
			return nil, err
		}
		job := res.GetJob()
		switch job.GetStatus() {
		case clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_SUCCEEDED:
			return job, nil
		case clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_FAILED:
			return nil, fmt.Errorf("space export failed: %s", job.GetErrorMessage())
		case clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_DELETED, clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_EXPIRED:
			return nil, fmt.Errorf("space export is %s", job.GetStatus().String())
		}
		select {
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "timed out waiting for space export")
		case <-ticker.C:
		}
	}
}

func downloadSpaceExport(ctx context.Context, client clientv1.ImportExportServiceClient, exportID string, outputPath string) error {
	stream, err := client.DownloadSpaceExport(ctx, &clientv1.DownloadSpaceExportRequest{ExportId: exportID})
	if err != nil {
		return err
	}
	out, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			return out.Close()
		}
		if err != nil {
			_ = out.Close()
			return err
		}
		if _, err := out.Write(res.GetChunk()); err != nil {
			_ = out.Close()
			return err
		}
	}
}
