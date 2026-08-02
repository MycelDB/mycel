package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/userbackup/archive"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func NewAdminUserBackupCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "user-backup", Short: "Export, validate, and restore user-scoped backup archives"}
	cmd.AddCommand(NewAdminUserBackupExportCommand(a), NewAdminUserBackupValidateCommand(a), NewAdminUserBackupImportCommand(a))
	return cmd
}

func NewAdminUserBackupExportCommand(a *app.App) *cobra.Command {
	var userID, username, outputPath, compression, sourceLabel string
	var includeBlobs, includeArchived, includeSystemDomains bool
	cmd := &cobra.Command{Use: "export", Short: "Export a selected user's visible spaces from this daemon", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(outputPath) == "" || outputPath == "-" {
			return fmt.Errorf("--file is required for user backups")
		}
		conn, operatorCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		userClient := adminv1.NewAdminUserServiceClient(conn)
		user, err := resolveAdminUser(cmd.Context(), userClient, operatorCtx, userID, username)
		if err != nil {
			return err
		}
		sessionRes, err := userClient.CreateUserSession(operatorCtx, &adminv1.CreateUserSessionRequest{UserId: user.GetUserId(), Client: &adminv1.AdminClientInfo{Name: "mycel-admin-user-backup", Version: "v1", DeviceLabel: "operator-export"}})
		if err != nil {
			return fmt.Errorf("create temporary user export session: %w", err)
		}
		defer func() {
			_, _ = userClient.RevokeUserSession(operatorCtx, &adminv1.RevokeUserSessionRequest{UserId: user.GetUserId(), AuthSessionId: sessionRes.GetAuthSessionId()})
		}()
		userCtx := metadata.AppendToOutgoingContext(cmd.Context(), "authorization", "Bearer "+sessionRes.GetAccessToken())
		spaces, err := listAllUserSpaces(userCtx, clientv1.NewSpaceServiceClient(conn), includeArchived)
		if err != nil {
			return err
		}
		sort.Slice(spaces, func(i, j int) bool { return spaces[i].GetSpaceId() < spaces[j].GetSpaceId() })
		manifest := archive.Manifest{CreatedAt: time.Now().UTC(), Source: archive.Source{Endpoint: a.DaemonAddr, Label: sourceLabel}, SubjectUser: archive.User{UserID: user.GetUserId(), Username: user.GetUsername(), State: user.GetState().String()}, Options: archive.Options{IncludeBlobs: includeBlobs, IncludeInactiveSpaces: includeArchived}}
		var entries []archive.Entry
		for _, sp := range spaces {
			domains, err := listAllUserDomains(userCtx, clientv1.NewDomainServiceClient(conn), sp.GetSpaceId(), includeSystemDomains)
			if err != nil {
				return err
			}
			sort.Slice(domains, func(i, j int) bool { return domains[i].GetDomainId() < domains[j].GetDomainId() })
			spaceEntry := archive.Space{SourceSpaceID: sp.GetSpaceId(), Name: sp.GetName(), OwnerUserID: sp.GetOwner().GetId()}
			for _, domain := range domains {
				data, counts, err := exportDomainDocument(userCtx, conn, sp.GetSpaceId(), domain.GetDomainId(), includeBlobs)
				if err != nil {
					return fmt.Errorf("export domain %s/%s: %w", sp.GetSpaceId(), domain.GetDomainId(), err)
				}
				dataPath := fmt.Sprintf("domains/%s/%s.json", sp.GetSpaceId(), domain.GetDomainId())
				entries = append(entries, archive.Entry{Path: dataPath, Data: data, ContentType: "application/json"})
				domainEntry := archive.Domain{SourceDomainID: domain.GetDomainId(), Key: domain.GetKey(), Name: domain.GetName(), Description: domain.GetDescription(), Default: domain.GetDefault(), System: domain.GetSystem(), DiscoveryMode: domain.GetDiscoveryMode().String(), SearchMode: domain.GetSearchMode().String(), SemanticMode: domain.GetSemanticMode().String(), ReadOnly: domain.GetReadOnly(), DataPath: dataPath}
				if schema, err := clientv1.NewSchemaServiceClient(conn).GetDomainSchema(userCtx, &clientv1.GetDomainSchemaRequest{DomainId: domain.GetDomainId()}); err == nil && strings.TrimSpace(schema.GetGwl()) != "" {
					schemaPath := fmt.Sprintf("schemas/%s/%s.gwl", sp.GetSpaceId(), domain.GetDomainId())
					entries = append(entries, archive.Entry{Path: schemaPath, Data: []byte(schema.GetGwl()), ContentType: "text/plain"})
					domainEntry.SchemaPath = schemaPath
				} else if err != nil && status.Code(err) != codes.NotFound {
					return fmt.Errorf("get schema for domain %s: %w", domain.GetDomainId(), err)
				}
				_ = counts
				spaceEntry.Domains = append(spaceEntry.Domains, domainEntry)
			}
			manifest.Spaces = append(manifest.Spaces, spaceEntry)
		}
		file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		if err := archive.Write(file, compression, manifest, entries); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		return a.Print(map[string]any{"file": outputPath, "user_id": user.GetUserId(), "username": user.GetUsername(), "spaces": len(manifest.Spaces), "files": len(entries)}, fmt.Sprintf("user backup exported: %s (%d spaces)\n", outputPath, len(manifest.Spaces)))
	}}
	cmd.Flags().StringVar(&userID, "user-id", "", "source user id")
	cmd.Flags().StringVar(&username, "source-username", "", "source username")
	cmd.Flags().StringVarP(&outputPath, "file", "f", "", "output archive path (.tar.zst recommended)")
	cmd.Flags().StringVar(&compression, "compression", archive.DefaultMethod, "compression: zstd, gzip, or none")
	cmd.Flags().StringVar(&sourceLabel, "source-label", "", "operator label for the authoritative source pod/endpoint")
	cmd.Flags().BoolVar(&includeBlobs, "include-blobs", true, "include blob payloads embedded in domain export streams")
	cmd.Flags().BoolVar(&includeArchived, "include-archived", false, "include archived spaces visible to the user")
	cmd.Flags().BoolVar(&includeSystemDomains, "include-system-domains", false, "include system/internal domains visible to the user")
	return cmd
}

func NewAdminUserBackupValidateCommand(a *app.App) *cobra.Command {
	var filePath, compression string
	cmd := &cobra.Command{Use: "validate", Aliases: []string{"check"}, Short: "Validate a user backup archive without connecting to a daemon", RunE: func(cmd *cobra.Command, args []string) error {
		backup, err := readUserBackupArchive(filePath, compression)
		if err != nil {
			return err
		}
		return a.Print(backup.Manifest, fmt.Sprintf("user backup valid: %s (%d spaces, %d files)\n", backup.Manifest.SubjectUser.Username, len(backup.Manifest.Spaces), len(backup.Manifest.Files)))
	}}
	cmd.Flags().StringVarP(&filePath, "file", "f", "", "input archive path")
	cmd.Flags().StringVar(&compression, "compression", "auto", "compression: auto, zstd, gzip, or none")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

func NewAdminUserBackupImportCommand(a *app.App) *cobra.Command {
	var filePath, compression, targetUserID, targetUsername, newPassword, mode string
	var execute, createUser, disabled, allowNameConflicts bool
	cmd := &cobra.Command{Use: "import", Short: "Plan or restore a user backup into a target user", RunE: func(cmd *cobra.Command, args []string) error {
		backup, err := readUserBackupArchive(filePath, compression)
		if err != nil {
			return err
		}
		if mode != "append" && mode != "upsert" {
			return fmt.Errorf("--domain-import-mode must be append or upsert")
		}
		if strings.TrimSpace(targetUserID) == "" && strings.TrimSpace(targetUsername) == "" {
			targetUsername = backup.Manifest.SubjectUser.Username
		}
		conn, operatorCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		userClient := adminv1.NewAdminUserServiceClient(conn)
		targetUser, err := resolveAdminUser(cmd.Context(), userClient, operatorCtx, targetUserID, targetUsername)
		if err != nil {
			if !createUser || strings.TrimSpace(targetUsername) == "" {
				return err
			}
			if !execute {
				plan, planErr := planUserBackupImportForNewUser(backup, targetUsername)
				if planErr != nil {
					return planErr
				}
				plan["dry_run"] = true
				return a.Print(plan, fmt.Sprintf("user backup import dry-run: %d spaces would be created for %s\n", len(backup.Manifest.Spaces), targetUsername))
			}
			created, createErr := userClient.CreateUser(operatorCtx, &adminv1.CreateUserRequest{Username: targetUsername, Password: optionalString(newPassword), Disabled: disabled})
			if createErr != nil {
				return fmt.Errorf("create target user: %w", createErr)
			}
			targetUser = created.GetUser()
		}
		plan, err := planUserBackupImport(cmd.Context(), conn, operatorCtx, backup, targetUser, allowNameConflicts)
		if err != nil {
			return err
		}
		plan["dry_run"] = !execute
		if !execute {
			return a.Print(plan, fmt.Sprintf("user backup import dry-run: %d spaces would be created for %s\n", len(backup.Manifest.Spaces), targetUser.GetUsername()))
		}
		restored, err := executeUserBackupImport(cmd.Context(), conn, operatorCtx, backup, targetUser, mode)
		if err != nil {
			return err
		}
		return a.Print(restored, fmt.Sprintf("user backup imported: %d spaces restored for %s\n", restored.SpacesCreated, targetUser.GetUsername()))
	}}
	cmd.Flags().StringVarP(&filePath, "file", "f", "", "input archive path")
	cmd.Flags().StringVar(&compression, "compression", "auto", "compression: auto, zstd, gzip, or none")
	cmd.Flags().StringVar(&targetUserID, "target-user-id", "", "existing restore target user id")
	cmd.Flags().StringVar(&targetUsername, "target-username", "", "existing or new restore target username")
	cmd.Flags().BoolVar(&createUser, "create-user", false, "create --target-username when it does not exist")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "optional password for --create-user (never read from backup)")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "create target user disabled")
	cmd.Flags().BoolVar(&execute, "execute", false, "perform restore; default is dry-run/plan only")
	cmd.Flags().BoolVar(&allowNameConflicts, "allow-space-name-conflicts", false, "allow restore when target already has spaces with the same names")
	cmd.Flags().StringVar(&mode, "domain-import-mode", "append", "domain import mode: append or upsert")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

func readUserBackupArchive(filePath, compression string) (*archive.Archive, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return archive.Read(f, compression)
}

func resolveAdminUser(ctx context.Context, client adminv1.AdminUserServiceClient, authCtx context.Context, userID, username string) (*adminv1.User, error) {
	userID = strings.TrimSpace(userID)
	username = strings.TrimSpace(username)
	if userID == "" && username == "" {
		return nil, fmt.Errorf("--user-id or --source-username/--target-username is required")
	}
	if userID != "" {
		res, err := client.GetUser(authCtx, &adminv1.GetUserRequest{UserId: userID})
		if err != nil {
			return nil, fmt.Errorf("get user %s: %w", userID, err)
		}
		return res.GetUser(), nil
	}
	res, err := client.FindUser(authCtx, &adminv1.FindUserRequest{Username: username})
	if err != nil {
		return nil, fmt.Errorf("find user %s: %w", username, err)
	}
	return res.GetUser(), nil
}

func listAllUserSpaces(ctx context.Context, client clientv1.SpaceServiceClient, includeArchived bool) ([]*clientv1.Space, error) {
	var out []*clientv1.Space
	for token := ""; ; {
		res, err := client.ListSpaces(ctx, &clientv1.ListSpacesRequest{PageSize: 500, PageToken: token, IncludeArchived: includeArchived})
		if err != nil {
			return nil, fmt.Errorf("list spaces: %w", err)
		}
		out = append(out, res.GetSpaces()...)
		if res.GetNextPageToken() == "" {
			return out, nil
		}
		token = res.GetNextPageToken()
	}
}

func listAllUserDomains(ctx context.Context, client clientv1.DomainServiceClient, spaceID string, includeSystem bool) ([]*clientv1.Domain, error) {
	var out []*clientv1.Domain
	for token := ""; ; {
		res, err := client.ListDomains(ctx, &clientv1.ListDomainsRequest{SpaceId: spaceID, PageSize: 500, PageToken: token, IncludeSystem: includeSystem})
		if err != nil {
			return nil, fmt.Errorf("list domains for space %s: %w", spaceID, err)
		}
		out = append(out, res.GetDomains()...)
		if res.GetNextPageToken() == "" {
			return out, nil
		}
		token = res.GetNextPageToken()
	}
}

type domainCounts struct{ Blobs, Nodes, Edges int }

func exportDomainDocument(ctx context.Context, conn grpc.ClientConnInterface, spaceID, domainID string, includeBlobs bool) ([]byte, domainCounts, error) {
	sessionClient := clientv1.NewSessionServiceClient(conn)
	txClient := clientv1.NewTransactionServiceClient(conn)
	session, err := sessionClient.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		return nil, domainCounts{}, err
	}
	sessionID := session.GetSession().GetSessionId()
	defer func() { _, _ = sessionClient.CloseSession(ctx, &clientv1.CloseSessionRequest{SessionId: sessionID}) }()
	tx, err := txClient.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: sessionID, Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY})
	if err != nil {
		return nil, domainCounts{}, err
	}
	txID := tx.GetTransaction().GetTransactionId()
	defer func() { _, _ = txClient.CloseTransaction(ctx, &clientv1.CloseTransactionRequest{TransactionId: txID}) }()
	stream, err := clientv1.NewImportExportServiceClient(conn).ExportDomain(ctx, &clientv1.ExportDomainRequest{TransactionId: txID, Format: clientv1.DomainExportFormat_DOMAIN_EXPORT_FORMAT_MYCEL_STREAM, Options: &clientv1.DomainExportOptions{IncludeBlobs: includeBlobs}})
	if err != nil {
		return nil, domainCounts{}, err
	}
	doc := domainJSONDocument{Format: "mycel-domain-json-v1"}
	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, domainCounts{}, err
		}
		if manifest := res.GetManifest(); manifest != nil {
			doc.Manifest = manifest
			continue
		}
		if record := res.GetRecord(); record != nil {
			if blobMetadata := record.GetBlobMetadata(); blobMetadata != nil {
				doc.BlobMetadata = append(doc.BlobMetadata, blobMetadata)
			}
			if blobChunk := record.GetBlobChunk(); blobChunk != nil {
				doc.BlobChunks = append(doc.BlobChunks, blobChunk)
			}
			if node := record.GetNode(); node != nil {
				doc.Nodes = append(doc.Nodes, node)
			}
			if edge := record.GetEdge(); edge != nil {
				doc.Edges = append(doc.Edges, edge)
			}
		}
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, domainCounts{}, err
	}
	raw = append(raw, '\n')
	return raw, domainCounts{Blobs: len(doc.BlobMetadata), Nodes: len(doc.Nodes), Edges: len(doc.Edges)}, nil
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type userBackupRestoreResult struct {
	UserID         string            `json:"user_id"`
	Username       string            `json:"username"`
	SpacesCreated  int               `json:"spaces_created"`
	DomainsCreated int               `json:"domains_created"`
	NodesImported  int64             `json:"nodes_imported"`
	EdgesImported  int64             `json:"edges_imported"`
	BlobsImported  int64             `json:"blobs_imported"`
	SpaceIDMap     map[string]string `json:"space_id_map"`
	DomainIDMap    map[string]string `json:"domain_id_map"`
	Warnings       []string          `json:"warnings,omitempty"`
}

type userBackupTotals struct {
	domains int
	nodes   int
	edges   int
	blobs   int
}

func userBackupTotalsFromArchive(backup *archive.Archive) (userBackupTotals, error) {
	var totals userBackupTotals
	for _, sp := range backup.Manifest.Spaces {
		for _, d := range sp.Domains {
			totals.domains++
			var doc domainJSONDocument
			if err := json.Unmarshal(backup.Entries[d.DataPath], &doc); err != nil {
				return totals, fmt.Errorf("decode domain data %s: %w", d.DataPath, err)
			}
			totals.nodes += len(doc.Nodes)
			totals.edges += len(doc.Edges)
			totals.blobs += len(doc.BlobMetadata)
		}
	}
	return totals, nil
}

func planUserBackupImportForNewUser(backup *archive.Archive, targetUsername string) (map[string]any, error) {
	totals, err := userBackupTotalsFromArchive(backup)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"source_user":          backup.Manifest.SubjectUser,
		"target_user_id":       "",
		"target_username":      targetUsername,
		"target_user_create":   true,
		"spaces_to_create":     len(backup.Manifest.Spaces),
		"domains_to_import":    totals.domains,
		"nodes_to_import":      totals.nodes,
		"edges_to_import":      totals.edges,
		"blobs_to_import":      totals.blobs,
		"space_name_conflicts": []map[string]string{},
		"preserves_graph_ids":  true,
		"preserves_space_ids":  false,
		"preserves_domain_ids": false,
	}, nil
}

func planUserBackupImport(ctx context.Context, conn grpc.ClientConnInterface, operatorCtx context.Context, backup *archive.Archive, target *adminv1.User, allowNameConflicts bool) (map[string]any, error) {
	userClient := adminv1.NewAdminUserServiceClient(conn)
	userCtx, revoke, err := temporaryUserAuthContext(operatorCtx, userClient, target.GetUserId(), "operator-import-plan")
	if err != nil {
		return nil, err
	}
	defer revoke()
	existing, err := listAllUserSpaces(userCtx, clientv1.NewSpaceServiceClient(conn), true)
	if err != nil {
		return nil, err
	}
	existingNames := map[string]string{}
	for _, sp := range existing {
		existingNames[strings.ToLower(sp.GetName())] = sp.GetSpaceId()
	}
	var conflicts []map[string]string
	var totalDomains, totalNodes, totalEdges, totalBlobs int
	for _, sp := range backup.Manifest.Spaces {
		if id := existingNames[strings.ToLower(sp.Name)]; id != "" {
			conflicts = append(conflicts, map[string]string{"source_space_id": sp.SourceSpaceID, "space_name": sp.Name, "target_space_id": id})
		}
		for _, d := range sp.Domains {
			totalDomains++
			var doc domainJSONDocument
			if err := json.Unmarshal(backup.Entries[d.DataPath], &doc); err != nil {
				return nil, fmt.Errorf("decode domain data %s: %w", d.DataPath, err)
			}
			totalNodes += len(doc.Nodes)
			totalEdges += len(doc.Edges)
			totalBlobs += len(doc.BlobMetadata)
		}
	}
	if len(conflicts) > 0 && !allowNameConflicts {
		return nil, fmt.Errorf("target user has %d space name conflict(s); rerun with --allow-space-name-conflicts or choose a fresh target", len(conflicts))
	}
	return map[string]any{
		"source_user":          backup.Manifest.SubjectUser,
		"target_user_id":       target.GetUserId(),
		"target_username":      target.GetUsername(),
		"spaces_to_create":     len(backup.Manifest.Spaces),
		"domains_to_import":    totalDomains,
		"nodes_to_import":      totalNodes,
		"edges_to_import":      totalEdges,
		"blobs_to_import":      totalBlobs,
		"space_name_conflicts": conflicts,
		"preserves_graph_ids":  true,
		"preserves_space_ids":  false,
		"preserves_domain_ids": false,
	}, nil
}

func executeUserBackupImport(ctx context.Context, conn grpc.ClientConnInterface, operatorCtx context.Context, backup *archive.Archive, target *adminv1.User, mode string) (userBackupRestoreResult, error) {
	userClient := adminv1.NewAdminUserServiceClient(conn)
	userCtx, revoke, err := temporaryUserAuthContext(operatorCtx, userClient, target.GetUserId(), "operator-import-execute")
	if err != nil {
		return userBackupRestoreResult{}, err
	}
	defer revoke()
	spaceClient := adminv1.NewAdminSpaceServiceClient(conn)
	domainClient := clientv1.NewDomainServiceClient(conn)
	schemaClient := clientv1.NewSchemaServiceClient(conn)
	result := userBackupRestoreResult{UserID: target.GetUserId(), Username: target.GetUsername(), SpaceIDMap: map[string]string{}, DomainIDMap: map[string]string{}}
	for _, sourceSpace := range backup.Manifest.Spaces {
		defaultDomain := firstDefaultDomain(sourceSpace.Domains)
		createSpace := &adminv1.CreateSpaceRequest{Name: sourceSpace.Name, OwnerUserId: target.GetUserId()}
		if defaultDomain != nil {
			createSpace.DefaultDomainKey = defaultDomain.Key
			createSpace.DefaultDomainName = defaultDomain.Name
		}
		createdSpace, err := spaceClient.CreateSpace(operatorCtx, createSpace)
		if err != nil {
			return result, fmt.Errorf("create target space for %s: %w", sourceSpace.Name, err)
		}
		targetSpaceID := createdSpace.GetSpace().GetSpaceId()
		result.SpacesCreated++
		result.SpaceIDMap[sourceSpace.SourceSpaceID] = targetSpaceID
		for _, sourceDomain := range sourceSpace.Domains {
			if sourceDomain.System {
				return result, fmt.Errorf("restoring system domain %s is not supported by safe user import", sourceDomain.SourceDomainID)
			}
			targetDomainID := ""
			if sourceDomain.Default {
				targetDomainID = createdSpace.GetDefaultDomainId()
				if err := updateRestoredDomainMetadata(userCtx, domainClient, targetSpaceID, targetDomainID, sourceDomain, false); err != nil {
					return result, err
				}
			} else {
				createdDomain, err := domainClient.CreateDomain(userCtx, &clientv1.CreateDomainRequest{SpaceId: targetSpaceID, Key: sourceDomain.Key, Name: sourceDomain.Name, Description: sourceDomain.Description, DiscoveryMode: parseDiscoveryMode(sourceDomain.DiscoveryMode), SearchMode: parseSearchMode(sourceDomain.SearchMode), SemanticMode: parseSemanticMode(sourceDomain.SemanticMode), ReadOnly: false})
				if err != nil {
					return result, fmt.Errorf("create target domain %s/%s: %w", sourceSpace.Name, sourceDomain.Name, err)
				}
				targetDomainID = createdDomain.GetDomain().GetDomainId()
				result.DomainsCreated++
			}
			result.DomainIDMap[sourceDomain.SourceDomainID] = targetDomainID
			if sourceDomain.SchemaPath != "" {
				if _, err := schemaClient.PutDomainSchema(userCtx, &clientv1.PutDomainSchemaRequest{DomainId: targetDomainID, Gwl: string(backup.Entries[sourceDomain.SchemaPath])}); err != nil {
					return result, fmt.Errorf("restore schema for domain %s: %w", sourceDomain.SourceDomainID, err)
				}
			}
			summary, err := importDomainDocument(userCtx, conn, targetSpaceID, targetDomainID, backup.Entries[sourceDomain.DataPath], mode)
			if err != nil {
				return result, fmt.Errorf("import domain %s: %w", sourceDomain.SourceDomainID, err)
			}
			if sourceDomain.ReadOnly {
				if err := updateRestoredDomainMetadata(userCtx, domainClient, targetSpaceID, targetDomainID, sourceDomain, true); err != nil {
					return result, err
				}
			}
			result.NodesImported += summary.GetNodesImported() + summary.GetNodesUpdated()
			result.EdgesImported += summary.GetEdgesImported() + summary.GetEdgesUpdated()
			result.BlobsImported += summary.GetBlobsImported()
			result.Warnings = append(result.Warnings, summary.GetWarnings()...)
		}
	}
	if len(result.Warnings) == 0 {
		result.Warnings = nil
	}
	return result, nil
}

func temporaryUserAuthContext(operatorCtx context.Context, userClient adminv1.AdminUserServiceClient, userID, label string) (context.Context, func(), error) {
	res, err := userClient.CreateUserSession(operatorCtx, &adminv1.CreateUserSessionRequest{UserId: userID, Client: &adminv1.AdminClientInfo{Name: "mycel-admin-user-backup", Version: "v1", DeviceLabel: label}})
	if err != nil {
		return nil, func() {}, fmt.Errorf("create temporary user session: %w", err)
	}
	revoke := func() {
		_, _ = userClient.RevokeUserSession(operatorCtx, &adminv1.RevokeUserSessionRequest{UserId: userID, AuthSessionId: res.GetAuthSessionId()})
	}
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+res.GetAccessToken()), revoke, nil
}

func firstDefaultDomain(domains []archive.Domain) *archive.Domain {
	for i := range domains {
		if domains[i].Default {
			return &domains[i]
		}
	}
	if len(domains) > 0 {
		return &domains[0]
	}
	return nil
}

func updateRestoredDomainMetadata(ctx context.Context, client clientv1.DomainServiceClient, spaceID, domainID string, source archive.Domain, includeReadOnly bool) error {
	paths := []string{}
	d := &clientv1.Domain{DomainId: domainID, SpaceId: spaceID}
	if source.Description != "" {
		d.Description = source.Description
		paths = append(paths, "description")
	}
	if source.DiscoveryMode != "" {
		d.DiscoveryMode = parseDiscoveryMode(source.DiscoveryMode)
		paths = append(paths, "discovery_mode")
	}
	if source.SearchMode != "" {
		d.SearchMode = parseSearchMode(source.SearchMode)
		paths = append(paths, "search_mode")
	}
	if source.SemanticMode != "" {
		d.SemanticMode = parseSemanticMode(source.SemanticMode)
		paths = append(paths, "semantic_mode")
	}
	if includeReadOnly && source.ReadOnly {
		d.ReadOnly = true
		paths = append(paths, "read_only")
	}
	if len(paths) == 0 {
		return nil
	}
	_, err := client.UpdateDomain(ctx, &clientv1.UpdateDomainRequest{SpaceId: spaceID, DomainId: domainID, Domain: d, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths}})
	if err != nil {
		return fmt.Errorf("update target domain metadata %s: %w", domainID, err)
	}
	return nil
}

func importDomainDocument(ctx context.Context, conn grpc.ClientConnInterface, spaceID, domainID string, raw []byte, mode string) (*clientv1.ImportSummary, error) {
	var doc domainJSONDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("invalid domain document: %w", err)
	}
	sessionClient := clientv1.NewSessionServiceClient(conn)
	txClient := clientv1.NewTransactionServiceClient(conn)
	session, err := sessionClient.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		return nil, err
	}
	sessionID := session.GetSession().GetSessionId()
	defer func() { _, _ = sessionClient.CloseSession(ctx, &clientv1.CloseSessionRequest{SessionId: sessionID}) }()
	tx, err := txClient.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: sessionID, Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		return nil, err
	}
	txID := tx.GetTransaction().GetTransactionId()
	committed := false
	defer func() {
		if !committed {
			_, _ = txClient.RollbackTransaction(ctx, &clientv1.RollbackTransactionRequest{TransactionId: txID})
		}
		_, _ = txClient.CloseTransaction(ctx, &clientv1.CloseTransactionRequest{TransactionId: txID})
	}()
	stream, err := clientv1.NewImportExportServiceClient(conn).ImportDomain(ctx)
	if err != nil {
		return nil, err
	}
	metadata := &clientv1.ImportDomainMetadata{TransactionId: txID, Format: clientv1.DomainImportFormat_DOMAIN_IMPORT_FORMAT_MYCEL_STREAM, Mode: parseDomainImportMode(mode), Options: &clientv1.DomainImportOptions{IncludeBlobs: len(doc.BlobMetadata) > 0 || len(doc.BlobChunks) > 0, PreserveIds: true}}
	if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Metadata{Metadata: metadata}}); err != nil {
		return nil, err
	}
	for _, blobMetadata := range doc.BlobMetadata {
		if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_BlobMetadata{BlobMetadata: blobMetadata}}}}); err != nil {
			return nil, err
		}
	}
	for _, blobChunk := range doc.BlobChunks {
		if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_BlobChunk{BlobChunk: blobChunk}}}}); err != nil {
			return nil, err
		}
	}
	for _, node := range doc.Nodes {
		if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_Node{Node: node}}}}); err != nil {
			return nil, err
		}
	}
	for _, edge := range doc.Edges {
		if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_Edge{Edge: edge}}}}); err != nil {
			return nil, err
		}
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	if _, err := txClient.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: txID}); err != nil {
		return nil, err
	}
	committed = true
	return res.GetSummary(), nil
}

func parseDiscoveryMode(raw string) clientv1.DomainDiscoveryMode {
	switch raw {
	case clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_EXPLICIT_ONLY.String(), "explicit-only", "explicit_only":
		return clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_EXPLICIT_ONLY
	case clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_HIDDEN.String(), "hidden":
		return clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_HIDDEN
	default:
		return clientv1.DomainDiscoveryMode_DOMAIN_DISCOVERY_MODE_NORMAL
	}
}

func parseSearchMode(raw string) clientv1.DomainSearchMode {
	switch raw {
	case clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_EXPLICIT_ONLY.String(), "explicit-only", "explicit_only":
		return clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_EXPLICIT_ONLY
	case clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_DISABLED.String(), "disabled":
		return clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_DISABLED
	default:
		return clientv1.DomainSearchMode_DOMAIN_SEARCH_MODE_NORMAL
	}
}

func parseSemanticMode(raw string) clientv1.DomainSemanticMode {
	switch raw {
	case clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_EXPLICIT_ONLY.String(), "explicit-only", "explicit_only":
		return clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_EXPLICIT_ONLY
	case clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_DISABLED.String(), "disabled":
		return clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_DISABLED
	default:
		return clientv1.DomainSemanticMode_DOMAIN_SEMANTIC_MODE_NORMAL
	}
}
