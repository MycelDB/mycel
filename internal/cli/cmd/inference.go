package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

type inferencePackageDocument struct {
	Name                      string                              `json:"name" yaml:"name"`
	Version                   string                              `json:"version" yaml:"version"`
	Source                    string                              `json:"source" yaml:"source"`
	Checksum                  string                              `json:"checksum" yaml:"checksum"`
	ModelEndpoints            []domainsemantic.ModelEndpoint      `json:"model_endpoints" yaml:"model_endpoints"`
	Models                    []domainsemantic.InferenceModel     `json:"models" yaml:"models"`
	VectorStores              []domainsemantic.VectorStoreBackend `json:"vector_stores" yaml:"vector_stores"`
	ModelEndpointCapabilities []capabilityDefinition              `json:"model_endpoint_capabilities" yaml:"model_endpoint_capabilities"`
}

type capabilityDefinition struct {
	ModelEndpoint     string                   `json:"model_endpoint" yaml:"model_endpoint"`
	ModelEndpointID   string                   `json:"model_endpoint_id" yaml:"model_endpoint_id"`
	Model             string                   `json:"model" yaml:"model"`
	ModelID           string                   `json:"model_id" yaml:"model_id"`
	Operation         domainsemantic.Operation `json:"operation" yaml:"operation"`
	Enabled           *bool                    `json:"enabled" yaml:"enabled"`
	ModelNameOverride string                   `json:"model_name_override" yaml:"model_name_override"`
	Metadata          map[string]any           `json:"metadata" yaml:"metadata"`
}

func NewInferenceCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "inference", Short: "Provision inference endpoints, models, credentials, profiles, grants, policies, decisions, and usage"}
	cmd.AddCommand(
		newInferencePackageCommand(a),
		newInferenceModelEndpointCommand(a),
		newInferenceModelCommand(a),
		newInferenceVectorStoreCommand(a),
		newInferenceCapabilityCommand(a),
		newInferenceCredentialCommand(a),
		newInferenceGrantCommand(a),
		newInferenceProfileCommand(a),
		newInferencePolicyCommand(a),
		newInferenceDecisionCommand(a),
		newInferenceUsageCommand(a),
	)
	return cmd
}

func newInferencePackageCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "package", Aliases: []string{"packages"}, Short: "Manage inference definition packages"}
	cmd.AddCommand(newInferencePackageApplyCommand(a), newInferencePackageListCommand(a))
	return cmd
}

func newInferencePackageApplyCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "apply FILE", Short: "Apply inference definitions from a YAML or JSON package", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonInferencePackageApply(cmd, a, args[0])
	}}
}

func runDaemonInferencePackageApply(cmd *cobra.Command, a *app.App, filePath string) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	var doc inferencePackageDocument
	if err := unmarshalYAMLWithJSONTags(raw, &doc); err != nil {
		return fmt.Errorf("invalid inference package: %w", err)
	}
	if strings.TrimSpace(doc.Name) == "" || strings.TrimSpace(doc.Version) == "" {
		return fmt.Errorf("invalid inference package: name and version are required")
	}
	req := &adminv1.AdminInferenceCatalogServiceApplyInferencePackageRequest{Name: doc.Name, Version: doc.Version, Source: firstNonEmpty(doc.Source, filePath), Checksum: doc.Checksum}
	for _, endpoint := range doc.ModelEndpoints {
		req.ModelEndpoints = append(req.ModelEndpoints, adminModelEndpointFromDomain(endpoint))
	}
	for _, model := range doc.Models {
		req.Models = append(req.Models, adminInferenceModelFromDomain(model))
	}
	for _, vectorStore := range doc.VectorStores {
		req.VectorStores = append(req.VectorStores, adminVectorStoreFromDomain(vectorStore))
	}
	for _, cap := range doc.ModelEndpointCapabilities {
		req.ModelEndpointCapabilities = append(req.ModelEndpointCapabilities, adminCapabilityDefinitionFromCLI(cap))
	}
	res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).ApplyInferencePackage(authCtx, req)
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("inference package applied: %s@%s\n", res.GetPackage().GetName(), res.GetPackage().GetVersion()))
}

func newInferencePackageListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken string
	cmd := &cobra.Command{Use: "list", Short: "List inference packages via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).ListInferencePackages(authCtx, &adminv1.AdminInferenceCatalogServiceListInferencePackagesRequest{PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, pkg := range res.GetPackages() {
			fmt.Printf("%s\t%s@%s\n", pkg.GetInferencePackageId(), pkg.GetName(), pkg.GetVersion())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	return cmd
}

func newInferenceCapabilityCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "capability", Aliases: []string{"capabilities"}, Short: "Manage model endpoint capabilities"}
	cmd.AddCommand(newInferenceCapabilityListCommand(a), newInferenceCapabilityAddCommand(a), newInferenceCapabilitySetEnabledCommand(a, true), newInferenceCapabilitySetEnabledCommand(a, false), newInferenceCapabilityDeleteCommand(a))
	return cmd
}

func newInferenceCapabilityListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken, operation string
	var includeDisabled bool
	cmd := &cobra.Command{Use: "list", Short: "List model endpoint capabilities via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).ListModelEndpointCapabilities(authCtx, &adminv1.AdminInferenceCatalogServiceListModelEndpointCapabilitiesRequest{PageSize: pageSize, PageToken: pageToken, Operation: operation, IncludeDisabled: includeDisabled})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, cap := range res.GetModelEndpointCapabilities() {
			fmt.Printf("%s\tendpoint=%s\tmodel=%s\t%s\tenabled=%t\n", cap.GetModelEndpointCapabilityId(), cap.GetModelEndpointId(), cap.GetModelId(), cap.GetOperation(), cap.GetEnabled())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().StringVar(&operation, "operation", "", "operation filter")
	cmd.Flags().BoolVar(&includeDisabled, "include-disabled", false, "include disabled capabilities")
	return cmd
}

func newInferenceCapabilitySetEnabledCommand(a *app.App, enabled bool) *cobra.Command {
	use := "disable CAPABILITY_ID"
	if enabled {
		use = "enable CAPABILITY_ID"
	}
	return &cobra.Command{Use: use, Short: "Enable or disable a model endpoint capability via daemon gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).SetModelEndpointCapabilityEnabled(authCtx, &adminv1.AdminInferenceCatalogServiceSetModelEndpointCapabilityEnabledRequest{ModelEndpointCapabilityId: args[0], Enabled: enabled})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("model endpoint capability %s: enabled=%t\n", res.GetModelEndpointCapability().GetModelEndpointCapabilityId(), res.GetModelEndpointCapability().GetEnabled()))
	}}
}

func newInferenceCapabilityDeleteCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "delete CAPABILITY_ID", Aliases: []string{"rm", "remove"}, Short: "Hard-delete an unreferenced model endpoint capability", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).DeleteModelEndpointCapability(authCtx, &adminv1.AdminInferenceCatalogServiceDeleteModelEndpointCapabilityRequest{ModelEndpointCapabilityId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("model endpoint capability deleted: %s\n", res.GetModelEndpointCapabilityId()))
	}}
}

func newInferenceCapabilityAddCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "add", Short: "Deprecated embedded capability provisioning", Deprecated: "use `inference package apply` to provision capabilities through the daemon", RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("embedded capability add is no longer supported; use `inference package apply`")
	}}
}

func newInferenceModelEndpointCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "endpoint", Aliases: []string{"endpoints", "model-endpoint", "model-endpoints"}, Short: "Manage model endpoints"}
	cmd.AddCommand(newInferenceModelEndpointListCommand(a), newInferenceModelEndpointSetEnabledCommand(a, true), newInferenceModelEndpointSetEnabledCommand(a, false), newInferenceModelEndpointDeleteCommand(a))
	return cmd
}

func newInferenceModelEndpointListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken string
	var includeDisabled bool
	cmd := &cobra.Command{Use: "list", Short: "List model endpoints via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).ListModelEndpoints(authCtx, &adminv1.AdminInferenceCatalogServiceListModelEndpointsRequest{PageSize: pageSize, PageToken: pageToken, IncludeDisabled: includeDisabled})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, endpoint := range res.GetModelEndpoints() {
			fmt.Printf("%s\t%s\t%s\tenabled=%t\n", endpoint.GetModelEndpointId(), endpoint.GetKey(), endpoint.GetConnectorType(), endpoint.GetEnabled())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().BoolVar(&includeDisabled, "include-disabled", false, "include disabled endpoints")
	return cmd
}

func newInferenceModelEndpointSetEnabledCommand(a *app.App, enabled bool) *cobra.Command {
	use := "disable ENDPOINT"
	if enabled {
		use = "enable ENDPOINT"
	}
	return &cobra.Command{Use: use, Short: "Enable or disable a model endpoint via daemon gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).SetModelEndpointEnabled(authCtx, &adminv1.AdminInferenceCatalogServiceSetModelEndpointEnabledRequest{ModelEndpoint: args[0], Enabled: enabled})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("model endpoint %s: enabled=%t\n", res.GetModelEndpoint().GetKey(), res.GetModelEndpoint().GetEnabled()))
	}}
}

func newInferenceModelEndpointDeleteCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "delete ENDPOINT", Aliases: []string{"rm", "remove"}, Short: "Hard-delete an unreferenced model endpoint", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).DeleteModelEndpoint(authCtx, &adminv1.AdminInferenceCatalogServiceDeleteModelEndpointRequest{ModelEndpoint: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("model endpoint deleted: %s\n", res.GetModelEndpointId()))
	}}
}

func newInferenceModelCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "model", Aliases: []string{"models"}, Short: "Manage inference models"}
	cmd.AddCommand(newInferenceModelListCommand(a), newInferenceModelDeleteCommand(a))
	return cmd
}

func newInferenceModelListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken, operation string
	cmd := &cobra.Command{Use: "list", Short: "List inference models via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).ListModels(authCtx, &adminv1.AdminInferenceCatalogServiceListModelsRequest{PageSize: pageSize, PageToken: pageToken, Operation: operation})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, model := range res.GetModels() {
			fmt.Printf("%s\t%s\t%s\tdimensions=%d\n", model.GetModelId(), model.GetKey(), model.GetOperation(), model.GetDimensions())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().StringVar(&operation, "operation", "", "operation filter")
	return cmd
}

func newInferenceModelDeleteCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "delete MODEL", Aliases: []string{"rm", "remove"}, Short: "Hard-delete an unreferenced inference model", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).DeleteModel(authCtx, &adminv1.AdminInferenceCatalogServiceDeleteModelRequest{Model: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("model deleted: %s\n", res.GetModelId()))
	}}
}

func newInferenceVectorStoreCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "vector-store", Aliases: []string{"vector-stores"}, Short: "Manage vector stores"}
	cmd.AddCommand(newInferenceVectorStoreListCommand(a), newInferenceVectorStoreSetEnabledCommand(a, true), newInferenceVectorStoreSetEnabledCommand(a, false), newInferenceVectorStoreDeleteCommand(a))
	return cmd
}

func newInferenceVectorStoreListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken string
	var includeDisabled bool
	cmd := &cobra.Command{Use: "list", Short: "List vector stores via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).ListVectorStores(authCtx, &adminv1.AdminInferenceCatalogServiceListVectorStoresRequest{PageSize: pageSize, PageToken: pageToken, IncludeDisabled: includeDisabled})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, store := range res.GetVectorStores() {
			fmt.Printf("%s\t%s\t%s\tenabled=%t\n", store.GetVectorStoreId(), store.GetKey(), store.GetType(), store.GetEnabled())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().BoolVar(&includeDisabled, "include-disabled", false, "include disabled vector stores")
	return cmd
}

func newInferenceVectorStoreSetEnabledCommand(a *app.App, enabled bool) *cobra.Command {
	use := "disable VECTOR_STORE"
	if enabled {
		use = "enable VECTOR_STORE"
	}
	return &cobra.Command{Use: use, Short: "Enable or disable a vector store via daemon gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).SetVectorStoreEnabled(authCtx, &adminv1.AdminInferenceCatalogServiceSetVectorStoreEnabledRequest{VectorStore: args[0], Enabled: enabled})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("vector store %s: enabled=%t\n", res.GetVectorStore().GetKey(), res.GetVectorStore().GetEnabled()))
	}}
}

func newInferenceVectorStoreDeleteCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "delete VECTOR_STORE", Aliases: []string{"rm", "remove"}, Short: "Hard-delete an unreferenced vector store", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCatalogServiceClient(conn).DeleteVectorStore(authCtx, &adminv1.AdminInferenceCatalogServiceDeleteVectorStoreRequest{VectorStore: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("vector store deleted: %s\n", res.GetVectorStoreId()))
	}}
}

func newInferenceCredentialCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "credential", Aliases: []string{"credentials"}, Short: "Manage inference credentials"}
	cmd.AddCommand(newInferenceCredentialAddCommand(a), newInferenceCredentialListCommand(a), newInferenceCredentialRotateCommand(a), newInferenceCredentialSetStatusCommand(a, domainsemantic.CredentialStatusActive), newInferenceCredentialSetStatusCommand(a, domainsemantic.CredentialStatusDisabled), newInferenceCredentialSetStatusCommand(a, domainsemantic.CredentialStatusRevoked), newInferenceCredentialDeleteCommand(a), newInferenceGrantCommand(a))
	return cmd
}

func newInferenceCredentialSetStatusCommand(a *app.App, status domainsemantic.CredentialStatus) *cobra.Command {
	use := "enable CREDENTIAL"
	aliases := []string{"active"}
	switch status {
	case domainsemantic.CredentialStatusDisabled:
		use = "disable CREDENTIAL"
		aliases = []string{"disabled"}
	case domainsemantic.CredentialStatusRevoked:
		use = "revoke CREDENTIAL"
		aliases = []string{"revoked"}
	}
	return &cobra.Command{Use: use, Aliases: aliases, Short: "Set credential lifecycle status via daemon gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCredentialServiceClient(conn).SetCredentialStatus(authCtx, &adminv1.AdminInferenceCredentialServiceSetCredentialStatusRequest{Credential: args[0], Status: string(status)})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("credential %s: status=%s\n", res.GetCredential().GetKey(), res.GetCredential().GetStatus()))
	}}
}

func newInferenceCredentialDeleteCommand(a *app.App) *cobra.Command {
	var deleteGrants, deleteSecret bool
	cmd := &cobra.Command{Use: "delete CREDENTIAL", Aliases: []string{"rm", "remove"}, Short: "Hard-delete an inference credential", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceCredentialServiceClient(conn).DeleteCredential(authCtx, &adminv1.AdminInferenceCredentialServiceDeleteCredentialRequest{Credential: args[0], DeleteGrants: deleteGrants, DeleteSecret: deleteSecret})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("credential deleted: %s (grants=%d secret_deleted=%t)\n", res.GetCredentialId(), res.GetCredentialGrantsDeleted(), res.GetSecretDeleted()))
	}}
	cmd.Flags().BoolVar(&deleteGrants, "delete-grants", false, "delete credential grants that reference this credential")
	cmd.Flags().BoolVar(&deleteSecret, "delete-secret", false, "delete the underlying secret if it is not shared")
	return cmd
}

func newInferenceCredentialAddCommand(a *app.App) *cobra.Command {
	var endpointRef, ownerUser, ownerType, ownerID, authType, secretValue, name string
	var isDefault, secretStdin bool
	cmd := &cobra.Command{Use: "create KEY", Aliases: []string{"add"}, Short: "Create an inference credential", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonInferenceCredentialAdd(cmd, a, args[0], endpointRef, ownerUser, ownerType, ownerID, authType, secretValue, secretStdin, name, isDefault)
	}}
	cmd.Flags().StringVar(&endpointRef, "model-endpoint", "", "model endpoint key or ID")
	cmd.Flags().StringVar(&ownerUser, "owner-user", "", "user ref/ID that owns the credential")
	cmd.Flags().StringVar(&ownerType, "owner-type", string(domainsemantic.CredentialOwnerUser), "owner type: user, space, organization, system")
	cmd.Flags().StringVar(&ownerID, "owner-id", "", "owner ID/ref")
	cmd.Flags().StringVar(&authType, "auth", string(domainsemantic.AuthModeAPIKey), "credential auth type")
	cmd.Flags().StringVar(&secretValue, "secret-value", "", "API key value (may be captured in shell history; prefer --secret-stdin)")
	cmd.Flags().BoolVar(&secretStdin, "secret-stdin", false, "read the API key value from stdin")
	cmd.Flags().StringVar(&name, "name", "", "credential display name")
	cmd.Flags().BoolVar(&isDefault, "default", false, "mark credential as default metadata")
	_ = cmd.MarkFlagRequired("model-endpoint")
	return cmd
}

func newInferenceCredentialRotateCommand(a *app.App) *cobra.Command {
	var secretValue string
	var secretStdin bool
	cmd := &cobra.Command{Use: "rotate CREDENTIAL", Short: "Rotate an inference credential secret", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		value, err := secretValueFromInput(cmd, secretValue, secretStdin)
		if err != nil {
			return err
		}
		req := &adminv1.AdminInferenceCredentialServiceRotateCredentialRequest{Credential: args[0], SecretValue: value}
		res, err := adminv1.NewAdminInferenceCredentialServiceClient(conn).RotateCredential(authCtx, req)
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("credential rotated: %s\n", res.GetCredential().GetCredentialId()))
	}}
	cmd.Flags().StringVar(&secretValue, "secret-value", "", "API key value (may be captured in shell history; prefer --secret-stdin)")
	cmd.Flags().BoolVar(&secretStdin, "secret-stdin", false, "read the API key value from stdin")
	return cmd
}

func newInferenceCredentialListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken, ownerType, ownerID, endpointID string
	var includeInactive bool
	cmd := &cobra.Command{Use: "list", Short: "List inference credentials via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &adminv1.AdminInferenceCredentialServiceListCredentialsRequest{PageSize: pageSize, PageToken: pageToken, OwnerType: ownerType, OwnerId: ownerID, IncludeInactive: includeInactive}
		if strings.TrimSpace(endpointID) != "" {
			req.ModelEndpointId = &endpointID
		}
		res, err := adminv1.NewAdminInferenceCredentialServiceClient(conn).ListCredentials(authCtx, req)
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, credential := range res.GetCredentials() {
			fmt.Printf("%s\t%s\towner=%s/%s\tendpoint=%s\tstatus=%s\n", credential.GetCredentialId(), credential.GetKey(), credential.GetOwnerType(), credential.GetOwnerId(), credential.GetModelEndpointId(), credential.GetStatus())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().StringVar(&ownerType, "owner-type", "", "owner type filter")
	cmd.Flags().StringVar(&ownerID, "owner-id", "", "owner ID/ref filter")
	cmd.Flags().StringVar(&endpointID, "model-endpoint-id", "", "model endpoint UUID filter")
	cmd.Flags().BoolVar(&includeInactive, "include-inactive", false, "include inactive credentials")
	return cmd
}

func newInferenceGrantCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, indexRef, nodeText, endpointRef, modelRef string
	var operations, granteePrincipalIDs, allowOnBehalfPrincipalIDs []string
	var allowBackgroundUse, includeDescendants, isDefault bool
	var priority int
	cmd := &cobra.Command{Use: "grant CREDENTIAL", Aliases: []string{"grants"}, Short: "Grant a credential for a space-owned processing scope", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonInferenceCredentialGrant(cmd, a, args[0], spaceIDText, domainRef, indexRef, nodeText, endpointRef, modelRef, operations, granteePrincipalIDs, allowOnBehalfPrincipalIDs, allowBackgroundUse, includeDescendants, isDefault, priority)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&indexRef, "semantic-index", "", "semantic index key or ID")
	cmd.Flags().StringVar(&nodeText, "node", "", "node ID scope")
	cmd.Flags().StringVar(&endpointRef, "model-endpoint", "", "optional model endpoint constraint")
	cmd.Flags().StringVar(&modelRef, "model", "", "optional model constraint")
	cmd.Flags().StringArrayVar(&operations, "operation", []string{string(domainsemantic.OperationEmbeddings)}, "operation allowed by grant")
	cmd.Flags().StringArrayVar(&granteePrincipalIDs, "grantee-principal-id", nil, "actor principal ID allowed by this grant; repeatable")
	cmd.Flags().StringArrayVar(&allowOnBehalfPrincipalIDs, "allow-on-behalf-of-principal-id", nil, "on-behalf-of principal ID allowed by this grant; repeatable")
	cmd.Flags().BoolVar(&allowBackgroundUse, "allow-background-use", false, "allow background semantic maintenance")
	cmd.Flags().BoolVar(&includeDescendants, "include-descendants", false, "include descendant nodes in node scope")
	cmd.Flags().BoolVar(&isDefault, "default", false, "mark grant as default metadata")
	cmd.Flags().IntVar(&priority, "priority", 0, "grant priority")
	cmd.AddCommand(newInferenceCredentialGrantListCommand(a), newInferenceCredentialGrantExpireCommand(a), newInferenceCredentialGrantDeleteCommand(a))
	return cmd
}

func newInferenceCredentialGrantExpireCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "expire GRANT_ID", Short: "Expire a credential grant via daemon gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceGrantServiceClient(conn).ExpireCredentialGrant(authCtx, &adminv1.AdminInferenceGrantServiceExpireCredentialGrantRequest{SpaceId: spaceID, CredentialGrantId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("credential grant expired: %s\n", res.GetCredentialGrant().GetCredentialGrantId()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferenceCredentialGrantDeleteCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "delete GRANT_ID", Aliases: []string{"rm", "remove"}, Short: "Hard-delete a credential grant", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceGrantServiceClient(conn).DeleteCredentialGrant(authCtx, &adminv1.AdminInferenceGrantServiceDeleteCredentialGrantRequest{SpaceId: spaceID, CredentialGrantId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("credential grant deleted: %s\n", res.GetCredentialGrantId()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferenceCredentialGrantListCommand(a *app.App) *cobra.Command {
	var spaceID, pageToken, credentialID string
	var pageSize int32
	var includeExpired bool
	cmd := &cobra.Command{Use: "list", Short: "List credential grants via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &adminv1.AdminInferenceGrantServiceListCredentialGrantsRequest{SpaceId: spaceID, PageSize: pageSize, PageToken: pageToken, IncludeExpired: includeExpired}
		if strings.TrimSpace(credentialID) != "" {
			req.CredentialId = &credentialID
		}
		res, err := adminv1.NewAdminInferenceGrantServiceClient(conn).ListCredentialGrants(authCtx, req)
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, grant := range res.GetCredentialGrants() {
			fmt.Printf("%s\tcredential=%s\tspace=%s\toperations=%s\n", grant.GetCredentialGrantId(), grant.GetCredentialId(), grant.GetScope().GetSpaceId(), strings.Join(grant.GetOperations(), ","))
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().StringVar(&credentialID, "credential-id", "", "credential UUID filter")
	cmd.Flags().BoolVar(&includeExpired, "include-expired", false, "include expired grants")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferencePolicyCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "policy", Aliases: []string{"policies"}, Short: "Manage inference content policies"}
	cmd.AddCommand(newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectAllow), newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectDeny), newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectRestrict), newInferencePolicyListCommand(a), newInferencePolicyExpireCommand(a), newInferencePolicyDeleteCommand(a), newInferenceDecisionCommand(a))
	return cmd
}

func newInferencePolicyEffectCommand(a *app.App, effect domainsemantic.PolicyEffect) *cobra.Command {
	var spaceIDText, domainRef, indexRef, nodeText, reason string
	var operations, privacyClasses []string
	var includeDescendants, noInference, disallowThirdParty, requireLocalEndpoint bool
	cmd := &cobra.Command{Use: string(effect), Short: fmt.Sprintf("Create a %s inference policy", effect), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonInferencePolicyCreate(cmd, a, effect, spaceIDText, domainRef, indexRef, nodeText, reason, operations, privacyClasses, includeDescendants, noInference, disallowThirdParty, requireLocalEndpoint)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&indexRef, "semantic-index", "", "semantic index key or ID")
	cmd.Flags().StringVar(&nodeText, "node", "", "node ID scope")
	cmd.Flags().StringArrayVar(&operations, "operation", []string{string(domainsemantic.OperationEmbeddings)}, "operation")
	cmd.Flags().StringArrayVar(&privacyClasses, "privacy-class", nil, "allowed privacy class")
	cmd.Flags().BoolVar(&includeDescendants, "include-descendants", false, "include descendant nodes in node scope")
	cmd.Flags().BoolVar(&noInference, "no-inference", false, "mark content as no-inference")
	cmd.Flags().BoolVar(&disallowThirdParty, "disallow-third-party", false, "disallow third-party endpoints")
	cmd.Flags().BoolVar(&requireLocalEndpoint, "local-only", false, "require local endpoint")
	cmd.Flags().StringVar(&reason, "reason", "", "policy reason")
	return cmd
}

func newInferencePolicyExpireCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "expire POLICY_ID", Short: "Expire an inference policy via daemon gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferencePolicyServiceClient(conn).ExpireInferencePolicy(authCtx, &adminv1.AdminInferencePolicyServiceExpireInferencePolicyRequest{SpaceId: spaceID, InferencePolicyId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("inference policy expired: %s\n", res.GetInferencePolicy().GetInferencePolicyId()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferencePolicyDeleteCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "delete POLICY_ID", Aliases: []string{"rm", "remove"}, Short: "Hard-delete an inference policy", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferencePolicyServiceClient(conn).DeleteInferencePolicy(authCtx, &adminv1.AdminInferencePolicyServiceDeleteInferencePolicyRequest{SpaceId: spaceID, InferencePolicyId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("inference policy deleted: %s\n", res.GetInferencePolicyId()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferenceDecisionCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "decision", Aliases: []string{"decisions"}, Short: "Inspect inference policy decisions"}
	cmd.AddCommand(newInferenceDecisionGetCommand(a))
	return cmd
}

func newInferenceDecisionGetCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "get POLICY_DECISION_ID", Short: "Get an inference policy decision", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferencePolicyServiceClient(conn).GetPolicyDecision(authCtx, &adminv1.AdminInferencePolicyServiceGetPolicyDecisionRequest{SpaceId: spaceID, PolicyDecisionId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("inference policy decision: %s %s\n", res.GetPolicyDecision().GetPolicyDecisionId(), res.GetPolicyDecision().GetAction().String()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferencePolicyListCommand(a *app.App) *cobra.Command {
	var spaceID, pageToken, effect string
	var pageSize int32
	var includeExpired bool
	cmd := &cobra.Command{Use: "list", Short: "List inference policies via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferencePolicyServiceClient(conn).ListInferencePolicies(authCtx, &adminv1.AdminInferencePolicyServiceListInferencePoliciesRequest{SpaceId: spaceID, PageSize: pageSize, PageToken: pageToken, Effect: effect, IncludeExpired: includeExpired})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, policy := range res.GetInferencePolicies() {
			fmt.Printf("%s\t%s\tspace=%s\toperations=%s\t%s\n", policy.GetInferencePolicyId(), policy.GetEffect(), policy.GetScope().GetSpaceId(), strings.Join(policy.GetOperations(), ","), policy.GetReason())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().StringVar(&effect, "effect", "", "effect filter")
	cmd.Flags().BoolVar(&includeExpired, "include-expired", false, "include expired policies")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func runDaemonInferenceCredentialAdd(cmd *cobra.Command, a *app.App, key, endpointRef, ownerUser, ownerType, ownerID, authType, secretValue string, secretStdin bool, name string, isDefault bool) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	value, err := secretValueFromInput(cmd, secretValue, secretStdin)
	if err != nil {
		return err
	}
	if ownerUser != "" {
		ownerType = string(domainsemantic.CredentialOwnerUser)
		ownerID = ownerUser
	}
	if ownerType == "" {
		ownerType = string(domainsemantic.CredentialOwnerUser)
	}
	req := &adminv1.AdminInferenceCredentialServiceCreateCredentialRequest{Key: key, DisplayName: firstNonEmpty(name, key), ModelEndpoint: endpointRef, OwnerType: ownerType, OwnerId: ownerID, AuthType: firstNonEmpty(authType, string(domainsemantic.AuthModeAPIKey)), SecretValue: value, IsDefault: isDefault}
	res, err := adminv1.NewAdminInferenceCredentialServiceClient(conn).CreateCredential(authCtx, req)
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("credential added: %s\n", res.GetCredential().GetCredentialId()))
}

func secretValueFromInput(cmd *cobra.Command, flagValue string, fromStdin bool) (string, error) {
	value := flagValue
	if fromStdin {
		if strings.TrimSpace(flagValue) != "" {
			return "", fmt.Errorf("use only one of --secret-value or --secret-stdin")
		}
		data, err := io.ReadAll(cmd.InOrStdin())
		if err != nil {
			return "", err
		}
		value = string(data)
	}
	value = strings.TrimRight(value, "\r\n")
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("API key is required; pass --secret-stdin or --secret-value")
	}
	return value, nil
}

func runDaemonInferenceCredentialGrant(cmd *cobra.Command, a *app.App, credentialRef, spaceIDText, domainRef, indexRef, nodeText, endpointRef, modelRef string, operations, granteePrincipalIDs, allowOnBehalfPrincipalIDs []string, allowBackgroundUse, includeDescendants, isDefault bool, priority int) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	domainID := ""
	if strings.TrimSpace(domainRef) != "" {
		domainID, err = daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
		if err != nil {
			return err
		}
	}
	req := &adminv1.AdminInferenceGrantServiceCreateCredentialGrantRequest{SpaceId: spaceID.String(), Credential: credentialRef, Scope: &adminv1.ProcessingScope{SpaceId: spaceID.String(), DomainId: domainID, SemanticIndexId: indexRef, NodeId: nodeText, IncludeDescendants: includeDescendants}, Operations: operations, ModelEndpoint: endpointRef, Model: modelRef, Priority: int32(priority), IsDefault: isDefault, AllowBackgroundUse: allowBackgroundUse, GranteePrincipalIds: granteePrincipalIDs, AllowOnBehalfOfPrincipalIds: allowOnBehalfPrincipalIDs}
	res, err := adminv1.NewAdminInferenceGrantServiceClient(conn).CreateCredentialGrant(authCtx, req)
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("credential grant added: %s\n", res.GetCredentialGrant().GetCredentialGrantId()))
}

func runDaemonInferencePolicyCreate(cmd *cobra.Command, a *app.App, effect domainsemantic.PolicyEffect, spaceIDText, domainRef, indexRef, nodeText, reason string, operations, privacyClasses []string, includeDescendants, noInference, disallowThirdParty, requireLocalEndpoint bool) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	domainID := ""
	if strings.TrimSpace(domainRef) != "" {
		domainID, err = daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
		if err != nil {
			return err
		}
	}
	res, err := adminv1.NewAdminInferencePolicyServiceClient(conn).CreateInferencePolicy(authCtx, &adminv1.AdminInferencePolicyServiceCreateInferencePolicyRequest{SpaceId: spaceID.String(), Scope: &adminv1.ProcessingScope{SpaceId: spaceID.String(), DomainId: domainID, SemanticIndexId: indexRef, NodeId: nodeText, IncludeDescendants: includeDescendants}, Effect: string(effect), Operations: operations, NoInference: noInference, AllowedPrivacyClasses: privacyClasses, DisallowThirdParty: disallowThirdParty, RequireLocalEndpoint: requireLocalEndpoint, Reason: reason})
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("inference policy added: %s\n", res.GetInferencePolicy().GetInferencePolicyId()))
}

func newInferenceUsageCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "usage", Short: "Inspect neutral inference usage telemetry"}
	cmd.AddCommand(newInferenceUsageListCommand(a), newInferenceUsageSummarizeCommand(a))
	return cmd
}

func newInferenceUsageListCommand(a *app.App) *cobra.Command {
	var f inferenceUsageFlags
	cmd := &cobra.Command{Use: "list", Short: "List inference usage events", RunE: func(cmd *cobra.Command, args []string) error {
		return runInferenceUsageList(cmd, a, f)
	}}
	bindInferenceUsageScopeFlags(cmd, &f)
	cmd.Flags().StringVar(&f.Operation, "operation", "", "operation filter: embeddings, chat, rerank, summarize, classify")
	cmd.Flags().StringVar(&f.UsageMode, "usage-mode", "", "usage mode filter: interactive, automation, background, semantic")
	cmd.Flags().StringVar(&f.Status, "status", "", "status filter: succeeded, failed, denied, canceled")
	cmd.Flags().StringVar(&f.ProfileID, "profile-id", "", "inference profile UUID filter")
	cmd.Flags().StringVar(&f.EndpointID, "endpoint-id", "", "model endpoint UUID filter")
	cmd.Flags().StringVar(&f.ModelID, "model-id", "", "model UUID filter")
	cmd.Flags().StringVar(&f.CredentialGrantID, "credential-grant-id", "", "credential grant UUID filter")
	cmd.Flags().StringVar(&f.AutomationID, "automation-id", "", "automation ID filter")
	cmd.Flags().StringVar(&f.AutomationRunID, "automation-run-id", "", "automation run ID filter")
	cmd.Flags().StringVar(&f.ActorPrincipalID, "actor-principal-id", "", "actor principal ID filter")
	cmd.Flags().StringVar(&f.OnBehalfOfPrincipalID, "on-behalf-of-principal-id", "", "on-behalf-of principal ID filter")
	cmd.Flags().Int32Var(&f.PageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&f.PageToken, "page-token", "", "page token")
	return cmd
}

func newInferenceUsageSummarizeCommand(a *app.App) *cobra.Command {
	var f inferenceUsageFlags
	cmd := &cobra.Command{Use: "summarize", Aliases: []string{"summary"}, Short: "Summarize inference usage telemetry", RunE: func(cmd *cobra.Command, args []string) error {
		return runInferenceUsageSummarize(cmd, a, f)
	}}
	bindInferenceUsageScopeFlags(cmd, &f)
	cmd.Flags().StringArrayVar(&f.GroupBy, "group-by", nil, "summary group field; repeatable")
	return cmd
}

type inferenceUsageFlags struct {
	SpaceID               string
	DomainRef             string
	SemanticIndexID       string
	NodeID                string
	IncludeDescendants    bool
	Operation             string
	UsageMode             string
	Status                string
	ProfileID             string
	EndpointID            string
	ModelID               string
	CredentialGrantID     string
	AutomationID          string
	AutomationRunID       string
	ActorPrincipalID      string
	OnBehalfOfPrincipalID string
	Since                 string
	Until                 string
	PageSize              int32
	PageToken             string
	GroupBy               []string
}

func bindInferenceUsageScopeFlags(cmd *cobra.Command, f *inferenceUsageFlags) {
	cmd.Flags().StringVar(&f.SpaceID, "space-id", "", "space ID")
	cmd.Flags().StringVar(&f.DomainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&f.SemanticIndexID, "semantic-index", "", "semantic index key or ID")
	cmd.Flags().StringVar(&f.NodeID, "node", "", "node ID scope")
	cmd.Flags().BoolVar(&f.IncludeDescendants, "include-descendants", false, "include descendant nodes in node scope")
	cmd.Flags().StringVar(&f.Since, "since", "", "include usage at or after RFC3339 timestamp")
	cmd.Flags().StringVar(&f.Until, "until", "", "include usage at or before RFC3339 timestamp")
}

func runInferenceUsageList(cmd *cobra.Command, a *app.App, f inferenceUsageFlags) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	scope, err := inferenceUsageScope(cmd, conn, authCtx, f)
	if err != nil {
		return err
	}
	op, err := optionalInferenceOperationFromFlag(f.Operation)
	if err != nil {
		return err
	}
	mode, err := inferenceUsageModeFromFlag(f.UsageMode)
	if err != nil {
		return err
	}
	status, err := inferenceUsageStatusFromFlag(f.Status)
	if err != nil {
		return err
	}
	since, err := parseOptionalTimestampFlag("--since", f.Since)
	if err != nil {
		return err
	}
	until, err := parseOptionalTimestampFlag("--until", f.Until)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminInferenceUsageServiceClient(conn).ListUsageEvents(authCtx, &adminv1.AdminInferenceUsageServiceListUsageEventsRequest{SpaceId: f.SpaceID, Scope: scope, Operation: op, UsageMode: mode, Status: status, InferenceProfileId: f.ProfileID, ModelEndpointId: f.EndpointID, ModelId: f.ModelID, CredentialGrantId: f.CredentialGrantID, AutomationId: f.AutomationID, AutomationRunId: f.AutomationRunID, SemanticIndexId: f.SemanticIndexID, ActorPrincipalId: f.ActorPrincipalID, OnBehalfOfPrincipalId: f.OnBehalfOfPrincipalID, Since: since, Until: until, PageSize: f.PageSize, PageToken: f.PageToken})
	if err != nil {
		return err
	}
	if a.Output == "json" {
		return a.Print(res, "")
	}
	for _, event := range res.GetUsageEvents() {
		fmt.Printf("%s\t%s\t%s\t%s\tspace=%s\ttokens=%d\n", event.GetUsageEventId(), event.GetOperation().String(), event.GetUsageMode().String(), event.GetStatus().String(), event.GetSpaceId(), event.GetTotalTokens())
	}
	if res.GetNextPageToken() != "" {
		fmt.Printf("next page token: %s\n", res.GetNextPageToken())
	}
	return nil
}

func runInferenceUsageSummarize(cmd *cobra.Command, a *app.App, f inferenceUsageFlags) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	scope, err := inferenceUsageScope(cmd, conn, authCtx, f)
	if err != nil {
		return err
	}
	since, err := parseOptionalTimestampFlag("--since", f.Since)
	if err != nil {
		return err
	}
	until, err := parseOptionalTimestampFlag("--until", f.Until)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminInferenceUsageServiceClient(conn).SummarizeUsage(authCtx, &adminv1.AdminInferenceUsageServiceSummarizeUsageRequest{SpaceId: f.SpaceID, Scope: scope, Since: since, Until: until, GroupBy: f.GroupBy})
	if err != nil {
		return err
	}
	if a.Output == "json" {
		return a.Print(res, "")
	}
	for _, summary := range res.GetSummaries() {
		fmt.Printf("%s\trequests=%d\tsucceeded=%d\tfailed=%d\tdenied=%d\ttokens=%d\n", formatUsageGroup(summary.GetGroup()), summary.GetRequestCount(), summary.GetSucceededCount(), summary.GetFailedCount(), summary.GetDeniedCount(), summary.GetTotalTokens())
	}
	return nil
}

func inferenceUsageScope(cmd *cobra.Command, conn *grpc.ClientConn, authCtx context.Context, f inferenceUsageFlags) (*commonv1.InferenceScope, error) {
	spaceID := strings.TrimSpace(f.SpaceID)
	domainID := ""
	if strings.TrimSpace(f.DomainRef) != "" {
		if spaceID == "" {
			return nil, fmt.Errorf("--space-id is required when --domain is set")
		}
		var err error
		domainID, err = daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID, f.DomainRef)
		if err != nil {
			return nil, err
		}
	}
	return &commonv1.InferenceScope{SpaceId: spaceID, DomainId: domainID, SemanticIndexId: f.SemanticIndexID, NodeId: f.NodeID, IncludeDescendants: f.IncludeDescendants}, nil
}

func formatUsageGroup(group map[string]string) string {
	if len(group) == 0 {
		return "all"
	}
	keys := make([]string, 0, len(group))
	for key := range group {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+group[key])
	}
	return strings.Join(parts, ",")
}

func parseOptionalTimestampFlag(name, value string) (*timestamppb.Timestamp, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("%s must be RFC3339: %w", name, err)
	}
	return timestamppb.New(parsed), nil
}

func inferenceUsageModeFromFlag(value string) (commonv1.InferenceUsageMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_UNSPECIFIED, nil
	case "interactive":
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_INTERACTIVE, nil
	case "automation":
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_AUTOMATION, nil
	case "background":
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_BACKGROUND, nil
	case "semantic":
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_SEMANTIC, nil
	default:
		return commonv1.InferenceUsageMode_INFERENCE_USAGE_MODE_UNSPECIFIED, fmt.Errorf("unsupported inference usage mode %q", value)
	}
}

func inferenceUsageStatusFromFlag(value string) (commonv1.InferenceUsageStatus, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_UNSPECIFIED, nil
	case "succeeded", "success":
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_SUCCEEDED, nil
	case "failed", "failure":
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_FAILED, nil
	case "denied":
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_DENIED, nil
	case "canceled", "cancelled":
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_CANCELED, nil
	default:
		return commonv1.InferenceUsageStatus_INFERENCE_USAGE_STATUS_UNSPECIFIED, fmt.Errorf("unsupported inference usage status %q", value)
	}
}

func newInferenceProfileCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "profile", Aliases: []string{"profiles"}, Short: "Manage space-scoped inference profiles"}
	cmd.AddCommand(newInferenceProfileCreateCommand(a), newInferenceProfileListCommand(a), newInferenceProfileGetCommand(a), newInferenceProfileSetEnabledCommand(a, true), newInferenceProfileSetEnabledCommand(a, false), newInferenceProfileDeleteCommand(a))
	return cmd
}

func newInferenceProfileCreateCommand(a *app.App) *cobra.Command {
	var spaceID, displayName, description, operation, purpose, responseFormat string
	var domains, capabilities, endpoints, models, features, privacyClasses []string
	var requireLocal, disallowThirdParty bool
	var temperature float64
	var maxInputTokens, maxOutputTokens int32
	cmd := &cobra.Command{Use: "create KEY", Short: "Create or update an inference profile", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		op, err := inferenceOperationFromFlag(operation)
		if err != nil {
			return err
		}
		privacy, err := inferencePrivacyRequirementFromFlags(privacyClasses, requireLocal, disallowThirdParty)
		if err != nil {
			return err
		}
		params := &commonv1.InferenceParameters{MaxInputTokens: maxInputTokens, MaxOutputTokens: maxOutputTokens, ResponseFormat: responseFormat}
		if cmd.Flags().Changed("temperature") {
			params.Temperature = &temperature
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		resolvedDomains, err := resolveAdminDomainRefs(cmd, conn, authCtx, spaceID, domains)
		if err != nil {
			return err
		}
		res, err := adminv1.NewAdminInferenceProfileServiceClient(conn).CreateInferenceProfile(authCtx, &adminv1.AdminInferenceProfileServiceCreateInferenceProfileRequest{SpaceId: spaceID, Key: args[0], DisplayName: displayName, Description: description, Operation: op, Purpose: purpose, DomainIds: resolvedDomains, CapabilityRefs: capabilities, EndpointRefs: endpoints, ModelRefs: models, RequiredFeatures: features, PrivacyRequirement: privacy, DefaultParameters: params, Enabled: true})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("inference profile saved: %s\n", res.GetInferenceProfile().GetKey()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	cmd.Flags().StringVar(&displayName, "display-name", "", "display name")
	cmd.Flags().StringVar(&description, "description", "", "description")
	cmd.Flags().StringVar(&operation, "operation", string(domainsemantic.OperationEmbeddings), "operation: embeddings, chat, rerank, summarize, classify")
	cmd.Flags().StringVar(&purpose, "purpose", "", "purpose label, such as automation or semantic")
	cmd.Flags().StringArrayVar(&domains, "domain", nil, "domain ID/ref allowed by this profile")
	cmd.Flags().StringArrayVar(&capabilities, "capability", nil, "capability ID/key candidate")
	cmd.Flags().StringArrayVar(&endpoints, "model-endpoint", nil, "endpoint ID/key candidate")
	cmd.Flags().StringArrayVar(&models, "model", nil, "model ID/key candidate")
	cmd.Flags().StringArrayVar(&features, "feature", nil, "required feature")
	cmd.Flags().StringArrayVar(&privacyClasses, "privacy-class", nil, "allowed privacy class: local_only, private, third_party")
	cmd.Flags().BoolVar(&requireLocal, "require-local-endpoint", false, "require a local endpoint")
	cmd.Flags().BoolVar(&disallowThirdParty, "disallow-third-party", false, "disallow third-party providers")
	cmd.Flags().Float64Var(&temperature, "temperature", 0, "default temperature")
	cmd.Flags().Int32Var(&maxInputTokens, "max-input-tokens", 0, "default max input tokens")
	cmd.Flags().Int32Var(&maxOutputTokens, "max-output-tokens", 0, "default max output tokens")
	cmd.Flags().StringVar(&responseFormat, "response-format", "", "default response format")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferenceProfileListCommand(a *app.App) *cobra.Command {
	var spaceID, pageToken, operation, domainID, purpose string
	var pageSize int32
	var includeDisabled bool
	cmd := &cobra.Command{Use: "list", Short: "List inference profiles via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		op, err := optionalInferenceOperationFromFlag(operation)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		resolvedDomainID := ""
		if strings.TrimSpace(domainID) != "" {
			resolvedDomains, err := resolveAdminDomainRefs(cmd, conn, authCtx, spaceID, []string{domainID})
			if err != nil {
				return err
			}
			if len(resolvedDomains) > 0 {
				resolvedDomainID = resolvedDomains[0]
			}
		}
		res, err := adminv1.NewAdminInferenceProfileServiceClient(conn).ListInferenceProfiles(authCtx, &adminv1.AdminInferenceProfileServiceListInferenceProfilesRequest{SpaceId: spaceID, PageSize: pageSize, PageToken: pageToken, DomainId: resolvedDomainID, Operation: op, Purpose: purpose, IncludeDisabled: includeDisabled})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, profile := range res.GetInferenceProfiles() {
			fmt.Printf("%s\t%s\t%s\tenabled=%t\n", profile.GetInferenceProfileId(), profile.GetKey(), profile.GetOperation().String(), profile.GetEnabled())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().StringVar(&operation, "operation", "", "operation filter")
	cmd.Flags().StringVar(&domainID, "domain", "", "domain ID filter")
	cmd.Flags().StringVar(&purpose, "purpose", "", "purpose filter")
	cmd.Flags().BoolVar(&includeDisabled, "include-disabled", false, "include disabled profiles")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferenceProfileGetCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "get PROFILE", Short: "Get an inference profile by key or ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceProfileServiceClient(conn).GetInferenceProfile(authCtx, &adminv1.AdminInferenceProfileServiceGetInferenceProfileRequest{SpaceId: spaceID, InferenceProfile: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("inference profile: %s\n", res.GetInferenceProfile().GetKey()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferenceProfileSetEnabledCommand(a *app.App, enabled bool) *cobra.Command {
	use := "disable PROFILE"
	if enabled {
		use = "enable PROFILE"
	}
	var spaceID string
	cmd := &cobra.Command{Use: use, Short: "Enable or disable an inference profile", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceProfileServiceClient(conn).SetInferenceProfileEnabled(authCtx, &adminv1.AdminInferenceProfileServiceSetInferenceProfileEnabledRequest{SpaceId: spaceID, InferenceProfile: args[0], Enabled: enabled})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("inference profile %s: enabled=%t\n", res.GetInferenceProfile().GetKey(), res.GetInferenceProfile().GetEnabled()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newInferenceProfileDeleteCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "delete PROFILE", Aliases: []string{"rm", "remove"}, Short: "Delete an inference profile", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceProfileServiceClient(conn).DeleteInferenceProfile(authCtx, &adminv1.AdminInferenceProfileServiceDeleteInferenceProfileRequest{SpaceId: spaceID, InferenceProfile: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("inference profile deleted: %s\n", res.GetInferenceProfileId()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func resolveAdminDomainRefs(cmd *cobra.Command, conn *grpc.ClientConn, authCtx context.Context, spaceID string, refs []string) ([]string, error) {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		resolved, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID, ref)
		if err != nil {
			return nil, err
		}
		out = append(out, resolved)
	}
	return out, nil
}

func inferenceOperationFromFlag(value string) (commonv1.InferenceOperation, error) {
	op, err := optionalInferenceOperationFromFlag(value)
	if err != nil {
		return commonv1.InferenceOperation_INFERENCE_OPERATION_UNSPECIFIED, err
	}
	if op == commonv1.InferenceOperation_INFERENCE_OPERATION_UNSPECIFIED {
		return op, fmt.Errorf("operation is required")
	}
	return op, nil
}

func optionalInferenceOperationFromFlag(value string) (commonv1.InferenceOperation, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_UNSPECIFIED, nil
	case "embedding", "embeddings":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_EMBEDDINGS, nil
	case "chat":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT, nil
	case "rerank":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_RERANK, nil
	case "summarize", "summary":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_SUMMARIZE, nil
	case "classify", "classification":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_CLASSIFY, nil
	default:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_UNSPECIFIED, fmt.Errorf("unsupported inference operation %q", value)
	}
}

func inferencePrivacyRequirementFromFlags(values []string, requireLocal, disallowThirdParty bool) (*commonv1.InferencePrivacyRequirement, error) {
	out := &commonv1.InferencePrivacyRequirement{RequireLocalEndpoint: requireLocal, DisallowThirdParty: disallowThirdParty}
	for _, value := range values {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "", "unspecified":
			continue
		case "local", "local_only", "local-only":
			out.AllowedPrivacyClasses = append(out.AllowedPrivacyClasses, commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_LOCAL_ONLY)
		case "private", "enterprise_private", "enterprise-private":
			out.AllowedPrivacyClasses = append(out.AllowedPrivacyClasses, commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_PRIVATE)
		case "third_party", "third-party", "public":
			out.AllowedPrivacyClasses = append(out.AllowedPrivacyClasses, commonv1.InferencePrivacyClass_INFERENCE_PRIVACY_CLASS_THIRD_PARTY)
		default:
			return nil, fmt.Errorf("unsupported inference privacy class %q", value)
		}
	}
	return out, nil
}

func adminModelEndpointFromDomain(endpoint domainsemantic.ModelEndpoint) *adminv1.ModelEndpoint {
	id := ""
	if endpoint.ID != [16]byte{} {
		id = endpoint.ID.String()
	}
	return &adminv1.ModelEndpoint{ModelEndpointId: id, Key: endpoint.Key, Name: endpoint.Name, ConnectorType: string(endpoint.ConnectorType), EndpointUrl: endpoint.EndpointURL, NetworkClass: string(endpoint.NetworkClass), PrivacyClass: string(endpoint.PrivacyClass), AuthModes: stringsFromSemanticAuthModes(endpoint.AuthModes), Operations: stringsFromSemanticOperations(endpoint.Operations), Enabled: endpoint.Enabled, Metadata: cliProtoStruct(endpoint.Metadata)}
}

func adminInferenceModelFromDomain(model domainsemantic.InferenceModel) *adminv1.InferenceModel {
	id := ""
	if model.ID != [16]byte{} {
		id = model.ID.String()
	}
	return &adminv1.InferenceModel{ModelId: id, Key: model.Key, Operation: string(model.Operation), ModelName: model.ModelName, ConnectorTypes: stringsFromSemanticConnectorTypes(model.ConnectorTypes), Dimensions: int32(model.Dimensions), Modality: model.Modality, VectorSpaceKey: model.VectorSpaceKey, Metadata: cliProtoStruct(model.Metadata)}
}

func adminVectorStoreFromDomain(store domainsemantic.VectorStoreBackend) *adminv1.VectorStore {
	id := ""
	if store.ID != [16]byte{} {
		id = store.ID.String()
	}
	return &adminv1.VectorStore{VectorStoreId: id, Key: store.Key, Name: store.Name, Type: string(store.Type), PrivacyClass: string(store.PrivacyClass), Enabled: store.Enabled, Config: cliProtoStruct(store.Config)}
}

func adminCapabilityDefinitionFromCLI(def capabilityDefinition) *adminv1.ModelEndpointCapabilityDefinition {
	enabled := true
	if def.Enabled != nil {
		enabled = *def.Enabled
	}
	return &adminv1.ModelEndpointCapabilityDefinition{ModelEndpoint: def.ModelEndpoint, ModelEndpointId: def.ModelEndpointID, Model: def.Model, ModelId: def.ModelID, Operation: string(def.Operation), Enabled: &enabled, ModelNameOverride: def.ModelNameOverride, Metadata: cliProtoStruct(def.Metadata)}
}

func cliProtoStruct(value map[string]any) *structpb.Struct {
	if value == nil {
		value = map[string]any{}
	}
	out, err := structpb.NewStruct(value)
	if err != nil {
		return &structpb.Struct{}
	}
	return out
}

func stringsFromSemanticAuthModes(values []domainsemantic.AuthMode) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}

func stringsFromSemanticOperations(values []domainsemantic.Operation) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}

func stringsFromSemanticConnectorTypes(values []domainsemantic.ConnectorType) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}

func unmarshalYAMLWithJSONTags(raw []byte, out any) error {
	var generic any
	if err := yaml.Unmarshal(raw, &generic); err != nil {
		return err
	}
	normalized := normalizeYAMLValue(generic)
	asJSON, err := json.Marshal(normalized)
	if err != nil {
		return err
	}
	return json.Unmarshal(asJSON, out)
}

func normalizeYAMLValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = normalizeYAMLValue(value)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[fmt.Sprint(key)] = normalizeYAMLValue(value)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, value := range typed {
			out[i] = normalizeYAMLValue(value)
		}
		return out
	default:
		return value
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
