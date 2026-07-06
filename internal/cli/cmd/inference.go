package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"
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
	cmd := &cobra.Command{Use: "inference", Short: "Provision inference endpoints, models, credentials, grants, and policies"}
	cmd.AddCommand(newInferencePackageCommand(a), newInferenceModelEndpointCommand(a), newInferenceModelCommand(a), newInferenceVectorStoreCommand(a), newInferenceCapabilityCommand(a), newInferenceCredentialCommand(a), newInferencePolicyCommand(a))
	return cmd
}

func newInferencePackageCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "package", Short: "Manage inference definition packages"}
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
	req := &adminv1.AdminInferenceServiceApplyInferencePackageRequest{Name: doc.Name, Version: doc.Version, Source: firstNonEmpty(doc.Source, filePath), Checksum: doc.Checksum}
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
	res, err := adminv1.NewAdminInferenceServiceClient(conn).ApplyInferencePackage(authCtx, req)
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ListInferencePackages(authCtx, &adminv1.AdminInferenceServiceListInferencePackagesRequest{PageSize: pageSize, PageToken: pageToken})
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
	cmd := &cobra.Command{Use: "capability", Short: "Manage model endpoint capabilities"}
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ListModelEndpointCapabilities(authCtx, &adminv1.AdminInferenceServiceListModelEndpointCapabilitiesRequest{PageSize: pageSize, PageToken: pageToken, Operation: operation, IncludeDisabled: includeDisabled})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).SetModelEndpointCapabilityEnabled(authCtx, &adminv1.AdminInferenceServiceSetModelEndpointCapabilityEnabledRequest{ModelEndpointCapabilityId: args[0], Enabled: enabled})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).DeleteModelEndpointCapability(authCtx, &adminv1.AdminInferenceServiceDeleteModelEndpointCapabilityRequest{ModelEndpointCapabilityId: args[0]})
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
	cmd := &cobra.Command{Use: "model-endpoint", Aliases: []string{"endpoint"}, Short: "Manage model endpoints"}
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ListModelEndpoints(authCtx, &adminv1.AdminInferenceServiceListModelEndpointsRequest{PageSize: pageSize, PageToken: pageToken, IncludeDisabled: includeDisabled})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).SetModelEndpointEnabled(authCtx, &adminv1.AdminInferenceServiceSetModelEndpointEnabledRequest{ModelEndpoint: args[0], Enabled: enabled})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).DeleteModelEndpoint(authCtx, &adminv1.AdminInferenceServiceDeleteModelEndpointRequest{ModelEndpoint: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("model endpoint deleted: %s\n", res.GetModelEndpointId()))
	}}
}

func newInferenceModelCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "model", Short: "Manage inference models"}
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ListModels(authCtx, &adminv1.AdminInferenceServiceListModelsRequest{PageSize: pageSize, PageToken: pageToken, Operation: operation})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).DeleteModel(authCtx, &adminv1.AdminInferenceServiceDeleteModelRequest{Model: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("model deleted: %s\n", res.GetModelId()))
	}}
}

func newInferenceVectorStoreCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "vector-store", Short: "Manage vector stores"}
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ListVectorStores(authCtx, &adminv1.AdminInferenceServiceListVectorStoresRequest{PageSize: pageSize, PageToken: pageToken, IncludeDisabled: includeDisabled})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).SetVectorStoreEnabled(authCtx, &adminv1.AdminInferenceServiceSetVectorStoreEnabledRequest{VectorStore: args[0], Enabled: enabled})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).DeleteVectorStore(authCtx, &adminv1.AdminInferenceServiceDeleteVectorStoreRequest{VectorStore: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("vector store deleted: %s\n", res.GetVectorStoreId()))
	}}
}

func newInferenceCredentialCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "credential", Short: "Manage inference credentials and grants"}
	cmd.AddCommand(newInferenceCredentialAddCommand(a), newInferenceCredentialListCommand(a), newInferenceCredentialSetStatusCommand(a, domainsemantic.CredentialStatusActive), newInferenceCredentialSetStatusCommand(a, domainsemantic.CredentialStatusDisabled), newInferenceCredentialSetStatusCommand(a, domainsemantic.CredentialStatusRevoked), newInferenceCredentialDeleteCommand(a), newInferenceCredentialGrantCommand(a))
	return cmd
}

func newInferenceCredentialSetStatusCommand(a *app.App, status domainsemantic.CredentialStatus) *cobra.Command {
	return &cobra.Command{Use: string(status) + " CREDENTIAL", Short: "Set credential lifecycle status via daemon gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminInferenceServiceClient(conn).SetCredentialStatus(authCtx, &adminv1.AdminInferenceServiceSetCredentialStatusRequest{Credential: args[0], Status: string(status)})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).DeleteCredential(authCtx, &adminv1.AdminInferenceServiceDeleteCredentialRequest{Credential: args[0], DeleteGrants: deleteGrants, DeleteSecret: deleteSecret})
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
	var endpointRef, ownerUser, ownerType, ownerID, authType, apiKey, apiKeyEnv, externalRef, name string
	var isDefault bool
	cmd := &cobra.Command{Use: "add KEY", Short: "Add an inference credential", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonInferenceCredentialAdd(cmd, a, args[0], endpointRef, ownerUser, ownerType, ownerID, authType, apiKey, apiKeyEnv, externalRef, name, isDefault)
	}}
	cmd.Flags().StringVar(&endpointRef, "model-endpoint", "", "model endpoint key or ID")
	cmd.Flags().StringVar(&ownerUser, "owner-user", "", "user ref/ID that owns the credential")
	cmd.Flags().StringVar(&ownerType, "owner-type", string(domainsemantic.CredentialOwnerUser), "owner type: user, space, organization, system")
	cmd.Flags().StringVar(&ownerID, "owner-id", "", "owner ID/ref")
	cmd.Flags().StringVar(&authType, "auth", string(domainsemantic.AuthModeAPIKey), "credential auth type")
	cmd.Flags().StringVar(&apiKey, "api-key", "", "secret value (prefer --api-key-env)")
	cmd.Flags().StringVar(&apiKeyEnv, "api-key-env", "", "environment variable containing the secret value")
	cmd.Flags().StringVar(&externalRef, "external-ref", "", "external secret reference")
	cmd.Flags().StringVar(&name, "name", "", "credential display name")
	cmd.Flags().BoolVar(&isDefault, "default", false, "mark credential as default metadata")
	_ = cmd.MarkFlagRequired("model-endpoint")
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
		req := &adminv1.AdminInferenceServiceListCredentialsRequest{PageSize: pageSize, PageToken: pageToken, OwnerType: ownerType, OwnerId: ownerID, IncludeInactive: includeInactive}
		if strings.TrimSpace(endpointID) != "" {
			req.ModelEndpointId = &endpointID
		}
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ListCredentials(authCtx, req)
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

func newInferenceCredentialGrantCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, indexRef, nodeText, endpointRef, modelRef string
	var operations []string
	var allowBackgroundUse, includeDescendants, isDefault bool
	var priority int
	cmd := &cobra.Command{Use: "grant CREDENTIAL", Short: "Grant a credential for a space-owned processing scope", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonInferenceCredentialGrant(cmd, a, args[0], spaceIDText, domainRef, indexRef, nodeText, endpointRef, modelRef, operations, allowBackgroundUse, includeDescendants, isDefault, priority)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&indexRef, "semantic-index", "", "semantic index key or ID")
	cmd.Flags().StringVar(&nodeText, "node", "", "node ID scope")
	cmd.Flags().StringVar(&endpointRef, "model-endpoint", "", "optional model endpoint constraint")
	cmd.Flags().StringVar(&modelRef, "model", "", "optional model constraint")
	cmd.Flags().StringArrayVar(&operations, "operation", []string{string(domainsemantic.OperationEmbeddings)}, "operation allowed by grant")
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ExpireCredentialGrant(authCtx, &adminv1.AdminInferenceServiceExpireCredentialGrantRequest{SpaceId: spaceID, CredentialGrantId: args[0]})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).DeleteCredentialGrant(authCtx, &adminv1.AdminInferenceServiceDeleteCredentialGrantRequest{SpaceId: spaceID, CredentialGrantId: args[0]})
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
		req := &adminv1.AdminInferenceServiceListCredentialGrantsRequest{SpaceId: spaceID, PageSize: pageSize, PageToken: pageToken, IncludeExpired: includeExpired}
		if strings.TrimSpace(credentialID) != "" {
			req.CredentialId = &credentialID
		}
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ListCredentialGrants(authCtx, req)
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
	cmd := &cobra.Command{Use: "policy", Short: "Manage inference content policies"}
	cmd.AddCommand(newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectAllow), newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectDeny), newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectRestrict), newInferencePolicyListCommand(a), newInferencePolicyExpireCommand(a), newInferencePolicyDeleteCommand(a))
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ExpireInferencePolicy(authCtx, &adminv1.AdminInferenceServiceExpireInferencePolicyRequest{SpaceId: spaceID, InferencePolicyId: args[0]})
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).DeleteInferencePolicy(authCtx, &adminv1.AdminInferenceServiceDeleteInferencePolicyRequest{SpaceId: spaceID, InferencePolicyId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("inference policy deleted: %s\n", res.GetInferencePolicyId()))
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
		res, err := adminv1.NewAdminInferenceServiceClient(conn).ListInferencePolicies(authCtx, &adminv1.AdminInferenceServiceListInferencePoliciesRequest{SpaceId: spaceID, PageSize: pageSize, PageToken: pageToken, Effect: effect, IncludeExpired: includeExpired})
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

func runDaemonInferenceCredentialAdd(cmd *cobra.Command, a *app.App, key, endpointRef, ownerUser, ownerType, ownerID, authType, apiKey, apiKeyEnv, externalRef, name string, isDefault bool) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	if apiKey == "" && apiKeyEnv != "" {
		apiKey = os.Getenv(apiKeyEnv)
	}
	if ownerUser != "" {
		ownerType = string(domainsemantic.CredentialOwnerUser)
		ownerID = ownerUser
	}
	if ownerType == "" {
		ownerType = string(domainsemantic.CredentialOwnerUser)
	}
	req := &adminv1.AdminInferenceServiceCreateCredentialRequest{Key: key, DisplayName: firstNonEmpty(name, key), ModelEndpoint: endpointRef, OwnerType: ownerType, OwnerId: ownerID, AuthType: firstNonEmpty(authType, string(domainsemantic.AuthModeAPIKey)), IsDefault: isDefault}
	if externalRef != "" {
		req.SecretMaterial = &adminv1.AdminInferenceServiceCreateCredentialRequest_ExternalRef{ExternalRef: externalRef}
	} else {
		req.SecretMaterial = &adminv1.AdminInferenceServiceCreateCredentialRequest_SecretValue{SecretValue: apiKey}
	}
	res, err := adminv1.NewAdminInferenceServiceClient(conn).CreateCredential(authCtx, req)
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("credential added: %s\n", res.GetCredential().GetCredentialId()))
}

func runDaemonInferenceCredentialGrant(cmd *cobra.Command, a *app.App, credentialRef, spaceIDText, domainRef, indexRef, nodeText, endpointRef, modelRef string, operations []string, allowBackgroundUse, includeDescendants, isDefault bool, priority int) error {
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
	req := &adminv1.AdminInferenceServiceCreateCredentialGrantRequest{SpaceId: spaceID.String(), Credential: credentialRef, Scope: &adminv1.ProcessingScope{SpaceId: spaceID.String(), DomainId: domainID, SemanticIndexId: indexRef, NodeId: nodeText, IncludeDescendants: includeDescendants}, Operations: operations, ModelEndpoint: endpointRef, Model: modelRef, Priority: int32(priority), IsDefault: isDefault, AllowBackgroundUse: allowBackgroundUse}
	res, err := adminv1.NewAdminInferenceServiceClient(conn).CreateCredentialGrant(authCtx, req)
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
	res, err := adminv1.NewAdminInferenceServiceClient(conn).CreateInferencePolicy(authCtx, &adminv1.AdminInferenceServiceCreateInferencePolicyRequest{SpaceId: spaceID.String(), Scope: &adminv1.ProcessingScope{SpaceId: spaceID.String(), DomainId: domainID, SemanticIndexId: indexRef, NodeId: nodeText, IncludeDescendants: includeDescendants}, Effect: string(effect), Operations: operations, NoInference: noInference, AllowedPrivacyClasses: privacyClasses, DisallowThirdParty: disallowThirdParty, RequireLocalEndpoint: requireLocalEndpoint, Reason: reason})
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("inference policy added: %s\n", res.GetInferencePolicy().GetInferencePolicyId()))
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
