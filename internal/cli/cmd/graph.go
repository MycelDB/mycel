package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func NewGraphCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "graph", Short: "Manage graph nodes and edges through daemon transactions"}
	cmd.AddCommand(newGraphNodeCommand(a), newGraphBlobNodeCommand(a), newGraphEdgeCommand(a), newGraphChildrenCommand(a), newGraphParentCommand(a))
	return cmd
}

func newGraphNodeCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "node", Short: "Manage graph nodes"}
	cmd.AddCommand(newGraphNodeCreateCommand(a), newGraphNodeGetCommand(a), newGraphNodeListCommand(a), newGraphNodeUpdateCommand(a), newGraphNodeDeleteCommand(a))
	return cmd
}

func newGraphBlobNodeCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "blob-node", Short: "Manage blob-backed graph nodes"}
	cmd.AddCommand(newGraphBlobNodeCreateCommand(a))
	return cmd
}

func newGraphBlobNodeCreateCommand(a *app.App) *cobra.Command {
	var transactionID, nodeID, templateID, propsJSON, propertiesJSON, payloadJSON, metaJSON, declaredMimeType, originalFilename string
	var labels []string
	cmd := &cobra.Command{Use: "create FILE", Short: "Create a blob-backed node in a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		properties, err := protoNodeMap(propertiesJSON, propsJSON)
		if err != nil {
			return err
		}
		payload, err := protoNodeMap(payloadJSON, "")
		if err != nil {
			return err
		}
		meta, err := protoNodeMap(metaJSON, "")
		if err != nil {
			return err
		}
		file, err := os.Open(args[0])
		if err != nil {
			return err
		}
		defer file.Close()
		if originalFilename == "" {
			originalFilename = filepath.Base(args[0])
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		stream, err := clientv1.NewGraphServiceClient(conn).CreateBlobNode(authCtx)
		if err != nil {
			return err
		}
		metadata := &clientv1.CreateBlobNodeMetadata{TransactionId: transactionID, DeclaredMimeType: declaredMimeType, OriginalFilename: originalFilename, Labels: labels, Properties: properties, Payload: payload, Meta: meta}
		if nodeID != "" {
			metadata.NodeId = &nodeID
		}
		if templateID != "" {
			metadata.TemplateId = &templateID
		}
		if err := stream.Send(&clientv1.CreateBlobNodeRequest{Part: &clientv1.CreateBlobNodeRequest_Metadata{Metadata: metadata}}); err != nil {
			return err
		}
		buf := make([]byte, blobUploadChunkSize)
		for {
			n, readErr := file.Read(buf)
			if n > 0 {
				chunk := append([]byte(nil), buf[:n]...)
				if err := stream.Send(&clientv1.CreateBlobNodeRequest{Part: &clientv1.CreateBlobNodeRequest_Chunk{Chunk: chunk}}); err != nil {
					return err
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				return readErr
			}
		}
		res, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("blob node created: %s (blob %s)\n", res.GetNode().GetNodeId(), res.GetBlob().GetBlobId()))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "optional node ID")
	cmd.Flags().StringVar(&templateID, "template-id", "", "optional template ID")
	cmd.Flags().StringArrayVar(&labels, "label", nil, "node label; repeatable")
	cmd.Flags().StringVar(&propertiesJSON, "properties-json", "", "node properties as JSON object")
	cmd.Flags().StringVar(&payloadJSON, "payload-json", "", "node payload as JSON object")
	cmd.Flags().StringVar(&metaJSON, "meta-json", "", "node metadata as JSON object")
	cmd.Flags().StringVar(&propsJSON, "props-json", "", "deprecated alias for --properties-json")
	cmd.Flags().StringVar(&declaredMimeType, "mime-type", "", "declared MIME type")
	cmd.Flags().StringVar(&originalFilename, "filename", "", "original filename metadata")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphNodeCreateCommand(a *app.App) *cobra.Command {
	var transactionID, nodeID, templateID, content, propsJSON, propertiesJSON, payloadJSON, metaJSON string
	var labels []string
	cmd := &cobra.Command{Use: "create", Short: "Create a node in a daemon graph transaction", RunE: func(cmd *cobra.Command, args []string) error {
		properties, payload, meta, err := parseNodeShape(propertiesJSON, propsJSON, payloadJSON, metaJSON, content)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &clientv1.CreateNodeRequest{TransactionId: transactionID, Node: &clientv1.NodeCreate{Labels: labels, Payload: payload, Properties: properties, Meta: meta}}
		if nodeID != "" {
			req.Node.NodeId = &nodeID
		}
		if templateID != "" {
			req.Node.TemplateId = &templateID
		}
		res, err := clientv1.NewGraphServiceClient(conn).CreateNode(authCtx, req)
		if err != nil {
			return err
		}
		return a.Print(res.GetNode(), fmt.Sprintf("node created: %s\n", res.GetNode().GetNodeId()))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "optional node ID")
	cmd.Flags().StringVar(&templateID, "template-id", "", "optional template ID")
	cmd.Flags().StringArrayVar(&labels, "label", nil, "node label; repeatable")
	cmd.Flags().StringVar(&content, "content", "", "node text payload; deprecated alias for --payload-json")
	cmd.Flags().StringVar(&propertiesJSON, "properties-json", "", "node properties as JSON object")
	cmd.Flags().StringVar(&payloadJSON, "payload-json", "", "node payload as JSON object")
	cmd.Flags().StringVar(&metaJSON, "meta-json", "", "node metadata as JSON object")
	cmd.Flags().StringVar(&propsJSON, "props-json", "", "deprecated alias for --properties-json")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphNodeGetCommand(a *app.App) *cobra.Command {
	var transactionID string
	cmd := &cobra.Command{Use: "get NODE_ID", Short: "Get a node in a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewGraphServiceClient(conn).GetNode(authCtx, &clientv1.GetNodeRequest{TransactionId: transactionID, NodeId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetNode(), fmt.Sprintf("%s\t%s\n", res.GetNode().GetNodeId(), previewText(nodePayloadText(res.GetNode()), 120)))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphNodeListCommand(a *app.App) *cobra.Command {
	var transactionID, pageToken string
	var pageSize int32
	cmd := &cobra.Command{Use: "list", Short: "List nodes in a daemon graph transaction", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewGraphServiceClient(conn).ListNodes(authCtx, &clientv1.ListNodesRequest{TransactionId: transactionID, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, node := range res.GetNodes() {
			fmt.Printf("%s\t%s\n", node.GetNodeId(), previewText(nodePayloadText(node), 120))
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphNodeUpdateCommand(a *app.App) *cobra.Command {
	var transactionID, content, propsJSON, propertiesJSON, payloadJSON, metaJSON, templateID, mask string
	var labels []string
	cmd := &cobra.Command{Use: "update NODE_ID", Short: "Update a node in a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		properties, payload, meta, err := parseNodeShape(propertiesJSON, propsJSON, payloadJSON, metaJSON, content)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		node := &clientv1.Node{NodeId: args[0], Labels: labels, Payload: payload, Properties: properties, Meta: meta}
		if templateID != "" {
			node.TemplateId = &templateID
		}
		req := &clientv1.UpdateNodeRequest{TransactionId: transactionID, Node: node, UpdateMask: parseFieldMask(mask)}
		res, err := clientv1.NewGraphServiceClient(conn).UpdateNode(authCtx, req)
		if err != nil {
			return err
		}
		return a.Print(res.GetNode(), fmt.Sprintf("node updated: %s\n", res.GetNode().GetNodeId()))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().StringArrayVar(&labels, "label", nil, "node label; repeatable")
	cmd.Flags().StringVar(&content, "content", "", "node text payload; deprecated alias for --payload-json")
	cmd.Flags().StringVar(&propertiesJSON, "properties-json", "", "node properties as JSON object")
	cmd.Flags().StringVar(&payloadJSON, "payload-json", "", "node payload as JSON object")
	cmd.Flags().StringVar(&metaJSON, "meta-json", "", "node metadata as JSON object")
	cmd.Flags().StringVar(&propsJSON, "props-json", "", "deprecated alias for --properties-json")
	cmd.Flags().StringVar(&templateID, "template-id", "", "template ID")
	cmd.Flags().StringVar(&mask, "mask", "", "comma-separated update mask paths, e.g. payload,properties,labels,meta")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphNodeDeleteCommand(a *app.App) *cobra.Command {
	var transactionID string
	var recursive bool
	cmd := &cobra.Command{Use: "delete NODE_ID", Short: "Delete a node in a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewGraphServiceClient(conn).DeleteNode(authCtx, &clientv1.DeleteNodeRequest{TransactionId: transactionID, NodeId: args[0], Recursive: recursive})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("node deleted: %s\n", args[0]))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().BoolVar(&recursive, "recursive", false, "delete descendants")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphEdgeCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "edge", Short: "Manage graph edges"}
	cmd.AddCommand(newGraphEdgeCreateCommand(a), newGraphEdgeGetCommand(a), newGraphEdgeListCommand(a), newGraphEdgeDeleteCommand(a))
	return cmd
}

func newGraphEdgeCreateCommand(a *app.App) *cobra.Command {
	var transactionID, edgeID, from, to, kind, propsJSON string
	cmd := &cobra.Command{Use: "create", Short: "Create an edge in a daemon graph transaction", RunE: func(cmd *cobra.Command, args []string) error {
		props, err := protoProps(propsJSON)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		edge := &clientv1.EdgeCreate{FromNodeId: from, ToNodeId: to, Kind: kind, Props: props}
		if edgeID != "" {
			edge.EdgeId = &edgeID
		}
		res, err := clientv1.NewGraphServiceClient(conn).CreateEdge(authCtx, &clientv1.CreateEdgeRequest{TransactionId: transactionID, Edge: edge})
		if err != nil {
			return err
		}
		return a.Print(res.GetEdge(), fmt.Sprintf("edge created: %s\n", res.GetEdge().GetEdgeId()))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().StringVar(&edgeID, "edge-id", "", "optional edge ID")
	cmd.Flags().StringVar(&from, "from", "", "from node ID")
	cmd.Flags().StringVar(&to, "to", "", "to node ID")
	cmd.Flags().StringVar(&kind, "kind", "contains", "edge kind")
	cmd.Flags().StringVar(&propsJSON, "props-json", "", "edge properties as JSON object")
	_ = cmd.MarkFlagRequired("transaction-id")
	_ = cmd.MarkFlagRequired("from")
	_ = cmd.MarkFlagRequired("to")
	return cmd
}

func newGraphEdgeGetCommand(a *app.App) *cobra.Command {
	var transactionID string
	cmd := &cobra.Command{Use: "get EDGE_ID", Short: "Get an edge", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewGraphServiceClient(conn).GetEdge(authCtx, &clientv1.GetEdgeRequest{TransactionId: transactionID, EdgeId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetEdge(), fmt.Sprintf("%s\t%s -> %s\t%s\n", res.GetEdge().GetEdgeId(), res.GetEdge().GetFromNodeId(), res.GetEdge().GetToNodeId(), res.GetEdge().GetKind()))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphEdgeListCommand(a *app.App) *cobra.Command {
	var transactionID, pageToken string
	var pageSize int32
	cmd := &cobra.Command{Use: "list", Short: "List edges", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewGraphServiceClient(conn).ListEdges(authCtx, &clientv1.ListEdgesRequest{TransactionId: transactionID, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, edge := range res.GetEdges() {
			fmt.Printf("%s\t%s -> %s\t%s\n", edge.GetEdgeId(), edge.GetFromNodeId(), edge.GetToNodeId(), edge.GetKind())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphEdgeDeleteCommand(a *app.App) *cobra.Command {
	var transactionID string
	cmd := &cobra.Command{Use: "delete EDGE_ID", Short: "Delete an edge", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewGraphServiceClient(conn).DeleteEdge(authCtx, &clientv1.DeleteEdgeRequest{TransactionId: transactionID, EdgeId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("edge deleted: %s\n", res.GetDeletedEdgeId()))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphChildrenCommand(a *app.App) *cobra.Command {
	var transactionID string
	cmd := &cobra.Command{Use: "children PARENT_NODE_ID", Short: "List contains edges for a parent", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewGraphServiceClient(conn).ListChildren(authCtx, &clientv1.ListChildrenRequest{TransactionId: transactionID, ParentNodeId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("children: %d\n", len(res.GetContainsEdges())))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func newGraphParentCommand(a *app.App) *cobra.Command {
	var transactionID string
	cmd := &cobra.Command{Use: "parent CHILD_NODE_ID", Short: "Get contains parent edge for a child", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewGraphServiceClient(conn).GetParent(authCtx, &clientv1.GetParentRequest{TransactionId: transactionID, ChildNodeId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("parent edge: %s\n", res.GetContainsEdge().GetEdgeId()))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "transaction ID")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func nodePayloadText(node *clientv1.Node) string {
	if node == nil || node.GetPayload() == nil {
		return ""
	}
	text, _ := node.GetPayload().AsMap()["text"].(string)
	return text
}

func protoStruct(value map[string]any) *structpb.Struct {
	out, err := structpb.NewStruct(value)
	if err != nil {
		return nil
	}
	return out
}

func parseNodeShape(propertiesJSON, propsJSON, payloadJSON, metaJSON, content string) (*structpb.Struct, *structpb.Struct, *structpb.Struct, error) {
	properties, err := protoNodeMap(propertiesJSON, propsJSON)
	if err != nil {
		return nil, nil, nil, err
	}
	payloadMap, err := app.ParseProps(payloadJSON)
	if err != nil {
		return nil, nil, nil, err
	}
	if payloadMap == nil {
		payloadMap = map[string]any{}
	}
	if content != "" {
		payloadMap["text"] = content
	}
	payload, err := structpb.NewStruct(payloadMap)
	if err != nil {
		return nil, nil, nil, err
	}
	meta, err := protoNodeMap(metaJSON, "")
	if err != nil {
		return nil, nil, nil, err
	}
	return properties, payload, meta, nil
}

func protoNodeMap(raw, legacyRaw string) (*structpb.Struct, error) {
	if strings.TrimSpace(raw) == "" {
		raw = legacyRaw
	}
	return protoProps(raw)
}

func protoProps(raw string) (*structpb.Struct, error) {
	props, err := app.ParseProps(raw)
	if err != nil {
		return nil, err
	}
	if props == nil {
		props = map[string]any{}
	}
	return structpb.NewStruct(props)
}

func parseFieldMask(raw string) *fieldmaskpb.FieldMask {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			paths = append(paths, part)
		}
	}
	return &fieldmaskpb.FieldMask{Paths: paths}
}
