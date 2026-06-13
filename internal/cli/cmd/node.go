package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	knotengine "martinbeauvais.com/mbgit/knotbase/knotdb/engine"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/cli/app"
	domainsession "martinbeauvais.com/mbgit/knotbase/knotdb/session"
)

func NewAddNodeCommand(a *app.App) *cobra.Command {
	var spaceIDText, content, propsJSON, templateIDText, parentIDText string
	var order int
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Add a node",
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			props, err := app.ParseProps(propsJSON)
			if err != nil {
				return err
			}
			in := domainsession.AddNodeInput{Content: content, Props: props}
			if templateIDText != "" {
				id, err := app.ParseUUID[graph.TemplateID](templateIDText)
				if err != nil {
					return err
				}
				in.TemplateID = &id
			}
			var parentID *graph.NodeID
			if parentIDText != "" {
				id, err := app.ParseUUID[graph.NodeID](parentIDText)
				if err != nil {
					return err
				}
				parentID = &id
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			sess, err := a.Engine.OpenSession(cmd.Context(), knotengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			defer sess.Close()
			node, err := sess.AddNode(cmd.Context(), in)
			if err != nil {
				return err
			}
			if parentID != nil {
				if _, err := sess.AddEdge(cmd.Context(), domainsession.AddEdgeInput{FromID: *parentID, ToID: node.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": order}}); err != nil {
					return err
				}
			}
			return a.Print(node, fmt.Sprintf("node added: %s\n", node.ID))
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&content, "content", "", "node content")
	cmd.Flags().StringVar(&propsJSON, "props-json", "", "node properties as JSON object")
	cmd.Flags().StringVar(&templateIDText, "template-id", "", "template ID")
	cmd.Flags().StringVar(&parentIDText, "parent-id", "", "parent node ID for a contains edge")
	cmd.Flags().IntVar(&order, "order", 0, "contains edge sibling order when --parent-id is set")
	return cmd
}

func NewGetNodeCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{
		Use:   "get NODE_ID",
		Short: "Get a node by ID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			nodeID, err := app.ParseUUID[graph.NodeID](args[0])
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			sess, err := a.Engine.OpenSession(cmd.Context(), knotengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			defer sess.Close()
			node, err := sess.GetNode(cmd.Context(), nodeID)
			if err != nil {
				return err
			}
			return a.Print(node, fmt.Sprintf("%s\t%s\n", node.ID, previewText(node.Content, 120)))
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	return cmd
}

func NewListNodesCommand(a *app.App) *cobra.Command {
	var spaceIDText, contains string
	var limit int
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List nodes in a space",
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			sess, err := a.Engine.OpenSession(cmd.Context(), knotengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			defer sess.Close()
			nodes, err := sess.ListNodes(cmd.Context())
			if err != nil {
				return err
			}
			needle := strings.ToLower(strings.TrimSpace(contains))
			filtered := make([]graph.Node, 0, len(nodes))
			for _, node := range nodes {
				if needle != "" && !strings.Contains(strings.ToLower(node.Content), needle) {
					continue
				}
				filtered = append(filtered, node)
				if limit > 0 && len(filtered) >= limit {
					break
				}
			}
			if a.Output == "json" {
				return a.Print(filtered, "")
			}
			app.RenderNodesTable(filtered)
			return nil
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&contains, "contains", "", "case-insensitive content substring filter")
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum nodes to display (0 for all)")
	return cmd
}

func NewDeleteNodeCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	var recursive bool
	cmd := &cobra.Command{
		Use:   "node NODE_ID",
		Short: "Hard-delete a node and incident edges; descendants require --recursive",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			nodeID, err := app.ParseUUID[graph.NodeID](args[0])
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			sess, err := a.Engine.OpenSession(cmd.Context(), knotengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			defer sess.Close()
			if err := sess.DeleteNode(cmd.Context(), domainsession.DeleteNodeInput{ID: nodeID, Recursive: recursive}); err != nil {
				return err
			}
			return a.Print(map[string]any{"deleted_node_id": nodeID, "space_id": spaceID}, fmt.Sprintf("node deleted: %s\n", nodeID))
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().BoolVar(&recursive, "recursive", false, "also delete descendant nodes")
	return cmd
}

func previewText(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if limit <= 0 || len([]rune(value)) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit]) + "…"
}
