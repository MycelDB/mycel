package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/myceldb/mycel/domain/graph"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	domainsession "github.com/myceldb/mycel/session"
	"github.com/spf13/cobra"
)

func NewAddBlobCommand(a *app.App) *cobra.Command {
	var spaceIDText, caption, propsJSON, templateIDText, parentIDText, mimeType string
	var order int
	cmd := &cobra.Command{
		Use:   "blob FILE",
		Short: "Add a blob node from a file (image, PDF, ...)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			props, err := app.ParseProps(propsJSON)
			if err != nil {
				return err
			}
			file, err := os.Open(args[0])
			if err != nil {
				return err
			}
			defer file.Close()
			if caption != "" {
				if props == nil {
					props = map[string]any{}
				}
				props["caption"] = caption
			}
			in := domainsession.AddBlobNodeInput{
				Reader:           file,
				DeclaredMimeType: mimeType,
				OriginalFilename: filepath.Base(args[0]),
				Props:            props,
			}
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
			sess, err := a.Engine.OpenSession(cmd.Context(), mycelengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			defer sess.Close()
			node, err := sess.AddBlobNode(cmd.Context(), in)
			if err != nil {
				return err
			}
			if parentID != nil {
				if _, err := sess.AddEdge(cmd.Context(), domainsession.AddEdgeInput{FromID: *parentID, ToID: node.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": order}}); err != nil {
					return err
				}
			}
			return a.Print(node, fmt.Sprintf("blob node added: %s (blob %s)\n", node.ID, *node.BlobRef))
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&caption, "caption", "", "optional caption text (stored as the caption prop)")
	cmd.Flags().StringVar(&propsJSON, "props-json", "", "node properties as JSON object")
	cmd.Flags().StringVar(&templateIDText, "template-id", "", "template ID (defaults to the system blob template)")
	cmd.Flags().StringVar(&mimeType, "mime-type", "", "declared MIME type (the sniffed type is authoritative)")
	cmd.Flags().StringVar(&parentIDText, "parent-id", "", "parent node ID for a contains edge")
	cmd.Flags().IntVar(&order, "order", 0, "contains edge sibling order when --parent-id is set")
	return cmd
}

func NewGetBlobCommand(a *app.App) *cobra.Command {
	var spaceIDText, outputPath string
	cmd := &cobra.Command{
		Use:   "blob NODE_ID",
		Short: "Download the blob attached to a node",
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
			sess, err := a.Engine.OpenSession(cmd.Context(), mycelengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			defer sess.Close()
			res, err := sess.GetBlob(cmd.Context(), domainsession.GetBlobInput{NodeID: nodeID})
			if err != nil {
				return err
			}
			defer res.Reader.Close()
			target := outputPath
			if target == "" {
				target = res.Meta.OriginalFilename
			}
			if target == "" {
				target = string(res.Meta.ID)
			}
			out, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, res.Reader); err != nil {
				out.Close()
				return err
			}
			if err := out.Close(); err != nil {
				return err
			}
			return a.Print(
				map[string]any{"node_id": nodeID, "blob_id": res.Meta.ID, "mime_type": res.Meta.MimeType, "size_bytes": res.Meta.SizeBytes, "output": target},
				fmt.Sprintf("blob written: %s (%d bytes, %s)\n", target, res.Meta.SizeBytes, res.Meta.MimeType),
			)
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVarP(&outputPath, "output-file", "o", "", "output file path (defaults to the original filename)")
	return cmd
}
