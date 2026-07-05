package cmd

import (
	"fmt"
	"strings"

	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

func NewChangeStreamCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "change-stream", Aliases: []string{"changes"}, Short: "Watch committed domain changes over daemon gRPC"}
	cmd.AddCommand(NewChangeStreamWatchCommand(a))
	return cmd
}

func NewChangeStreamWatchCommand(a *app.App) *cobra.Command {
	var spaceID, domainRef string
	var includeCurrent bool
	var afterRevision int64
	var maxEvents int
	cmd := &cobra.Command{Use: "watch", Short: "Watch committed graph changes for a domain", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		domainID := strings.TrimSpace(domainRef)
		if domainID == "" {
			domainID = graph.DefaultDomainKey
		}
		resolvedDomainID, err := daemonResolveDomainID(cmd.Context(), conn, authCtx, spaceID, domainID)
		if err != nil {
			return err
		}
		req := &clientv1.WatchDomainChangesRequest{SpaceId: spaceID, DomainId: resolvedDomainID, IncludeCurrent: includeCurrent}
		if afterRevision >= 0 {
			req.AfterRevision = &afterRevision
		}
		stream, err := clientv1.NewChangeStreamServiceClient(conn).WatchDomainChanges(authCtx, req)
		if err != nil {
			return err
		}
		seen := 0
		for {
			msg, err := stream.Recv()
			if err != nil {
				return err
			}
			seen++
			if a.Output == "json" {
				raw, err := protojson.MarshalOptions{Multiline: false}.Marshal(msg)
				if err != nil {
					return err
				}
				fmt.Println(string(raw))
			} else {
				printChangeStreamMessage(msg)
			}
			if maxEvents > 0 && seen >= maxEvents {
				return nil
			}
		}
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", graph.DefaultDomainKey, "domain key or ID")
	cmd.Flags().BoolVar(&includeCurrent, "include-current", false, "emit current checkpoint before watching")
	cmd.Flags().Int64Var(&afterRevision, "after-revision", -1, "resume after revision; negative disables resume")
	cmd.Flags().IntVar(&maxEvents, "max-events", 0, "stop after this many stream messages; 0 watches forever")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func printChangeStreamMessage(msg *clientv1.WatchDomainChangesResponse) {
	if checkpoint := msg.GetCheckpoint(); checkpoint != nil {
		fmt.Printf("checkpoint\tspace=%s\tdomain=%s\trevision=%d\n", checkpoint.GetSpaceId(), checkpoint.GetDomainId(), checkpoint.GetCurrentRevision())
		return
	}
	if heartbeat := msg.GetHeartbeat(); heartbeat != nil {
		fmt.Printf("heartbeat\t%s\n", heartbeat.GetHeartbeatTime().AsTime().Format("2006-01-02T15:04:05Z07:00"))
		return
	}
	if event := msg.GetEvent(); event != nil {
		fmt.Printf("event\trevision=%d\tcommit=%s\tchanges=%d\n", event.GetRevision(), event.GetCommitId(), len(event.GetChanges()))
		for _, change := range event.GetChanges() {
			fmt.Printf("change\t%s", change.GetType().String())
			if node := change.GetNode(); node != nil {
				fmt.Printf("\tnode=%s", node.GetNodeId())
			} else if nodeID := change.GetNodeId(); nodeID != "" {
				fmt.Printf("\tnode=%s", nodeID)
			}
			if edge := change.GetEdge(); edge != nil {
				fmt.Printf("\tedge=%s", edge.GetEdgeId())
			} else if edgeID := change.GetEdgeId(); edgeID != "" {
				fmt.Printf("\tedge=%s", edgeID)
			}
			fmt.Println()
		}
	}
}
