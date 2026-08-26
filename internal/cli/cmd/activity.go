package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
)

func NewAdminActivityCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "activity", Short: "Inspect and append daemon activity events"}
	cmd.AddCommand(newAdminActivityListCommand(a), newAdminActivityGetCommand(a), newAdminActivityAppendCommand(a))
	return cmd
}

func newAdminActivityListCommand(a *app.App) *cobra.Command {
	var pageSize int32
	var pageToken string
	var severities, categories, types []string
	cmd := &cobra.Command{Use: "list", Short: "List activity events", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &adminv1.ListActivityEventsRequest{PageSize: pageSize, PageToken: pageToken, Severities: severities, Categories: categories, Types: types}
		res, err := adminv1.NewAdminActivityServiceClient(conn).ListActivityEvents(authCtx, req)
		if err != nil {
			return fmt.Errorf("list activity events: %w", err)
		}
		return a.Print(res, fmt.Sprintf("activity events: %d\n", len(res.GetEvents())))
	}}
	cmd.Flags().Int32Var(&pageSize, "page-size", 50, "maximum events to return")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "pagination token")
	cmd.Flags().StringSliceVar(&severities, "severity", nil, "filter by severity")
	cmd.Flags().StringSliceVar(&categories, "category", nil, "filter by category")
	cmd.Flags().StringSliceVar(&types, "type", nil, "filter by event type")
	return cmd
}

func newAdminActivityGetCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "get EVENT_ID", Short: "Get an activity event", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		eventID := strings.TrimSpace(args[0])
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminActivityServiceClient(conn).GetActivityEvent(authCtx, &adminv1.GetActivityEventRequest{EventId: eventID})
		if err != nil {
			return fmt.Errorf("get activity event: %w", err)
		}
		return a.Print(res.GetEvent(), "activity event: "+res.GetEvent().GetEventId()+"\n")
	}}
}

func newAdminActivityAppendCommand(a *app.App) *cobra.Command {
	var file string
	cmd := &cobra.Command{Use: "append --file event.json", Short: "Append an activity event", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(file) == "" {
			return fmt.Errorf("--file is required")
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		event := &adminv1.ActivityEvent{}
		if err := protojson.Unmarshal(data, event); err != nil {
			return fmt.Errorf("decode activity event JSON: %w", err)
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminActivityServiceClient(conn).AppendActivityEvent(authCtx, &adminv1.AppendActivityEventRequest{Event: event})
		if err != nil {
			return fmt.Errorf("append activity event: %w", err)
		}
		return a.Print(res, "activity event appended: "+res.GetEvent().GetEventId()+"\n")
	}}
	cmd.Flags().StringVar(&file, "file", "", "activity event JSON file")
	return cmd
}
