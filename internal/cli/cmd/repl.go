package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewReplCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "repl",
		Short: "Start an interactive MycelDB REPL",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunREPL(cmd.Context(), a, os.Stdin, os.Stdout)
		},
	}
}

func RunREPL(ctx context.Context, a *app.App, in io.Reader, out io.Writer) error {
	fmt.Fprintln(out, "mycel REPL. Use login <username> <password>, connect space <space>, connect domain <domain>, gql <query>, help, or exit.")
	scanner := bufio.NewScanner(in)
	commands := replCommandBuffer{}
	for {
		fmt.Fprint(out, a.Prompt())
		if !scanner.Scan() {
			break
		}
		completed, err := commands.feed(scanner.Text())
		if err != nil {
			fmt.Fprintln(out, "error:", err)
			continue
		}
		for _, line := range completed {
			exit, err := runREPLCommand(ctx, a, line, out)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					fmt.Fprintln(out, "error:", err)
				}
				continue
			}
			if exit {
				return nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if commands.pending() {
		return fmt.Errorf("incomplete continued REPL command; finish the command or remove the trailing \\")
	}
	return nil
}

func runREPLCommand(ctx context.Context, a *app.App, line string, out io.Writer) (bool, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return false, nil
	}
	if strings.HasPrefix(line, "gql ") {
		return false, runREPLGQL(ctx, a, strings.TrimSpace(strings.TrimPrefix(line, "gql ")), out)
	}
	args, err := splitArgs(line)
	if err != nil {
		return false, err
	}
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "exit", "quit":
		return true, nil
	case "login":
		if len(args) < 3 {
			return false, fmt.Errorf("usage: login USERNAME PASSWORD")
		}
		a.UserRef = args[1]
		a.Password = args[2]
		conn, _, login, err := loginDaemonPrincipal(ctx, a)
		if err != nil {
			return false, err
		}
		_ = conn.Close()
		fmt.Fprintf(out, "logged in as %s\n", login.GetPrincipal().GetUsername())
		return false, nil
	case "logout":
		a.UserRef = ""
		a.Password = ""
		a.ClearCurrentConnection()
		fmt.Fprintln(out, "logged out")
		return false, nil
	case "connect":
		return false, runREPLConnect(ctx, a, args[1:], out)
	case "space":
		if len(args) > 1 && (args[1] == "set" || args[1] == "unset") {
			return false, runREPLSpace(ctx, a, args[1:], out)
		}
		return false, runREPLCobraCommand(ctx, a, args, out)
	case "disconnect":
		a.ClearCurrentConnection()
		fmt.Fprintln(out, "disconnected")
		return false, nil
	case "\\c":
		return false, runREPLConnectAlias(ctx, a, args[1:], out)
	case "set_space":
		return false, fmt.Errorf("usage: space set SPACE_ID")
	case "unset_space":
		return false, fmt.Errorf("usage: space unset")
	case "help":
		root := NewRootCommand(a, true)
		root.SetOut(out)
		root.SetErr(out)
		_ = root.Help()
		fmt.Fprintln(out, "\nREPL shortcuts:")
		fmt.Fprintln(out, "  connect space <space-id-or-name>")
		fmt.Fprintln(out, "  connect domain <domain-id-or-key-or-name>")
		fmt.Fprintln(out, "  connect <space-id-or-name>[/<domain-id-or-key-or-name>]")
		fmt.Fprintln(out, "  \\c <space-id-or-name>[/<domain-id-or-key-or-name>]")
		fmt.Fprintln(out, "  disconnect")
		fmt.Fprintln(out, "  gql <GQL query text>")
		return false, nil
	default:
		return false, runREPLCobraCommand(ctx, a, args, out)
	}
}

func runREPLCobraCommand(ctx context.Context, a *app.App, args []string, out io.Writer) error {
	root := NewRootCommand(a, true)
	root.SetArgs(args)
	root.SetOut(out)
	root.SetErr(out)
	return root.ExecuteContext(ctx)
}

type replCommandBuffer struct {
	buf strings.Builder
}

func (b *replCommandBuffer) feed(line string) ([]string, error) {
	line, continued := stripREPLLineContinuation(line)
	line = strings.TrimSpace(line)
	if line == "" && b.buf.Len() == 0 {
		return nil, nil
	}
	if b.buf.Len() > 0 && line != "" {
		b.buf.WriteByte(' ')
	}
	b.buf.WriteString(line)
	if continued {
		return nil, nil
	}
	assembled := strings.TrimSpace(b.buf.String())
	b.buf.Reset()
	if assembled == "" {
		return nil, nil
	}
	return splitREPLCommands(assembled), nil
}

func (b *replCommandBuffer) pending() bool {
	return strings.TrimSpace(b.buf.String()) != ""
}

func stripREPLLineContinuation(line string) (string, bool) {
	trimmed := strings.TrimRight(line, " \t\r")
	if trimmed == "" || !strings.HasSuffix(trimmed, "\\") || trailingBackslashInQuote(trimmed) {
		return line, false
	}
	return strings.TrimRight(trimmed[:len(trimmed)-1], " \t\r"), true
}

func trailingBackslashInQuote(s string) bool {
	inQuote := rune(0)
	escaped := false
	runes := []rune(s)
	for i, r := range runes {
		if i == len(runes)-1 && r == '\\' {
			return inQuote != 0
		}
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if inQuote != 0 {
			if r == inQuote {
				inQuote = 0
			}
			continue
		}
		if r == '\'' || r == '"' {
			inQuote = r
		}
	}
	return false
}

func splitREPLCommands(s string) []string {
	var commands []string
	var current strings.Builder
	inQuote := rune(0)
	escaped := false
	flush := func() {
		cmd := strings.TrimSpace(current.String())
		if cmd != "" {
			commands = append(commands, cmd)
		}
		current.Reset()
	}
	for _, r := range s {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			current.WriteRune(r)
			escaped = true
			continue
		}
		if inQuote != 0 {
			current.WriteRune(r)
			if r == inQuote {
				inQuote = 0
			}
			continue
		}
		if r == '\'' || r == '"' {
			current.WriteRune(r)
			inQuote = r
			continue
		}
		if r == ';' {
			flush()
			continue
		}
		current.WriteRune(r)
	}
	flush()
	return commands
}

func runREPLSpace(ctx context.Context, a *app.App, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: space set SPACE_ID or space unset")
	}
	switch args[0] {
	case "set":
		if len(args) != 2 {
			return fmt.Errorf("usage: space set SPACE_ID")
		}
		id, err := app.ParseUUID[domainspace.SpaceID](args[1])
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(ctx, a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSpaceServiceClient(conn).GetSpace(authCtx, &clientv1.GetSpaceRequest{SpaceId: id.String()})
		if err != nil {
			return err
		}
		a.SetCurrentSpace(id, res.GetSpace().GetName())
		a.ClearCurrentDomain()
		fmt.Fprintf(out, "space set: %s\n", id)
		return nil
	case "unset":
		if len(args) != 1 {
			return fmt.Errorf("usage: space unset")
		}
		a.ClearCurrentConnection()
		fmt.Fprintln(out, "space unset")
		return nil
	default:
		return fmt.Errorf("usage: space set SPACE_ID or space unset")
	}
}

func runREPLConnectAlias(ctx context.Context, a *app.App, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: \\c SPACE[/DOMAIN]")
	}
	return connectSpaceDomainTarget(ctx, a, args[0], out)
}

func runREPLConnect(ctx context.Context, a *app.App, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: connect [space] SPACE[/DOMAIN] or connect domain DOMAIN")
	}
	switch args[0] {
	case "space":
		if len(args) != 2 {
			return fmt.Errorf("usage: connect space SPACE")
		}
		return connectSpaceDomainTarget(ctx, a, args[1], out)
	case "domain":
		if len(args) != 2 {
			return fmt.Errorf("usage: connect domain DOMAIN")
		}
		return connectCurrentDomain(ctx, a, args[1], out)
	default:
		if len(args) != 1 {
			return fmt.Errorf("usage: connect SPACE[/DOMAIN]")
		}
		return connectSpaceDomainTarget(ctx, a, args[0], out)
	}
}

func connectSpaceDomainTarget(ctx context.Context, a *app.App, target string, out io.Writer) error {
	spaceRef, domainRef, _ := strings.Cut(strings.TrimSpace(target), "/")
	if spaceRef == "" {
		return fmt.Errorf("space reference is required")
	}
	conn, authCtx, _, err := loginDaemonPrincipal(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	space, err := resolveVisibleSpace(ctx, clientv1.NewSpaceServiceClient(conn), authCtx, spaceRef)
	if err != nil {
		return err
	}
	spaceID, err := app.ParseUUID[domainspace.SpaceID](space.GetSpaceId())
	if err != nil {
		return err
	}
	a.SetCurrentSpace(spaceID, space.GetName())
	a.ClearCurrentDomain()
	domainClient := clientv1.NewDomainServiceClient(conn)
	var domain *clientv1.Domain
	if strings.TrimSpace(domainRef) != "" {
		domain, err = resolveVisibleDomain(ctx, domainClient, authCtx, space.GetSpaceId(), domainRef)
	} else {
		domain, err = resolveDefaultDaemonDomain(domainClient, authCtx, space.GetSpaceId())
	}
	if err == nil && domain != nil {
		a.SetCurrentDomain(domain.GetDomainId(), domain.GetKey(), domain.GetName())
		fmt.Fprintf(out, "connected to space %s (%s) domain %s (%s)\n", space.GetName(), space.GetSpaceId(), firstNonEmptyText(domain.GetKey(), domain.GetName()), domain.GetDomainId())
		return nil
	}
	if strings.TrimSpace(domainRef) != "" {
		a.ClearCurrentConnection()
		return err
	}
	fmt.Fprintf(out, "connected to space %s (%s); no default domain selected: %v\n", space.GetName(), space.GetSpaceId(), err)
	return nil
}

func connectCurrentDomain(ctx context.Context, a *app.App, domainRef string, out io.Writer) error {
	if a.CurrentSpaceID == nil {
		return fmt.Errorf("no space connected; use connect space <space-id-or-name>")
	}
	conn, authCtx, _, err := loginDaemonPrincipal(ctx, a)
	if err != nil {
		return err
	}
	defer conn.Close()
	domain, err := resolveVisibleDomain(ctx, clientv1.NewDomainServiceClient(conn), authCtx, a.CurrentSpaceID.String(), domainRef)
	if err != nil {
		return err
	}
	a.SetCurrentDomain(domain.GetDomainId(), domain.GetKey(), domain.GetName())
	fmt.Fprintf(out, "connected to domain %s (%s)\n", firstNonEmptyText(domain.GetKey(), domain.GetName()), domain.GetDomainId())
	return nil
}

func runREPLGQL(ctx context.Context, a *app.App, queryText string, out io.Writer) error {
	if strings.TrimSpace(queryText) == "" {
		return fmt.Errorf("GQL query text is required")
	}
	if a.CurrentSpaceID == nil {
		return fmt.Errorf("no space connected; use connect space <space-id-or-name>")
	}
	return runGQL(ctx, a, gqlRunOptions{QueryText: queryText, SpaceIDText: a.CurrentSpaceID.String(), DomainID: a.CurrentDomainID, DomainKey: a.CurrentDomainKey, RequireDomain: true, Out: out})
}

func resolveVisibleSpace(ctx context.Context, client clientv1.SpaceServiceClient, authCtx context.Context, ref string) (*clientv1.Space, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("space reference is required")
	}
	if _, err := uuid.Parse(ref); err == nil {
		res, err := client.GetSpace(authCtx, &clientv1.GetSpaceRequest{SpaceId: ref})
		if err != nil {
			return nil, err
		}
		return res.GetSpace(), nil
	}
	res, err := client.ListSpaces(authCtx, &clientv1.ListSpacesRequest{PageSize: 1000})
	if err != nil {
		return nil, err
	}
	var exact []*clientv1.Space
	var folded []*clientv1.Space
	for _, space := range res.GetSpaces() {
		if space.GetName() == ref {
			exact = append(exact, space)
		}
		if strings.EqualFold(space.GetName(), ref) {
			folded = append(folded, space)
		}
	}
	if len(exact) == 1 {
		return exact[0], nil
	}
	if len(exact) > 1 {
		return nil, fmt.Errorf("space name %q is ambiguous", ref)
	}
	if len(folded) == 1 {
		return folded[0], nil
	}
	if len(folded) > 1 {
		return nil, fmt.Errorf("space name %q is ambiguous", ref)
	}
	return nil, fmt.Errorf("space %q not found", ref)
}

func resolveVisibleDomain(ctx context.Context, client clientv1.DomainServiceClient, authCtx context.Context, spaceID, ref string) (*clientv1.Domain, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("domain reference is required")
	}
	if _, err := uuid.Parse(ref); err == nil {
		res, err := client.GetDomain(authCtx, &clientv1.GetDomainRequest{SpaceId: spaceID, DomainId: ref})
		if err == nil {
			return res.GetDomain(), nil
		}
		if status.Code(err) != codes.NotFound {
			return nil, err
		}
	}
	res, err := client.GetDomain(authCtx, &clientv1.GetDomainRequest{SpaceId: spaceID, Key: ref})
	if err == nil {
		return res.GetDomain(), nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, err
	}
	listed, err := client.ListDomains(authCtx, &clientv1.ListDomainsRequest{SpaceId: spaceID, PageSize: 1000})
	if err != nil {
		return nil, err
	}
	var exact []*clientv1.Domain
	var folded []*clientv1.Domain
	for _, domain := range listed.GetDomains() {
		if domain.GetName() == ref {
			exact = append(exact, domain)
		}
		if strings.EqualFold(domain.GetName(), ref) {
			folded = append(folded, domain)
		}
	}
	if len(exact) == 1 {
		return exact[0], nil
	}
	if len(exact) > 1 {
		return nil, fmt.Errorf("domain name %q is ambiguous", ref)
	}
	if len(folded) == 1 {
		return folded[0], nil
	}
	if len(folded) > 1 {
		return nil, fmt.Errorf("domain name %q is ambiguous", ref)
	}
	return nil, fmt.Errorf("domain %q not found", ref)
}

func resolveDefaultDaemonDomain(client clientv1.DomainServiceClient, authCtx context.Context, spaceID string) (*clientv1.Domain, error) {
	res, err := client.ListDomains(authCtx, &clientv1.ListDomainsRequest{SpaceId: spaceID, PageSize: 1000})
	if err != nil {
		return nil, err
	}
	domains := res.GetDomains()
	var defaults []*clientv1.Domain
	for _, domain := range domains {
		if domain.GetDefault() {
			defaults = append(defaults, domain)
		}
	}
	if len(defaults) == 1 {
		return defaults[0], nil
	}
	if len(defaults) > 1 {
		return nil, fmt.Errorf("space %s has multiple default domains", spaceID)
	}
	if len(domains) == 1 {
		return domains[0], nil
	}
	if len(domains) == 0 {
		return nil, fmt.Errorf("space %s has no domains", spaceID)
	}
	return nil, fmt.Errorf("space %s has no default domain; use connect domain <domain-id-or-key>", spaceID)
}

func splitArgs(s string) ([]string, error) {
	var args []string
	var b strings.Builder
	inQuote := rune(0)
	escaped := false
	for _, r := range s {
		if escaped {
			b.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if inQuote != 0 {
			if r == inQuote {
				inQuote = 0
			} else {
				b.WriteRune(r)
			}
			continue
		}
		if r == '\'' || r == '"' {
			inQuote = r
			continue
		}
		if r == ' ' || r == '\t' {
			if b.Len() > 0 {
				args = append(args, b.String())
				b.Reset()
			}
			continue
		}
		b.WriteRune(r)
	}
	if escaped {
		b.WriteRune('\\')
	}
	if inQuote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if b.Len() > 0 {
		args = append(args, b.String())
	}
	return args, nil
}
