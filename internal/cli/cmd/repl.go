package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
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
	fmt.Fprintln(out, "mycel REPL. Use login <username> <password>, space set <space_id>, space unset, help, or exit.")
	scanner := bufio.NewScanner(in)
	for {
		fmt.Fprint(out, "mycel> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		args, err := splitArgs(line)
		if err != nil {
			fmt.Fprintln(out, "error:", err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		switch args[0] {
		case "exit", "quit":
			return nil
		case "login":
			if len(args) < 3 {
				fmt.Fprintln(out, "usage: login USERNAME PASSWORD")
				continue
			}
			a.UserRef = args[1]
			a.Password = args[2]
			a.Token = ""
			conn, _, login, err := loginDaemonUser(ctx, a)
			if err != nil {
				fmt.Fprintln(out, "error:", err)
				continue
			}
			_ = conn.Close()
			fmt.Fprintf(out, "logged in as %s\n", login.GetPrincipal().GetUsername())
		case "logout":
			a.Token = ""
			a.UserRef = ""
			a.Password = ""
			a.CurrentSpaceID = nil
			fmt.Fprintln(out, "logged out")
		case "set_space":
			fmt.Fprintln(out, "usage: space set SPACE_ID")
			continue
		case "unset_space":
			fmt.Fprintln(out, "usage: space unset")
			continue
		case "help":
			root := NewRootCommand(a, true)
			root.SetOut(out)
			root.SetErr(out)
			_ = root.Help()
		default:
			root := NewRootCommand(a, true)
			root.SetArgs(args)
			root.SetOut(out)
			root.SetErr(out)
			if err := root.ExecuteContext(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					fmt.Fprintln(out, "error:", err)
				}
			}
		}
	}
	return scanner.Err()
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
