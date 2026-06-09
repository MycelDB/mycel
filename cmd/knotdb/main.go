package main

import (
	"os"

	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/cli/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
