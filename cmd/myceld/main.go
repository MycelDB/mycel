package main

import (
	"context"
	"os"

	"github.com/myceldb/mycel/internal/daemon/app"
)

func main() {
	os.Exit(app.Run(context.Background()))
}
