//go:build !wails

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "desktop UI requires build tags, e.g.:")
	fmt.Fprintln(os.Stderr, "  go build -tags wails,production,webkit2_41 -o bin/smart-redis-queue ./cmd/desktop")
	os.Exit(1)
}
