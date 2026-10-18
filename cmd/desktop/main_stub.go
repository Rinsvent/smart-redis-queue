//go:build !wails && !android && !ios

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "desktop UI requires build tags, e.g.:")
	fmt.Fprintln(os.Stderr, "  go build -tags wails,production,gtk3 -o bin/smart-redis-queue ./cmd/desktop")
	os.Exit(1)
}
