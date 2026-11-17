// Command curntvalv is the single binary for the curntValv server and CLI.
package main

import (
	"fmt"
	"os"
)

var version = "0.0.0-dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("curntvalv", version)
		return
	}
	fmt.Fprintln(os.Stderr, "curntvalv", version, "- early development, no commands yet (see docs/ROADMAP.md)")
	os.Exit(1)
}
