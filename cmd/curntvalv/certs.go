package main

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Vympel87/curntvalv/internal/security"
)

// certsCmd writes a CA and node certificates for trying TLS (design D15).
func certsCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("certs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, `usage: curntvalv certs [--out DIR] [--days N] NODE...

Creates a CA (ca.crt, ca.key) and a certificate per node. NODE is the
node's host names and IPs, comma separated, as other nodes and clients
reach it; the first one names the files, e.g.

  curntvalv certs node1,10.0.0.1 node2,10.0.0.2 node3,10.0.0.3

Keep ca.key away from the nodes; they need ca.crt and their own files.
For production use your own CA.

`)
		fs.PrintDefaults()
	}
	out := fs.String("out", "certs", "directory to write to (existing files are not overwritten)")
	days := fs.Int("days", 825, "validity in days")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return usageErr("certs needs at least one NODE (see curntvalv certs -h)")
	}
	if *days < 1 {
		return usageErr("--days must be at least 1")
	}
	var nodes [][]string
	for _, a := range fs.Args() {
		var hosts []string
		for _, h := range strings.Split(a, ",") {
			if h = strings.TrimSpace(h); h != "" {
				hosts = append(hosts, h)
			}
		}
		if len(hosts) == 0 {
			return usageErr("empty NODE %q", a)
		}
		nodes = append(nodes, hosts)
	}
	files, err := security.WriteFiles(*out, nodes, time.Duration(*days)*24*time.Hour)
	if err != nil {
		return err
	}
	for _, f := range files {
		fmt.Fprintln(stdout, "wrote", f)
	}
	return nil
}
