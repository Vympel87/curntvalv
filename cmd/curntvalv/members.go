package main

import (
	"context"
	"log/slog"
	"maps"
	"time"

	"github.com/Vympel87/curntvalv/internal/raft"
)

// peerSetter takes the current set of peers. *transport.Transport and
// *transport.Server implement it.
type peerSetter interface {
	SetPeers(peers map[uint64]string)
}

// syncMembers keeps the transport's peers equal to the cluster
// configuration (design D18) until ctx ends. Until the node knows a
// configuration (a joining node) the --peers addresses are used; members
// without an address (configurations from --peers lacking one) keep the
// --peers address.
func syncMembers(ctx context.Context, node *raft.Node, initial map[uint64]string, log *slog.Logger, sets ...peerSetter) {
	cur := maps.Clone(initial)
	warned := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
		st, err := node.Status(ctx)
		if err != nil || len(st.Config.Members) == 0 {
			continue
		}
		next := map[uint64]string{}
		for _, m := range st.Config.Members {
			next[m.ID] = m.Addr
			if m.Addr == "" {
				next[m.ID] = initial[m.ID]
			}
		}
		if maps.Equal(next, cur) {
			continue
		}
		if !warned && st.ConfigIndex != 0 && !sameIDs(next, initial) {
			log.Info("cluster membership comes from the log, not --peers", "members", st.Config.String())
			warned = true
		}
		log.Info("cluster members changed", "members", st.Config.String())
		for _, s := range sets {
			s.SetPeers(next)
		}
		cur = next
	}
}

func sameIDs(a, b map[uint64]string) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if _, ok := b[id]; !ok {
			return false
		}
	}
	return true
}
