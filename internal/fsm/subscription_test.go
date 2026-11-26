package fsm

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
	"github.com/Vympel87/curntvalv/internal/raft"
)

func subCmd(t *testing.T, f *FSM, idx uint64, action pb.SubscriptionCommand_Action, name string, gen, offset uint64, drop bool) {
	t.Helper()
	b, err := EncodeSubscription(&pb.SubscriptionCommand{Action: action, Name: name, Generation: gen, Offset: offset, Drop: drop})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(raft.Entry{Index: idx, Type: raft.EntryNormal, Data: b}); err != nil {
		t.Fatal(err)
	}
}

func sub(t *testing.T, f *FSM, name string) *pb.Subscription {
	t.Helper()
	s, ok := f.Subscription(name)
	if !ok {
		t.Fatalf("no subscription %s", name)
	}
	return s
}

func TestSubscriptions(t *testing.T) {
	f := New()
	const (
		create = pb.SubscriptionCommand_ACTION_CREATE
		commit = pb.SubscriptionCommand_ACTION_COMMIT
		reset  = pb.SubscriptionCommand_ACTION_RESET
		del    = pb.SubscriptionCommand_ACTION_DELETE
	)
	subCmd(t, f, 1, create, "billing", 0, 0, false)
	subCmd(t, f, 2, create, "billing", 0, 99, true) // exists: unchanged
	subCmd(t, f, 3, create, "audit", 0, 40, true)
	if s := sub(t, f, "billing"); s.GetOffset() != 0 || s.GetDrop() || s.GetCreatedIndex() != 1 {
		t.Fatalf("%v", s)
	}
	subCmd(t, f, 4, commit, "billing", 0, 10, false)
	subCmd(t, f, 5, commit, "billing", 0, 7, false) // backwards: ignored
	subCmd(t, f, 6, commit, "nope", 0, 7, false)    // unknown: ignored
	if s := sub(t, f, "billing"); s.GetOffset() != 10 {
		t.Fatalf("%v", s)
	}
	subCmd(t, f, 7, reset, "billing", 0, 3, false)
	subCmd(t, f, 8, commit, "billing", 0, 20, false) // old generation: ignored
	if s := sub(t, f, "billing"); s.GetOffset() != 3 || s.GetGeneration() != 1 {
		t.Fatalf("after reset %v", s)
	}
	subCmd(t, f, 9, commit, "billing", 1, 5, false)
	if s := sub(t, f, "billing"); s.GetOffset() != 5 {
		t.Fatalf("%v", s)
	}
	if subs := f.Subscriptions(); len(subs) != 2 || subs[0].GetName() != "audit" || subs[1].GetName() != "billing" {
		t.Fatalf("%v", subs)
	}

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := New()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(sub(t, g, "billing"), sub(t, f, "billing")) || !proto.Equal(sub(t, g, "audit"), sub(t, f, "audit")) {
		t.Fatal("restored subscriptions differ")
	}
	if again, _ := g.Snapshot(); string(again) != string(snap) {
		t.Fatal("snapshot not deterministic")
	}

	subCmd(t, f, 10, del, "audit", 0, 0, false)
	if _, ok := f.Subscription("audit"); ok {
		t.Fatal("deleted subscription still there")
	}
	// Returned subscriptions are copies.
	s := sub(t, f, "billing")
	s.Offset = 1000
	if sub(t, f, "billing").GetOffset() != 5 {
		t.Fatal("shared state")
	}
	for _, bad := range []*pb.SubscriptionCommand{{Action: create, Name: "Bad Name"}, {Name: "ok"}} {
		if _, err := EncodeSubscription(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%v: %v", bad, err)
		}
	}
	// A version 6 snapshot has none.
	h := New()
	if err := h.Restore(asVersion6(t, mustSnapshot(t, New()))); err != nil || len(h.Subscriptions()) != 0 {
		t.Fatalf("v6: %v", err)
	}
}
