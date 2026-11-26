package fsm

import (
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/Vympel87/curntvalv/internal/pb/curntvalv/v1"
)

// Named subscriptions (design D22): a subscriber's position kept in the
// cluster. Commits move it forward within a generation, like sink offsets
// (D12); a reset starts a new generation.

// subscriptionKey is the key of every subscription meta command; the body
// is a pb.SubscriptionCommand.
const subscriptionKey = "subscription"

// EncodeSubscription returns the log payload of a subscription command.
func EncodeSubscription(cmd *pb.SubscriptionCommand) ([]byte, error) {
	if !ValidSinkName(cmd.GetName()) {
		return nil, fmt.Errorf("%w: bad subscription name %q (lower case letters, digits, _ and -)", ErrInvalid, cmd.GetName())
	}
	if cmd.GetAction() == pb.SubscriptionCommand_ACTION_UNSPECIFIED {
		return nil, fmt.Errorf("%w: subscription command without action", ErrInvalid)
	}
	body, err := marshalOpts.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("fsm: encode subscription command: %w", err)
	}
	b := []byte{byte(KindMeta), byte(opSubscription)}
	b = appendBytes(b, []byte(subscriptionKey))
	return append(b, body...), nil
}

func (f *FSM) applySubscriptionCommand(index uint64, body []byte) error {
	cmd := &pb.SubscriptionCommand{}
	if err := proto.Unmarshal(body, cmd); err != nil {
		return fmt.Errorf("%w: subscription command: %v", ErrCorrupt, err)
	}
	name := cmd.GetName()
	cur, ok := f.subs[name]
	switch cmd.GetAction() {
	case pb.SubscriptionCommand_ACTION_CREATE:
		if !ok {
			f.subs[name] = &pb.Subscription{Name: name, Offset: cmd.GetOffset(), Drop: cmd.GetDrop(), CreatedIndex: index}
		}
	case pb.SubscriptionCommand_ACTION_COMMIT:
		if ok && cmd.GetGeneration() == cur.GetGeneration() && cmd.GetOffset() > cur.GetOffset() {
			cur.Offset = cmd.GetOffset()
		}
	case pb.SubscriptionCommand_ACTION_RESET:
		if ok {
			cur.Offset, cur.Generation = cmd.GetOffset(), cur.GetGeneration()+1
		}
	case pb.SubscriptionCommand_ACTION_DELETE:
		delete(f.subs, name)
	default:
		// From a newer version; ignoring it is deterministic.
	}
	return nil
}

// Subscription returns a named subscription.
func (f *FSM) Subscription(name string) (*pb.Subscription, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, ok := f.subs[name]
	if !ok {
		return nil, false
	}
	return proto.Clone(s).(*pb.Subscription), true
}

// Subscriptions returns all named subscriptions by name.
func (f *FSM) Subscriptions() []*pb.Subscription {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*pb.Subscription, 0, len(f.subs))
	for _, s := range f.subs {
		out = append(out, proto.Clone(s).(*pb.Subscription))
	}
	slices.SortFunc(out, func(a, b *pb.Subscription) int { return strings.Compare(a.GetName(), b.GetName()) })
	return out
}
