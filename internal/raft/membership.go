package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Cluster membership changes, one server at a time (Ongaro's thesis §4.1,
// design D18). A configuration is stored in the log as an EntryConfig entry
// and takes effect on a node as soon as the entry is in its log, committed
// or not. A leader starts a change only when the previous one is committed
// and it has committed an entry of its own term (the fix for the bug in
// single-server changes found in 2015). New servers join as learners: they
// receive the log but neither vote nor count for a majority until promoted.

// Member is one server of the cluster.
type Member struct {
	ID    uint64
	Addr  string // where the other servers reach it; opaque to Raft
	Voter bool   // false: a learner
}

// Configuration is the set of servers, sorted by ID.
type Configuration struct {
	Members []Member
}

// Member returns the member with the given id.
func (c Configuration) Member(id uint64) (Member, bool) {
	for _, m := range c.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// IsVoter reports whether id is a voting member.
func (c Configuration) IsVoter(id uint64) bool {
	m, ok := c.Member(id)
	return ok && m.Voter
}

// Voters returns the ids of the voting members.
func (c Configuration) Voters() []uint64 {
	var out []uint64
	for _, m := range c.Members {
		if m.Voter {
			out = append(out, m.ID)
		}
	}
	return out
}

func (c Configuration) quorum() int { return len(c.Voters())/2 + 1 }

func (c Configuration) clone() Configuration {
	return Configuration{Members: slices.Clone(c.Members)}
}

func (c Configuration) String() string {
	parts := make([]string, len(c.Members))
	for i, m := range c.Members {
		kind := "voter"
		if !m.Voter {
			kind = "learner"
		}
		parts[i] = fmt.Sprintf("%d=%s(%s)", m.ID, m.Addr, kind)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// ConfChangeType is the kind of a membership change.
type ConfChangeType uint8

const (
	// ConfAddLearner adds a server that receives the log but does not vote.
	ConfAddLearner ConfChangeType = iota + 1
	// ConfPromote makes a learner a voter.
	ConfPromote
	// ConfRemove removes a server, voter or learner.
	ConfRemove
)

// ConfChange is one membership change.
type ConfChange struct {
	Type ConfChangeType
	ID   uint64
	Addr string // ConfAddLearner
}

var (
	// ErrConfChangeInProgress means the leader cannot start a membership
	// change yet: the previous one is not committed, or it has not
	// committed an entry of its own term. Retrying soon succeeds.
	ErrConfChangeInProgress = errors.New("raft: a membership change is in progress")
	// ErrInvalidConfChange means the change does not apply to the current
	// configuration (e.g. adding an existing server, removing the last
	// voter).
	ErrInvalidConfChange = errors.New("raft: invalid membership change")
)

// apply returns the configuration after cc.
func (c Configuration) apply(cc ConfChange) (Configuration, error) {
	if cc.ID == 0 {
		return c, fmt.Errorf("%w: server id 0", ErrInvalidConfChange)
	}
	out := c.clone()
	i := slices.IndexFunc(out.Members, func(m Member) bool { return m.ID == cc.ID })
	switch cc.Type {
	case ConfAddLearner:
		if i >= 0 {
			return c, fmt.Errorf("%w: server %d is already a member", ErrInvalidConfChange, cc.ID)
		}
		if cc.Addr == "" {
			return c, fmt.Errorf("%w: server %d needs an address", ErrInvalidConfChange, cc.ID)
		}
		for _, m := range out.Members {
			if m.Addr == cc.Addr {
				return c, fmt.Errorf("%w: address %s is used by server %d", ErrInvalidConfChange, cc.Addr, m.ID)
			}
		}
		out.Members = append(out.Members, Member{ID: cc.ID, Addr: cc.Addr})
		slices.SortFunc(out.Members, func(a, b Member) int { return compareU64(a.ID, b.ID) })
	case ConfPromote:
		if i < 0 || out.Members[i].Voter {
			return c, fmt.Errorf("%w: server %d is not a learner", ErrInvalidConfChange, cc.ID)
		}
		out.Members[i].Voter = true
	case ConfRemove:
		if i < 0 {
			return c, fmt.Errorf("%w: server %d is not a member", ErrInvalidConfChange, cc.ID)
		}
		if out.Members[i].Voter && len(out.Voters()) == 1 {
			return c, fmt.Errorf("%w: server %d is the last voter", ErrInvalidConfChange, cc.ID)
		}
		out.Members = slices.Delete(out.Members, i, i+1)
	default:
		return c, fmt.Errorf("%w: unknown change type %d", ErrInvalidConfChange, cc.Type)
	}
	return out, nil
}

const confVersion = 1

// encodeConfig returns the data of an EntryConfig entry:
//
//	| version u8 | count uvarint | (id u64 LE | voter u8 | addr len uvarint | addr)* |
func encodeConfig(c Configuration) []byte {
	b := []byte{confVersion}
	b = binary.AppendUvarint(b, uint64(len(c.Members)))
	for _, m := range c.Members {
		b = binary.LittleEndian.AppendUint64(b, m.ID)
		v := byte(0)
		if m.Voter {
			v = 1
		}
		b = append(b, v)
		b = binary.AppendUvarint(b, uint64(len(m.Addr)))
		b = append(b, m.Addr...)
	}
	return b
}

func decodeConfig(b []byte) (Configuration, error) {
	bad := func(why string) (Configuration, error) {
		return Configuration{}, fmt.Errorf("%w: configuration: %s", ErrCorruptEntry, why)
	}
	if len(b) == 0 || b[0] != confVersion {
		return bad("unknown version")
	}
	n, k := binary.Uvarint(b[1:])
	if k <= 0 || n > uint64(len(b)) {
		return bad("bad count")
	}
	rest := b[1+k:]
	var c Configuration
	for range n {
		if len(rest) < 9 || rest[8] > 1 {
			return bad("truncated member")
		}
		m := Member{ID: binary.LittleEndian.Uint64(rest), Voter: rest[8] == 1}
		l, k := binary.Uvarint(rest[9:])
		if k <= 0 || l > uint64(len(rest)-9-k) {
			return bad("bad address")
		}
		m.Addr = string(rest[9+k : 9+k+int(l)])
		rest = rest[9+k+int(l):]
		if m.ID == 0 || (len(c.Members) > 0 && c.Members[len(c.Members)-1].ID >= m.ID) {
			return bad("ids not ascending")
		}
		c.Members = append(c.Members, m)
	}
	if len(rest) != 0 {
		return bad("trailing bytes")
	}
	return c, nil
}

// snapshotMagic starts the data of a snapshot that carries a configuration.
// Snapshots taken before the first membership change hold only the state
// machine's data, as before, so older nodes can still install them.
var snapshotMagic = []byte{0xff, 'c', 'v', 'c', 'o', 'n', 'f', 1}

// wrapSnapshot prepends the configuration to the state machine's data.
func wrapSnapshot(c *Configuration, app []byte) []byte {
	if c == nil {
		return app
	}
	conf := encodeConfig(*c)
	b := slices.Concat(snapshotMagic, binary.AppendUvarint(nil, uint64(len(conf))), conf)
	return append(b, app...)
}

// unwrapSnapshot splits snapshot data into the configuration (nil if it has
// none) and the state machine's data.
func unwrapSnapshot(b []byte) (*Configuration, []byte, error) {
	if !bytes.HasPrefix(b, snapshotMagic) {
		return nil, b, nil
	}
	rest := b[len(snapshotMagic):]
	l, k := binary.Uvarint(rest)
	if k <= 0 || l > uint64(len(rest)-k) {
		return nil, nil, fmt.Errorf("%w: bad configuration header", ErrCorruptSnapshot)
	}
	c, err := decodeConfig(rest[k : k+int(l)])
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
	}
	return &c, rest[k+int(l):], nil
}
