package raft

import "fmt"

// MessageType is the kind of a Message.
type MessageType uint8

const (
	// MsgVote is the RequestVote RPC.
	MsgVote MessageType = iota + 1
	// MsgVoteResp is the reply to MsgVote.
	MsgVoteResp
	// MsgApp is the AppendEntries RPC (also used as heartbeat).
	MsgApp
	// MsgAppResp is the reply to MsgApp and to MsgSnap.
	MsgAppResp
	// MsgSnap is the InstallSnapshot RPC (§7). Snapshots hold only metadata
	// (design D5), so they are sent in one piece (offset 0, done = true).
	MsgSnap
)

func (t MessageType) String() string {
	switch t {
	case MsgVote:
		return "MsgVote"
	case MsgVoteResp:
		return "MsgVoteResp"
	case MsgApp:
		return "MsgApp"
	case MsgAppResp:
		return "MsgAppResp"
	case MsgSnap:
		return "MsgSnap"
	default:
		return fmt.Sprintf("MessageType(%d)", uint8(t))
	}
}

// Message carries one Raft RPC or its reply. RPCs are modeled as one-way
// messages: a reply is just another message, and any message may be lost,
// delayed, duplicated or reordered by the Transport.
type Message struct {
	Type MessageType
	From uint64
	To   uint64
	Term uint64 // sender's current term

	// MsgVote: the candidate's last log entry. MsgAppResp (rejected): the
	// follower's last index, which lets the leader skip back faster.
	LastLogIndex uint64
	LastLogTerm  uint64

	// MsgApp: the entry before Entries. MsgAppResp: echoes the request's
	// PrevLogIndex so the leader can ignore stale rejections.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// MsgVoteResp.
	VoteGranted bool

	// MsgAppResp. On success MatchIndex is the index of the last entry the
	// follower now knows to match the leader's log.
	Success    bool
	MatchIndex uint64

	// MsgSnap.
	Snapshot *Snapshot

	// MsgApp/MsgSnap: the leader's latest read-only request id. Replies echo
	// it; a majority of replies proves the leader was still leader after
	// the request was made (§8).
	ReadID uint64
}

// Snapshot is a state machine snapshot together with its position.
type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// Transport sends messages to other nodes. Send must not block; delivery is
// best effort.
type Transport interface {
	Send(m Message)
}

// Applier is the replicated state machine. Apply receives committed entries
// in index order. Snapshot returns the state after the last applied entry;
// Restore replaces the whole state with a snapshot. A restarted node first
// restores its latest snapshot (if any) and then applies the entries after
// it. Every error is fatal for the node.
type Applier interface {
	Apply(e Entry) error
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}
