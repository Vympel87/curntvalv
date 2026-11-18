package fsm

import (
	"encoding/binary"
	"fmt"
	"maps"
	"regexp"
)

// sinkName is the allowed form of a sink name.
var sinkName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// ValidSinkName reports whether name may be used as a sink name.
func ValidSinkName(name string) bool { return sinkName.MatchString(name) }

// SinkPosition is the stored progress of a sink (design D12).
type SinkPosition struct {
	// Offset is the last log index the sink has delivered. 0 means nothing:
	// the sink starts with the oldest retained entry.
	Offset uint64
	// Gen counts resets. An offset command for another generation is
	// ignored, so a runner that has not yet noticed a reset cannot undo it.
	Gen uint64
}

// EncodeSinkOffset returns the log payload that records that sink name, in
// generation gen, has delivered everything up to and including log index
// offset (design D4). Offsets only move forward: applying an offset lower
// than or equal to the stored one changes nothing, so a late or repeated
// command is harmless.
func EncodeSinkOffset(name string, gen, offset uint64) ([]byte, error) {
	if !ValidSinkName(name) {
		return nil, fmt.Errorf("%w: bad sink name %q", ErrInvalid, name)
	}
	b := []byte{byte(KindMeta), byte(opSinkOffset)}
	b = appendBytes(b, []byte(name))
	b = binary.LittleEndian.AppendUint64(b, offset)
	return binary.LittleEndian.AppendUint64(b, gen), nil
}

// EncodeSinkReset returns the log payload that sets the offset of sink name
// to offset, also backwards, and starts a new generation. Offset 0 means
// "from the oldest retained entry".
func EncodeSinkReset(name string, offset uint64) ([]byte, error) {
	if !ValidSinkName(name) {
		return nil, fmt.Errorf("%w: bad sink name %q", ErrInvalid, name)
	}
	b := []byte{byte(KindMeta), byte(opSinkReset)}
	b = appendBytes(b, []byte(name))
	return binary.LittleEndian.AppendUint64(b, offset), nil
}

func (f *FSM) applySinkOffset(name, rest []byte) error {
	var off, gen uint64
	switch len(rest) {
	case 8: // written before generations existed (M5)
		off = binary.LittleEndian.Uint64(rest)
	case 16:
		off = binary.LittleEndian.Uint64(rest[:8])
		gen = binary.LittleEndian.Uint64(rest[8:])
	default:
		return fmt.Errorf("%w: bad sink offset", ErrCorrupt)
	}
	cur := f.sinks[string(name)]
	if gen == cur.Gen && off > cur.Offset {
		cur.Offset = off
		f.sinks[string(name)] = cur
	}
	return nil
}

func (f *FSM) applySinkReset(name, rest []byte) error {
	if len(rest) != 8 {
		return fmt.Errorf("%w: bad sink reset", ErrCorrupt)
	}
	cur := f.sinks[string(name)]
	f.sinks[string(name)] = SinkPosition{Offset: binary.LittleEndian.Uint64(rest), Gen: cur.Gen + 1}
	return nil
}

// SinkPosition returns the stored position of a sink. ok is false for a sink
// that never stored one.
func (f *FSM) SinkPosition(name string) (pos SinkPosition, ok bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	pos, ok = f.sinks[name]
	return pos, ok
}

// SinkOffset returns the stored offset of a sink: the last log index it has
// delivered. ok is false for a sink that never stored one.
func (f *FSM) SinkOffset(name string) (offset uint64, ok bool) {
	pos, ok := f.SinkPosition(name)
	return pos.Offset, ok
}

// LastChange returns the index of the last applied entry that holds row
// changes; a sink whose offset is at or past it has nothing to deliver.
func (f *FSM) LastChange() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastChange
}

// SinkPositions returns the stored positions of all sinks.
func (f *FSM) SinkPositions() map[string]SinkPosition {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return maps.Clone(f.sinks)
}
