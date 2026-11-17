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

// EncodeSinkOffset returns the log payload that records that sink name has
// delivered everything up to and including log index offset (design D4).
// Offsets only move forward: applying an offset lower than or equal to the
// stored one changes nothing, so a late or repeated command is harmless.
func EncodeSinkOffset(name string, offset uint64) ([]byte, error) {
	if !ValidSinkName(name) {
		return nil, fmt.Errorf("%w: bad sink name %q", ErrInvalid, name)
	}
	b := []byte{byte(KindMeta), byte(opSinkOffset)}
	b = appendBytes(b, []byte(name))
	return binary.LittleEndian.AppendUint64(b, offset), nil
}

func (f *FSM) applySinkOffset(name, rest []byte) error {
	if len(rest) != 8 {
		return fmt.Errorf("%w: bad sink offset", ErrCorrupt)
	}
	off := binary.LittleEndian.Uint64(rest)
	if off > f.sinks[string(name)] {
		f.sinks[string(name)] = off
	}
	return nil
}

// SinkOffset returns the stored offset of a sink: the last log index it has
// delivered. ok is false for a sink that never stored one.
func (f *FSM) SinkOffset(name string) (offset uint64, ok bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	offset, ok = f.sinks[name]
	return offset, ok
}

// LastChange returns the index of the last applied entry that holds row
// changes; a sink whose offset is at or past it has nothing to deliver.
func (f *FSM) LastChange() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastChange
}

// SinkOffsets returns the stored offsets of all sinks.
func (f *FSM) SinkOffsets() map[string]uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return maps.Clone(f.sinks)
}
