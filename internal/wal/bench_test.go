package wal

import "testing"

func benchAppend(b *testing.B, noSync bool, batch int) {
	l, err := Open(b.TempDir(), Options{NoSync: noSync})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	entry := make([]byte, 512)
	entries := make([][]byte, batch)
	for i := range entries {
		entries[i] = entry
	}
	b.SetBytes(int64(len(entry) * batch))
	b.ResetTimer()
	for range b.N {
		if _, err := l.Append(entries...); err != nil {
			b.Fatal(err)
		}
	}
}

// One append per entry with fsync, as one-row transactions arrive; and 64
// entries per append, as Raft batches them under load.
func BenchmarkAppendSync1(b *testing.B)    { benchAppend(b, false, 1) }
func BenchmarkAppendSync64(b *testing.B)   { benchAppend(b, false, 64) }
func BenchmarkAppendNoSync64(b *testing.B) { benchAppend(b, true, 64) }
