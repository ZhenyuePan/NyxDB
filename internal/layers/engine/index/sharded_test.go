package index

import "testing"

func TestShardedRangeRouting(t *testing.T) {
	shardCount := 4
	s := NewSharded(shardCount, func() Indexer { return NewBTree() })

	cases := []struct {
		key  []byte
		want int
	}{
		{[]byte{0x00}, 0},
		{[]byte{0x3F}, 0},
		{[]byte{0x40}, 1},
		{[]byte{0x7F}, 1},
		{[]byte{0x80}, 2},
		{[]byte{0xBF}, 2},
		{[]byte{0xC0}, 3},
		{[]byte{0xFF}, 3},
		{[]byte{0x3F, 0xFF}, 0},
		{[]byte{0x40, 0x00}, 1},
		{[]byte{0xBF, 0xFF}, 2},
		{[]byte{0xC0, 0x00}, 3},
	}

	for _, tc := range cases {
		if got := s.shardFor(tc.key); got != tc.want {
			t.Fatalf("shardFor(%x) = %d, want %d", tc.key, got, tc.want)
		}
	}
}
