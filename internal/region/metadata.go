package region

// ID uniquely identifies a Region.
type ID uint64

// KeyRange describes the inclusive-exclusive key range handled by a Region.
type KeyRange struct {
	Start []byte
	End   []byte // empty slice denotes infinity
}

// Epoch tracks structural changes of a Region.
type Epoch struct {
	// Version increases when the key range of a Region changes (split/merge).
	Version uint64
	// ConfVersion increases when the peer set changes (add/remove peers).
	ConfVersion uint64
}

// PeerRole distinguishes voting members from learners.
type PeerRole int

const (
	// Voter is a full voting member of the Region's Raft group.
	Voter PeerRole = iota
	// Learner only receives logs; not part of quorum until promoted.
	Learner
)

// Peer describes a Region replica hosted on a Store.
type Peer struct {
	ID      uint64
	StoreID uint64
	Role    PeerRole
}

// State captures the lifecycle of a Region.
type State int

const (
	// StateActive indicates the Region is serving traffic.
	StateActive State = iota
	// StateSplitting indicates the Region is splitting its key range.
	StateSplitting
	// StateMerging indicates the Region is merging with another Region.
	StateMerging
	// StateTombstone indicates the Region has been removed.
	StateTombstone
)

// Region aggregates metadata describing a single shard of the keyspace.
type Region struct {
	ID     ID
	Range  KeyRange
	Epoch  Epoch
	Peers  []Peer
	State  State
	Leader uint64 // Peer ID currently considered leader (best-effort hint)
}

// ContainsKey reports whether the region manages the provided key.
func (r *Region) ContainsKey(key []byte) bool {
	if r == nil {
		return false
	}
	if len(r.Range.Start) > 0 && string(key) < string(r.Range.Start) {
		return false
	}
	if len(r.Range.End) > 0 && string(key) >= string(r.Range.End) {
		return false
	}
	return true
}

// Clone returns a shallow copy of the Region metadata for safe mutation.
func (r *Region) Clone() Region {
	if r == nil {
		return Region{}
	}
	cp := *r
	cp.Range = KeyRange{
		Start: append([]byte(nil), r.Range.Start...),
		End:   append([]byte(nil), r.Range.End...),
	}
	if len(r.Peers) > 0 {
		cp.Peers = append([]Peer(nil), r.Peers...)
	}
	return cp
}
