// Package consistency defines distributed consistency levels and related helpers.
package consistency

// ConsistencyLevel controls how many replicas must acknowledge an operation.
type ConsistencyLevel int

const (
	// ONE requires acknowledgement from a single replica.
	ONE ConsistencyLevel = iota + 1
	// QUORUM requires acknowledgement from a majority of replicas.
	QUORUM
	// ALL requires acknowledgement from every replica.
	ALL
)

// DefaultReadConsistency is the default consistency level applied to reads.
const DefaultReadConsistency = ONE

// DefaultWriteConsistency is the default consistency level applied to writes.
const DefaultWriteConsistency = QUORUM

// RequiredAcks returns the minimum number of replica acknowledgements needed
// to satisfy the consistency level given a replication factor N.
//
// ONE   → 1
// QUORUM → floor(N/2) + 1
// ALL   → N
//
// If replicationFactor is less than 1, it is treated as 1.
func (c ConsistencyLevel) RequiredAcks(replicationFactor int) int {
	if replicationFactor < 1 {
		replicationFactor = 1
	}
	switch c {
	case ONE:
		return 1
	case QUORUM:
		return replicationFactor/2 + 1
	case ALL:
		return replicationFactor
	default:
		return 1
	}
}

// String returns a human-readable representation of the consistency level.
func (c ConsistencyLevel) String() string {
	switch c {
	case ONE:
		return "ONE"
	case QUORUM:
		return "QUORUM"
	case ALL:
		return "ALL"
	default:
		return "UNKNOWN"
	}
}
