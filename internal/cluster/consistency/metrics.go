package consistency

import "sync/atomic"

// Metrics tracks thread-safe atomic counters for quorum consistency operations.
type Metrics struct {
	successfulQuorumReads  uint64
	successfulQuorumWrites uint64
	failedQuorumReads      uint64
	failedQuorumWrites      uint64
	readRepairsPerformed   uint64
}

// MetricsSnapshot holds a point-in-time copy of metrics counters.
type MetricsSnapshot struct {
	SuccessfulQuorumReads  uint64 `json:"successful_quorum_reads"`
	SuccessfulQuorumWrites uint64 `json:"successful_quorum_writes"`
	FailedQuorumReads      uint64 `json:"failed_quorum_reads"`
	FailedQuorumWrites      uint64 `json:"failed_quorum_writes"`
	ReadRepairsPerformed   uint64 `json:"read_repairs_performed"`
}

// NewMetrics creates a new initialized Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// IncSuccessfulQuorumReads increments the successful quorum read counter.
func (m *Metrics) IncSuccessfulQuorumReads() {
	atomic.AddUint64(&m.successfulQuorumReads, 1)
}

// IncSuccessfulQuorumWrites increments the successful quorum write counter.
func (m *Metrics) IncSuccessfulQuorumWrites() {
	atomic.AddUint64(&m.successfulQuorumWrites, 1)
}

// IncFailedQuorumReads increments the failed quorum read counter.
func (m *Metrics) IncFailedQuorumReads() {
	atomic.AddUint64(&m.failedQuorumReads, 1)
}

// IncFailedQuorumWrites increments the failed quorum write counter.
func (m *Metrics) IncFailedQuorumWrites() {
	atomic.AddUint64(&m.failedQuorumWrites, 1)
}

// IncReadRepairsPerformed increments the read repairs performed counter.
func (m *Metrics) IncReadRepairsPerformed() {
	atomic.AddUint64(&m.readRepairsPerformed, 1)
}

// Snapshot returns a point-in-time copy of all metrics counters.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		SuccessfulQuorumReads:  atomic.LoadUint64(&m.successfulQuorumReads),
		SuccessfulQuorumWrites: atomic.LoadUint64(&m.successfulQuorumWrites),
		FailedQuorumReads:      atomic.LoadUint64(&m.failedQuorumReads),
		FailedQuorumWrites:      atomic.LoadUint64(&m.failedQuorumWrites),
		ReadRepairsPerformed:   atomic.LoadUint64(&m.readRepairsPerformed),
	}
}
