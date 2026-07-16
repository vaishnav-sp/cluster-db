package cluster

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Manager coordinates the in-memory membership state with a simple monitor loop.
type Manager struct {
	mu             sync.RWMutex
	membership     *Membership
	logger         *zap.Logger
	stopCh         chan struct{}
	stoppedCh      chan struct{}
	heartbeatTick  time.Duration
	failureTimeout time.Duration
	localNodeID    string
	localAddress   string
	started        bool
}

// NewManager creates a cluster manager with the provided membership state.
func NewManager(membership *Membership, logger *zap.Logger, heartbeatInterval, failureTimeout time.Duration, nodeID, nodeAddress string) *Manager {
	if membership == nil {
		membership = NewMembership()
	}
	return &Manager{
		membership:     membership,
		logger:         logger,
		heartbeatTick:  heartbeatInterval,
		failureTimeout: failureTimeout,
		localNodeID:    nodeID,
		localAddress:   nodeAddress,
		stopCh:         make(chan struct{}),
		stoppedCh:      make(chan struct{}),
	}
}

// Membership returns the underlying membership store.
func (m *Manager) Membership() *Membership {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.membership
}

// Start registers the local node, starts the monitor loop, and elects the initial leader.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.started = true
	m.stopCh = make(chan struct{})
	m.stoppedCh = make(chan struct{})
	m.mu.Unlock()

	m.membership.AddNode(Node{ID: m.localNodeID, Address: m.localAddress, Status: Alive, Version: "cluster/v1"})
	m.logger.Info("cluster node registered",
		zap.String("node_id", m.localNodeID),
		zap.String("address", m.localAddress),
		zap.String("status", "alive"),
	)
	m.electLeader()

	go m.monitorLoop(ctx)
	m.logger.Info("cluster monitor started",
		zap.String("node_id", m.localNodeID),
		zap.Duration("heartbeat_interval", m.heartbeatTick),
		zap.Duration("failure_timeout", m.failureTimeout),
	)
	return nil
}

// Stop stops the monitor loop and shuts down the manager.
func (m *Manager) Stop() {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	close(m.stopCh)
	m.started = false
	stopCh := m.stopCh
	stoppedCh := m.stoppedCh
	m.mu.Unlock()

	<-stoppedCh
	m.logger.Info("cluster monitor stopped", zap.String("node_id", m.localNodeID))
	_ = stopCh
}

func (m *Manager) monitorLoop(ctx context.Context) {
	defer close(m.stoppedCh)
	ticker := time.NewTicker(m.heartbeatTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.mu.RLock()
			membership := m.membership
			m.mu.RUnlock()
			if membership == nil {
				return
			}
			if err := membership.UpdateHeartbeat(m.localNodeID); err != nil {
				m.logger.Warn("cluster heartbeat update failed", zap.String("node_id", m.localNodeID), zap.Error(err))
			}
			m.electLeader()
		}
	}
}

func (m *Manager) electLeader() {
	leader, ok := m.membership.ElectLeader()
	if ok {
		m.logger.Info("cluster leader elected",
			zap.String("node_id", leader.ID),
			zap.String("address", leader.Address),
		)
		return
	}
	m.logger.Info("cluster leader election skipped",
		zap.String("node_id", m.localNodeID),
		zap.String("reason", "no alive nodes"),
	)
}
