package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/cluster/consistency"
	clusterhashring "github.com/vaishnav-sp/cluster-db/internal/cluster/hashring"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
)

// Manager coordinates the in-memory membership state with a simple monitor loop.
type Manager struct {
	mu                sync.RWMutex
	membership        *Membership
	logger            *zap.Logger
	stopCh            chan struct{}
	stoppedCh         chan struct{}
	heartbeatTick     time.Duration
	failureTimeout    time.Duration
	localNodeID       string
	localAddress      string
	started           bool

	ReplicationFactor int
	WriteQuorum       int
	ReadQuorum        int

	Metrics   *consistency.Metrics
	hashRing  *clusterhashring.HashRing
	gossipEngine any // *gossip.Engine, kept interface/any or type to avoid circular dependency
}
// NewManager creates a cluster manager with the provided membership state.
func NewManager(membership *Membership, logger *zap.Logger, heartbeatInterval, failureTimeout time.Duration, nodeID, nodeAddress string) *Manager {
	if membership == nil {
		membership = NewMembership()
	}
	manager := &Manager{
		membership:     membership,
		logger:         logger,
		heartbeatTick:  heartbeatInterval,
		failureTimeout: failureTimeout,
		localNodeID:    nodeID,
		localAddress:   nodeAddress,
		stopCh:         make(chan struct{}),
		stoppedCh:      make(chan struct{}),
		hashRing:       clusterhashring.New(100),
		Metrics:        consistency.NewMetrics(),
	}
	if membership != nil {
		membership.setChangeHook(func() { manager.syncHashRing() })
	}
	return manager
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
	m.syncHashRing()
	if m.logger != nil {
		m.logger.Info("cluster node registered",
			zap.String("node_id", m.localNodeID),
			zap.String("address", m.localAddress),
			zap.String("status", "alive"),
		)
	}
	m.electLeader()

	go m.monitorLoop(ctx)
	m.mu.RLock()
	ge, _ := m.gossipEngine.(GossipHandler)
	m.mu.RUnlock()
	if ge != nil {
		_ = ge.Start(ctx)
	}

	if m.logger != nil {
		m.logger.Info("cluster monitor started",
			zap.String("node_id", m.localNodeID),
			zap.Duration("heartbeat_interval", m.heartbeatTick),
			zap.Duration("failure_timeout", m.failureTimeout),
		)
	}
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
	ge, _ := m.gossipEngine.(GossipHandler)
	m.mu.Unlock()

	if ge != nil {
		ge.Stop()
	}

	<-stoppedCh
	if m.logger != nil {
		m.logger.Info("cluster monitor stopped", zap.String("node_id", m.localNodeID))
	}
	_ = stopCh
}

// GossipHandler abstracts the Gossip engine operations.
type GossipHandler interface {
	Start(ctx context.Context) error
	Stop()
	HandleInboundGossip(req clusterRPC.GossipRequest) clusterRPC.GossipResponse
}

// SetGossipEngine registers a gossip engine with the manager.
func (m *Manager) SetGossipEngine(engine GossipHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gossipEngine = engine
}

// HandleGossip handles an incoming gossip request.
func (m *Manager) HandleGossip(req clusterRPC.GossipRequest) (clusterRPC.GossipResponse, error) {
	m.mu.RLock()
	ge, ok := m.gossipEngine.(GossipHandler)
	m.mu.RUnlock()

	if ok && ge != nil {
		return ge.HandleInboundGossip(req), nil
	}
	return clusterRPC.GossipResponse{Accepted: true, Message: "ok"}, nil
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
			m.sendHeartbeats()
			m.electLeader()
		}
	}
}

// JoinCluster joins a remote cluster node and synchronizes membership state.
func (m *Manager) JoinCluster(address string) ([]Node, error) {
	if address == "" {
		return nil, fmt.Errorf("cluster: missing address")
	}

	client := clusterRPC.NewClient(m.heartbeatTick)
	ctx, cancel := context.WithTimeout(context.Background(), m.heartbeatTick)
	defer cancel()

	resp, err := client.JoinCluster(ctx, address, clusterRPC.JoinRequest{NodeID: m.localNodeID, Address: m.localAddress})
	if err != nil {
		return nil, fmt.Errorf("cluster: join %s: %w", address, err)
	}
	if !resp.Accepted {
		return nil, fmt.Errorf("cluster: join %s rejected", address)
	}

	m.membership.AddNode(Node{ID: m.localNodeID, Address: m.localAddress, Status: Alive, LastHeartbeat: time.Now().UTC(), Version: "cluster/v1"})
	m.applyMembershipSnapshot(resp.Members)
	m.syncHashRing()
	m.electLeader()
	return m.membership.ListNodes(), nil
}

// HandleJoin handles an inbound join request.
func (m *Manager) HandleJoin(req clusterRPC.JoinRequest) (clusterRPC.JoinResponse, error) {
	if req.NodeID == "" {
		return clusterRPC.JoinResponse{}, fmt.Errorf("cluster: invalid join request")
	}

	node := Node{ID: req.NodeID, Address: req.Address, Status: Alive, LastHeartbeat: time.Now().UTC(), Version: "cluster/v1"}
	m.membership.AddNode(node)
	m.syncHashRing()
	m.electLeader()

	return clusterRPC.JoinResponse{Accepted: true, Members: m.membershipSnapshot()}, nil
}

// HandleHeartbeat handles an inbound heartbeat request.
func (m *Manager) HandleHeartbeat(req clusterRPC.HeartbeatRequest) (clusterRPC.HeartbeatResponse, error) {
	if req.NodeID == "" {
		return clusterRPC.HeartbeatResponse{}, fmt.Errorf("cluster: invalid heartbeat request")
	}

	if err := m.membership.UpdateHeartbeat(req.NodeID); err != nil {
		m.membership.AddNode(Node{ID: req.NodeID, Address: req.Address, Status: Alive, LastHeartbeat: time.Now().UTC(), Version: "cluster/v1"})
	} else {
		m.syncHashRing()
	}
	m.syncHashRing()
	return clusterRPC.HeartbeatResponse{Accepted: true}, nil
}

// HandleLeave handles an inbound leave request.
func (m *Manager) HandleLeave(req clusterRPC.LeaveRequest) (clusterRPC.LeaveResponse, error) {
	if req.NodeID == "" {
		return clusterRPC.LeaveResponse{}, fmt.Errorf("cluster: invalid leave request")
	}

	m.membership.RemoveNode(req.NodeID)
	m.syncHashRing()
	m.electLeader()
	return clusterRPC.LeaveResponse{Accepted: true}, nil
}

// LeaveCluster notifies peers that this node is leaving and stops its heartbeat loop.
func (m *Manager) LeaveCluster() error {
	client := clusterRPC.NewClient(m.heartbeatTick)
	for _, node := range m.membership.ListNodes() {
		if node.ID == m.localNodeID || node.Address == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), m.heartbeatTick)
		_, _ = client.LeaveCluster(ctx, node.Address, clusterRPC.LeaveRequest{NodeID: m.localNodeID})
		cancel()
	}

	m.membership.RemoveNode(m.localNodeID)
	m.syncHashRing()
	m.electLeader()
	m.Stop()
	return nil
}

func (m *Manager) sendHeartbeats() {
	if m.heartbeatTick <= 0 {
		return
	}

	client := clusterRPC.NewClient(m.heartbeatTick)
	for _, node := range m.membership.ListNodes() {
		if node.ID == m.localNodeID || node.Status != Alive || node.Address == "" {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), m.heartbeatTick)
		_, err := client.SendHeartbeat(ctx, node.Address, clusterRPC.HeartbeatRequest{NodeID: m.localNodeID, Address: m.localAddress})
		cancel()
		if err != nil {
			continue
		}
		if err := m.membership.UpdateHeartbeat(node.ID); err != nil {
			m.logger.Debug("cluster heartbeat update skipped", zap.String("node_id", node.ID), zap.Error(err))
		}
	}
}

func (m *Manager) applyMembershipSnapshot(members []clusterRPC.MemberInfo) {
	for _, member := range members {
		if member.ID == "" {
			continue
		}
		status := Alive
		switch member.Status {
		case "suspect":
			status = Suspect
		case "dead":
			status = Dead
		}
		m.membership.AddNode(Node{ID: member.ID, Address: member.Address, Status: status, LastHeartbeat: time.Now().UTC(), IsLeader: member.IsLeader, Version: "cluster/v1"})
	}
	m.syncHashRing()
}

func (m *Manager) membershipSnapshot() []clusterRPC.MemberInfo {
	nodes := m.membership.ListNodes()
	members := make([]clusterRPC.MemberInfo, 0, len(nodes))
	for _, node := range nodes {
		members = append(members, clusterRPC.MemberInfo{ID: node.ID, Address: node.Address, Status: node.Status.String(), IsLeader: node.IsLeader})
	}
	return members
}

func (m *Manager) electLeader() {
	previousLeader, hadPreviousLeader := m.membership.Leader()
	leader, ok := m.membership.ElectLeader()
	m.syncHashRing()
	if !ok {
		if m.logger != nil && hadPreviousLeader {
			m.logger.Info("cluster leader cleared",
				zap.String("node_id", previousLeader.ID),
			)
		}
		return
	}
	if m.logger != nil && (!hadPreviousLeader || previousLeader.ID != leader.ID) {
		m.logger.Info("cluster leader elected",
			zap.String("node_id", leader.ID),
			zap.String("address", leader.Address),
		)
	}
}

// Owner returns the node responsible for the provided key if one exists.
func (m *Manager) Owner(key string) (Node, bool) {
	m.syncHashRing()
	if m.hashRing == nil {
		return Node{}, false
	}
	nodeID, ok := m.hashRing.GetNode(key)
	if !ok {
		return Node{}, false
	}
	node, found := m.membership.GetNode(nodeID)
	if !found {
		return Node{}, false
	}
	return node, true
}

// Owners returns the list of nodes responsible for the key (primary + replicas).
func (m *Manager) Owners(key string, replicationFactor int) ([]Node, bool) {
	m.syncHashRing()
	m.mu.RLock()
	ring := m.hashRing
	m.mu.RUnlock()

	if ring == nil {
		return nil, false
	}
	nodeIDs, ok := ring.ReplicaOwners(key, replicationFactor)
	if !ok {
		return nil, false
	}
	var nodes []Node
	for _, id := range nodeIDs {
		node, found := m.membership.GetNode(id)
		if found {
			nodes = append(nodes, node)
		}
	}
	if len(nodes) == 0 {
		return nil, false
	}
	return nodes, true
}


// LocalNode returns the node that represents the local manager instance.
func (m *Manager) LocalNode() Node {
	m.syncHashRing()
	if m.localNodeID == "" {
		return Node{}
	}
	node, ok := m.membership.GetNode(m.localNodeID)
	if !ok {
		return Node{}
	}
	return node
}

// Leader returns the current leader as known to the manager's membership state.
func (m *Manager) Leader() (Node, bool) {
	m.syncHashRing()
	return m.membership.Leader()
}

func (m *Manager) syncHashRing() {
	if m == nil || m.membership == nil {
		return
	}
	if m.hashRing == nil {
		m.hashRing = clusterhashring.New(100)
	}

	nodes := m.membership.ListNodes()
	ring := clusterhashring.New(100)
	for _, node := range nodes {
		if node.ID == "" || node.Status != Alive {
			continue
		}
		ring.AddNode(node.ID)
	}

	m.mu.Lock()
	m.hashRing = ring
	m.mu.Unlock()
}

// IsLocalNode reports whether the given node ID matches the local node.
// Use this helper to avoid duplicating owner-vs-local comparisons at call sites.
func (m *Manager) IsLocalNode(nodeID string) bool {
	return m.localNodeID != "" && m.localNodeID == nodeID
}
