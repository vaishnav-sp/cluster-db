package gossip

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
)

// Engine manages background gossip state dissemination and failure detection.
type Engine struct {
	mu             sync.RWMutex
	localNodeID    string
	localAddress   string
	membership     *cluster.Membership
	client         *clusterRPC.Client
	logger         *zap.Logger
	interval       time.Duration
	fanout         int
	failureTimeout time.Duration
	selector       *PeerSelector
	stopCh         chan struct{}
	stoppedCh      chan struct{}
	started        bool
}

// Config holds configuration parameters for the gossip engine.
type Config struct {
	LocalNodeID    string
	LocalAddress   string
	Membership     *cluster.Membership
	Client         *clusterRPC.Client
	Logger         *zap.Logger
	Interval       time.Duration
	Fanout         int
	FailureTimeout time.Duration
}

// NewEngine constructs a new gossip Engine.
func NewEngine(cfg Config) *Engine {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.Fanout <= 0 {
		cfg.Fanout = 3
	}
	if cfg.FailureTimeout <= 0 {
		cfg.FailureTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}

	return &Engine{
		localNodeID:    cfg.LocalNodeID,
		localAddress:   cfg.LocalAddress,
		membership:     cfg.Membership,
		client:         cfg.Client,
		logger:         cfg.Logger,
		interval:       cfg.Interval,
		fanout:         cfg.Fanout,
		failureTimeout: cfg.FailureTimeout,
		selector:       NewPeerSelector(),
	}
}

// Start begins the background gossip loop.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return nil
	}
	e.started = true
	e.stopCh = make(chan struct{})
	e.stoppedCh = make(chan struct{})
	e.mu.Unlock()

	go e.runLoop(ctx)
	return nil
}

// Stop gracefully terminates the gossip background loop.
func (e *Engine) Stop() {
	e.mu.Lock()
	if !e.started {
		e.mu.Unlock()
		return
	}
	close(e.stopCh)
	e.started = false
	stoppedCh := e.stoppedCh
	e.mu.Unlock()

	<-stoppedCh
}

func (e *Engine) runLoop(ctx context.Context) {
	defer close(e.stoppedCh)

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.gossipCycle(ctx)
			e.evaluateFailureDetector()
		}
	}
}

func (e *Engine) gossipCycle(ctx context.Context) {
	if e.membership == nil {
		return
	}

	nodes := e.membership.ListNodes()
	peers := e.selector.SelectRandomPeers(nodes, e.fanout, e.localNodeID)
	if len(peers) == 0 {
		return
	}

	localSnapshot := e.GetLocalStateSnapshot()

	for _, peer := range peers {
		if peer.Address == "" {
			continue
		}

		req := clusterRPC.GossipRequest{
			SenderID:   e.localNodeID,
			SenderAddr: e.localAddress,
			Nodes:      localSnapshot,
		}

		e.logger.Debug("gossip exchange initiated",
			zap.String("local_node", e.localNodeID),
			zap.String("peer_node", peer.ID),
			zap.String("peer_addr", peer.Address),
		)

		if e.client != nil {
			gossipCtx, cancel := context.WithTimeout(ctx, e.interval)
			resp, err := e.client.Gossip(gossipCtx, peer.Address, req)
			cancel()

			if err == nil && resp.Accepted {
				e.logger.Info("gossip exchange",
					zap.String("peer_id", peer.ID),
					zap.Int("remote_nodes_count", len(resp.Nodes)),
				)
				if len(resp.Nodes) > 0 {
					e.MergeMembership(ToGossipNodeStates(resp.Nodes))
				}
			}
		}
	}
}

// HandleInboundGossip processes an incoming gossip request and returns the local membership state.
func (e *Engine) HandleInboundGossip(req clusterRPC.GossipRequest) clusterRPC.GossipResponse {
	if len(req.Nodes) > 0 {
		e.MergeMembership(ToGossipNodeStates(req.Nodes))
	}

	snapshot := e.GetLocalStateSnapshot()
	return clusterRPC.GossipResponse{
		Accepted: true,
		Message:  "ok",
		Nodes:    snapshot,
	}
}

// GetLocalStateSnapshot returns current membership state formatted for RPC/Gossip.
func (e *Engine) GetLocalStateSnapshot() []clusterRPC.GossipNodeInfo {
	if e.membership == nil {
		return nil
	}

	nodes := e.membership.ListNodes()
	res := make([]clusterRPC.GossipNodeInfo, 0, len(nodes))
	for _, n := range nodes {
		res = append(res, clusterRPC.GossipNodeInfo{
			ID:            n.ID,
			Address:       n.Address,
			Status:        n.Status.String(),
			Version:       n.Version,
			LastHeartbeat: n.LastHeartbeat,
		})
	}
	return res
}

// MergeMembership applies deterministic merge rules on remote membership states.
func (e *Engine) MergeMembership(remoteNodes []GossipNodeState) {
	if e.membership == nil {
		return
	}

	for _, remote := range remoteNodes {
		if remote.NodeID == "" {
			continue
		}

		existing, found := e.membership.GetNode(remote.NodeID)
		if !found {
			e.membership.AddNode(remote.ToNode())
			e.logger.Info("membership merged",
				zap.String("action", "add"),
				zap.String("node_id", remote.NodeID),
				zap.String("status", remote.Status.String()),
			)
			continue
		}

		shouldUpdate, updatedNode := MergeNodeStates(existing, remote.ToNode())
		if shouldUpdate {
			e.membership.AddNode(updatedNode)

			if existing.Status != updatedNode.Status {
				if updatedNode.Status == cluster.Suspect {
					e.logger.Warn("node suspected",
						zap.String("node_id", updatedNode.ID),
						zap.String("previous_status", existing.Status.String()),
					)
				} else if updatedNode.Status == cluster.Dead {
					e.logger.Error("node dead",
						zap.String("node_id", updatedNode.ID),
						zap.String("previous_status", existing.Status.String()),
					)
				}
			}

			e.logger.Info("membership merged",
				zap.String("action", "update"),
				zap.String("node_id", updatedNode.ID),
				zap.String("status", updatedNode.Status.String()),
				zap.String("version", updatedNode.Version),
			)
		}
	}
}

func (e *Engine) evaluateFailureDetector() {
	if e.membership == nil {
		return
	}

	now := time.Now().UTC()
	nodes := e.membership.ListNodes()

	for _, n := range nodes {
		if n.ID == e.localNodeID {
			continue
		}
		if n.Status == cluster.Dead {
			continue
		}

		elapsed := now.Sub(n.LastHeartbeat)
		if n.Status == cluster.Alive && elapsed > e.failureTimeout {
			_ = e.membership.MarkSuspect(n.ID)
			e.logger.Warn("node suspected",
				zap.String("node_id", n.ID),
				zap.Duration("elapsed", elapsed),
			)
		} else if n.Status == cluster.Suspect && elapsed > 2*e.failureTimeout {
			_ = e.membership.MarkDead(n.ID)
			e.logger.Error("node dead",
				zap.String("node_id", n.ID),
				zap.Duration("elapsed", elapsed),
			)
		}
	}
}

// MergeNodeStates compares local and remote node state and returns whether the state should update and the new state.
func MergeNodeStates(existing, remote cluster.Node) (bool, cluster.Node) {
	vComp := CompareVersions(remote.Version, existing.Version)
	if vComp > 0 {
		return true, remote
	}
	if vComp < 0 {
		return false, existing
	}

	merged := existing
	updated := false

	if remote.LastHeartbeat.After(existing.LastHeartbeat) {
		merged.LastHeartbeat = remote.LastHeartbeat
		updated = true
	}

	if remote.LastHeartbeat.After(existing.LastHeartbeat) || remote.LastHeartbeat.Equal(existing.LastHeartbeat) {
		if statusPriority(remote.Status) > statusPriority(existing.Status) {
			merged.Status = remote.Status
			updated = true
		}
	}

	if remote.Address != "" && existing.Address != remote.Address {
		merged.Address = remote.Address
		updated = true
	}

	return updated, merged
}

func statusPriority(s cluster.Status) int {
	switch s {
	case cluster.Alive:
		return 3
	case cluster.Suspect:
		return 2
	case cluster.Dead:
		return 1
	default:
		return 0
	}
}

// CompareVersions compares two version strings. Returns 1 if v1 > v2, -1 if v1 < v2, 0 if v1 == v2.
func CompareVersions(v1, v2 string) int {
	if v1 == v2 {
		return 0
	}
	if v1 == "" {
		return -1
	}
	if v2 == "" {
		return 1
	}

	n1, err1 := parseVersionNum(v1)
	n2, err2 := parseVersionNum(v2)

	if err1 == nil && err2 == nil {
		if n1 > n2 {
			return 1
		}
		if n1 < n2 {
			return -1
		}
		return 0
	}

	return strings.Compare(v1, v2)
}

func parseVersionNum(v string) (int64, error) {
	cleaned := strings.TrimPrefix(strings.TrimPrefix(v, "cluster/"), "v")
	return strconv.ParseInt(cleaned, 10, 64)
}
