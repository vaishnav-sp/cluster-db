// Package sharding provides document shard ownership helpers.
package sharding

import (
	"hash/fnv"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
)

// ComputeShard returns the FNV-1a shard hash for a document ID.
func ComputeShard(documentID string) uint32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(documentID))
	return hasher.Sum32()
}

// ShardOwner resolves a document ID through the cluster manager's existing
// consistent hash ring.
func ShardOwner(manager *cluster.Manager, documentID string) (cluster.Node, bool) {
	if manager == nil {
		return cluster.Node{}, false
	}
	return manager.Owner(documentID)
}
