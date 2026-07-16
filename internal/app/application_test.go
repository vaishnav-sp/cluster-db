package app

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
)

func TestApplicationInitializesClusterManager(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	app := &Application{Logger: logger, Cluster: cluster.NewManager(cluster.NewMembership(), logger, time.Second, time.Second, "node-1", "127.0.0.1:9000")}

	if err := app.Cluster.Start(context.Background()); err != nil {
		t.Fatalf("start cluster manager: %v", err)
	}
	defer app.Cluster.Stop()

	leader, ok := app.Cluster.Membership().Leader()
	if !ok || leader.ID != "node-1" {
		t.Fatalf("leader = %+v, want node-1", leader)
	}
}
