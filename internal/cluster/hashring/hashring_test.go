package hashring

import (
	"testing"
)

func TestHashRingEmptyRing(t *testing.T) {
	ring := New(5)
	got, ok := ring.GetNode("key")
	if ok {
		t.Fatalf("GetNode() = %q, want no node", got)
	}
	if got := ring.Count(); got != 0 {
		t.Fatalf("Count() = %d, want 0", got)
	}
	if nodes := ring.Nodes(); len(nodes) != 0 {
		t.Fatalf("Nodes() = %v, want empty", nodes)
	}
}

func TestHashRingSingleNode(t *testing.T) {
	ring := New(10)
	ring.AddNode("node-1")
	got, ok := ring.GetNode("key-1")
	if !ok || got != "node-1" {
		t.Fatalf("GetNode() = %q, %v, want %q, true", got, ok, "node-1")
	}
	if got := ring.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
}

func TestHashRingMultipleNodes(t *testing.T) {
	ring := New(10)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")
	if got := ring.Count(); got != 3 {
		t.Fatalf("Count() = %d, want 3", got)
	}
	nodes := ring.Nodes()
	if len(nodes) != 3 {
		t.Fatalf("len(Nodes()) = %d, want 3", len(nodes))
	}
}

func TestHashRingDuplicateAddNodeIsIgnored(t *testing.T) {
	ring := New(10)
	ring.AddNode("node-1")
	ring.AddNode("node-1")
	if got := ring.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
}

func TestHashRingRemoveNode(t *testing.T) {
	ring := New(10)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.RemoveNode("node-1")
	if got := ring.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
	got, ok := ring.GetNode("key-1")
	if !ok || got != "node-2" {
		t.Fatalf("GetNode() = %q, %v, want %q, true", got, ok, "node-2")
	}
}

func TestHashRingRemoveMissingNodeIsNoOp(t *testing.T) {
	ring := New(10)
	ring.AddNode("node-1")
	ring.RemoveNode("node-2")
	if got := ring.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
}

func TestHashRingLookupStability(t *testing.T) {
	ring := New(20)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")
	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		first, _ := ring.GetNode(key)
		second, _ := ring.GetNode(key)
		if first != second {
			t.Fatalf("GetNode(%q) unstable: %q != %q", key, first, second)
		}
	}
}

func TestHashRingDeterministicBehavior(t *testing.T) {
	ring1 := New(20)
	ring2 := New(20)
	for _, node := range []string{"node-1", "node-2", "node-3"} {
		ring1.AddNode(node)
		ring2.AddNode(node)
	}
	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		got1, _ := ring1.GetNode(key)
		got2, _ := ring2.GetNode(key)
		if got1 != got2 {
			t.Fatalf("GetNode(%q) = %q, want %q", key, got1, got2)
		}
	}
}
