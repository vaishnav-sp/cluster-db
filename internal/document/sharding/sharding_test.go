package sharding

import "testing"

func TestComputeShardUsesFNV1a(t *testing.T) {
	if got, want := ComputeShard("doc1"), uint32(1726715500); got != want {
		t.Fatalf("ComputeShard(\"doc1\") = %d, want %d", got, want)
	}
}
