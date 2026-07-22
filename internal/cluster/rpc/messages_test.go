package rpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMessageJSONRoundTrip(t *testing.T) {
	original := HeartbeatRequest{NodeID: "node-1", Address: "127.0.0.1:9000", Timestamp: time.Unix(123, 0).UTC()}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded HeartbeatRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.NodeID != original.NodeID || decoded.Address != original.Address || !decoded.Timestamp.Equal(original.Timestamp) {
		t.Fatalf("round trip mismatch: got %+v want %+v", decoded, original)
	}
}

func TestClientServerCommunication(t *testing.T) {
	server := httptest.NewServer(NewServer().Handler())
	defer server.Close()

	srv := NewServer()
	srv.HeartbeatHandler = func(req HeartbeatRequest) (HeartbeatResponse, error) {
		return HeartbeatResponse{Accepted: true, Message: "ok"}, nil
	}
	srv.JoinHandler = func(req JoinRequest) (JoinResponse, error) {
		return JoinResponse{Accepted: true, Message: "ok"}, nil
	}
	srv.LeaveHandler = func(req LeaveRequest) (LeaveResponse, error) {
		return LeaveResponse{Accepted: true, Message: "ok"}, nil
	}
	srv.AppendHandler = func(req AppendEntriesRequest) (AppendEntriesResponse, error) {
		return AppendEntriesResponse{Accepted: true, Message: "ok"}, nil
	}

	client := NewClient(time.Second)
	resp, err := client.SendHeartbeat(context.Background(), strings.TrimPrefix(server.URL, "http://"), HeartbeatRequest{NodeID: "node-1"})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	if !resp.Accepted {
		t.Fatal("heartbeat response not accepted")
	}
}

func TestInvalidPayloads(t *testing.T) {
	server := httptest.NewServer(NewServer().Handler())
	defer server.Close()

	client := NewClient(time.Second)
	_, err := client.SendHeartbeat(context.Background(), strings.TrimPrefix(server.URL, "http://"), HeartbeatRequest{})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestTimeoutHandling(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.SendHeartbeat(ctx, strings.TrimPrefix(server.URL, "http://"), HeartbeatRequest{NodeID: "node-1"})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// TestReplicaGetRPC_LocalRead verifies that the ReplicaGet RPC endpoint reads
// only from its handler and returns the stored value.
func TestReplicaGetRPC_LocalRead(t *testing.T) {
	stored := map[string][]byte{"mykey": []byte("myvalue")}

	srv := NewServer()
	srv.ReplicaGetHandler = func(req ReplicaGetRequest) (ReplicaGetResponse, error) {
		v, ok := stored[req.Key]
		if !ok {
			return ReplicaGetResponse{Found: false}, nil
		}
		return ReplicaGetResponse{Found: true, Value: v}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := NewClient(time.Second)
	addr := strings.TrimPrefix(ts.URL, "http://")

	// Existing key
	resp, err := client.ReplicaGet(context.Background(), addr, ReplicaGetRequest{Key: "mykey"})
	if err != nil {
		t.Fatalf("ReplicaGet error: %v", err)
	}
	if !resp.Found {
		t.Fatal("expected Found=true")
	}
	if string(resp.Value) != "myvalue" {
		t.Fatalf("value = %q, want %q", resp.Value, "myvalue")
	}

	// Missing key
	resp2, err := client.ReplicaGet(context.Background(), addr, ReplicaGetRequest{Key: "nokey"})
	if err != nil {
		t.Fatalf("ReplicaGet error: %v", err)
	}
	if resp2.Found {
		t.Fatal("expected Found=false for missing key")
	}
}

// TestReplicaGetRPC_EmptyKey verifies that an empty key is rejected with 400.
func TestReplicaGetRPC_EmptyKey(t *testing.T) {
	srv := NewServer()
	srv.ReplicaGetHandler = func(req ReplicaGetRequest) (ReplicaGetResponse, error) {
		return ReplicaGetResponse{Found: false}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Send empty key via raw HTTP to inspect status code
	resp, err := http.Post(ts.URL+"/cluster/replica/get", "application/json", strings.NewReader(`{"key":""}`))
	if err != nil {
		t.Fatalf("post error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", resp.StatusCode)
	}
}

// TestReplicaGetRPC_NilHandler ensures a nil handler returns 501.
func TestReplicaGetRPC_NilHandler(t *testing.T) {
	srv := NewServer()
	// ReplicaGetHandler is intentionally nil
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/cluster/replica/get", "application/json", strings.NewReader(`{"key":"k"}`))
	if err != nil {
		t.Fatalf("post error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("expected 501 Not Implemented, got %d", resp.StatusCode)
	}
}

// TestReplicaGetRPC_MethodNotAllowed verifies that non-POST requests are rejected.
func TestReplicaGetRPC_MethodNotAllowed(t *testing.T) {
	srv := NewServer()
	srv.ReplicaGetHandler = func(req ReplicaGetRequest) (ReplicaGetResponse, error) {
		return ReplicaGetResponse{Found: true}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/cluster/replica/get", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", resp.StatusCode)
	}
}
