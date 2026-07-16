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
