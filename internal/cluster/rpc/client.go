package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client sends cluster RPC messages over HTTP.
type Client struct {
	httpClient *http.Client
}

// NewClient creates a new RPC client with a configurable timeout.
func NewClient(timeout time.Duration) *Client {
	return &Client{httpClient: &http.Client{Timeout: timeout}}
}

// SendHeartbeat sends a heartbeat to a cluster node.
func (c *Client) SendHeartbeat(ctx context.Context, address string, req HeartbeatRequest) (HeartbeatResponse, error) {
	var resp HeartbeatResponse
	if err := c.doJSON(ctx, address, "/cluster/heartbeat", req, &resp); err != nil {
		return HeartbeatResponse{}, err
	}
	return resp, nil
}

// JoinCluster sends a join request to a cluster node.
func (c *Client) JoinCluster(ctx context.Context, address string, req JoinRequest) (JoinResponse, error) {
	var resp JoinResponse
	if err := c.doJSON(ctx, address, "/cluster/join", req, &resp); err != nil {
		return JoinResponse{}, err
	}
	return resp, nil
}

// LeaveCluster sends a leave request to a cluster node.
func (c *Client) LeaveCluster(ctx context.Context, address string, req LeaveRequest) (LeaveResponse, error) {
	var resp LeaveResponse
	if err := c.doJSON(ctx, address, "/cluster/leave", req, &resp); err != nil {
		return LeaveResponse{}, err
	}
	return resp, nil
}

// AppendEntries sends append entries to a cluster node.
func (c *Client) AppendEntries(ctx context.Context, address string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	if err := c.doJSON(ctx, address, "/cluster/append", req, &resp); err != nil {
		return AppendEntriesResponse{}, err
	}
	return resp, nil
}

func (c *Client) doJSON(ctx context.Context, address, path string, payload any, out any) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(payload); err != nil {
		return fmt.Errorf("rpc client: encode payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+path, &body)
	if err != nil {
		return fmt.Errorf("rpc client: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("rpc client: request %s%s: %w", address, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("rpc client: request %s%s: unexpected status %s", address, path, resp.Status)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("rpc client: decode response: %w", err)
	}
	return nil
}

// KVGet sends a KV get request to a cluster node.
func (c *Client) KVGet(ctx context.Context, address string, req KVGetRequest) (KVGetResponse, error) {
	var resp KVGetResponse
	if err := c.doJSON(ctx, address, "/cluster/kv/get", req, &resp); err != nil {
		return KVGetResponse{}, err
	}
	return resp, nil
}

// KVPut sends a KV put request to a cluster node.
func (c *Client) KVPut(ctx context.Context, address string, req KVPutRequest) (KVPutResponse, error) {
	var resp KVPutResponse
	if err := c.doJSON(ctx, address, "/cluster/kv/put", req, &resp); err != nil {
		return KVPutResponse{}, err
	}
	return resp, nil
}

// KVDelete sends a KV delete request to a cluster node.
func (c *Client) KVDelete(ctx context.Context, address string, req KVDeleteRequest) (KVDeleteResponse, error) {
	var resp KVDeleteResponse
	if err := c.doJSON(ctx, address, "/cluster/kv/delete", req, &resp); err != nil {
		return KVDeleteResponse{}, err
	}
	return resp, nil
}

// ReplicaPut sends a replicated KV write to a replica node.
func (c *Client) ReplicaPut(ctx context.Context, address string, req ReplicaPutRequest) (ReplicaPutResponse, error) {
	var resp ReplicaPutResponse
	if err := c.doJSON(ctx, address, "/cluster/replica/put", req, &resp); err != nil {
		return ReplicaPutResponse{}, err
	}
	return resp, nil
}

// ReplicaDelete sends a replicated KV delete to a replica node.
func (c *Client) ReplicaDelete(ctx context.Context, address string, req ReplicaDeleteRequest) (ReplicaDeleteResponse, error) {
	var resp ReplicaDeleteResponse
	if err := c.doJSON(ctx, address, "/cluster/replica/delete", req, &resp); err != nil {
		return ReplicaDeleteResponse{}, err
	}
	return resp, nil
}


