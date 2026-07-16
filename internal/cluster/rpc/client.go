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
