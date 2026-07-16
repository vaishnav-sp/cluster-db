package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client performs simple HTTP requests between ClusterDB nodes.
type Client struct {
	httpClient *http.Client
}

// NewClient creates a node-to-node HTTP client with the provided timeout.
func NewClient(timeout time.Duration) *Client {
	return &Client{httpClient: &http.Client{Timeout: timeout}}
}

// Ping checks whether a node is reachable.
func (c *Client) Ping(ctx context.Context, address string) error {
	if err := validateAddress(address); err != nil {
		return err
	}
	url := joinURL(address, "/health")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("cluster client: create ping request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cluster client: ping %s: %w", address, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cluster client: ping %s: unexpected status %s", address, resp.Status)
	}
	return nil
}

// Get fetches a value for key from the node.
func (c *Client) Get(ctx context.Context, address, key string) ([]byte, error) {
	if err := validateAddress(address); err != nil {
		return nil, err
	}
	requestURL := joinURL(address, fmt.Sprintf("/kv/%s", url.PathEscape(key)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("cluster client: create get request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cluster client: get %s: %w", address, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cluster client: read get response: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("cluster client: get %s: %s: %s", address, resp.Status, strings.TrimSpace(string(body)))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cluster client: get %s: unexpected status %s", address, resp.Status)
	}
	return body, nil
}

// Put writes a key/value pair to the node.
func (c *Client) Put(ctx context.Context, address, key string, value []byte) error {
	if err := validateAddress(address); err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]string{"value": string(value)})
	if err != nil {
		return fmt.Errorf("cluster client: marshal put payload: %w", err)
	}
	requestURL := joinURL(address, fmt.Sprintf("/kv/%s", url.PathEscape(key)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("cluster client: create put request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cluster client: put %s: %w", address, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("cluster client: put %s: unexpected status %s", address, resp.Status)
	}
	return nil
}

// Delete removes a key from the node.
func (c *Client) Delete(ctx context.Context, address, key string) error {
	if err := validateAddress(address); err != nil {
		return err
	}
	requestURL := joinURL(address, fmt.Sprintf("/kv/%s", url.PathEscape(key)))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURL, nil)
	if err != nil {
		return fmt.Errorf("cluster client: create delete request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cluster client: delete %s: %w", address, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("cluster client: delete %s: unexpected status %s", address, resp.Status)
	}
	return nil
}

func validateAddress(address string) error {
	if strings.TrimSpace(address) == "" {
		return fmt.Errorf("cluster client: invalid address: empty")
	}
	parsed, err := url.Parse("http://" + address)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("cluster client: invalid address %q", address)
	}
	return nil
}

func joinURL(address, path string) string {
	return "http://" + strings.TrimRight(address, "/") + path
}
