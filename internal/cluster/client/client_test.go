package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPingSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(time.Second)
	if err := client.Ping(context.Background(), strings.TrimPrefix(server.URL, "http://")); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestPingFailure(t *testing.T) {
	client := NewClient(time.Second)
	if err := client.Ping(context.Background(), "127.0.0.1:1"); err == nil {
		t.Fatal("expected ping failure")
	}
}

func TestGetSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/foo" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("bar"))
	}))
	defer server.Close()

	client := NewClient(time.Second)
	value, err := client.Get(context.Background(), strings.TrimPrefix(server.URL, "http://"), "foo")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "bar" {
		t.Fatalf("value = %q, want bar", value)
	}
}

func TestGet404(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("missing"))
	}))
	defer server.Close()

	client := NewClient(time.Second)
	_, err := client.Get(context.Background(), strings.TrimPrefix(server.URL, "http://"), "missing")
	if err == nil {
		t.Fatal("expected get error")
	}
}

func TestPutSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := NewClient(time.Second)
	if err := client.Put(context.Background(), strings.TrimPrefix(server.URL, "http://"), "foo", []byte("bar")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
}

func TestDeleteSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewClient(time.Second)
	if err := client.Delete(context.Background(), strings.TrimPrefix(server.URL, "http://"), "foo"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
}

func TestTimeoutBehavior(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.Ping(ctx, strings.TrimPrefix(server.URL, "http://")); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestInvalidAddress(t *testing.T) {
	client := NewClient(time.Second)
	if err := client.Ping(context.Background(), ""); err == nil {
		t.Fatal("expected invalid address error")
	}
	if err := client.Ping(context.Background(), "http://bad"); err == nil {
		t.Fatal("expected invalid address error")
	}
}

func TestClientUsesContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewClient(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Get(ctx, strings.TrimPrefix(server.URL, "http://"), "foo"); err == nil {
		t.Fatal("expected context error")
	}
}

func TestGetPathEscaping(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/kv/a%2Fb" {
			t.Fatalf("unexpected escaped path: %s", r.URL.EscapedPath())
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := NewClient(time.Second)
	value, err := client.Get(context.Background(), strings.TrimPrefix(server.URL, "http://"), "a/b")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "ok" {
		t.Fatalf("value = %q, want ok", value)
	}
}

func ExampleClient_Ping() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(time.Second)
	_ = client.Ping(context.Background(), strings.TrimPrefix(server.URL, "http://"))
	fmt.Println("ok")
	// Output: ok
}
