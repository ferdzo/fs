package api

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestUnreadBodyDrainRespectsBodyTimeout is a real-TCP regression for the
// post-handler drain net/http performs when a handler never reads the request
// body. Before the fix the middleware cleared the read deadline as soon as the
// handler returned, so a client that declared Content-Length: 1 and sent no
// body could hold the connection (and its listener slot) forever.
func TestUnreadBodyDrainRespectsBodyTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond

	handler, _ := newShortTimeoutHandler(t, timeout)
	ts := httptest.NewServer(handler.router)
	defer ts.Close()
	addr := ts.Listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// GET /healthz advertises a one-byte body that we never send. The handler
	// does not read the body, so net/http tries to drain it after the response.
	if _, err := io.WriteString(conn,
		"GET /healthz HTTP/1.1\r\nHost: "+addr+"\r\nContent-Length: 1\r\n\r\n"); err != nil {
		t.Fatalf("write request: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Once the body deadline passes the server must stop waiting for the
	// missing byte and close the connection; a read timeout here means the
	// drain was unbounded.
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the server to close the connection after the body deadline")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("unread-body drain exceeded the body timeout: %v", err)
	}
}

// TestKeepaliveSurvivesUnreadBodyDrain ensures bounding the drain does not
// regress normal HTTP/1.1 keepalive: the same TCP connection must still serve a
// second request, even after idling longer than the body read timeout.
func TestKeepaliveSurvivesUnreadBodyDrain(t *testing.T) {
	const timeout = 150 * time.Millisecond

	handler, _ := newShortTimeoutHandler(t, timeout)
	ts := httptest.NewServer(handler.router)
	defer ts.Close()
	addr := ts.Listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	// First request sends a body the handler ignores; net/http drains it.
	if _, err := io.WriteString(conn,
		"GET /healthz HTTP/1.1\r\nHost: "+addr+"\r\nContent-Length: 1\r\n\r\nx"); err != nil {
		t.Fatalf("write first request: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read first response: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", resp.StatusCode)
	}

	// Idle past the body read timeout, then reuse the connection.
	time.Sleep(2 * timeout)
	if _, err := io.WriteString(conn,
		"GET /healthz HTTP/1.1\r\nHost: "+addr+"\r\n\r\n"); err != nil {
		t.Fatalf("write second request: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	resp2, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("second request on keepalive connection failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want 200", resp2.StatusCode)
	}
}
