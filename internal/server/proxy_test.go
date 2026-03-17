package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

// startEchoServer starts a simple TCP echo server on 127.0.0.1:0 and returns its address and a close function.
func startEchoServer(t *testing.T) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen failed: %v", err)
	}
	addr = ln.Addr().String()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stop:
					return
				default:
					// Listener closed or transient error: exit on stop.
					return
				}
			}
			// Echo in a goroutine; close when done.
			go func(c net.Conn) {
				defer c.Close()
				// Simple echo: copy what we read back to the same connection.
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()

	closeFn = func() {
		close(stop)
		_ = ln.Close()
		wg.Wait()
	}
	return addr, closeFn
}

// getFreeLocalAddr returns a free local address like 127.0.0.1:<port> by binding :0 and closing.
func getFreeLocalAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("getFreeLocalAddr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// waitForListening tries to connect to addr until success or timeout.
func waitForListening(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		d := net.Dialer{Timeout: 50 * time.Millisecond}
		conn, err := d.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start listening on %s within %s: last err: %v", addr, timeout, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestProxyEcho(t *testing.T) {
	// Upstream echo server.
	upAddr, upClose := startEchoServer(t)
	defer upClose()

	// Proxy address.
	proxyAddr := getFreeLocalAddr(t)

	opts := Options{
		DialTimeout:  500 * time.Millisecond,
		IdleTimeout:  0, // no idle cutoff during test
		DrainTimeout: 2 * time.Second,
		KeepAlive:    15 * time.Second,
		TCPNoDelay:   false,
		BufferBytes:  32 << 10,
		ListenAddr:   proxyAddr,
		UpstreamAddr: upAddr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewProxyServer(opts, zap.NewNop())

	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	// Wait for the proxy listener to be up.
	waitForListening(t, proxyAddr, 2*time.Second)

	// Connect a client to the proxy.
	c, err := net.DialTimeout("tcp", proxyAddr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("client dial proxy failed: %v", err)
	}
	defer c.Close()

	// Roundtrip: write -> read same bytes.
	msg := []byte("hello through proxy\n")
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write(msg); err != nil {
		t.Fatalf("client write failed: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("client read failed: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("unexpected echo, got %q want %q", buf, msg)
	}

	// Shutdown the proxy cleanly.
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("proxy exited with error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("proxy did not shut down in time")
	}
}

func TestProxyIdleTimeoutClosesIdleConn(t *testing.T) {
	// Upstream echo server.
	upAddr, upClose := startEchoServer(t)
	defer upClose()

	proxyAddr := getFreeLocalAddr(t)

	opts := Options{
		DialTimeout:  300 * time.Millisecond,
		IdleTimeout:  200 * time.Millisecond, // short idle timeout
		DrainTimeout: 2 * time.Second,
		KeepAlive:    10 * time.Second,
		ListenAddr:   proxyAddr,
		UpstreamAddr: upAddr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProxyServer(opts, zap.NewNop())

	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitForListening(t, proxyAddr, 2*time.Second)

	c, err := net.DialTimeout("tcp", proxyAddr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("client dial proxy failed: %v", err)
	}
	defer c.Close()

	// Do not send anything; the idle timeout should close the connection.
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	b := make([]byte, 1)
	_, rerr := c.Read(b) // Expect EOF or timeout after the proxy closes
	if rerr == nil {
		t.Fatal("expected read error due to idle timeout, got nil")
	}
	var ne net.Error
	if errors.Is(rerr, io.EOF) {
		// OK: closed by remote
	} else if errors.As(rerr, &ne) && ne.Timeout() {
		// If we hit client-side deadline, try a write to see if the conn is closed
		_ = c.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		if _, werr := c.Write([]byte("x")); werr == nil {
			t.Fatalf("expected write error to closed conn; got nil")
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("proxy exited with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not shut down in time")
	}
}

func TestProxyGracefulShutdownWithDrainTimeout(t *testing.T) {
	// Upstream echo server.
	upAddr, upClose := startEchoServer(t)
	defer upClose()

	proxyAddr := getFreeLocalAddr(t)

	opts := Options{
		DialTimeout:  300 * time.Millisecond,
		IdleTimeout:  0,                      // no idle cutoff; read blocks
		DrainTimeout: 200 * time.Millisecond, // we expect force-close after this
		KeepAlive:    10 * time.Second,
		ListenAddr:   proxyAddr,
		UpstreamAddr: upAddr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := NewProxyServer(opts, zap.NewNop())

	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitForListening(t, proxyAddr, 2*time.Second)

	// Connect a client.
	c, err := net.DialTimeout("tcp", proxyAddr, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("client dial proxy failed: %v", err)
	}
	defer c.Close()

	// Handshake: send 1 byte and read it back to ensure the proxy accepted
	// and the upstream echo is wired. This guarantees the conn is tracked.
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatalf("pre-shutdown write failed: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(1 * time.Second))
	b := make([]byte, 1)
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatalf("pre-shutdown read failed: %v", err)
	}
	if b[0] != 'x' {
		t.Fatalf("unexpected echo payload: %q", b[0])
	}
	// Clear the deadline again to avoid influencing shutdown behavior.
	_ = c.SetReadDeadline(time.Time{})

	// Trigger shutdown; with one active, idle connection, the proxy should
	// wait until DrainTimeout, then force-close.
	start := time.Now()
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("proxy exited with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not shut down within 2s")
	}
	elapsed := time.Since(start)

	// Allow for scheduling jitter. We only check a lower bound because upper
	// bound can vary under load/CI.
	minExpected := time.Duration(float64(opts.DrainTimeout) * 0.75)
	if elapsed < minExpected {
		t.Fatalf("shutdown returned too quickly (got %v, want >= %v ~ DrainTimeout %v)",
			elapsed, minExpected, opts.DrainTimeout)
	}

	// After shutdown, socket should be closed; reading should error/EOF.
	_ = c.SetReadDeadline(time.Now().Add(1 * time.Second))
	one := make([]byte, 1)
	if _, rerr := c.Read(one); rerr == nil {
		t.Fatal("expected read error or EOF on client after server shutdown")
	}
}

func TestProxyDialFailureDoesNotCrash(t *testing.T) {
	// Upstream not listening: pick a free port, then do not start a server on it.
	bogusUpstream := getFreeLocalAddr(t) // no listener will be started here

	proxyAddr := getFreeLocalAddr(t)
	opts := Options{
		DialTimeout:  100 * time.Millisecond,
		IdleTimeout:  0,
		DrainTimeout: 1 * time.Second,
		KeepAlive:    10 * time.Second,
		ListenAddr:   proxyAddr,
		UpstreamAddr: bogusUpstream,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProxyServer(opts, zap.NewNop())

	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitForListening(t, proxyAddr, 2*time.Second)

	// Connect a client; the proxy will accept, fail to dial upstream, and close.
	c, err := net.DialTimeout("tcp", proxyAddr, 300*time.Millisecond)
	if err != nil {
		t.Fatalf("client dial proxy failed: %v", err)
	}
	defer c.Close()

	// A small write will likely succeed (until proxy closes); read should fail quickly.
	_ = c.SetDeadline(time.Now().Add(500 * time.Millisecond))
	_, _ = c.Write([]byte("ping"))
	reader := bufio.NewReader(c)
	_, rerr := reader.ReadByte()
	if rerr == nil {
		t.Fatal("expected read error due to upstream dial failure")
	}

	// Cleanup
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("proxy exited with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not shut down in time")
	}
}

// ---------------------------------------------------------------------------
// Unit tests for defaultQueueParams
// ---------------------------------------------------------------------------

func TestDefaultQueueParams(t *testing.T) {
	cases := []struct {
		class         string
		wantLimit     int
		wantTarget    time.Duration
		wantInterval  time.Duration
		wantClassField string
	}{
		{"gold", 1000, 30 * time.Millisecond, 100 * time.Millisecond, "gold"},
		{"standard", 500, 20 * time.Millisecond, 100 * time.Millisecond, "standard"},
		{"background", 200, 10 * time.Millisecond, 100 * time.Millisecond, "background"},
		{"unknown", 200, 10 * time.Millisecond, 100 * time.Millisecond, "unknown"},
	}

	for _, tc := range cases {
		t.Run(tc.class, func(t *testing.T) {
			p := defaultQueueParams(tc.class)
			if p.Limit != tc.wantLimit {
				t.Errorf("Limit: got %d, want %d", p.Limit, tc.wantLimit)
			}
			if p.CoDelTarget != tc.wantTarget {
				t.Errorf("CoDelTarget: got %v, want %v", p.CoDelTarget, tc.wantTarget)
			}
			if p.CoDelInterval != tc.wantInterval {
				t.Errorf("CoDelInterval: got %v, want %v", p.CoDelInterval, tc.wantInterval)
			}
			if p.Class != tc.wantClassField {
				t.Errorf("Class: got %q, want %q", p.Class, tc.wantClassField)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Unit tests for NewProxyServer defaults
// ---------------------------------------------------------------------------

func TestNewProxyServerDefaultClass(t *testing.T) {
	s := NewProxyServer(Options{MaxInflight: 4}, zap.NewNop())
	if s.options.DefaultClass != "standard" {
		t.Errorf("DefaultClass: got %q, want \"standard\"", s.options.DefaultClass)
	}
}

func TestNewProxyServerDefaultClassNotOverridden(t *testing.T) {
	s := NewProxyServer(Options{DefaultClass: "gold"}, zap.NewNop())
	if s.options.DefaultClass != "gold" {
		t.Errorf("DefaultClass: got %q, want \"gold\"", s.options.DefaultClass)
	}
}

func TestNewProxyServerWorkerCountFromMaxInflight(t *testing.T) {
	s := NewProxyServer(Options{MaxInflight: 8}, zap.NewNop())
	if s.options.WorkerCount != 8 {
		t.Errorf("WorkerCount: got %d, want 8 (from MaxInflight)", s.options.WorkerCount)
	}
}

func TestNewProxyServerWorkerCountDefault(t *testing.T) {
	// Both WorkerCount and MaxInflight are zero → falls back to 64.
	s := NewProxyServer(Options{}, zap.NewNop())
	if s.options.WorkerCount != 64 {
		t.Errorf("WorkerCount: got %d, want 64", s.options.WorkerCount)
	}
}

func TestNewProxyServerWorkerCountExplicit(t *testing.T) {
	// Explicit WorkerCount is preserved even when MaxInflight is set.
	s := NewProxyServer(Options{WorkerCount: 16, MaxInflight: 32}, zap.NewNop())
	if s.options.WorkerCount != 16 {
		t.Errorf("WorkerCount: got %d, want 16", s.options.WorkerCount)
	}
}

func TestNewProxyServerQueuesCreated(t *testing.T) {
	s := NewProxyServer(Options{MaxInflight: 2}, zap.NewNop())
	for _, cls := range []string{"gold", "standard", "background"} {
		if _, ok := s.queues[cls]; !ok {
			t.Errorf("queue for class %q not created", cls)
		}
	}
}

// ---------------------------------------------------------------------------
// Unit tests for writeAll
// ---------------------------------------------------------------------------

func TestWriteAll(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	data := []byte("hello, writeAll!")

	errCh := make(chan error, 1)
	go func() {
		errCh <- writeAll(client, data)
	}()

	buf := make([]byte, len(data))
	if _, err := io.ReadFull(server, buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf) != string(data) {
		t.Errorf("got %q, want %q", buf, data)
	}
	if err := <-errCh; err != nil {
		t.Errorf("writeAll returned unexpected error: %v", err)
	}
}

func TestWriteAllClosedDest(t *testing.T) {
	client, server := net.Pipe()
	server.Close() // close the read side immediately

	err := writeAll(client, []byte("data that cannot be delivered"))
	client.Close()
	if err == nil {
		t.Error("expected error writing to closed connection, got nil")
	}
}

// ---------------------------------------------------------------------------
// Unit tests for copyWithIdle
// ---------------------------------------------------------------------------

func TestCopyWithIdleCopiesData(t *testing.T) {
	src, srcPeer := net.Pipe()
	dst, dstPeer := net.Pipe()
	defer src.Close()
	defer dst.Close()

	data := []byte("copy this data")

	copyDone := make(chan error, 1)
	go func() {
		copyDone <- copyWithIdle(dst, src, 0, 0)
	}()

	// Deliver data into src.
	if _, err := srcPeer.Write(data); err != nil {
		t.Fatalf("srcPeer write: %v", err)
	}

	// Read it out from dst.
	buf := make([]byte, len(data))
	_ = dstPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(dstPeer, buf); err != nil {
		t.Fatalf("dstPeer read: %v", err)
	}
	if string(buf) != string(data) {
		t.Errorf("got %q, want %q", buf, data)
	}

	// Closing srcPeer signals EOF; copyWithIdle should return a non-nil error.
	srcPeer.Close()
	dstPeer.Close()
	select {
	case err := <-copyDone:
		if err == nil {
			t.Error("expected non-nil error (EOF) from copyWithIdle, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("copyWithIdle did not return after src closed")
	}
}

func TestCopyWithIdleCustomBuffer(t *testing.T) {
	src, srcPeer := net.Pipe()
	dst, dstPeer := net.Pipe()
	defer src.Close()
	defer dst.Close()

	data := []byte("custom buffer path")

	copyDone := make(chan error, 1)
	go func() {
		// bufSize > 0 exercises the custom-buffer branch.
		copyDone <- copyWithIdle(dst, src, 512, 0)
	}()

	if _, err := srcPeer.Write(data); err != nil {
		t.Fatalf("srcPeer write: %v", err)
	}

	buf := make([]byte, len(data))
	_ = dstPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(dstPeer, buf); err != nil {
		t.Fatalf("dstPeer read: %v", err)
	}
	if string(buf) != string(data) {
		t.Errorf("got %q, want %q", buf, data)
	}

	srcPeer.Close()
	dstPeer.Close()
	select {
	case <-copyDone:
	case <-time.After(time.Second):
		t.Fatal("copyWithIdle did not return after src closed")
	}
}

func TestCopyWithIdleTimeout(t *testing.T) {
	src, srcPeer := net.Pipe()
	dst, dstPeer := net.Pipe()
	defer srcPeer.Close()
	defer dstPeer.Close()
	defer src.Close()
	defer dst.Close()

	idleTimeout := 100 * time.Millisecond

	copyDone := make(chan error, 1)
	go func() {
		copyDone <- copyWithIdle(dst, src, 0, idleTimeout)
	}()

	// Send nothing; copyWithIdle should hit the read deadline and return.
	select {
	case err := <-copyDone:
		if err == nil {
			t.Fatal("expected timeout error, got nil")
		}
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Errorf("expected net timeout error, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("copyWithIdle did not return after idle timeout")
	}
}

// ---------------------------------------------------------------------------
// Integration test: multiple concurrent clients
// ---------------------------------------------------------------------------

func TestProxyMultipleConcurrentClients(t *testing.T) {
	upAddr, upClose := startEchoServer(t)
	defer upClose()

	proxyAddr := getFreeLocalAddr(t)
	opts := Options{
		DialTimeout:  500 * time.Millisecond,
		IdleTimeout:  0,
		DrainTimeout: 2 * time.Second,
		MaxInflight:  10,
		BufferBytes:  4096,
		ListenAddr:   proxyAddr,
		UpstreamAddr: upAddr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProxyServer(opts, zap.NewNop())

	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitForListening(t, proxyAddr, 2*time.Second)

	const numClients = 5
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			c, err := net.DialTimeout("tcp", proxyAddr, 500*time.Millisecond)
			if err != nil {
				t.Errorf("client %d: dial failed: %v", id, err)
				return
			}
			defer c.Close()

			msg := fmt.Sprintf("hello from client %d\n", id)
			_ = c.SetDeadline(time.Now().Add(2 * time.Second))

			if _, err := c.Write([]byte(msg)); err != nil {
				t.Errorf("client %d: write failed: %v", id, err)
				return
			}
			buf := make([]byte, len(msg))
			if _, err := io.ReadFull(c, buf); err != nil {
				t.Errorf("client %d: read failed: %v", id, err)
				return
			}
			if string(buf) != msg {
				t.Errorf("client %d: echo mismatch: got %q, want %q", id, buf, msg)
			}
		}(i)
	}

	wg.Wait()
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("proxy exited with error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("proxy did not shut down in time")
	}
}
