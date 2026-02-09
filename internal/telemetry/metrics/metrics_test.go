package metrics

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestHealthAndMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewMetricsServer(Options{
		ListenerAddress: "127.0.0.1:0", // pick a free port
		HealthPath:    "/healthz",
		MetricsPath:   "/metrics",
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start metrics server: %v", err)
	}

	// Wait a moment for readiness
	time.Sleep(100 * time.Millisecond)
	base := "http://" + srv.Addr()

	// Health
	res, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("health get: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("health status: %d", res.StatusCode)
	}

	// Metrics
	res, err = http.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("metrics get: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("metrics status: %d", res.StatusCode)
	}
	b, _ := io.ReadAll(res.Body)
	if len(b) == 0 {
		t.Fatalf("expected some metrics payload")
	}
}
