package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Option struct {
	HealthPath      string
	MetricsPath     string
	ListenerAddress string
}

type Server struct {
	listener net.Listener
	httpServ *http.Server
	isReady  atomic.Bool
	opts     Option
}

func NewMetricsServer(opts Option) *Server {
	return &Server{
		opts: opts,
	}
}

func (s *Server) mux() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(s.opts.HealthPath, func(w http.ResponseWriter, r *http.Request) {
		if s.isReady.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("not ready"))
		}
	})

	mux.Handle(s.opts.MetricsPath, promhttp.Handler())

	mux.HandleFunc("/debug/pprof", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func (s *Server) start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.opts.ListenerAddress)

	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.listener = ln

	s.httpServ = &http.Server{
		Handler:      s.mux(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		s.isReady.Store(true)
	}()

	go func() {
		<-ctx.Done()
		_ = s.httpServ.Shutdown(context.Background())
	}()

	go func() {
		s.httpServ.Serve(ln)
	}()

	return nil
}
