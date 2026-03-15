package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sahariardev/fluxGate/internal/control"
	"github.com/sahariardev/fluxGate/internal/telemetry/logging"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	metricInflightCapacity = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "proxy_inflight_capacity",
		Help: "The capacity of the inflight proxy server",
	})

	metricInflightCurrent = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "proxy_inflight_requests",
		Help: "The total number of inflight requests",
	})

	metricAdmissionTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "proxy_admission_total",
		Help: "The total number of admission requests",
	})

	metricsRejectedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_rejected_total",
		Help: "The total number of rejected requests",
	}, []string{"reason"})
)

func init() {
	prometheus.MustRegister(metricInflightCapacity,
		metricInflightCurrent,
		metricAdmissionTotal,
		metricsRejectedTotal)
}

type Options struct {
	DialTimeout  time.Duration
	IdleTimeout  time.Duration
	DrainTimeout time.Duration

	KeepAlive  time.Duration
	TCPNoDelay bool

	BufferBytes  int
	ListenAddr   string
	UpstreamAddr string

	MaxInflight int
}

type ProxyServer struct {
	options  Options
	ln       net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	shutdown int32
	limiter  *control.Limit
	logger   *zap.Logger
}

func NewProxyServer(options Options, logger *zap.Logger) *ProxyServer {
	return &ProxyServer{
		options: options,
		conns:   make(map[net.Conn]struct{}),
		limiter: control.NewLimit(int32(options.MaxInflight)),
		logger:  logging.WithComponent(logger, "proxy"),
	}
}

func (s *ProxyServer) Run(ctx context.Context) error {
	metricInflightCapacity.Set(float64(s.options.MaxInflight))
	metricInflightCurrent.Set(0)

	ln, err := net.Listen("tcp", s.options.ListenAddr)

	if err != nil {
		return fmt.Errorf("failed to listen %s: %w", s.options.ListenAddr,
			err)
	}

	s.ln = ln

	s.logger.Info("proxy server started",
		zap.String("listen", s.options.ListenAddr),
		zap.String("upstream", s.options.UpstreamAddr))

	go func() {
		<-ctx.Done()
		atomic.StoreInt32(&s.shutdown, 1)
		err = s.ln.Close()

		if err != nil {
			s.logger.Warn("failed to close listener", zap.Error(err))
		}
	}()

	var tempDelay time.Duration
	for {
		conn, err := s.ln.Accept()

		if err != nil {

			if atomic.LoadInt32(&s.shutdown) == 1 {
				break
			}

			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 50 * time.Millisecond
				} else {
					tempDelay *= 2

					if tempDelay > 1*time.Second {
						tempDelay = 1 * time.Second
					}
				}

				s.logger.Warn("temporary accept error, retrying",
					zap.Error(err), zap.Duration("retry_in", tempDelay))

				time.Sleep(tempDelay)
				continue
			}

			s.logger.Error("failed to accept connection", zap.Error(err))
			continue
		}

		if !s.limiter.TryAcquire() {
			metricsRejectedTotal.WithLabelValues("rejected").Inc()
			//write response message to connection
			_ = conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			_, _ = conn.Write([]byte("proxy busy\n\n"))
			_ = conn.Close()

			continue
		}

		metricAdmissionTotal.Inc()
		metricInflightCurrent.Set(float64(s.limiter.Size()))

		tempDelay = 0
		s.wg.Add(1)
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() {
				s.limiter.Release()
				metricInflightCurrent.Set(float64(s.limiter.Size()))

				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()

				_ = c.Close()
			}()
			s.handleConnection(ctx, c)
		}(conn)
	}

	//shutting down server
	if s.options.DrainTimeout > 0 {
		drainDone := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(drainDone)
		}()

		select {
		case <-drainDone:
		case <-time.After(s.options.DrainTimeout):
			s.forceCloseAll()
			<-drainDone
		}

	} else {
		s.wg.Wait()
	}

	s.logger.Info("proxy server shut down")
	return nil
}

func (s *ProxyServer) forceCloseAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for c := range s.conns {
		_ = c.Close()
	}
}

func (s *ProxyServer) handleConnection(ctx context.Context, client net.Conn) {
	clientTCP, _ := client.(*net.TCPConn)
	if clientTCP != nil {
		_ = clientTCP.SetKeepAlive(s.options.KeepAlive > 0)

		if s.options.KeepAlive > 0 {
			_ = clientTCP.SetKeepAlivePeriod(s.options.KeepAlive)
		}

		_ = clientTCP.SetNoDelay(s.options.TCPNoDelay)
	}

	dialCtx := ctx

	if s.options.DialTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, s.options.DialTimeout)

		defer cancel()
	}

	d := &net.Dialer{
		Timeout:   s.options.DialTimeout,
		KeepAlive: s.options.KeepAlive,
	}

	upstream, err := d.DialContext(dialCtx, "tcp", s.options.UpstreamAddr)
	if err != nil {
		s.logger.Error("failed to dial upstream",
			zap.String("upstream", s.options.UpstreamAddr), zap.Error(err))
		return
	}

	defer upstream.Close()

	upstreamTCP, _ := upstream.(*net.TCPConn)

	if upstreamTCP != nil {
		_ = upstreamTCP.SetNoDelay(s.options.TCPNoDelay)
	}

	if s.options.IdleTimeout > 0 {
		_ = client.SetDeadline(time.Now().Add(s.options.IdleTimeout))
		_ = upstream.SetDeadline(time.Now().Add(s.options.IdleTimeout))
	}

	var cpWg sync.WaitGroup
	cpWg.Add(2)

	errorOnce := sync.Once{}
	logFirstError := func(direction string, err error) {
		if err == nil {
			return
		}

		errorOnce.Do(func() {
			s.logger.Debug("pipe error", zap.String("direction", direction), zap.Error(err))
		})
	}

	//client->upstream
	go func() {
		defer cpWg.Done()

		if err := copyWithIdle(upstream, client, s.options.BufferBytes, s.options.IdleTimeout); err != nil {
			logFirstError("pipe client->upstream", err)
		}

		if tc, ok := upstream.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}

		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.CloseRead()
		}
	}()

	//upstream -> client
	go func() {
		defer cpWg.Done()

		if err := copyWithIdle(client, upstream, s.options.BufferBytes, s.options.IdleTimeout); err != nil {
			logFirstError("pipe upstream->client", err)
		}

		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}

		if tc, ok := upstream.(*net.TCPConn); ok {
			_ = tc.CloseRead()
		}
	}()

	cpWg.Wait()
}

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, 32<<10)
	},
}

func copyWithIdle(dst net.Conn, src net.Conn, bufSize int, idle time.Duration) error {
	var buf []byte

	if bufSize > 0 {
		buf = make([]byte, bufSize)
	} else {
		buf = bufPool.Get().([]byte)
		defer bufPool.Put(buf)
	}

	for {
		if idle > 0 {
			_ = src.SetReadDeadline(time.Now().Add(idle))
		}

		n, err := src.Read(buf)

		if n > 0 {
			if idle > 0 {
				_ = dst.SetWriteDeadline(time.Now().Add(idle))
			}

			if werr := writeAll(dst, buf[:n]); werr != nil {
				return werr
			}
		}

		if err != nil {
			return err
		}
	}
}

func writeAll(w net.Conn, buf []byte) error {
	total := 0

	for total < len(buf) {
		n, err := w.Write(buf[total:])
		if n > 0 {
			total += n
		}

		if err != nil {
			return err
		}
	}

	return nil
}
