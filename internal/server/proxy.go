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

	WorkerCount  int
	DefaultClass string
	QueueParams  map[string]queue.Params
	SchedulerCfg queue.Config
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

	queues    map[string]*queue.ClassQueue
	scheduler *queue.Scheduler
	workReady chan struct{}
}

func NewProxyServer(options Options, logger *zap.Logger) *ProxyServer {
	if options.DefaultClass == "" {
		options.DefaultClass = "standard"
	}

	if options.WorkerCount <= 0 {
		options.WorkerCount = options.MaxInflight
	}

	if options.WorkerCount <= 0 {
		options.WorkerCount = 64
	}

	classes := []string{"gold", "standard", "background"}
	queues := make(map[string]*queue.ClassQueue, len(classes))

	for _, cls := range classes {
		params, ok := options.QueueParams[cls]

		if !ok {
			params = defaultQueueParams(cls)
		}
		queues[cls] = queue.New(params, logger)
	}

	queue.RegisterMetrics()

	return &ProxyServer{
		options:   options,
		conns:     make(map[net.Conn]struct{}),
		limiter:   control.NewLimit(options.MaxInflight),
		logger:    logging.WithComponent(logger, "proxy"),
		scheduler: queue.NewScheduler(options.SchedulerCfg, logger),
		workReady: make(chan struct{}, options.WorkerCount),
		queues:    queues,
	}
}

func defaultQueueParams(cls string) queue.Params {
	switch cls {
	case "gold":
		return queue.Params{Limit: 1000, CoDelTarget: 30 * time.Millisecond, CoDelInterval: 100 * time.Millisecond, Class: cls}
	case "standard":
		return queue.Params{Limit: 500, CoDelTarget: 20 * time.Millisecond, CoDelInterval: 100 * time.Millisecond, Class: cls}
	default:
		return queue.Params{Limit: 200, CoDelTarget: 10 * time.Millisecond, CoDelInterval: 100 * time.Millisecond, Class: cls}
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

	for i := 0; i < s.options.WorkerCount; i++ {
		s.wg.Add(1)
		go s.worker(ctx)
	}

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

		cls := s.classify(conn)

		item := queue.Item{
			Class:    cls,
			Enqueued: time.Now(),
			Conn:     conn,
		}

		q, ok := s.queues[cls]

		if !ok {
			_ = conn.Close()
			continue
		}

		if !q.Enqueue(item) {
			metricsRejectedTotal.WithLabelValues("queue_full").Inc()
			_ = conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			_, _ = conn.Write([]byte("proxy busy\n\n"))
			_ = conn.Close()
			continue
		}

		select {
		case s.workReady <- struct{}{}:
		default:
		}
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

func (s *ProxyServer) classify(_ net.Conn) string {
	return s.options.DefaultClass
}

func (s *ProxyServer) worker(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.workReady:
		}

		for {
			item, admitted, codelDrop := s.scheduler.Next(s.queues, time.Now())

			if codelDrop {
				if conn, ok := item.Conn.(net.Conn); ok && conn != nil {
					metricsRejectedTotal.WithLabelValues("codel").Inc()
					_ = conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
					_, _ = conn.Write([]byte("dropped\n\n"))
					_ = conn.Close()
				}

				continue
			}

			if admitted {
				break
			}

			if !s.limiter.TryAcquire() {
				s.queues[item.Class].Requeue(item)
				metricsRejectedTotal.WithLabelValues("inflight_full").Inc()
				select {
				case s.workReady <- struct{}{}:
				default:
				}
				break
			}

			conn := item.Conn.(net.Conn)
			metricAdmissionTotal.Inc()
			metricInflightCurrent.Set(float64(s.limiter.Size()))

			s.mu.Lock()
			s.conns[conn] = struct{}{}
			s.mu.Unlock()

			s.wg.Add(1)
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
	}
}

func (s *ProxyServer) forceCloseAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for c := range s.conns {
		_ = c.Close()
	}

	for _, q := range s.queues {
		unserved := q.Close()
		for _, it := range unserved {
			if conn, ok := it.Conn.(net.Conn); ok && conn != nil {
				_ = conn.Close()
			}
		}
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
