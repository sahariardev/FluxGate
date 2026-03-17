package queue

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	Depth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxy_queue_depth",
			Help: "Current queue depth per class",
		},
		[]string{"class"},
	)
	WaitSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "proxy_queue_wait_seconds",
			Help:    "Current queue wait seconds per class",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"class"},
	)
	DropsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_queue_drops_total",
			Help: "Total number of dropped packets",
		},
		[]string{"class", "reason"},
	)
	SchedAdmissions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_queue_sched_admissions",
			Help: "Number of schedules admitted",
		},
		[]string{"class"},
	)
)

var registerOnce sync.Once

func RegisterMetrics() {
	registerOnce.Do(func() {
		prometheus.MustRegister(Depth, WaitSeconds, DropsTotal, SchedAdmissions)
		for _, c := range []string{"gold", "standard", "background"} {
			Depth.WithLabelValues(c).Set(0)
			WaitSeconds.WithLabelValues(c).Observe(0)
			DropsTotal.WithLabelValues(c, "Overflow").Add(0)
			DropsTotal.WithLabelValues(c, "CoDel").Add(0)
			SchedAdmissions.WithLabelValues(c).Add(0)
		}
	})
}
