package queue

import (
	"sync"
	"time"

	"github.com/sahariardev/fluxGate/internal/telemetry/logging"
	"go.uber.org/zap"
)

type Config struct {
	Weights  map[string]int
	MinShare map[string]float64
	Classes  []string
}

type Scheduler struct {
	cfg          Config
	deficit      map[string]int
	lastIndex    int
	mu           sync.Mutex
	totalAdmits  int64
	admitByClass map[string]int64
	logger       *zap.Logger
}

func NewScheduler(cfg Config, logger *zap.Logger) *Scheduler {
	if cfg.Weights == nil {
		cfg.Weights = map[string]int{"gold": 5, "standard": 3, "background": 1}
	}

	if cfg.MinShare == nil {
		cfg.MinShare = map[string]float64{"gold": .5}
	}

	if len(cfg.Classes) == 0 {
		cfg.Classes = []string{"gold", "standard", "background"}
	}

	l := logging.WithComonent(logger, "scheduler")
	l.Info("scheduler created",
		zap.Strings("classes", cfg.Classes),
		zap.Any("weights", cfg.Weights),
		zap.Any("min_share", cfg.MinShare))

	return &Scheduler{
		cfg:          cfg,
		deficit:      make(map[string]int),
		admitByClass: make(map[string]int64),
		logger:       l,
	}
}

func (s *Scheduler) chooseClass(nonEmpty map[string]bool) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.totalAdmits >= 20 {
		for cls, min := range s.cfg.MinShare {
			if !nonEmpty[cls] || min <= 0 {
				continue
			}

			share := float64(s.admitByClass[cls]) / float64(s.totalAdmits)

			if share+1.0/float64(s.totalAdmits) < min {
				return cls
			}
		}
	}

	for i := 0; i < len(s.cfg.Classes); i++ {
		s.lastIndex = (s.lastIndex + 1) % len(s.cfg.Classes)
		cls := s.cfg.Classes[s.lastIndex]

		if !nonEmpty[cls] {
			continue
		}

		quantum := s.cfg.Weights[cls]

		if quantum <= 1 {
			quantum = 1
		}

		s.deficit[cls] += quantum

		if s.deficit[cls] >= 0 {
			s.deficit[cls]--
			return cls
		}
	}

	for _, cls := range s.cfg.Classes {
		if nonEmpty[cls] {
			return cls
		}
	}

	return ""
}

func (s *Scheduler) NoteAdmit(cls string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalAdmits++
	s.admitByClass[cls]++
	SchedAdmissions.WithLabelValues(cls).Inc()
	s.logger.Debug("admitted", zap.String("class", cls), zap.Int64("total", s.totalAdmits))
}

func (s *Scheduler) Next(queues map[string]ClassQueue, now time.Time) (Item, bool, bool) {
	nonEmpty := map[string]bool{}

	for cls, q := range queues{
		if q.Len() > 0 {
			nonEmpty[cls] = true
		}
	}

	if len(nonEmpty) == 0 {
		return Item{}, false, false
	}

	cls := s.chooseClass(nonEmpty)

	if cls == "" {
		return Item{}, false, false
	}

	queue := queues[cls]

	it, reason, ok := queue.Dequeue(now)
	
	if !ok && reason == DropCoDel {
		s.logger.Warn("coDel drop during scheduling", zap.String("class", cls))
		return Item{Class: cls}, false, true
	}

	if ok {
		s.NoteAdmit(cls)
		return it, true, false
	}

	return Item{}, false, false
}
