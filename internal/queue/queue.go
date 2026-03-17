package queue

import (
	"container/list"
	"sync"
	"time"

	"github.com/sahariardev/fluxGate/internal/codel"
	"github.com/sahariardev/fluxGate/internal/telemetry/logging"
	"go.uber.org/zap"
)

type Item struct {
	Class    string
	Enqueued time.Time
	Conn     any
}

type DropReason string

const (
	DropOverflow DropReason = "Overflow"
	DropCoDel    DropReason = "CoDel"
)

type ClassQueue struct {
	class string
	limit int

	ll     *list.List
	mu     sync.Mutex
	cdl    *codel.Controller
	close  bool
	logger *zap.Logger
}

type Params struct {
	Class         string
	Limit         int
	CoDelTarget   time.Duration
	CoDelInterval time.Duration
}

func New(p Params, logger *zap.Logger) *ClassQueue {
	if p.Limit < 0 {
		p.Limit = 0
	}

	q := &ClassQueue{
		class:  p.Class,
		limit:  p.Limit,
		ll:     list.New(),
		cdl:    codel.NewController(p.CoDelTarget, p.CoDelInterval),
		logger: logging.WithComponent(logger, "queue."+p.Class),
	}

	q.logger.Info("queue created", zap.Int("limit", p.Limit),
		zap.Duration("codel_target", p.CoDelTarget),
		zap.Duration("codel_interval", p.CoDelInterval))

	Depth.WithLabelValues(p.Class).Set(0)
	return q
}

func (q *ClassQueue) Enqueue(e Item) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.close {
		DropsTotal.WithLabelValues(q.class, string(DropOverflow)).Inc()
		q.logger.Warn("enqueue rejected, queue closed")
		return false
	}
	q.ll.PushBack(e)
	Depth.WithLabelValues(q.class).Set(float64(q.ll.Len()))
	return true
}

func (q *ClassQueue) Dequeue(now time.Time) (Item, DropReason, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.ll.Len() == 0 {
		return Item{}, "", false
	}

	q.cdl.BeginInterval(now)
	front := q.ll.Front()
	it := front.Value.(Item)
	soj := now.Sub(it.Enqueued.Local())
	q.cdl.TakeNote(soj)

	if q.cdl.ShouldDrop(soj) {
		q.ll.Remove(front)
		DropsTotal.WithLabelValues(q.class, string(DropCoDel)).Inc()
		Depth.WithLabelValues(q.class).Set(float64(q.ll.Len()))
		q.logger.Warn("item dropped by CoDel", zap.Duration("sojourn", soj))
		return it, DropCoDel, false
	}

	q.ll.Remove(front)
	Depth.WithLabelValues(q.class).Set(float64(q.ll.Len()))
	return it, "", true
}

func (q *ClassQueue) Requeue(it Item) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.close {
		DropsTotal.WithLabelValues(q.class, string(DropOverflow)).Inc()
		q.logger.Warn("requeue rejected, queue closed")
		return
	}

	if q.ll.Len() == q.limit {
		DropsTotal.WithLabelValues(q.class, string(DropOverflow)).Inc()
		q.logger.Warn("requeue rejected, queue full", zap.Int("limit", q.limit))
		return
	}

	q.ll.PushFront(it)
	Depth.WithLabelValues(q.class).Set(float64(q.ll.Len()))
}

func (q *ClassQueue) Close() []Item {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.close = true

	var items []Item

	for e := q.ll.Front(); e != nil; e = e.Next() {
		it := e.Value.(Item)
		items = append(items, it)
	}

	q.logger.Info("queue closed", zap.Int("drained_items", len(items)))
	q.ll.Init()
	Depth.WithLabelValues(q.class).Set(float64(q.ll.Len()))
	return items
}

func (q *ClassQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ll.Len()
}