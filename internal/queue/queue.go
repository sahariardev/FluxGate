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
	Conn     any
	EnquedAt time.Time
}

type DropReason string

const (
	DropOverFlow DropReason = "overflow"
	DropCoDel    DropReason = "codel"
)

type ClassQueue struct {
	items      *list.List
	controller *codel.Controller
	class      string
	limit      int

	close  bool
	mu     sync.Mutex
	logger *zap.Logger
}

type QueueParams struct {
	Class         string
	Limit         int
	CoDelTarget   time.Duration
	CoDelInterval time.Duration
}

func New(p QueueParams, logger *zap.Logger) *ClassQueue {
	if p.Limit < 0 {
		p.Limit = 0
	}

	q := &ClassQueue{
		items:      list.New(),
		class:      p.Class,
		limit:      p.Limit,
		controller: codel.NewController(p.CoDelTarget, p.CoDelInterval),
		close:      false,
		logger:     logging.WithComonent(logger, "queue."+p.Class),
	}

	q.logger.Info("queue created", zap.Int("limit", p.Limit),
		zap.Duration("codel_target", p.CoDelTarget),
		zap.Duration("codel_interval", p.CoDelInterval))

	Depth.WithLabelValues(p.Class).Set(0)

	return q
}

func (cq *ClassQueue) Enqueue(item Item) bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	defer cq.refreshDeptMetrics()

	if cq.close {
		DropTotals.WithLabelValues(cq.class, string(DropOverFlow)).Inc()
		cq.logger.Warn("queue closed, enqueue rejected")
		return false
	}

	if cq.limit == cq.items.Len() {
		DropTotals.WithLabelValues(cq.class, string(DropOverFlow)).Inc()
		return false
	}

	cq.items.PushBack(item)

	return true
}

func (cq *ClassQueue) Dequeue(now time.Time) (Item, DropReason, bool) {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	defer cq.refreshDeptMetrics()

	if cq.items.Len() == 0 {
		return Item{}, "", false
	}

	cq.controller.BeginInterval(now)
	item := cq.items.Front().Value.(Item)
	cq.items.Remove(cq.items.Front())
	soj := now.Sub(item.EnquedAt)
	cq.controller.TakeNote(soj)

	if cq.controller.ShouldDrop(soj) {
		DropTotals.WithLabelValues(cq.class, string(DropCoDel)).Inc()
		cq.logger.Warn("item dropped by CoDel", zap.Duration("sojurn", soj))

		return Item{}, DropCoDel, false
	}

	return item, "", true

}

func (cq *ClassQueue) ReQueue(item Item) {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	defer cq.refreshDeptMetrics()

	if cq.close {
		DropTotals.WithLabelValues(cq.class, string(DropOverFlow)).Inc()
		cq.logger.Warn("requeue rejected, queue closed")

		return
	}

	if cq.limit == cq.items.Len() {
		DropTotals.WithLabelValues(cq.class, string(DropOverFlow)).Inc()
		cq.logger.Warn("requeue rejected, queue full", zap.Int("limit", cq.limit))
		return
	}

	cq.items.PushFront(item)
}

func (cq *ClassQueue) Close() []Item {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	defer cq.refreshDeptMetrics()

	cq.close = true

	var items []Item

	for e := cq.items.Front(); e != nil; e = e.Next() {
		it := e.Value.(Item)

		items = append(items, it)
	}

	cq.logger.Info("queue closed", zap.Int("drained_items", len(items)))

	cq.items.Init()
	return items
}

func (cq *ClassQueue) Len() int {
	cq.mu.Lock()
	defer cq.mu.Unlock()

	return cq.items.Len()
}

func (cq *ClassQueue) refreshDeptMetrics() {
	Depth.WithLabelValues(cq.class).Set(float64(cq.items.Len()))
}
