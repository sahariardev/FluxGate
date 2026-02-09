package codel

import "time"

type Controller struct {
	target time.Duration
	interval time.Duration
	minSojourn time.Duration
	dropping bool
	endInterval time.Time
	hasSeenPacket bool
}

func NewController(target, intgerval time.Duration) *Controller {
	return &Controller{
		target: target,
		interval: intgerval,
	}
}

func (c *Controller) BeginInterval(now time.Time) {
	if c.endInterval.IsZero() {
		c.endInterval = now.Add(c.interval)
		c.hasSeenPacket = false
	    c.minSojourn = 0
		return
	}

	if !now.Before(c.endInterval) {
		if c.hasSeenPacket && c.minSojourn > c.target {
			c.dropping = true
		} else {
			c.dropping = false
		}

		c.endInterval = now.Add(c.interval)
		c.hasSeenPacket = false
		c.minSojourn = 0;
	}
}

func (c *Controller) TakeNote(sojorun time.Duration) {
	if !c.hasSeenPacket || c.minSojourn > sojorun {
		c.minSojourn = sojorun
		c.hasSeenPacket = true
	}
}

func (c *Controller) ShouldDrop(headSojorun time.Duration) bool {
	return c.dropping && headSojorun > c.target
}