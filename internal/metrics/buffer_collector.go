package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"sqs-fargate-consumer-v2/internal/models"
)

type BufferMetricsCollector struct {
	// Metrics storage
	totalSize           atomic.Int64
	overflowCount       atomic.Int32
	totalMessagesIn     atomic.Int64
	totalMessagesOut    atomic.Int64
	messagesPerPriority map[models.Priority]*atomic.Int64

	// Queue usage tracking
	highPriorityUsage   atomic.Int64
	mediumPriorityUsage atomic.Int64
	lowPriorityUsage    atomic.Int64

	// Wait time tracking
	waitTimes    []time.Duration
	waitTimeIdx  int
	maxWaitTimes int
	waitTimeMu   sync.RWMutex

	// Last update tracking
	lastMetricsUpdate time.Time
	mu                sync.RWMutex
}

func NewBufferMetricsCollector() *BufferMetricsCollector {
	return &BufferMetricsCollector{
		messagesPerPriority: map[models.Priority]*atomic.Int64{
			models.PriorityHigh:   {},
			models.PriorityMedium: {},
			models.PriorityLow:    {},
		},
		maxWaitTimes:      1000,
		waitTimes:         make([]time.Duration, 1000),
		lastMetricsUpdate: time.Now(),
	}
}

func (c *BufferMetricsCollector) OnMessageEnqueued(msg *models.Message) {
	if msg == nil {
		return
	}

	c.totalSize.Add(msg.Size)
	c.totalMessagesIn.Add(1)
	c.messagesPerPriority[msg.Priority].Add(1)
}

func (c *BufferMetricsCollector) OnMessageDequeued(msg *models.Message) {
	if msg == nil {
		return
	}

	c.totalSize.Add(-msg.Size)
	c.totalMessagesOut.Add(1)

	if !msg.EnqueuedAt.IsZero() {
		waitTime := time.Since(msg.EnqueuedAt)
		c.recordWaitTime(waitTime)
	}
}

func (c *BufferMetricsCollector) OnBufferOverflow(priority models.Priority) {
	c.overflowCount.Add(1)
}

func (c *BufferMetricsCollector) OnQueueSizeChanged(priority models.Priority, currentSize, capacity int) {
	usage := float64(currentSize) / float64(capacity)
	usageScaled := int64(usage * 100)

	switch priority {
	case models.PriorityHigh:
		c.highPriorityUsage.Store(usageScaled)
	case models.PriorityMedium:
		c.mediumPriorityUsage.Store(usageScaled)
	case models.PriorityLow:
		c.lowPriorityUsage.Store(usageScaled)
	}
}

func (c *BufferMetricsCollector) recordWaitTime(waitTime time.Duration) {
	c.waitTimeMu.Lock()
	defer c.waitTimeMu.Unlock()

	c.waitTimes[c.waitTimeIdx] = waitTime
	c.waitTimeIdx = (c.waitTimeIdx + 1) % c.maxWaitTimes
}

func (c *BufferMetricsCollector) calculateWaitTimeMetrics() (time.Duration, time.Duration, map[string]int64) {
	c.waitTimeMu.RLock()
	defer c.waitTimeMu.RUnlock()

	if len(c.waitTimes) == 0 {
		return 0, 0, make(map[string]int64)
	}

	var total time.Duration
	var max time.Duration
	histogram := make(map[string]int64)

	count := 0
	for _, wt := range c.waitTimes {
		if wt == 0 { // Skip empty slots
			continue
		}

		count++
		total += wt
		if wt > max {
			max = wt
		}

		// Update histogram
		switch {
		case wt < 100*time.Millisecond:
			histogram["0-100ms"]++
		case wt < 500*time.Millisecond:
			histogram["100-500ms"]++
		case wt < time.Second:
			histogram["500-1000ms"]++
		case wt < 5*time.Second:
			histogram["1s-5s"]++
		case wt < 10*time.Second:
			histogram["5s-10s"]++
		default:
			histogram["10s+"]++
		}
	}

	if count == 0 {
		return 0, 0, histogram
	}

	return total / time.Duration(count), max, histogram
}

func (c *BufferMetricsCollector) GetMetrics() models.BufferMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()

	avgWait, maxWait, histogram := c.calculateWaitTimeMetrics()
	timeSinceLastUpdate := time.Since(c.lastMetricsUpdate)
	messageRate := float64(c.totalMessagesOut.Load()) / timeSinceLastUpdate.Seconds()

	metrics := models.BufferMetrics{
		HighPriorityUsage:      float64(c.highPriorityUsage.Load()) / 100.0,
		MediumPriorityUsage:    float64(c.mediumPriorityUsage.Load()) / 100.0,
		LowPriorityUsage:       float64(c.lowPriorityUsage.Load()) / 100.0,
		TotalSize:              c.totalSize.Load(),
		OverflowCount:          c.overflowCount.Load(),
		AverageWaitTime:        avgWait,
		MaxWaitTime:            maxWait,
		WaitTimeHistogram:      histogram,
		TotalMessagesIn:        c.totalMessagesIn.Load(),
		TotalMessagesOut:       c.totalMessagesOut.Load(),
		MessageProcessingRate:  messageRate,
		HighPriorityMessages:   c.messagesPerPriority[models.PriorityHigh].Load(),
		MediumPriorityMessages: c.messagesPerPriority[models.PriorityMedium].Load(),
		LowPriorityMessages:    c.messagesPerPriority[models.PriorityLow].Load(),
	}

	c.lastMetricsUpdate = time.Now()
	return metrics
}
