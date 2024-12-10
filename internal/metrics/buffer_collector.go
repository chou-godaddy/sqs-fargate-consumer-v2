package metrics

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"sqs-fargate-consumer-v2/internal/models"
)

type BufferMetricsCollector struct {
	// Frequently accessed metrics - atomic values
	bufferLength     atomic.Int64
	bufferCapacity   atomic.Int64
	totalMessagesIn  atomic.Int64
	totalMessagesOut atomic.Int64
	totalSize        atomic.Int64

	// Priority tracking for reporting purposes only
	highPriorityMessages   atomic.Int64
	mediumPriorityMessages atomic.Int64
	lowPriorityMessages    atomic.Int64

	// Metrics for monitoring - protected by mutex
	mu                     sync.RWMutex
	messageWaitTimes       map[string]time.Time
	waitTimeHistogram      map[string]int64
	lastProcessingRateCalc time.Time
}

func NewBufferMetricsCollector() *BufferMetricsCollector {
	return &BufferMetricsCollector{
		messageWaitTimes:  make(map[string]time.Time),
		waitTimeHistogram: make(map[string]int64),
	}
}

// Fast access methods for operational metrics
func (c *BufferMetricsCollector) GetBufferUtilization() (float64, bool) {
	if c == nil {
		return 0, false
	}

	bufLen := float64(c.bufferLength.Load())
	bufCap := float64(c.bufferCapacity.Load())

	if bufCap == 0 {
		return 0, false
	}

	return bufLen / bufCap, true
}

func (c *BufferMetricsCollector) GetMessageCounts() (in, out int64, ok bool) {
	if c == nil {
		return 0, 0, false
	}
	in = c.totalMessagesIn.Load()
	out = c.totalMessagesOut.Load()
	return in, out, true
}

func (c *BufferMetricsCollector) GetPriorityMessageCounts() (high, medium, low int64, ok bool) {
	if c == nil {
		return 0, 0, 0, false
	}
	return c.highPriorityMessages.Load(), c.mediumPriorityMessages.Load(), c.lowPriorityMessages.Load(), true
}

func (c *BufferMetricsCollector) GetTotalSize() (size int64, ok bool) {
	if c == nil {
		return 0, false
	}
	return c.totalSize.Load(), true
}

// BufferMetricsEmitter interface implementation
func (c *BufferMetricsCollector) OnMessageEnqueued(message *models.Message) {
	c.totalMessagesIn.Add(1)
	c.totalSize.Add(message.Size)

	// Still track by priority for monitoring purposes
	switch message.Priority {
	case models.PriorityHigh:
		c.highPriorityMessages.Add(1)
	case models.PriorityMedium:
		c.mediumPriorityMessages.Add(1)
	case models.PriorityLow:
		c.lowPriorityMessages.Add(1)
	}

	c.mu.Lock()
	c.messageWaitTimes[message.MessageID] = time.Now()
	c.mu.Unlock()

	log.Printf("[BufferMetricsCollector] Message enqueued. Total messages in: %d",
		c.totalMessagesIn.Load())
}

func (c *BufferMetricsCollector) OnMessageDequeued(message *models.Message) {
	c.totalMessagesOut.Add(1)
	c.totalSize.Add(-message.Size)

	c.mu.Lock()
	if enqueueTime, exists := c.messageWaitTimes[message.MessageID]; exists {
		waitTime := time.Since(enqueueTime)
		bucket := getWaitTimeBucket(waitTime)
		c.waitTimeHistogram[bucket]++
		delete(c.messageWaitTimes, message.MessageID)
	}
	c.mu.Unlock()

	log.Printf("[BufferMetricsCollector] Message dequeued. Total messages out: %d",
		c.totalMessagesOut.Load())
}

func (c *BufferMetricsCollector) OnBufferSizeChanged(currentSize, capacity int) {
	c.bufferLength.Store(int64(currentSize))
	c.bufferCapacity.Store(int64(capacity))
}

// GetMetrics returns all metrics for monitoring/CloudWatch
func (c *BufferMetricsCollector) GetMetrics() (models.BufferMetrics, bool) {
	if c == nil {
		return models.BufferMetrics{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Calculate wait time metrics
	var avgWaitTime time.Duration
	var maxWaitTime time.Duration
	now := time.Now()

	waitTimeCount := len(c.messageWaitTimes)
	if waitTimeCount > 0 {
		var totalWait time.Duration
		for _, enqueueTime := range c.messageWaitTimes {
			wait := now.Sub(enqueueTime)
			totalWait += wait
			if wait > maxWaitTime {
				maxWaitTime = wait
			}
		}
		avgWaitTime = totalWait / time.Duration(waitTimeCount)
	}

	// Calculate processing rate
	var processingRate float64
	if !c.lastProcessingRateCalc.IsZero() {
		duration := now.Sub(c.lastProcessingRateCalc).Seconds()
		if duration > 0 {
			processingRate = float64(c.totalMessagesOut.Load()) / duration
		}
	}
	c.lastProcessingRateCalc = now

	bufferUsage, _ := c.GetBufferUtilization()

	// Create copy of wait time histogram
	histogramCopy := make(map[string]int64, len(c.waitTimeHistogram))
	for k, v := range c.waitTimeHistogram {
		histogramCopy[k] = v
	}

	return models.BufferMetrics{
		BufferUsage:            bufferUsage,
		CurrentSize:            c.bufferLength.Load(),
		BufferCapacity:         c.bufferCapacity.Load(),
		TotalSize:              c.totalSize.Load(),
		TotalMessagesIn:        c.totalMessagesIn.Load(),
		TotalMessagesOut:       c.totalMessagesOut.Load(),
		HighPriorityMessages:   c.highPriorityMessages.Load(),
		MediumPriorityMessages: c.mediumPriorityMessages.Load(),
		LowPriorityMessages:    c.lowPriorityMessages.Load(),
		AverageWaitTime:        avgWaitTime,
		MaxWaitTime:            maxWaitTime,
		WaitTimeHistogram:      histogramCopy,
		MessageProcessingRate:  processingRate,
		LastUpdateTime:         now,
	}, true
}

func getWaitTimeBucket(d time.Duration) string {
	switch {
	case d < time.Second:
		return "0-1s"
	case d < 5*time.Second:
		return "1-5s"
	case d < 10*time.Second:
		return "5-10s"
	case d < 30*time.Second:
		return "10-30s"
	case d < time.Minute:
		return "30-60s"
	default:
		return ">60s"
	}
}
