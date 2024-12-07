package metrics

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sqs-fargate-consumer-v2/internal/models"
)

type BufferMetricsCollector struct {
	// Frequently accessed metrics - atomic values
	highPriorityLength     atomic.Int64
	mediumPriorityLength   atomic.Int64
	lowPriorityLength      atomic.Int64
	highPriorityCapacity   atomic.Int64
	mediumPriorityCapacity atomic.Int64
	lowPriorityCapacity    atomic.Int64
	totalMessagesIn        atomic.Int64
	totalMessagesOut       atomic.Int64
	totalSize              atomic.Int64
	highPriorityMessages   atomic.Int64
	mediumPriorityMessages atomic.Int64
	lowPriorityMessages    atomic.Int64
	overflowCount          atomic.Int32

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
func (c *BufferMetricsCollector) GetBufferUtilization() (high, medium, low float64, ok bool) {
	if c == nil {
		return 0, 0, 0, false
	}

	highLen := float64(c.highPriorityLength.Load())
	medLen := float64(c.mediumPriorityLength.Load())
	lowLen := float64(c.lowPriorityLength.Load())

	highCap := float64(c.highPriorityCapacity.Load())
	medCap := float64(c.mediumPriorityCapacity.Load())
	lowCap := float64(c.lowPriorityCapacity.Load())

	// If no capacities are set, metrics aren't initialized
	if highCap == 0 && medCap == 0 && lowCap == 0 {
		return 0, 0, 0, false
	}

	if highCap > 0 {
		high = highLen / highCap
	}
	if medCap > 0 {
		medium = medLen / medCap
	}
	if lowCap > 0 {
		low = lowLen / lowCap
	}

	return high, medium, low, true
}

func (c *BufferMetricsCollector) GetMessageCounts() (in, out int64, ok bool) {
	if c == nil {
		return 0, 0, false
	}
	in = c.totalMessagesIn.Load()
	out = c.totalMessagesOut.Load()
	if in == 0 && out == 0 {
		log.Printf("[BufferMetricsCollector] Warning: Both in/out counts are 0. Called from: %s",
			identifyStackTrace()) // Add helper to get caller info
	}
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

func (c *BufferMetricsCollector) GetOverflowCount() (count int32, ok bool) {
	if c == nil {
		return 0, false
	}
	return c.overflowCount.Load(), true
}

// BufferMetricsEmitter interface implementation
func (c *BufferMetricsCollector) OnMessageEnqueued(message *models.Message) {
	prevIn := c.totalMessagesIn.Load()
	c.totalMessagesIn.Add(1)
	log.Printf("[BufferMetricsCollector] Message enqueued. In count: %d -> %d",
		prevIn, c.totalMessagesIn.Load())
	c.totalSize.Add(message.Size)

	switch message.Priority {
	case models.PriorityHigh:
		c.highPriorityMessages.Add(1)
	case models.PriorityMedium:
		c.mediumPriorityMessages.Add(1)
	case models.PriorityLow:
		c.lowPriorityMessages.Add(1)
	}

	// Track wait time
	c.mu.Lock()
	c.messageWaitTimes[message.MessageID] = time.Now()
	c.mu.Unlock()
}

func (c *BufferMetricsCollector) OnMessageDequeued(message *models.Message) {
	prevOut := c.totalMessagesOut.Load()
	c.totalMessagesOut.Add(1)
	log.Printf("[BufferMetricsCollector] Message dequeued. Out count: %d -> %d",
		prevOut, c.totalMessagesOut.Load())
	c.totalSize.Add(-message.Size)

	// Update wait time metrics
	c.mu.Lock()
	if enqueueTime, exists := c.messageWaitTimes[message.MessageID]; exists {
		waitTime := time.Since(enqueueTime)
		bucket := getWaitTimeBucket(waitTime)
		c.waitTimeHistogram[bucket]++
		delete(c.messageWaitTimes, message.MessageID)
	}
	c.mu.Unlock()
}

func (c *BufferMetricsCollector) OnBufferOverflow(priority models.Priority) {
	c.overflowCount.Add(1)
}

func (c *BufferMetricsCollector) OnQueueSizeChanged(priority models.Priority, currentSize, capacity int) {
	switch priority {
	case models.PriorityHigh:
		c.highPriorityLength.Store(int64(currentSize))
		c.highPriorityCapacity.Store(int64(capacity))
	case models.PriorityMedium:
		c.mediumPriorityLength.Store(int64(currentSize))
		c.mediumPriorityCapacity.Store(int64(capacity))
	case models.PriorityLow:
		c.lowPriorityLength.Store(int64(currentSize))
		c.lowPriorityCapacity.Store(int64(capacity))
	}
}

// GetMetrics returns all metrics for monitoring/CloudWatch
func (c *BufferMetricsCollector) GetMetrics() (models.BufferMetrics, bool) {
	if c == nil {
		return models.BufferMetrics{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Calculate wait time metrics safely
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

	// Calculate processing rate safely
	var processingRate float64
	if !c.lastProcessingRateCalc.IsZero() {
		duration := now.Sub(c.lastProcessingRateCalc).Seconds()
		if duration > 0 {
			processingRate = float64(c.totalMessagesOut.Load()) / duration
		}
	}
	c.lastProcessingRateCalc = now

	// Get real-time metrics
	high, medium, low, ok := c.GetBufferUtilization()
	if !ok {
		return models.BufferMetrics{}, false
	}

	// Create copy of wait time histogram
	histogramCopy := make(map[string]int64, len(c.waitTimeHistogram))
	for k, v := range c.waitTimeHistogram {
		histogramCopy[k] = v
	}

	return models.BufferMetrics{
		HighPriorityUsage:      high,
		MediumPriorityUsage:    medium,
		LowPriorityUsage:       low,
		TotalSize:              c.totalSize.Load(),
		TotalMessagesIn:        c.totalMessagesIn.Load(),
		TotalMessagesOut:       c.totalMessagesOut.Load(),
		HighPriorityMessages:   c.highPriorityMessages.Load(),
		MediumPriorityMessages: c.mediumPriorityMessages.Load(),
		LowPriorityMessages:    c.lowPriorityMessages.Load(),
		OverflowCount:          c.overflowCount.Load(),
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

func identifyStackTrace() string {
	pc := make([]uintptr, 10)
	n := runtime.Callers(2, pc)
	if n == 0 {
		return "unknown"
	}
	pc = pc[:n]
	frames := runtime.CallersFrames(pc)
	var trace strings.Builder
	for {
		frame, more := frames.Next()
		if !strings.Contains(frame.Function, "runtime.") {
			trace.WriteString(fmt.Sprintf("%s:%d", frame.Function, frame.Line))
			break
		}
		if !more {
			break
		}
	}
	return trace.String()
}
