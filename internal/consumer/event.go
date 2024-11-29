package consumer

import (
	"context"
	"fmt"
	"sqs-fargate-consumer-v2/internal/config"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Priority int

const (
	PriorityLow Priority = iota + 1
	PriorityMedium
	PriorityHigh
)

type Event struct {
	ID           string
	QueueURL     string
	Message      *types.Message
	Priority     Priority
	ReceivedAt   time.Time
	ProcessCount int
	Size         int64 // Message size in bytes
	EnqueuedAt   time.Time
}

type BufferMetrics struct {
	HighPriorityUtilization   float64
	MediumPriorityUtilization float64
	LowPriorityUtilization    float64
	HighPriorityOverflows     int64
	MediumPriorityOverflows   int64
	LowPriorityOverflows      int64
	AverageWaitTime           time.Duration
	DrainRate                 float64 // Messages per second
	TotalMemoryUsage          int64   // Bytes
	CurrentSize               int     // Current total buffer size
}

type EventBuffer struct {
	highPriority   chan *Event
	mediumPriority chan *Event
	lowPriority    chan *Event
	config         config.BufferConfig

	// Atomic counters for metrics
	highOverflows   atomic.Int64
	mediumOverflows atomic.Int64
	lowOverflows    atomic.Int64
	totalMemory     atomic.Int64

	// Buffer scaling
	currentSize   atomic.Int32
	lastScaleTime time.Time
	scalingMu     sync.Mutex

	// Monitoring
	metricsWindow []bufferMetricPoint
	metricsMu     sync.RWMutex
}

type bufferMetricPoint struct {
	timestamp   time.Time
	drainRate   float64
	utilization float64
	overflows   int64
	memoryUsage int64
}

func NewEventBuffer(cfg config.BufferConfig, workerCount int) *EventBuffer {
	// Calculate initial sizes based on worker count and configuration
	totalSize := calculateInitialSize(cfg, workerCount)

	highSize := int(float64(totalSize) * cfg.HighPriorityPercent)
	mediumSize := int(float64(totalSize) * cfg.MediumPriorityPercent)
	lowSize := int(float64(totalSize) * cfg.LowPriorityPercent)

	return &EventBuffer{
		highPriority:   make(chan *Event, highSize),
		mediumPriority: make(chan *Event, mediumSize),
		lowPriority:    make(chan *Event, lowSize),
		config:         cfg,
		metricsWindow:  make([]bufferMetricPoint, 0, 1000),
		lastScaleTime:  time.Now(),
	}
}

func calculateInitialSize(cfg config.BufferConfig, workerCount int) int {
	// Base size on worker count and typical batch size
	baseSize := workerCount * 10 // 10 messages per batch

	// Ensure size is within configured limits
	if baseSize < cfg.InitialSize {
		baseSize = cfg.InitialSize
	}
	if baseSize > cfg.MaxSize {
		baseSize = cfg.MaxSize
	}

	return baseSize
}

func (eb *EventBuffer) Push(event *Event) error {
	// Set enqueue time for latency tracking
	event.EnqueuedAt = time.Now()

	// Check message size limits
	if event.Size > eb.config.MaxMessageSize {
		return fmt.Errorf("message exceeds size limit")
	}

	// Check memory limits
	if eb.totalMemory.Load()+event.Size > eb.config.MemoryLimit {
		return fmt.Errorf("buffer memory limit exceeded")
	}

	// Try to push to appropriate priority channel
	var err error
	switch event.Priority {
	case PriorityHigh:
		select {
		case eb.highPriority <- event:
			eb.totalMemory.Add(event.Size)
		default:
			eb.highOverflows.Add(1)
			err = fmt.Errorf("high priority buffer full")
		}
	case PriorityMedium:
		select {
		case eb.mediumPriority <- event:
			eb.totalMemory.Add(event.Size)
		default:
			eb.mediumOverflows.Add(1)
			err = fmt.Errorf("medium priority buffer full")
		}
	case PriorityLow:
		select {
		case eb.lowPriority <- event:
			eb.totalMemory.Add(event.Size)
		default:
			eb.lowOverflows.Add(1)
			err = fmt.Errorf("low priority buffer full")
		}
	}

	// Record metrics
	eb.recordMetrics()

	// Check if buffer needs to be scaled
	if err != nil && eb.shouldScale() {
		eb.scaleBuffer()
	}

	return err
}

func (eb *EventBuffer) Pop(ctx context.Context) (*Event, error) {
	// Try all priority levels in order
	for _, ch := range []struct {
		channel chan *Event
		wait    bool
	}{
		{eb.highPriority, false},
		{eb.mediumPriority, false},
		{eb.lowPriority, true}, // Only wait on lowest priority
	} {
		if ch.wait {
			select {
			case event := <-ch.channel:
				eb.totalMemory.Add(-event.Size)
				eb.recordMetrics()
				return event, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			select {
			case event := <-ch.channel:
				eb.totalMemory.Add(-event.Size)
				eb.recordMetrics()
				return event, nil
			default:
				continue
			}
		}
	}

	return nil, nil
}

func (eb *EventBuffer) shouldScale() bool {
	eb.scalingMu.Lock()
	defer eb.scalingMu.Unlock()

	// Check if enough time has passed since last scale
	if time.Since(eb.lastScaleTime) < time.Minute {
		return false
	}

	// Check overflow thresholds
	totalOverflows := eb.highOverflows.Load() + eb.mediumOverflows.Load() + eb.lowOverflows.Load()
	if totalOverflows > eb.config.MaxOverflowCount {
		return true
	}

	metrics := eb.getMetrics()
	avgUtilization := (metrics.HighPriorityUtilization +
		metrics.MediumPriorityUtilization +
		metrics.LowPriorityUtilization) / 3

	return avgUtilization > eb.config.ScaleUpThreshold
}

func (eb *EventBuffer) scaleBuffer() {
	eb.scalingMu.Lock()
	defer eb.scalingMu.Unlock()

	currentSize := eb.currentSize.Load()
	newSize := int32(float64(currentSize) * eb.config.ScaleIncrement)

	// Check maximum size limit
	if newSize > int32(eb.config.MaxSize) {
		newSize = int32(eb.config.MaxSize)
	}

	// Only scale if there's an actual increase
	if newSize <= currentSize {
		return
	}

	// Calculate new channel sizes
	highSize := int(float64(newSize) * eb.config.HighPriorityPercent)
	medSize := int(float64(newSize) * eb.config.MediumPriorityPercent)
	lowSize := int(float64(newSize) * eb.config.LowPriorityPercent)

	// Create new channels
	newHigh := make(chan *Event, highSize)
	newMed := make(chan *Event, medSize)
	newLow := make(chan *Event, lowSize)

	// Transfer existing messages
	eb.transferMessages(eb.highPriority, newHigh)
	eb.transferMessages(eb.mediumPriority, newMed)
	eb.transferMessages(eb.lowPriority, newLow)

	// Update channels
	eb.highPriority = newHigh
	eb.mediumPriority = newMed
	eb.lowPriority = newLow

	eb.currentSize.Store(newSize)
	eb.lastScaleTime = time.Now()
}

func (eb *EventBuffer) transferMessages(old, new chan *Event) {
	// Close old channel to prevent new messages
	close(old)

	// Transfer existing messages
	for msg := range old {
		new <- msg
	}
}

func (eb *EventBuffer) recordMetrics() {
	metrics := eb.getMetrics()

	eb.metricsMu.Lock()
	defer eb.metricsMu.Unlock()

	// Add new metric point
	point := bufferMetricPoint{
		timestamp:   time.Now(),
		drainRate:   metrics.DrainRate,
		utilization: (metrics.HighPriorityUtilization + metrics.MediumPriorityUtilization + metrics.LowPriorityUtilization) / 3,
		overflows:   metrics.HighPriorityOverflows + metrics.MediumPriorityOverflows + metrics.LowPriorityOverflows,
		memoryUsage: metrics.TotalMemoryUsage,
	}

	eb.metricsWindow = append(eb.metricsWindow, point)

	// Trim old metrics
	cutoff := time.Now().Add(-time.Hour)
	for i, point := range eb.metricsWindow {
		if point.timestamp.After(cutoff) {
			eb.metricsWindow = eb.metricsWindow[i:]
			break
		}
	}

	// Keep only the last maxDataPoints
	if len(eb.metricsWindow) > 1000 {
		eb.metricsWindow = eb.metricsWindow[len(eb.metricsWindow)-1000:]
	}
}

func (eb *EventBuffer) getMetrics() BufferMetrics {
	highLen := len(eb.highPriority)
	medLen := len(eb.mediumPriority)
	lowLen := len(eb.lowPriority)

	highCap := cap(eb.highPriority)
	medCap := cap(eb.mediumPriority)
	lowCap := cap(eb.lowPriority)

	// Calculate drain rate over the last minute
	var drainRate float64
	eb.metricsMu.RLock()
	if len(eb.metricsWindow) > 0 {
		now := time.Now()
		oneMinuteAgo := now.Add(-time.Minute)
		var messageCount int64
		for i := len(eb.metricsWindow) - 1; i >= 0; i-- {
			point := eb.metricsWindow[i]
			if point.timestamp.Before(oneMinuteAgo) {
				break
			}
			messageCount += point.overflows
		}
		drainRate = float64(messageCount) / 60.0 // messages per second
	}
	eb.metricsMu.RUnlock()

	return BufferMetrics{
		HighPriorityUtilization:   float64(highLen) / float64(highCap),
		MediumPriorityUtilization: float64(medLen) / float64(medCap),
		LowPriorityUtilization:    float64(lowLen) / float64(lowCap),
		HighPriorityOverflows:     eb.highOverflows.Load(),
		MediumPriorityOverflows:   eb.mediumOverflows.Load(),
		LowPriorityOverflows:      eb.lowOverflows.Load(),
		DrainRate:                 drainRate,
		TotalMemoryUsage:          eb.totalMemory.Load(),
		CurrentSize:               highCap + medCap + lowCap,
	}
}

func (eb *EventBuffer) GetCurrentSize() int {
	return int(eb.currentSize.Load())
}

func (eb *EventBuffer) GetBufferCapacity() (high, medium, low int) {
	return cap(eb.highPriority), cap(eb.mediumPriority), cap(eb.lowPriority)
}

func (eb *EventBuffer) GetBufferLengths() (high, medium, low int) {
	return len(eb.highPriority), len(eb.mediumPriority), len(eb.lowPriority)
}

func (b *BufferMetrics) GetAverageUtilization() float64 {
	return (b.HighPriorityUtilization +
		b.MediumPriorityUtilization +
		b.LowPriorityUtilization) / 3.0
}

// Reset clears all buffers and resets metrics
func (eb *EventBuffer) Reset() {
	// Create new channels with same capacity
	highCap := cap(eb.highPriority)
	medCap := cap(eb.mediumPriority)
	lowCap := cap(eb.lowPriority)

	eb.highPriority = make(chan *Event, highCap)
	eb.mediumPriority = make(chan *Event, medCap)
	eb.lowPriority = make(chan *Event, lowCap)

	// Reset counters
	eb.highOverflows.Store(0)
	eb.mediumOverflows.Store(0)
	eb.lowOverflows.Store(0)
	eb.totalMemory.Store(0)

	// Reset metrics
	eb.metricsMu.Lock()
	eb.metricsWindow = make([]bufferMetricPoint, 0, 1000)
	eb.metricsMu.Unlock()
}
