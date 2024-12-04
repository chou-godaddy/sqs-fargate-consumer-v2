package consumer

import (
	"context"
	"fmt"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/interfaces"
	"sqs-fargate-consumer-v2/internal/models"
	"sync"
	"sync/atomic"
	"time"
)

// MessageBuffer manages priority-based message queues
type MessageBufferImpl struct {
	highPriority   chan *models.Message
	mediumPriority chan *models.Message
	lowPriority    chan *models.Message

	totalSize     atomic.Int64
	maxSize       int64
	overflowCount atomic.Int32

	metrics models.BufferMetrics
	mu      sync.RWMutex

	// Shutdown handling
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

func NewMessageBuffer(config config.BufferConfig) interfaces.MessageBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	return &MessageBufferImpl{
		highPriority:   make(chan *models.Message, config.HighPrioritySize),
		mediumPriority: make(chan *models.Message, config.MediumPrioritySize),
		lowPriority:    make(chan *models.Message, config.LowPrioritySize),
		maxSize:        config.MaxMessageSize,
		ctx:            ctx,
		cancelFunc:     cancel,
	}
}

func (b *MessageBufferImpl) Start(ctx context.Context) error {
	log.Printf("[Buffer] Starting with capacities - High: %d, Medium: %d, Low: %d",
		cap(b.highPriority), cap(b.mediumPriority), cap(b.lowPriority))

	// Start metrics collection routine
	b.wg.Add(1)
	go b.collectMetrics()

	return nil
}

func (b *MessageBufferImpl) collectMetrics() {
	defer b.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.updateMetrics()
		}
	}
}

// Push adds a message to the appropriate priority queue
func (b *MessageBufferImpl) Push(msg *models.Message) error {
	if msg == nil {
		return fmt.Errorf("cannot push nil message")
	}

	if msg.Size > b.maxSize {
		return fmt.Errorf("message size %d exceeds maximum %d", msg.Size, b.maxSize)
	}

	msg.EnqueuedAt = time.Now()

	// Try to push to appropriate queue
	var pushed bool
	var err error

	switch msg.Priority {
	case models.PriorityHigh:
		select {
		case b.highPriority <- msg:
			pushed = true
		default:
			err = fmt.Errorf("high priority buffer full")
		}

	case models.PriorityMedium:
		select {
		case b.mediumPriority <- msg:
			pushed = true
		default:
			err = fmt.Errorf("medium priority buffer full")
		}

	case models.PriorityLow:
		select {
		case b.lowPriority <- msg:
			pushed = true
		default:
			err = fmt.Errorf("low priority buffer full")
		}
	}

	if pushed {
		b.totalSize.Add(msg.Size)
		log.Printf("[Buffer] Pushed message %s to %s priority queue (Size: %d)",
			msg.MessageID, getPriorityString(msg.Priority), msg.Size)
	} else {
		b.overflowCount.Add(1)
		log.Printf("[Buffer] Failed to push message %s: %v", msg.MessageID, err)
	}

	b.updateMetrics()
	return err
}

// Pop retrieves the next message based on priority
func (b *MessageBufferImpl) Pop(ctx context.Context) (*models.Message, error) {
	// Try high priority first
	select {
	case msg := <-b.highPriority:
		b.totalSize.Add(-msg.Size)
		b.updateMetrics()
		return msg, nil
	default:
	}

	// Try medium priority
	select {
	case msg := <-b.mediumPriority:
		b.totalSize.Add(-msg.Size)
		b.updateMetrics()
		return msg, nil
	default:
	}

	// Try low priority with timeout
	select {
	case msg := <-b.lowPriority:
		b.totalSize.Add(-msg.Size)
		b.updateMetrics()
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil, nil
	}
}

// GetMetrics returns current buffer metrics
func (b *MessageBufferImpl) GetMetrics() models.BufferMetrics {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.metrics
}

func (b *MessageBufferImpl) updateMetrics() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metrics = models.BufferMetrics{
		HighPriorityUsage:   float64(len(b.highPriority)) / float64(cap(b.highPriority)),
		MediumPriorityUsage: float64(len(b.mediumPriority)) / float64(cap(b.mediumPriority)),
		LowPriorityUsage:    float64(len(b.lowPriority)) / float64(cap(b.lowPriority)),
		TotalSize:           b.totalSize.Load(),
		OverflowCount:       b.overflowCount.Load(),
	}
}

func getPriorityString(p models.Priority) string {
	switch p {
	case models.PriorityHigh:
		return "high"
	case models.PriorityMedium:
		return "medium"
	case models.PriorityLow:
		return "low"
	default:
		return "unknown"
	}
}

func (b *MessageBufferImpl) Shutdown(ctx context.Context) error {
	log.Printf("[Buffer] Initiating shutdown...")
	b.cancelFunc()

	// Wait for metrics collection to stop
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	// Wait for shutdown with timeout
	select {
	case <-ctx.Done():
		return fmt.Errorf("buffer shutdown timed out: %w", ctx.Err())
	case <-done:
		// Drain remaining messages
		close(b.highPriority)
		close(b.mediumPriority)
		close(b.lowPriority)

		log.Printf("[Buffer] Shutdown completed. Final metrics - Messages in buffer: %d, Overflows: %d",
			b.totalSize.Load(), b.overflowCount.Load())
		return nil
	}
}
