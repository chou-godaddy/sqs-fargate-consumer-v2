package consumer

import (
	"context"
	"fmt"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/interfaces"
	"sqs-fargate-consumer-v2/internal/models"
	"sync"
	"time"
)

// MessageBuffer manages priority-based message queues
type MessageBufferImpl struct {
	highPriority   chan *models.Message
	mediumPriority chan *models.Message
	lowPriority    chan *models.Message
	maxSize        int64

	metricsEmitter interfaces.BufferMetricsEmitter

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

func (b *MessageBufferImpl) SetMetricsEmitter(emitter interfaces.BufferMetricsEmitter) {
	b.metricsEmitter = emitter
}

func (b *MessageBufferImpl) Start(ctx context.Context) error {
	log.Printf("[Buffer] Starting with capacities - High: %d, Medium: %d, Low: %d", cap(b.highPriority), cap(b.mediumPriority), cap(b.lowPriority))

	// Start metrics collection routine
	b.wg.Add(1)
	go b.monitorQueueSizes()

	return nil
}

func (b *MessageBufferImpl) monitorQueueSizes() {
	defer b.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			if b.metricsEmitter != nil {
				highUsage := float64(len(b.highPriority)) / float64(cap(b.highPriority))
				mediumUsage := float64(len(b.mediumPriority)) / float64(cap(b.mediumPriority))
				lowUsage := float64(len(b.lowPriority)) / float64(cap(b.lowPriority))

				b.metricsEmitter.OnQueueSizeChanged(models.PriorityHigh, len(b.highPriority), cap(b.highPriority))
				b.metricsEmitter.OnQueueSizeChanged(models.PriorityMedium, len(b.mediumPriority), cap(b.mediumPriority))
				b.metricsEmitter.OnQueueSizeChanged(models.PriorityLow, len(b.lowPriority), cap(b.lowPriority))

				// Log both buffer occupancy and processing throughput
				log.Printf("[Buffer] Channel utilization - High: %.2f%%, Medium: %.2f%%, Low: %.2f%%", highUsage*100, mediumUsage*100, lowUsage*100)
			}
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
		if b.metricsEmitter != nil {
			b.metricsEmitter.OnMessageEnqueued(msg)
		}
		log.Printf("[Buffer] Pushed message %s to %s priority queue (Size: %d)", msg.MessageID, getPriorityString(msg.Priority), msg.Size)
	} else {
		if b.metricsEmitter != nil {
			b.metricsEmitter.OnBufferOverflow(msg.Priority)
		}
		log.Printf("[Buffer] Failed to push message %s: %v", msg.MessageID, err)
	}

	return err
}

// Pop retrieves the next message based on priority
func (b *MessageBufferImpl) Pop(ctx context.Context) (*models.Message, error) {
	// Try high priority first
	select {
	case msg := <-b.highPriority:
		if b.metricsEmitter != nil {
			b.metricsEmitter.OnMessageDequeued(msg)
		}
		return msg, nil
	default:
	}

	// Try medium priority
	select {
	case msg := <-b.mediumPriority:
		if b.metricsEmitter != nil {
			b.metricsEmitter.OnMessageDequeued(msg)
		}
		return msg, nil
	default:
	}

	// Try low priority with timeout
	select {
	case msg := <-b.lowPriority:
		if b.metricsEmitter != nil {
			b.metricsEmitter.OnMessageDequeued(msg)
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil, nil
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

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("buffer shutdown timed out: %w", ctx.Err())
	case <-done:
		close(b.highPriority)
		close(b.mediumPriority)
		close(b.lowPriority)
		log.Printf("[Buffer] Shutdown completed")
		return nil
	}
}
