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
	messages chan *models.Message
	maxSize  int64
	capacity int

	metricsEmitter interfaces.BufferMetricsEmitter

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

func NewMessageBuffer(config config.BufferConfig) interfaces.MessageBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	return &MessageBufferImpl{
		messages:   make(chan *models.Message, config.TotalCapacity),
		maxSize:    config.MaxMessageSize,
		capacity:   config.TotalCapacity,
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

func (b *MessageBufferImpl) SetMetricsEmitter(emitter interfaces.BufferMetricsEmitter) {
	b.metricsEmitter = emitter
}

func (b *MessageBufferImpl) Start(ctx context.Context) error {
	log.Printf("[Buffer] Starting with capacity: %d", b.capacity)

	// Start metrics collection routine
	b.wg.Add(1)
	go b.monitorBufferSize()

	return nil
}

func (b *MessageBufferImpl) monitorBufferSize() {
	defer b.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			if b.metricsEmitter != nil {
				currentSize := len(b.messages)
				b.metricsEmitter.OnBufferSizeChanged(currentSize, b.capacity)

				usage := float64(currentSize) / float64(b.capacity)
				log.Printf("[Buffer] Channel utilization: %.2f%% (%d/%d)", usage*100, currentSize, b.capacity)
			}
		}
	}
}

// Push adds a message to the buffer, blocking if the buffer is full
func (b *MessageBufferImpl) Push(msg *models.Message) error {
	if msg == nil {
		return fmt.Errorf("cannot push nil message")
	}

	if msg.Size > b.maxSize {
		return models.ErrMessageTooLarge
	}

	msg.EnqueuedAt = time.Now()

	// Block until either the message is pushed or context is done
	select {
	case <-b.ctx.Done():
		return models.ErrBufferShuttingDown
	case b.messages <- msg:
		if b.metricsEmitter != nil {
			b.metricsEmitter.OnMessageEnqueued(msg)
		}
		log.Printf("[Buffer] Pushed message %s (Size: %d)",
			msg.MessageID, msg.Size)
		return nil
	}
}

// Pop retrieves the next message from the buffer
func (b *MessageBufferImpl) Pop(ctx context.Context) (*models.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-b.messages:
		if b.metricsEmitter != nil {
			b.metricsEmitter.OnMessageDequeued(msg)
		}
		return msg, nil
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
		close(b.messages)
		log.Printf("[Buffer] Shutdown completed")
		return nil
	}
}
