package consumer

import (
	"context"
	"fmt"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sqs-fargate-consumer-v2/internal/scheduler"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type ConsumerWorker struct {
	id              string
	buffer          *EventBuffer
	scheduler       *scheduler.Scheduler
	sqsClient       *sqs.Client
	collector       *metrics.Collector
	status          atomic.Int32
	lastPollTime    time.Time
	messageCount    atomic.Int64
	processingCount atomic.Int32
}

type ConsumerWorkerMetrics struct {
	ID           string
	Status       int32
	MessageCount int64
	LastPollTime time.Time
}

const (
	ConsumerWorkerStatusIdle    int32 = 0
	ConsumerWorkerStatusPolling int32 = 1
	ConsumerWorkerStatusStopped int32 = 2
)

func NewConsumerWorker(id string, buffer *EventBuffer, scheduler *scheduler.Scheduler, sqsClient *sqs.Client, collector *metrics.Collector) *ConsumerWorker {
	return &ConsumerWorker{
		id:        id,
		buffer:    buffer,
		scheduler: scheduler,
		sqsClient: sqsClient,
		collector: collector,
	}
}

func (w *ConsumerWorker) Start(ctx context.Context) error {
	log.Printf("[ConsumerWorker %s] Starting worker", w.id)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if w.status.Load() == ConsumerWorkerStatusStopped {
				log.Printf("[ConsumerWorker %s] Stopped", w.id)
				return nil
			}

			if !w.shouldPoll() {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			queue, err := w.scheduler.SelectQueue()
			if err != nil {
				w.collector.RecordError("", "select_queue_error")
				log.Printf("[ConsumerWorker %s] Error selecting queue: %v", w.id, err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if queue == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			maxMessages := w.calculateMaxMessages()
			if maxMessages <= 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			log.Printf("[ConsumerWorker %s] Polling %d messages from queue %s", w.id, maxMessages, queue.Name)

			w.status.Store(ConsumerWorkerStatusPolling)
			messages, err := w.pollMessages(ctx, queue, maxMessages)
			w.status.Store(ConsumerWorkerStatusIdle)
			if err != nil {
				w.collector.RecordError(queue.Name, "poll_error")
				log.Printf("[ConsumerWorker %s] Error polling queue %s: %v", w.id, queue.Name, err)
				time.Sleep(time.Second)
				continue
			}

			log.Printf("[ConsumerWorker %s] Successfully polled %d messages from queue %s", w.id, len(messages), queue.Name)

			// Process received messages
			successful := 0
			for _, msg := range messages {
				event := &Event{
					QueueURL:   queue.URL,
					QueueName:  queue.Name,
					Message:    &msg,
					Priority:   Priority(queue.Priority),
					ReceivedAt: time.Now(),
					Size:       int64(len(*msg.Body)),
				}

				if err := w.buffer.Push(event); err != nil {
					log.Printf("[ConsumerWorker %s] Failed to push message to buffer: %v", w.id, err)
					w.collector.RecordError(queue.Name, "buffer_push_error")
					break
				}

				successful++
				w.messageCount.Add(1)
				w.collector.RecordProcessingStarted(queue.Name, int(event.Priority))

				log.Printf("[ConsumerWorker %s] Successfully pushed message %s to buffer. Total messages processed: %d",
					w.id, *msg.MessageId, w.messageCount.Load())
			}

			if successful > 0 {
				log.Printf("[ConsumerWorker %s] Successfully processed batch of %d/%d messages from queue %s",
					w.id, successful, len(messages), queue.Name)
			}

			w.lastPollTime = time.Now()

			// Add a small delay between polls to prevent tight loops
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (w *ConsumerWorker) shouldPoll() bool {
	metrics := w.buffer.GetMetrics()

	// Check buffer utilization
	if metrics.GetAverageUtilization() > 0.9 {
		log.Printf("[ConsumerWorker %s] Skipping poll due to high buffer utilization (%.2f%%)",
			w.id, metrics.GetAverageUtilization()*100)
		return false
	}

	// Check memory usage
	if metrics.TotalMemoryUsage > (w.buffer.config.MemoryLimit * 90 / 100) {
		log.Printf("[ConsumerWorker %s] Skipping poll due to high memory usage (%d/%d bytes)",
			w.id, metrics.TotalMemoryUsage, w.buffer.config.MemoryLimit)
		return false
	}

	return true
}
func (w *ConsumerWorker) calculateMaxMessages() int32 {
	metrics := w.buffer.GetMetrics()

	// Calculate available capacity in each priority level
	highCapacity := cap(w.buffer.highPriority) - len(w.buffer.highPriority)
	medCapacity := cap(w.buffer.mediumPriority) - len(w.buffer.mediumPriority)
	lowCapacity := cap(w.buffer.lowPriority) - len(w.buffer.lowPriority)

	// Calculate memory headroom
	memoryHeadroom := w.buffer.config.MemoryLimit - metrics.TotalMemoryUsage

	// Estimate messages that could fit in memory (assuming average message size)
	avgMessageSize := int64(10 * 1024) // 10KB average, adjust based on your needs
	maxByMemory := int32(memoryHeadroom / avgMessageSize)

	// Take minimum of available capacity and memory constraints
	maxMessages := int32(min(highCapacity, medCapacity, lowCapacity))
	maxMessages = min(maxMessages, maxByMemory)

	// Never poll more than 10 messages at once (SQS limit)
	return min(maxMessages, 10)
}

func (w *ConsumerWorker) pollMessages(ctx context.Context, queue *config.QueueConfig, maxMessages int32) ([]types.Message, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.URL,
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     20,
		AttributeNames: []types.QueueAttributeName{
			"ApproximateReceiveCount",
			"SentTimestamp",
		},
		MessageAttributeNames: []string{"All"},
	}

	log.Printf("[ConsumerWorker %s] Starting long poll on queue %s for max %d messages",
		w.id, queue.Name, maxMessages)

	result, err := w.sqsClient.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("receive message error: %w", err)
	}

	log.Printf("[ConsumerWorker %s] Received %d messages from queue %s",
		w.id, len(result.Messages), queue.Name)

	return result.Messages, nil
}

func (w *ConsumerWorker) Stop() {
	w.status.Store(ConsumerWorkerStatusStopped)
}

func (w *ConsumerWorker) IsPolling() bool {
	return w.status.Load() == ConsumerWorkerStatusPolling
}

func (w *ConsumerWorker) GetMetrics() ConsumerWorkerMetrics {
	return ConsumerWorkerMetrics{
		ID:           w.id,
		Status:       w.status.Load(),
		MessageCount: w.messageCount.Load(),
		LastPollTime: w.lastPollTime,
	}
}
