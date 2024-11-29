package consumer

import (
	"context"
	"fmt"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sqs-fargate-consumer-v2/internal/scheduler"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Worker struct {
	id              string
	buffer          *EventBuffer
	scheduler       *scheduler.Scheduler
	sqsClient       *sqs.Client
	collector       *metrics.Collector
	status          atomic.Int32 // 0: idle, 1: polling, 2: stopped
	lastPollTime    time.Time
	messageCount    atomic.Int64
	processingCount atomic.Int32
}

type WorkerMetrics struct {
	ID           string
	Status       int32
	MessageCount int64
	LastPollTime time.Time
}

const (
	WorkerStatusIdle    int32 = 0
	WorkerStatusPolling int32 = 1
	WorkerStatusStopped int32 = 2
)

func NewWorker(id string, buffer *EventBuffer, scheduler *scheduler.Scheduler, sqsClient *sqs.Client, collector *metrics.Collector) *Worker {
	return &Worker{
		id:        id,
		buffer:    buffer,
		scheduler: scheduler,
		sqsClient: sqsClient,
		collector: collector,
	}
}

func (w *Worker) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if w.status.Load() == WorkerStatusStopped {
				return nil
			}

			// Check if we should poll based on buffer capacity
			if !w.shouldPoll() {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Get queue to poll from scheduler
			queue, err := w.scheduler.SelectQueue()
			if err != nil {
				w.collector.RecordError("", "select_queue_error")
				fmt.Printf("Error selecting queue: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if queue == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Calculate how many messages we can safely poll
			maxMessages := w.calculateMaxMessages()
			if maxMessages <= 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			w.status.Store(WorkerStatusPolling)
			messages, err := w.pollMessages(ctx, queue, maxMessages)
			w.status.Store(WorkerStatusIdle)

			if err != nil {
				w.collector.RecordError(queue.URL, "poll_error")
				fmt.Printf("Error polling messages: %v", err)
				time.Sleep(time.Second) // Backoff on error
				continue
			}

			// Process received messages
			for _, msg := range messages {
				event := &Event{
					QueueURL:   queue.URL,
					Message:    &msg,
					Priority:   Priority(queue.Priority),
					ReceivedAt: time.Now(),
					Size:       int64(len(*msg.Body)),
				}

				// Try to push to buffer, stop polling if buffer is full
				if err := w.buffer.Push(event); err != nil {
					fmt.Printf("Error pushing event to buffer: %v", err)
					w.collector.RecordError(queue.URL, "buffer_full")
					break
				}

				w.messageCount.Add(1)
			}

			w.lastPollTime = time.Now()
		}
	}
}

func (w *Worker) shouldPoll() bool {
	metrics := w.buffer.getMetrics()

	// Don't poll if buffer utilization is too high
	if metrics.HighPriorityUtilization > 0.9 ||
		metrics.MediumPriorityUtilization > 0.9 ||
		metrics.LowPriorityUtilization > 0.9 {
		return false
	}

	// Don't poll if memory usage is too high
	if metrics.TotalMemoryUsage > (w.buffer.config.MemoryLimit * 90 / 100) {
		return false
	}

	return true
}

func (w *Worker) calculateMaxMessages() int32 {
	metrics := w.buffer.getMetrics()

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

func (w *Worker) pollMessages(ctx context.Context, queue *config.QueueConfig, maxMessages int32) ([]types.Message, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.URL,
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     20, // Long polling
		AttributeNames: []types.QueueAttributeName{
			"ApproximateReceiveCount",
			"SentTimestamp",
		},
		MessageAttributeNames: []string{"All"},
	}

	result, err := w.sqsClient.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, err
	}

	return result.Messages, nil
}

func (w *Worker) Stop() {
	w.status.Store(WorkerStatusStopped)
}

func (w *Worker) IsPolling() bool {
	return w.status.Load() == WorkerStatusPolling
}

func (w *Worker) GetMetrics() WorkerMetrics {
	return WorkerMetrics{
		ID:           w.id,
		Status:       w.status.Load(),
		MessageCount: w.messageCount.Load(),
		LastPollTime: w.lastPollTime,
	}
}
