package models

import (
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type MetricPoint struct {
	Timestamp     time.Time
	MessageCount  int64
	InFlightCount int64
}

// QueueMetrics represents queue-specific metrics
type QueueMetrics struct {
	MessageCount     int64
	InFlightCount    int64
	LastUpdateTime   time.Time
	ErrorCount       atomic.Int64
	ProcessedCount   atomic.Int64
	ProcessingTime   atomic.Int64 // Total processing time in milliseconds
	MaxLatency       atomic.Int64 // Maximum latency observed in milliseconds
	HistoricalPoints []MetricPoint
}

// Message represents a wrapped SQS message with metadata
type Message struct {
	QueueURL      string
	QueueName     string
	Priority      Priority
	MessageID     string
	Body          []byte
	ReceiptHandle *string
	Size          int64
	ReceivedAt    time.Time
	EnqueuedAt    time.Time
	RetryCount    int
}

// BufferMetrics represents buffer utilization metrics
type BufferMetrics struct {
	// Channel utilization metrics
	HighPriorityUsage   float64 // Percentage of high priority channel capacity used (0-1)
	MediumPriorityUsage float64 // Percentage of medium priority channel capacity used (0-1)
	LowPriorityUsage    float64 // Percentage of low priority channel capacity used (0-1)

	// Message counts
	TotalSize              int64 // Total size of all messages currently in buffer
	TotalMessagesIn        int64 // Total messages pushed to buffer
	TotalMessagesOut       int64 // Total messages popped from buffer
	HighPriorityMessages   int64 // Count of high priority messages processed
	MediumPriorityMessages int64 // Count of medium priority messages processed
	LowPriorityMessages    int64 // Count of low priority messages processed
	OverflowCount          int32 // Number of times buffer was full when trying to push

	// Wait time metrics
	AverageWaitTime   time.Duration    // Average time messages spend in buffer
	MaxWaitTime       time.Duration    // Maximum time any message spent in buffer
	WaitTimeHistogram map[string]int64 // Distribution of wait times in various buckets

	// Processing metrics
	MessageProcessingRate float64 // Messages processed per second

	// Point-in-time metrics
	LastUpdateTime time.Time // When these metrics were last updated
}

// ProcessorMetrics represents metrics specific to message processing
type ProcessorMetrics struct {
	ActiveWorkers  int
	ProcessedCount int64
	ErrorCount     int64
	ProcessingRate float64
	ErrorRate      float64
}

// Priority levels for message processing
type Priority int

const (
	PriorityLow Priority = iota + 1
	PriorityMedium
	PriorityHigh
)

// NewMessage creates a new buffer message from an SQS message
func NewMessage(sqsMsg *types.Message, queueURL, queueName string, priority Priority) *Message {
	return &Message{
		QueueURL:      queueURL,
		QueueName:     queueName,
		Priority:      priority,
		MessageID:     *sqsMsg.MessageId,
		Body:          []byte(*sqsMsg.Body),
		ReceiptHandle: sqsMsg.ReceiptHandle,
		Size:          int64(len(*sqsMsg.Body)),
		ReceivedAt:    time.Now(),
		RetryCount:    0,
	}
}
