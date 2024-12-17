package models

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var (
	ErrBufferShuttingDown = errors.New("buffer is shutting down")
	ErrMessageTooLarge    = errors.New("message exceeds maximum size")
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
	QueueURL          string
	QueueName         string
	Priority          Priority
	MessageID         string
	Body              []byte
	ReceiptHandle     *string
	Size              int64
	ReceivedAt        time.Time
	EnqueuedAt        time.Time
	RetryCount        int
	ProcessorGroupID  string
	ProcessorWorkerID string
}

// BufferMetrics represents buffer utilization metrics
type BufferMetrics struct {
	// Basic buffer metrics
	BufferUsage    float64 // Percentage of buffer capacity used (0-1)
	CurrentSize    int64   // Current number of messages in buffer
	BufferCapacity int64   // Total buffer capacity
	TotalSize      int64   // Total size of all messages in bytes

	// Message counts
	TotalMessagesIn  int64 // Total messages pushed to buffer
	TotalMessagesOut int64 // Total messages popped from buffer

	// Priority tracking (for monitoring only)
	HighPriorityMessages   int64
	MediumPriorityMessages int64
	LowPriorityMessages    int64

	// Performance metrics
	AverageWaitTime       time.Duration    // Average time messages spend in buffer
	MaxWaitTime           time.Duration    // Maximum time any message spent in buffer
	WaitTimeHistogram     map[string]int64 // Distribution of wait times
	MessageProcessingRate float64          // Messages processed per second
	LastUpdateTime        time.Time        // When these metrics were last updated
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
