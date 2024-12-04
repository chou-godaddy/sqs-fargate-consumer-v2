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

// WorkerMetrics represents worker-specific metrics
type WorkerMetrics struct {
	ID           string
	Status       int32
	MessageCount int64
	LastActive   time.Time
}

// BufferMetrics represents buffer utilization metrics
type BufferMetrics struct {
	HighPriorityUsage   float64
	MediumPriorityUsage float64
	LowPriorityUsage    float64
	TotalSize           int64
	OverflowCount       int32
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
