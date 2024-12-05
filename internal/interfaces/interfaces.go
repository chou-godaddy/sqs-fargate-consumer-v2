package interfaces

import (
	"context"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/models"
	"time"
)

// Component represents a core system component with standard lifecycle methods
type Component interface {
	Start(context.Context) error
	Shutdown(context.Context) error
}

// MetricsCollector defines the interface for collecting and reporting metrics
type MetricsCollector interface {
	Component
	RecordError(queueName string)
	RecordProcessed(queueName string, duration time.Duration)
	GetQueueMetrics(queueName string) *models.QueueMetrics
	GetAllQueueMetrics() map[string]*models.QueueMetrics
	GetBufferMetricsCollector() BufferMetricsCollector
}

// MessageBuffer defines the interface for priority-based message buffering
type MessageBuffer interface {
	Component
	Push(*models.Message) error
	Pop(context.Context) (*models.Message, error)
	SetMetricsEmitter(BufferMetricsEmitter)
}

// Scheduler defines the interface for queue selection and management
type Scheduler interface {
	Component
	SelectQueue() (*config.QueueConfig, error)
}

// Worker represents a generic worker interface
type Worker interface {
	Start(context.Context) error
	Stop()
	GetMetrics() models.WorkerMetrics
}

// MessageProcessor defines the interface for processing messages from the buffer
type MessageProcessor interface {
	Component
	// GetMetrics returns current processor metrics
	GetMetrics() models.ProcessorMetrics
}

// BufferMetricsEmitter defines the interface for emitting buffer metrics
type BufferMetricsEmitter interface {
	OnMessageEnqueued(message *models.Message)
	OnMessageDequeued(message *models.Message)
	OnBufferOverflow(priority models.Priority)
	OnQueueSizeChanged(priority models.Priority, currentSize, capacity int)
}

type BufferMetricsCollector interface {
	BufferMetricsEmitter
	GetMetrics() models.BufferMetrics
}
