package processor

import (
	"context"
	"fmt"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/consumer"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sync"
	"time"
)

type MessageHandler func(context.Context, *consumer.Event) error

type MessageProcessor struct {
	config    config.ProcessorConfig
	buffer    *consumer.EventBuffer
	handler   MessageHandler
	collector *metrics.Collector

	// Worker pool management
	workers    chan struct{} // Semaphore for controlling concurrency
	workersMu  sync.RWMutex
	processing sync.WaitGroup

	// Processor state
	currentLoad    int64 // Current number of messages being processed
	processedCount int64 // Total number of messages processed
	lastScaleCheck time.Time
	shutdownSignal chan struct{}
	isShuttingDown bool
	mu             sync.RWMutex
}

func NewMessageProcessor(config config.ProcessorConfig, buffer *consumer.EventBuffer, handler MessageHandler, collector *metrics.Collector) *MessageProcessor {
	return &MessageProcessor{
		config:         config,
		buffer:         buffer,
		handler:        handler,
		collector:      collector,
		workers:        make(chan struct{}, config.MaxConcurrency),
		shutdownSignal: make(chan struct{}),
	}
}

func (p *MessageProcessor) Start(ctx context.Context) error {
	// Initialize worker pool with minimum concurrency
	for i := 0; i < p.config.MinConcurrency; i++ {
		p.workers <- struct{}{}
	}

	// Start scaling goroutine
	go p.monitorAndScale(ctx)

	// Start main processing loop
	go p.processLoop(ctx)

	return nil
}

func (p *MessageProcessor) processLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.shutdownSignal:
			return
		default:
			// Try to get a worker from the pool
			select {
			case worker := <-p.workers:
				// Got a worker, try to get a message
				if event, err := p.buffer.Pop(ctx); err == nil && event != nil {
					p.processing.Add(1)
					go func() {
						defer p.processing.Done()
						defer func() { p.workers <- worker }() // Return worker to pool

						p.processMessage(ctx, event)
					}()
				} else {
					// No message available, return worker immediately
					p.workers <- worker
					time.Sleep(10 * time.Millisecond) // Prevent tight loop
				}
			default:
				// No workers available, wait briefly
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (p *MessageProcessor) processMessage(ctx context.Context, event *consumer.Event) {
	startTime := time.Now()

	// Create timeout context for message processing
	msgCtx, cancel := context.WithTimeout(ctx, p.config.ProcessTimeout)
	defer cancel()

	// Process the message
	err := p.handler(msgCtx, event)

	duration := time.Since(startTime)

	if err != nil {
		p.collector.RecordError(event.QueueURL, "process_error")
		fmt.Printf("Error processing message: %v\n", err)
	} else {
		p.collector.RecordProcessingComplete(event.QueueURL, int(event.Priority), duration)
	}
}

func (p *MessageProcessor) monitorAndScale(ctx context.Context) {
	ticker := time.NewTicker(p.config.ScaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.shutdownSignal:
			return
		case <-ticker.C:
			p.adjustConcurrency()
		}
	}
}

func (p *MessageProcessor) adjustConcurrency() {
	p.mu.Lock()
	defer p.mu.Unlock()

	metrics := p.buffer.GetMetrics()

	// Calculate average buffer utilization
	avgUtilization := (metrics.HighPriorityUtilization +
		metrics.MediumPriorityUtilization +
		metrics.LowPriorityUtilization) / 3.0

	currentWorkers := cap(p.workers)

	if avgUtilization > p.config.ScaleUpThreshold {
		// Scale up workers if below max
		if currentWorkers < p.config.MaxConcurrency {
			newSize := min(currentWorkers+5, p.config.MaxConcurrency)
			p.resizeWorkerPool(newSize)
		}
	} else if avgUtilization < p.config.ScaleDownThreshold {
		// Scale down workers if above min
		if currentWorkers > p.config.MinConcurrency {
			newSize := max(currentWorkers-1, p.config.MinConcurrency)
			p.resizeWorkerPool(newSize)
		}
	}
}

func (p *MessageProcessor) resizeWorkerPool(newSize int) {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	// Create new worker pool
	newWorkers := make(chan struct{}, newSize)

	// Transfer existing workers or add new ones
	currentSize := len(p.workers)
	if newSize > currentSize {
		// Copy existing workers
		for i := 0; i < currentSize; i++ {
			<-p.workers
			newWorkers <- struct{}{}
		}
		// Add new workers
		for i := currentSize; i < newSize; i++ {
			newWorkers <- struct{}{}
		}
	} else {
		// Copy subset of workers
		for i := 0; i < newSize; i++ {
			<-p.workers
			newWorkers <- struct{}{}
		}
		// Drain remaining workers
		for i := newSize; i < currentSize; i++ {
			<-p.workers
		}
	}

	// Replace worker pool
	close(p.workers)
	p.workers = newWorkers
}

func (p *MessageProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.isShuttingDown = true
	close(p.shutdownSignal)
	p.mu.Unlock()

	// Wait for all processors to finish
	doneCh := make(chan struct{})
	go func() {
		p.processing.Wait()
		close(doneCh)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
		return nil
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
