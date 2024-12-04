package processor

import (
	"context"
	"fmt"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/consumer"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type MessageHandler func(context.Context, *consumer.Event) error

type MessageProcessor struct {
	config    config.ProcessorConfig
	buffer    *consumer.EventBuffer
	handler   MessageHandler
	collector *metrics.Collector
	client    *sqs.Client

	// Worker management
	activeWorkers   map[string]context.CancelFunc
	workersMu       sync.RWMutex
	workerSemaphore chan struct{}

	// Processing state
	currentLoad    atomic.Int64
	processedCount atomic.Int64
	lastScaleCheck time.Time
	shutdownSignal chan struct{}
	isShuttingDown bool
	mu             sync.RWMutex
}

func NewMessageProcessor(config config.ProcessorConfig, buffer *consumer.EventBuffer, handler MessageHandler, collector *metrics.Collector, client *sqs.Client) *MessageProcessor {
	return &MessageProcessor{
		config:          config,
		buffer:          buffer,
		handler:         handler,
		collector:       collector,
		client:          client,
		activeWorkers:   make(map[string]context.CancelFunc),
		workerSemaphore: make(chan struct{}, config.MaxConcurrency),
		shutdownSignal:  make(chan struct{}),
	}
}

func (p *MessageProcessor) Start(ctx context.Context) error {
	log.Printf("[Processor] Starting with initial concurrency: %d", p.config.MinConcurrency)

	// Start initial workers
	for i := 0; i < p.config.MinConcurrency; i++ {
		if err := p.startNewWorker(ctx); err != nil {
			return fmt.Errorf("failed to start initial worker: %w", err)
		}
	}

	// Start scaling monitoring
	go p.monitorAndScale(ctx)

	return nil
}

func (p *MessageProcessor) startNewWorker(ctx context.Context) error {
	workerID := fmt.Sprintf("processor-worker-%d", time.Now().UnixNano())
	workerCtx, cancel := context.WithCancel(ctx)

	p.workersMu.Lock()
	p.activeWorkers[workerID] = cancel
	p.workersMu.Unlock()

	go func() {
		defer func() {
			p.workersMu.Lock()
			delete(p.activeWorkers, workerID)
			p.workersMu.Unlock()
			cancel()
		}()

		p.processLoop(workerCtx, workerID)
	}()

	log.Printf("[Processor] Started new worker: %s", workerID)
	return nil
}

func (p *MessageProcessor) processLoop(ctx context.Context, workerID string) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Processor] Worker %s stopping due to context cancellation", workerID)
			return
		case <-p.shutdownSignal:
			log.Printf("[Processor] Worker %s stopping due to shutdown signal", workerID)
			return
		default:
			// Try to acquire worker semaphore
			select {
			case p.workerSemaphore <- struct{}{}:
				event, err := p.buffer.Pop(ctx)
				if err != nil {
					<-p.workerSemaphore
					if err != context.Canceled {
						log.Printf("[Processor] Worker %s error popping message: %v", workerID, err)
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if event == nil {
					<-p.workerSemaphore
					time.Sleep(100 * time.Millisecond)
					continue
				}

				p.currentLoad.Add(1)
				startTime := time.Now()

				log.Printf("[Processor] Worker %s processing message %s from queue %s",
					workerID, *event.Message.MessageId, event.QueueName)

				if err := p.processMessage(ctx, event); err != nil {
					log.Printf("[Processor] Worker %s failed to process message %s: %v",
						workerID, *event.Message.MessageId, err)
				}

				duration := time.Since(startTime)
				p.currentLoad.Add(-1)
				p.processedCount.Add(1)
				<-p.workerSemaphore

				log.Printf("[Processor] Worker %s completed message %s in %v. Total processed: %d",
					workerID, *event.Message.MessageId, duration, p.processedCount.Load())

			case <-ctx.Done():
				return
			case <-p.shutdownSignal:
				return
			}
		}
	}
}

func (p *MessageProcessor) processMessage(ctx context.Context, event *consumer.Event) error {
	msgCtx, cancel := context.WithTimeout(ctx, p.config.ProcessTimeout.Duration)
	defer cancel()

	startTime := time.Now()
	err := p.handler(msgCtx, event)
	duration := time.Since(startTime)

	if err != nil {
		p.collector.RecordError(event.QueueName, "process_error")
		log.Printf("[Processor] Error processing message %s: %v", *event.Message.MessageId, err)
		return err
	}

	if err := p.deleteMessage(msgCtx, event); err != nil {
		log.Printf("[Processor] Failed to delete message %s: %v", *event.Message.MessageId, err)
		return err
	}

	p.collector.RecordProcessingComplete(event.QueueName, int(event.Priority), duration)
	return nil
}

func (p *MessageProcessor) deleteMessage(ctx context.Context, event *consumer.Event) error {
	log.Printf("Deleting message: %s from queue: %s\n", *event.Message.MessageId, event.QueueURL)

	input := &sqs.DeleteMessageInput{
		QueueUrl:      &event.QueueURL,
		ReceiptHandle: event.Message.ReceiptHandle,
	}

	if _, err := p.client.DeleteMessage(ctx, input); err != nil {
		p.collector.RecordError(event.QueueName, "delete_error")
		log.Printf("Error deleting message: %v\n", err)
		return fmt.Errorf("failed to delete message: %v", err)
	}

	return nil
}

func (p *MessageProcessor) monitorAndScale(ctx context.Context) {
	ticker := time.NewTicker(p.config.ScaleInterval.Duration)
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
	currentLoad := p.currentLoad.Load()

	p.workersMu.RLock()
	currentWorkers := len(p.activeWorkers)
	p.workersMu.RUnlock()

	log.Printf("[Processor] Scaling check - Current workers: %d, Load: %d, Buffer utilization: %.2f%%",
		currentWorkers, currentLoad, metrics.GetAverageUtilization()*100)

	if metrics.GetAverageUtilization() > p.config.ScaleUpThreshold {
		if currentWorkers < p.config.MaxConcurrency {
			targetWorkers := min(currentWorkers+5, p.config.MaxConcurrency)
			workersToAdd := targetWorkers - currentWorkers

			log.Printf("[Processor] Scaling up by adding %d workers (current: %d, target: %d)",
				workersToAdd, currentWorkers, targetWorkers)

			for i := 0; i < workersToAdd; i++ {
				if err := p.startNewWorker(context.Background()); err != nil {
					log.Printf("[Processor] Failed to start new worker during scale up: %v", err)
					break
				}
			}
		}
	} else if metrics.GetAverageUtilization() < p.config.ScaleDownThreshold {
		if currentWorkers > p.config.MinConcurrency {
			workersToRemove := 1
			targetWorkers := max(currentWorkers-workersToRemove, p.config.MinConcurrency)

			log.Printf("[Processor] Scaling down by removing %d workers (current: %d, target: %d)",
				workersToRemove, currentWorkers, targetWorkers)

			// Get the workers to remove
			p.workersMu.Lock()
			count := 0
			for workerID, cancel := range p.activeWorkers {
				if count >= workersToRemove || len(p.activeWorkers) <= p.config.MinConcurrency {
					break
				}
				log.Printf("[Processor] Gracefully stopping worker %s", workerID)
				cancel()
				count++
			}
			p.workersMu.Unlock()
		}
	}
}

func (p *MessageProcessor) Shutdown(ctx context.Context) error {
	log.Printf("[Processor] Initiating shutdown sequence")

	p.mu.Lock()
	if p.isShuttingDown {
		p.mu.Unlock()
		return fmt.Errorf("shutdown already in progress")
	}
	p.isShuttingDown = true
	close(p.shutdownSignal)
	p.mu.Unlock()

	// Create shutdown tracking channel
	allWorkersDone := make(chan struct{})

	go func() {
		// Cancel all worker contexts
		p.workersMu.Lock()
		log.Printf("[Processor] Stopping %d active workers", len(p.activeWorkers))
		for workerID, cancel := range p.activeWorkers {
			log.Printf("[Processor] Sending shutdown signal to worker %s", workerID)
			cancel()
		}
		p.workersMu.Unlock()

		// Wait for workers to finish
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			p.workersMu.RLock()
			activeCount := len(p.activeWorkers)
			p.workersMu.RUnlock()

			if activeCount == 0 {
				break
			}

			log.Printf("[Processor] Waiting for %d workers to complete", activeCount)
			<-ticker.C
		}

		close(allWorkersDone)
	}()

	// Wait for either context timeout or workers to finish
	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown timed out: %w", ctx.Err())
	case <-allWorkersDone:
		log.Printf("[Processor] All workers have stopped. Total messages processed: %d", p.processedCount.Load())
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
