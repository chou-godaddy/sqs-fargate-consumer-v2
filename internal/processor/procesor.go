package processor

import (
	"context"
	"fmt"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/dependencies"
	"sqs-fargate-consumer-v2/internal/interfaces"
	"sqs-fargate-consumer-v2/internal/models"
	"sqs-fargate-consumer-v2/internal/workflow"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type MessageProcessorImpl struct {
	instanceID             string
	config                 config.ProcessorConfig
	buffer                 interfaces.MessageBuffer
	sqsClient              *sqs.Client
	deps                   dependencies.WorkflowDependencies
	collector              interfaces.MetricsCollector
	bufferMetricsCollector interfaces.BufferMetricsCollector

	workers     map[string]*Worker
	workerCount atomic.Int32
	mu          sync.RWMutex

	processedCount atomic.Int64
	errorCount     atomic.Int64

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	scalingMu         sync.Mutex // Mutex for scaling operations
	lastScaleUpTime   atomic.Int64
	lastScaleDownTime atomic.Int64
}

type Worker struct {
	id            string
	status        atomic.Int32
	msgCount      atomic.Int64
	stopChan      chan struct{}
	processor     *MessageProcessorImpl
	lastActive    time.Time
	mu            sync.RWMutex
	domainsWorker *workflow.RegistrarDomainsWorker
}

const (
	workerStatusIdle int32 = iota
	workerStatusProcessing
	workerStatusStopped
)

func NewMessageProcessor(
	config config.ProcessorConfig,
	buffer interfaces.MessageBuffer,
	deps dependencies.WorkflowDependencies,
	collector interfaces.MetricsCollector,
	bufferMetricsCollector interfaces.BufferMetricsCollector,
) interfaces.MessageProcessor {
	instanceID := fmt.Sprintf("processor-group-%d", time.Now().UnixNano())
	log.Printf("[ProcessorGroup] Creating new instance: %s", instanceID)
	ctx, cancel := context.WithCancel(context.Background())
	return &MessageProcessorImpl{
		instanceID:             instanceID,
		config:                 config,
		buffer:                 buffer,
		sqsClient:              deps.GetSQSClient(),
		deps:                   deps,
		collector:              collector,
		workers:                make(map[string]*Worker),
		ctx:                    ctx,
		cancelFunc:             cancel,
		bufferMetricsCollector: bufferMetricsCollector,
	}
}

func (mp *MessageProcessorImpl) Start(ctx context.Context) error {
	log.Printf("[Processor %s] Starting with initial workers: %d", mp.instanceID, mp.config.MinWorkers)

	// Start initial workers
	for i := 0; i < mp.config.MinWorkers; i++ {
		if err := mp.startWorker(); err != nil {
			return fmt.Errorf("failed to start initial worker: %w", err)
		}
	}

	// Start scaling routine
	go mp.monitorAndScale()

	return nil
}

func (mp *MessageProcessorImpl) startWorker() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.workerCount.Load() >= int32(mp.config.MaxWorkers) {
		return fmt.Errorf("maximum worker count reached")
	}

	workerID := fmt.Sprintf("processor-worker-%d", time.Now().UnixNano())
	worker := &Worker{
		id:            workerID,
		stopChan:      make(chan struct{}),
		processor:     mp,
		domainsWorker: workflow.NewRegistrarDomainsWorker(mp.deps),
	}

	mp.workers[workerID] = worker
	mp.workerCount.Add(1)

	mp.wg.Add(1)
	go mp.runWorker(worker)

	log.Printf("[ProcessorGroup %s] Started new worker %s. Total workers: %d", mp.instanceID, workerID, mp.workerCount.Load())
	return nil
}

func (mp *MessageProcessorImpl) runWorker(worker *Worker) {
	defer mp.wg.Done()
	defer func() {
		mp.mu.Lock()
		delete(mp.workers, worker.id)
		mp.mu.Unlock()
		mp.workerCount.Add(-1)
	}()

	for {
		select {
		case <-mp.ctx.Done():
			return
		case <-worker.stopChan:
			return
		default:
			if err := worker.processMessage(mp.ctx); err != nil {
				if err != context.Canceled {
					log.Printf("[Worker %s] Error processing message: %v", worker.id, err)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (w *Worker) processMessage(ctx context.Context) error {
	// Set worker status
	w.status.Store(workerStatusIdle)

	// Pop message from buffer
	msg, err := w.processor.buffer.Pop(ctx)
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}
	msg.ProcessorGroupID = w.processor.instanceID
	msg.ProcessorWorkerID = w.id

	w.status.Store(workerStatusProcessing)
	defer w.status.Store(workerStatusIdle)

	startTime := time.Now()

	// Process message with timeout
	processCtx, cancel := context.WithTimeout(ctx, w.processor.config.ProcessTimeout.Duration)
	defer cancel()

	log.Printf("[Worker %s] Processing message id %s, priority %d from queue %s.", w.id, msg.MessageID, msg.Priority, msg.QueueName)

	// Process the message using the domains worker
	if err := w.domainsWorker.HandleWorkflowEvent(processCtx, msg); err != nil {
		w.processor.collector.RecordError(msg.QueueName)
		w.processor.errorCount.Add(1)
		return fmt.Errorf("worker %s failed to process message: %w", w.id, err)
	}

	// Delete message from SQS after successful processing
	if err := w.deleteMessage(processCtx, msg); err != nil {
		w.processor.collector.RecordError(msg.QueueName)
		w.processor.errorCount.Add(1)
		return fmt.Errorf("worker %s failed to delete message: %w", w.id, err)
	}

	// Update worker-specific metrics
	w.msgCount.Add(1)
	w.mu.Lock()
	w.lastActive = time.Now()
	w.mu.Unlock()

	// Record processing metrics
	processingDuration := time.Since(startTime)
	w.processor.processedCount.Add(1)
	w.processor.collector.RecordProcessed(msg.QueueName, processingDuration)

	log.Printf("[Worker %s] Successfully processed message id %s in %v", w.id, msg.MessageID, processingDuration)

	return nil
}

func (w *Worker) deleteMessage(ctx context.Context, msg *models.Message) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      &msg.QueueURL,
		ReceiptHandle: msg.ReceiptHandle,
	}

	if _, err := w.processor.sqsClient.DeleteMessage(ctx, input); err != nil {
		return fmt.Errorf("deleting message from SQS: %w", err)
	}

	return nil
}

func (mp *MessageProcessorImpl) monitorAndScale() {
	ticker := time.NewTicker(mp.config.ScaleInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-mp.ctx.Done():
			return
		case <-ticker.C:
			// Get metrics without holding scaling lock
			bufferUsage, ok := mp.bufferMetricsCollector.GetBufferUtilization()
			if !ok {
				continue
			}

			messagesIn, messagesOut, ok := mp.bufferMetricsCollector.GetMessageCounts()
			if !ok {
				messagesIn, messagesOut = 0, 0
			}

			backlog := messagesIn - messagesOut
			currentWorkers := mp.workerCount.Load()

			// Calculate worker utilization
			mp.mu.RLock()
			activeWorkers := 0
			for _, worker := range mp.workers {
				if worker.status.Load() == workerStatusProcessing {
					activeWorkers++
				}
			}
			mp.mu.RUnlock()

			var utilizationRate float64
			if currentWorkers > 0 {
				utilizationRate = float64(activeWorkers) / float64(currentWorkers)
			}

			// Log metrics
			log.Printf("[ProcessorGroup %s] Scaling metrics - Workers: %d/%d (%.2f%% utilized), Backlog: %d, Buffer Usage: %.2f%%",
				mp.instanceID, currentWorkers, mp.config.MaxWorkers, utilizationRate*100,
				backlog, bufferUsage*100)

			// Check cool down periods
			now := time.Now()
			lastScaleUp := time.Unix(0, mp.lastScaleUpTime.Load())
			lastScaleDown := time.Unix(0, mp.lastScaleDownTime.Load())

			if lastScaleUp.Add(mp.config.ScaleUpCoolDown.Duration).After(now) ||
				lastScaleDown.Add(mp.config.ScaleDownCoolDown.Duration).After(now) {
				continue
			}

			// Evaluate scaling conditions
			bufferPressure := bufferUsage > mp.config.ScaleThreshold
			workerPressure := utilizationRate > mp.config.ScaleThreshold
			backlogThreshold := int64(mp.config.MaxWorkers * 10)
			backlogPressure := backlog > backlogThreshold

			mp.scalingMu.Lock()

			if currentWorkers < int32(mp.config.MaxWorkers) &&
				(bufferPressure || workerPressure || backlogPressure) {
				// Scale up by configured step size
				workersToAdd := min(
					mp.config.ScaleUpStep,
					int(int32(mp.config.MaxWorkers)-currentWorkers),
				)

				if workersToAdd > 0 {
					log.Printf("[ProcessorGroup %s] Scaling up by %d workers (current: %d)",
						mp.instanceID, workersToAdd, currentWorkers)

					for i := 0; i < workersToAdd; i++ {
						if err := mp.startWorker(); err != nil {
							log.Printf("[ProcessorGroup %s] Failed to scale up: %v", mp.instanceID, err)
							break
						}
					}
					mp.lastScaleUpTime.Store(now.UnixNano())
				}
			} else if currentWorkers > int32(mp.config.MinWorkers) {
				lowUtilization := utilizationRate < mp.config.ScaleThreshold/2
				lowBacklog := backlog < int64(mp.config.MinWorkers*5)

				if lowUtilization && !bufferPressure && lowBacklog {
					// Scale down by configured step size
					workersToRemove := min(
						mp.config.ScaleDownStep,
						int(currentWorkers-int32(mp.config.MinWorkers)),
					)

					if workersToRemove > 0 {
						log.Printf("[ProcessorGroup %s] Scaling down by %d workers (current: %d)",
							mp.instanceID, workersToRemove, currentWorkers)

						for i := 0; i < workersToRemove; i++ {
							mp.stopWorker()
						}
						mp.lastScaleDownTime.Store(now.UnixNano())
					}
				}
			}

			mp.scalingMu.Unlock()
		}
	}
}

func (mp *MessageProcessorImpl) stopWorker() {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Find an idle worker to stop
	for _, worker := range mp.workers {
		if worker.status.Load() == workerStatusIdle {
			close(worker.stopChan)
			log.Printf("[ProcessorGroup %s] Stopped worker %s", mp.instanceID, worker.id)
			return
		}
	}
}

func (mp *MessageProcessorImpl) Shutdown(ctx context.Context) error {
	log.Printf("[ProcessorGroup %s] Initiating shutdown...", mp.instanceID)
	mp.cancelFunc()

	// Stop all workers
	mp.mu.Lock()
	for _, worker := range mp.workers {
		close(worker.stopChan)
	}
	mp.mu.Unlock()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		mp.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		log.Printf("[ProcessorGroup %s] Shutdown completed. Processed %d messages, %d errors", mp.instanceID, mp.processedCount.Load(), mp.errorCount.Load())
		return nil
	}
}

// GetMetrics returns current processor metrics
func (mp *MessageProcessorImpl) GetMetrics() models.ProcessorMetrics {
	mp.mu.RLock()
	activeWorkers := 0
	for _, worker := range mp.workers {
		if worker.status.Load() == workerStatusProcessing {
			activeWorkers++
		}
	}
	mp.mu.RUnlock()

	return models.ProcessorMetrics{
		ActiveWorkers:  activeWorkers,
		ProcessedCount: mp.processedCount.Load(),
		ErrorCount:     mp.errorCount.Load(),
		ProcessingRate: mp.calculateProcessingRate(),
		ErrorRate:      mp.calculateErrorRate(),
	}
}

func (mp *MessageProcessorImpl) calculateProcessingRate() float64 {
	// Calculate messages processed per second over the last minute
	// Implementation would track timestamps of processed messages
	return float64(mp.processedCount.Load()) / 60.0
}

func (mp *MessageProcessorImpl) calculateErrorRate() float64 {
	processed := mp.processedCount.Load()
	if processed == 0 {
		return 0
	}
	return float64(mp.errorCount.Load()) / float64(processed)
}
