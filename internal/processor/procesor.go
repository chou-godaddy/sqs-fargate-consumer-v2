package processor

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/interfaces"
	"sqs-fargate-consumer-v2/internal/models"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"golang.org/x/exp/rand"
)

type MessageProcessorImpl struct {
	config                 config.ProcessorConfig
	buffer                 interfaces.MessageBuffer
	sqsClient              *sqs.Client
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

	scalingMu sync.Mutex // Mutex for scaling operations
}

type Worker struct {
	id         string
	status     atomic.Int32
	msgCount   atomic.Int64
	stopChan   chan struct{}
	processor  *MessageProcessorImpl
	lastActive time.Time
	mu         sync.RWMutex
}

const (
	workerStatusIdle int32 = iota
	workerStatusProcessing
	workerStatusStopped
)

func NewMessageProcessor(
	config config.ProcessorConfig,
	buffer interfaces.MessageBuffer,
	sqsClient *sqs.Client,
	collector interfaces.MetricsCollector,
	bufferMetricsCollector interfaces.BufferMetricsCollector,
) interfaces.MessageProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &MessageProcessorImpl{
		config:                 config,
		buffer:                 buffer,
		sqsClient:              sqsClient,
		collector:              collector,
		workers:                make(map[string]*Worker),
		ctx:                    ctx,
		cancelFunc:             cancel,
		bufferMetricsCollector: bufferMetricsCollector,
	}
}

func (mp *MessageProcessorImpl) Start(ctx context.Context) error {
	log.Printf("[Processor] Starting with initial workers: %d", mp.config.MinWorkers)

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

	workerID := fmt.Sprintf("processor-%d", time.Now().UnixNano())
	worker := &Worker{
		id:        workerID,
		stopChan:  make(chan struct{}),
		processor: mp,
	}

	mp.workers[workerID] = worker
	mp.workerCount.Add(1)

	mp.wg.Add(1)
	go mp.runWorker(worker)

	log.Printf("[Processor] Started new worker %s. Total workers: %d", workerID, mp.workerCount.Load())
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

	w.status.Store(workerStatusProcessing)
	defer w.status.Store(workerStatusIdle)

	startTime := time.Now()

	// Process message with timeout
	processCtx, cancel := context.WithTimeout(ctx, w.processor.config.ProcessTimeout.Duration)
	defer cancel()

	// Generate random processing duration
	var randomDuration time.Duration
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback to simple time-based random calculation if crypto/rand fails
		randomSeconds := time.Now().UnixNano()%2 + 1 // 1 to 3 seconds
		randomDuration = time.Duration(randomSeconds) * time.Second
	} else {
		randomInt := binary.BigEndian.Uint64(b[:])
		randomSeconds := (randomInt % 2) + 1 // 1 to 3 seconds
		randomDuration = time.Duration(randomSeconds) * time.Second
	}

	log.Printf("[Worker %s] Processing message id %s, priority %d from queue %s. Expected duration: %v", w.id, msg.MessageID, msg.Priority, msg.QueueName, randomDuration)

	// Simulate processing with random duration
	select {
	case <-processCtx.Done():
		return fmt.Errorf("message id %s processing timed out after %v", msg.MessageID, time.Since(startTime))
	case <-time.After(randomDuration):
		// Processing completed
	}

	log.Printf("[Worker %s] Finished processing message id %s, body: %s in %v", w.id, msg.MessageID, string(msg.Body), randomDuration)

	// Delete message from SQS
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
			high, medium, low, ok := mp.bufferMetricsCollector.GetBufferUtilization()
			if !ok {
				log.Printf("[Processor] Warning: Buffer utilization metrics not available")
				continue
			}

			messagesIn, messagesOut, ok := mp.bufferMetricsCollector.GetMessageCounts()
			if !ok {
				log.Printf("[Processor] Warning: Message count metrics not available")
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
			log.Printf("[Processor] Scaling metrics - Workers: %d/%d (%.2f%% utilized), Backlog: %d, Buffer Usage: High=%.2f%%, Medium=%.2f%%, Low=%.2f%%",
				currentWorkers, mp.config.MaxWorkers, utilizationRate*100,
				backlog, high*100, medium*100, low*100)

			// Evaluate scaling conditions
			shouldScaleUp := false
			shouldScaleDown := false

			bufferPressure := high > mp.config.ScaleThreshold ||
				medium > mp.config.ScaleThreshold ||
				low > mp.config.ScaleThreshold

			workerPressure := utilizationRate > mp.config.ScaleThreshold
			backlogThreshold := int64(mp.config.MaxWorkers * 10)
			backlogPressure := backlog > backlogThreshold

			// Determine scaling direction
			if currentWorkers < int32(mp.config.MaxWorkers) {
				if bufferPressure {
					log.Printf("[Processor] Buffer pressure detected (High: %.2f%%, Medium: %.2f%%, Low: %.2f%%)",
						high*100, medium*100, low*100)
					shouldScaleUp = true
				}
				if workerPressure {
					log.Printf("[Processor] Worker utilization pressure detected (%.2f%%)",
						utilizationRate*100)
					shouldScaleUp = true
				}
				if backlogPressure {
					log.Printf("[Processor] Message backlog pressure detected (%d messages)",
						backlog)
					shouldScaleUp = true
				}
			}

			if currentWorkers > int32(mp.config.MinWorkers) {
				lowUtilization := utilizationRate < mp.config.ScaleThreshold/2
				lowBacklog := backlog < int64(mp.config.MinWorkers*5)

				if lowUtilization && !bufferPressure && lowBacklog {
					log.Printf("[Processor] Low pressure detected - Utilization: %.2f%%, Backlog: %d",
						utilizationRate*100, backlog)
					shouldScaleDown = true
				}
			}

			// Only acquire scaling lock when actually scaling
			if shouldScaleUp || shouldScaleDown {
				mp.scalingMu.Lock()
				// Recheck conditions after acquiring lock
				currentWorkers = mp.workerCount.Load()

				if shouldScaleUp && currentWorkers < int32(mp.config.MaxWorkers) {
					log.Printf("[Processor] Scaling up worker count: %d -> %d",
						currentWorkers, currentWorkers+2)
					// Scale up by 2 workers
					for i := 0; i < 2; i++ {
						if err := mp.startWorker(); err != nil {
							log.Printf("[Processor] Failed to scale up: %v", err)
						}
					}
				} else if shouldScaleDown && currentWorkers > int32(mp.config.MinWorkers) {
					log.Printf("[Processor] Scaling down worker count: %d -> %d",
						currentWorkers, currentWorkers-1)
					mp.stopWorker()
				}
				mp.scalingMu.Unlock()
			}
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
			log.Printf("[Processor] Stopped worker %s", worker.id)
			return
		}
	}
}

func (mp *MessageProcessorImpl) Shutdown(ctx context.Context) error {
	log.Printf("[Processor] Initiating shutdown...")
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
		log.Printf("[Processor] Shutdown completed. Processed %d messages, %d errors", mp.processedCount.Load(), mp.errorCount.Load())
		return nil
	}
}

// GetMetrics returns current processor metrics
func (mp *MessageProcessorImpl) GetMetrics() models.ProcessorMetrics {
	mp.mu.RLock()
	activeWorkers := len(mp.workers)
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
