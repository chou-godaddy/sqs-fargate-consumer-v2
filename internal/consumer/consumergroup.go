package consumer

import (
	"context"
	"fmt"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/interfaces"
	"sqs-fargate-consumer-v2/internal/models"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type ConsumerGroup struct {
	instanceID             string
	config                 config.ConsumerConfig
	sqsClient              *sqs.Client
	scheduler              interfaces.Scheduler
	buffer                 interfaces.MessageBuffer
	collector              interfaces.MetricsCollector
	bufferMetricsCollector interfaces.BufferMetricsCollector

	workers     map[string]*Consumer
	workerCount atomic.Int32
	workerMu    sync.RWMutex

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	lastScaleUpTime     atomic.Int64
	lastScaleDownTime   atomic.Int64
	lastSuccessfulScale atomic.Int64
	scalingMu           sync.Mutex
	scalingInProgress   atomic.Bool
}

type Consumer struct {
	id       string
	queueURL string
	status   atomic.Int32
	msgCount atomic.Int64
	lastPoll atomic.Int64
	stopChan chan struct{}
	stopped  atomic.Bool
}

const (
	consumerStatusIdle int32 = iota
	consumerStatusPolling
	consumerStatusStopped
)

func NewConsumerGroup(
	config config.ConsumerConfig,
	sqsClient *sqs.Client,
	scheduler interfaces.Scheduler,
	buffer interfaces.MessageBuffer,
	collector interfaces.MetricsCollector,
	bufferMetricsCollector interfaces.BufferMetricsCollector,
) *ConsumerGroup {
	instanceID := fmt.Sprintf("consumer-group-%d", time.Now().UnixNano())
	log.Printf("[ConsumerGroup] Creating new instance: %s", instanceID)
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerGroup{
		instanceID:             instanceID,
		config:                 config,
		sqsClient:              sqsClient,
		scheduler:              scheduler,
		buffer:                 buffer,
		collector:              collector,
		workers:                make(map[string]*Consumer),
		ctx:                    ctx,
		cancelFunc:             cancel,
		bufferMetricsCollector: bufferMetricsCollector,
	}
}

func (cg *ConsumerGroup) Start(ctx context.Context) error {
	cg.ctx, cg.cancelFunc = context.WithCancel(ctx)
	log.Printf("[ConsumerGroup] Starting with initial workers: %d", cg.config.MinWorkers)

	// Start initial workers
	for i := 0; i < cg.config.MinWorkers; i++ {
		if err := cg.startWorker(); err != nil {
			return fmt.Errorf("failed to start initial worker: %w", err)
		}
	}

	if cg.config.MinWorkers < cg.config.MaxWorkers {
		// Start scaling routine
		go cg.monitorAndScale()
	}

	return nil
}

func (cg *ConsumerGroup) startWorker() error {
	cg.workerMu.Lock()
	defer cg.workerMu.Unlock()

	if cg.workerCount.Load() >= int32(cg.config.MaxWorkers) {
		return fmt.Errorf("maximum worker count reached")
	}

	workerID := fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	consumer := &Consumer{
		id:       workerID,
		stopChan: make(chan struct{}),
	}

	cg.workers[workerID] = consumer
	cg.workerCount.Add(1)

	cg.wg.Add(1)
	go cg.runWorker(consumer)

	log.Printf("[ConsumerGroup] Started new worker %s. Total workers: %d", workerID, cg.workerCount.Load())
	return nil
}

func (cg *ConsumerGroup) runWorker(consumer *Consumer) {
	defer func() {
		cg.wg.Done()
		cg.workerMu.Lock()
		if _, exists := cg.workers[consumer.id]; exists {
			delete(cg.workers, consumer.id)
			// Only decrement count if this wasn't an explicit stop
			if !consumer.stopped.Load() {
				cg.workerCount.Add(-1)
				log.Printf("[ConsumerGroup] Worker %s exited naturally, new count: %d",
					consumer.id, cg.workerCount.Load())
			}
		}
		cg.workerMu.Unlock()

		if r := recover(); r != nil {
			log.Printf("[ConsumerGroup] Recovered from panic in worker %s: %v", consumer.id, r)
		}
	}()

	for {
		select {
		case <-cg.ctx.Done():
			return
		case <-consumer.stopChan:
			return
		default:
			if err := cg.pollAndProcess(consumer); err != nil {
				log.Printf("[Consumer %s] Error polling messages: %v", consumer.id, err)
				time.Sleep(time.Second) // Backoff on error
			}
		}
	}
}

func (cg *ConsumerGroup) pollAndProcess(consumer *Consumer) error {
	// Check minimum poll interval
	lastPollTime := time.Unix(0, consumer.lastPoll.Load())
	if time.Since(lastPollTime) < cg.config.MinPollInterval.Duration {
		time.Sleep(cg.config.MinPollInterval.Duration - time.Since(lastPollTime))
		return nil
	}

	// Get queue from scheduler
	queue, err := cg.scheduler.SelectQueue()
	if err != nil {
		return fmt.Errorf("selecting queue: %w", err)
	}
	if queue == nil {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	// Set status to polling since we have a queue to work on
	if !consumer.status.CompareAndSwap(consumerStatusIdle, consumerStatusPolling) {
		return nil
	}
	defer consumer.status.Store(consumerStatusIdle)

	// Poll and process messages
	if err := cg.pollQueue(consumer, queue); err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil // Normal shutdown or timeout, don't log as error
		}
		return err
	}

	return nil
}

func (cg *ConsumerGroup) pollQueue(consumer *Consumer, queue *config.QueueConfig) error {
	// Create a timeout context for the entire batch processing
	batchCtx, cancel := context.WithTimeout(cg.ctx, 30*time.Second)
	defer cancel()

	result, err := cg.sqsClient.ReceiveMessage(batchCtx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.URL,
		MaxNumberOfMessages: int32(cg.config.MaxBatchSize),
		WaitTimeSeconds:     20,
		AttributeNames: []types.QueueAttributeName{
			"ApproximateReceiveCount",
			"SentTimestamp",
		},
		MessageAttributeNames: []string{"All"},
	})

	if err != nil {
		return fmt.Errorf("failed to receive messages: %w", err)
	}

	log.Printf("[Consumer %s] Received %d messages from queue %s (Priority: %d)",
		consumer.id, len(result.Messages), queue.Name, queue.Priority)

	messageCount := 0
	for _, msg := range result.Messages {
		message := models.NewMessage(&msg, queue.URL, queue.Name, models.Priority(queue.Priority))

		// With blocking channels, we'll keep trying until context is done
		select {
		case <-batchCtx.Done():
			log.Printf("[Consumer %s] Context done while pushing message %s: %v",
				consumer.id, message.MessageID, batchCtx.Err())
			return batchCtx.Err()

		default:
			// This will block until the message is pushed or context is done
			err := cg.buffer.Push(message)
			if err != nil {
				if err == models.ErrMessageTooLarge {
					log.Printf("[Consumer %s] Message %s exceeds size limit, skipping",
						consumer.id, message.MessageID)
					continue
				}
				return fmt.Errorf("failed to push message: %w", err)
			}
			messageCount++
		}
	}

	if messageCount > 0 {
		consumer.msgCount.Add(int64(messageCount))
		consumer.lastPoll.Store(time.Now().UnixNano())
		log.Printf("[Consumer %s] Successfully buffered %d messages from queue %s (Priority: %d)",
			consumer.id, messageCount, queue.Name, queue.Priority)
	}

	return nil
}

func (cg *ConsumerGroup) monitorAndScale() {
	ticker := time.NewTicker(cg.config.ScaleInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-cg.ctx.Done():
			return
		case <-ticker.C:
			// Get current time stamp for this scaling attempt
			now := time.Now()
			lastScale := time.Unix(0, cg.lastSuccessfulScale.Load())
			if time.Since(lastScale) < cg.config.ScaleInterval.Duration {
				continue // Skip this tick if not enough time has passed
			}

			// If we can acquire the scaling lock, proceed
			if cg.scalingInProgress.CompareAndSwap(false, true) {
				cg.lastSuccessfulScale.Store(now.UnixNano())
				cg.adjustWorkerCount()
				cg.scalingInProgress.Store(false)
			}
		}
	}
}

func (cg *ConsumerGroup) shouldScale() int {
	currentWorkers := cg.workerCount.Load()

	// Get queue metrics
	queueMetrics := cg.collector.GetAllQueueMetrics()
	totalQueuedMessages := 0
	for _, metrics := range queueMetrics {
		totalQueuedMessages += int(metrics.MessageCount)
	}

	// Check buffer capacity
	bufferUsage, ok := cg.bufferMetricsCollector.GetBufferUtilization()
	if !ok {
		return 0
	}

	// Calculate optimal messages per worker based on scale interval
	// Long polling is 20s, batch size is maxBatchSize
	scaleIntervalSecs := cg.config.ScaleInterval.Duration.Seconds()
	pollsPerInterval := scaleIntervalSecs / 20.0 // 20s is long polling interval
	optimalMsgsPerWorker := pollsPerInterval * float64(cg.config.MaxBatchSize)

	// Target 70% of the theoretical maximum
	targetMsgsPerWorker := optimalMsgsPerWorker * cg.config.ScaleThreshold

	// Calculate current messages per worker
	messagesPerWorker := float64(totalQueuedMessages) / float64(currentWorkers)

	log.Printf("[ConsumerGroup] Scaling metrics - Workers: %d, Messages: %d, Msgs/Worker: %.2f, "+
		"Target Msgs/Worker: %.2f, Buffer Usage: %.2f, Scale Interval: %.2fs",
		currentWorkers, totalQueuedMessages, messagesPerWorker, targetMsgsPerWorker,
		bufferUsage, scaleIntervalSecs)

	// Scale up if above target and buffer has room
	if messagesPerWorker > targetMsgsPerWorker && bufferUsage < cg.config.ScaleThreshold {
		log.Printf("[ConsumerGroup] Scale up suggested - msgs/worker: %.2f exceeds target: %.2f",
			messagesPerWorker, targetMsgsPerWorker)
		return 1
	}

	// Scale down if below 50% of target
	if messagesPerWorker < (targetMsgsPerWorker * 0.5) {
		log.Printf("[ConsumerGroup] Scale down suggested - msgs/worker: %.2f below 50%% of target: %.2f",
			messagesPerWorker, targetMsgsPerWorker)
		return -1
	}

	return 0
}

func (cg *ConsumerGroup) adjustWorkerCount() {
	cg.scalingMu.Lock()
	defer cg.scalingMu.Unlock()

	scale := cg.shouldScale()
	currentWorkers := cg.workerCount.Load()
	now := time.Now()

	workersBefore := cg.workerCount.Load()
	log.Printf("[ConsumerGroup %s] Scaling check - Current workers: %d, Scale direction: %d",
		cg.instanceID, workersBefore, scale)

	switch scale {
	case 1: // Scale up
		currentWorkers = cg.workerCount.Load() // Get fresh count
		if currentWorkers >= int32(cg.config.MaxWorkers) {
			return
		}

		workersToAdd := min(
			cg.config.ScaleUpStep,
			int(int32(cg.config.MaxWorkers)-currentWorkers),
		)

		if workersToAdd > 0 {
			for i := 0; i < workersToAdd; i++ {
				if err := cg.startWorker(); err != nil {
					break
				}
			}
			cg.lastScaleUpTime.Store(now.UnixNano())
			log.Printf("[ConsumerGroup] Scaled up by %d workers. New count: %d",
				workersToAdd, cg.workerCount.Load())
		}

	case -1: // Scale down
		currentWorkers = cg.workerCount.Load() // Get fresh count
		if currentWorkers <= int32(cg.config.MinWorkers) {
			log.Printf("[ConsumerGroup] Cannot scale down: at minimum workers (%d)", currentWorkers)
			return
		}

		workersToRemove := min(
			cg.config.ScaleDownStep,
			int(currentWorkers-int32(cg.config.MinWorkers)),
		)

		if workersToRemove > 0 {
			successfullyRemoved := 0
			for i := 0; i < workersToRemove; i++ {
				beforeCount := cg.workerCount.Load()
				cg.stopWorker()
				afterCount := cg.workerCount.Load()
				if afterCount < beforeCount {
					successfullyRemoved++
				}
			}
			if successfullyRemoved > 0 {
				cg.lastScaleDownTime.Store(now.UnixNano())
				log.Printf("[ConsumerGroup] Successfully scaled down by %d workers. New count: %d",
					successfullyRemoved, cg.workerCount.Load())
			}
		}
	}
}

func (cg *ConsumerGroup) stopWorker() {
	cg.workerMu.Lock()
	defer cg.workerMu.Unlock()

	for id, worker := range cg.workers {
		if worker.status.Load() == consumerStatusIdle {
			select {
			case <-worker.stopChan: // Channel already closed
				continue
			default:
				if worker.stopped.CompareAndSwap(false, true) {
					close(worker.stopChan)
					delete(cg.workers, id)
					cg.workerCount.Add(-1)
					log.Printf("[ConsumerGroup] Stopped worker %s, new count: %d",
						id, cg.workerCount.Load())
					return
				}
			}
		}
	}
}

func (cg *ConsumerGroup) Shutdown(ctx context.Context) error {
	log.Printf("[ConsumerGroup] Initiating shutdown...")
	cg.cancelFunc()

	cg.workerMu.Lock()
	for _, worker := range cg.workers {
		select {
		case <-worker.stopChan:
			continue
		default:
			close(worker.stopChan)
		}
	}
	cg.workerMu.Unlock()

	done := make(chan struct{})
	go func() {
		cg.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("consumer group shutdown timed out: %w", ctx.Err())
	case <-done:
		log.Printf("[ConsumerGroup] Shutdown completed successfully")
		return nil
	}
}
