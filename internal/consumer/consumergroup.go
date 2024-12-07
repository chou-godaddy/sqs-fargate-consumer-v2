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
	log.Printf("[ConsumerGroup] Starting with initial workers: %d", cg.config.MinWorkers)

	// Start initial workers
	for i := 0; i < cg.config.MinWorkers; i++ {
		if err := cg.startWorker(); err != nil {
			return fmt.Errorf("failed to start initial worker: %w", err)
		}
	}

	// Start scaling routine
	go cg.monitorAndScale()

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

func (cg *ConsumerGroup) shouldThrottleForPriority(priority int, consumer *Consumer) (bool, float64) {
	high, medium, low, ok := cg.bufferMetricsCollector.GetBufferUtilization()
	if !ok {
		return true, 0 // Throttle if we can't get metrics
	}

	var bufferUsage float64
	switch priority {
	case 3:
		bufferUsage = high
	case 2:
		bufferUsage = medium
	case 1:
		bufferUsage = low
	}

	if bufferUsage > cg.config.ScaleThreshold {
		log.Printf("[Consumer %s] Priority %d buffer pressure (%.2f%%), backing off",
			consumer.id, priority, bufferUsage*100)
		return true, bufferUsage
	}

	return false, bufferUsage
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

	// Check if we should throttle for this priority
	if shouldThrottle, bufferUsage := cg.shouldThrottleForPriority(queue.Priority, consumer); shouldThrottle {
		time.Sleep(time.Duration(float64(cg.config.PollBackoffInterval.Duration) * bufferUsage))
		return nil
	}

	// Poll and process messages
	return cg.pollQueue(consumer, queue)
}

func (cg *ConsumerGroup) pollQueue(consumer *Consumer, queue *config.QueueConfig) error {
	result, err := cg.sqsClient.ReceiveMessage(cg.ctx, &sqs.ReceiveMessageInput{
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

	messageCount := 0
	for _, msg := range result.Messages {
		message := models.NewMessage(&msg, queue.URL, queue.Name, models.Priority(queue.Priority))

		if err := cg.buffer.Push(message); err != nil {
			log.Printf("[Consumer %s] Failed to buffer message %s: %v",
				consumer.id, message.MessageID, err)
			continue
		}

		messageCount++
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

func (cg *ConsumerGroup) shouldScale(highUsage, mediumUsage, lowUsage float64, messagesIn, messagesOut int64) int {
	maxBufferUsage := max(highUsage, max(mediumUsage, lowUsage))
	currentWorkers := cg.workerCount.Load()

	// Check cooldown periods
	now := time.Now()
	if time.Unix(0, cg.lastScaleUpTime.Load()).Add(cg.config.ScaleUpCoolDown.Duration).After(now) {
		return 0
	}
	if time.Unix(0, cg.lastScaleDownTime.Load()).Add(cg.config.ScaleDownCoolDown.Duration).After(now) {
		return 0
	}

	// Emergency scale down takes precedence
	if maxBufferUsage > cg.config.ScaleThreshold {
		log.Printf("[ConsumerGroup] Emergency scale down - buffer usage %.2f%% above threshold. Current workers: %d",
			maxBufferUsage*100, currentWorkers)
		return -1
	}

	// Regular scaling conditions
	if messagesIn == messagesOut {
		if maxBufferUsage < cg.config.ScaleThreshold/3 {
			log.Printf("[ConsumerGroup] Scale down - no backlog and low buffer usage (%.2f%%). Current workers: %d",
				maxBufferUsage*100, currentWorkers)
			return -1
		}
	} else if messagesIn > messagesOut {
		if maxBufferUsage < cg.config.ScaleThreshold {
			log.Printf("[ConsumerGroup] Scale up - has backlog and buffer space (%.2f%%). Current workers: %d",
				maxBufferUsage*100, currentWorkers)
			return 1
		}
	}

	return 0
}

func (cg *ConsumerGroup) adjustWorkerCount() {
	cg.scalingMu.Lock()
	defer cg.scalingMu.Unlock()

	high, medium, low, ok := cg.bufferMetricsCollector.GetBufferUtilization()
	if !ok {
		return
	}

	messagesIn, messagesOut, ok := cg.bufferMetricsCollector.GetMessageCounts()
	if !ok {
		return
	}

	scale := cg.shouldScale(high, medium, low, messagesIn, messagesOut)
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

		log.Printf("[ConsumerGroup] Attempting to remove %d workers from current count %d",
			workersToRemove, currentWorkers)

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
