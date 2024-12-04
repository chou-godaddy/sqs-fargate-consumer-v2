package consumer

import (
	"context"
	"fmt"
	"log"
	"math"
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
	config    config.ConsumerConfig
	sqsClient *sqs.Client
	scheduler interfaces.Scheduler
	buffer    interfaces.MessageBuffer
	collector interfaces.MetricsCollector

	workers     map[string]*Consumer
	workerCount atomic.Int32
	mu          sync.RWMutex

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

type Consumer struct {
	id       string
	queueURL string
	status   atomic.Int32
	msgCount atomic.Int64
	lastPoll time.Time
	stopChan chan struct{}
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
) *ConsumerGroup {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerGroup{
		config:     config,
		sqsClient:  sqsClient,
		scheduler:  scheduler,
		buffer:     buffer,
		collector:  collector,
		workers:    make(map[string]*Consumer),
		ctx:        ctx,
		cancelFunc: cancel,
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
	cg.mu.Lock()
	defer cg.mu.Unlock()

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

	log.Printf("[ConsumerGroup] Started new worker %s. Total workers: %d",
		workerID, cg.workerCount.Load())
	return nil
}

func (cg *ConsumerGroup) runWorker(consumer *Consumer) {
	defer cg.wg.Done()
	defer func() {
		cg.mu.Lock()
		delete(cg.workers, consumer.id)
		cg.mu.Unlock()
		cg.workerCount.Add(-1)
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
	consumer.status.Store(consumerStatusPolling)
	defer consumer.status.Store(consumerStatusIdle)

	// Select queue to poll
	queue, err := cg.scheduler.SelectQueue()
	if err != nil {
		return fmt.Errorf("selecting queue: %w", err)
	}
	if queue == nil {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	// Poll messages
	result, err := cg.sqsClient.ReceiveMessage(cg.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queue.URL,
		MaxNumberOfMessages: int32(cg.config.MaxBatchSize),
		WaitTimeSeconds:     20,
		AttributeNames: []types.QueueAttributeName{
			"ApproximateReceiveCount",
			"SentTimestamp",
		},
		// Add MessageAttributeNames to get any custom attributes
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		return fmt.Errorf("failed to receive messages: %v", err)
	}

	// Process received messages
	for _, msg := range result.Messages {
		message := &models.Message{
			QueueURL:   queue.URL,
			QueueName:  queue.Name,
			Priority:   models.Priority(queue.Priority),
			MessageID:  *msg.MessageId,
			Body:       []byte(*msg.Body),
			ReceiptHandle: msg.ReceiptHandle,
			Size:       int64(len(*msg.Body)),
			ReceivedAt: time.Now(),
		}

		if err := cg.buffer.Push(message); err != nil {
			log.Printf("[Consumer %s] Failed to buffer message %s: %v",
				consumer.id, message.MessageID, err)
			continue
		}

		consumer.msgCount.Add(1)
		consumer.lastPoll = time.Now()
	}

	log.Printf("[Consumer %s] Polled %d messages from queue %s and pushed to message buffer", consumer.id, len(result.Messages), queue.Name)

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
			totalMessages := int64(0)
			totalInFlight := int64(0)
			avgProcessingTime := float64(0)
			activeQueues := 0

			// Calculate metrics across all queues
			for _, queueConfig := range cg.config.Queues {
				if metrics := cg.collector.GetQueueMetrics(queueConfig.Name); metrics != nil {
					totalMessages += metrics.MessageCount
					totalInFlight += metrics.InFlightCount
					if metrics.ProcessedCount.Load() > 0 {
						avgProcessingTime += float64(metrics.ProcessingTime.Load()) / float64(metrics.ProcessedCount.Load())
						activeQueues++
					}
				}
			}

			if activeQueues > 0 {
				avgProcessingTime /= float64(activeQueues)
			}

			currentWorkers := cg.workerCount.Load()
			desiredWorkers := calculateDesiredWorkers(
				totalMessages,
				totalInFlight,
				avgProcessingTime,
				cg.config,
			)

			// Apply scaling decisions
			if desiredWorkers > int64(currentWorkers) {
				for i := 0; i < min(int(desiredWorkers-int64(currentWorkers)), 3); i++ {
					if err := cg.startWorker(); err != nil {
						log.Printf("[ConsumerGroup] Failed to scale up: %v", err)
						break
					}
				}
			} else if desiredWorkers < int64(currentWorkers) && currentWorkers > int32(cg.config.MinWorkers) {
				cg.stopWorker()
			}
		}
	}
}

func calculateDesiredWorkers(totalMessages, inFlight int64, avgProcessingTime float64, config config.ConsumerConfig) int64 {
	// Calculate processing capacity needed
	if totalMessages == 0 && inFlight == 0 {
		return int64(config.MinWorkers)
	}

	// Estimate messages per worker per second
	messagesPerWorkerPerSecond := 1000.0 / max(avgProcessingTime, 100.0)

	// Calculate needed workers based on total workload
	totalWorkload := float64(totalMessages + inFlight)
	desiredWorkers := int64(math.Ceil(totalWorkload / (messagesPerWorkerPerSecond * float64(config.ScaleInterval.Seconds()))))

	// Apply boundaries
	desiredWorkers = max(int64(config.MinWorkers), min(desiredWorkers, int64(config.MaxWorkers)))

	// Add headroom for bursts
	if totalMessages > 0 {
		burstFactor := math.Log1p(float64(totalMessages)) / math.Log1p(1000.0)
		desiredWorkers = int64(float64(desiredWorkers) * (1 + burstFactor*0.2))
	}

	return desiredWorkers
}

func (cg *ConsumerGroup) stopWorker() {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	// Find an idle worker to stop
	for _, worker := range cg.workers {
		if worker.status.Load() == consumerStatusIdle {
			close(worker.stopChan)
			log.Printf("[ConsumerGroup] Stopped worker %s", worker.id)
			return
		}
	}
}

func (cg *ConsumerGroup) Shutdown(ctx context.Context) error {
	log.Printf("[ConsumerGroup] Initiating shutdown...")
	cg.cancelFunc()

	// Stop all workers
	cg.mu.Lock()
	for _, worker := range cg.workers {
		close(worker.stopChan)
	}
	cg.mu.Unlock()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		cg.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		log.Printf("[ConsumerGroup] Shutdown completed")
		return nil
	}
}
