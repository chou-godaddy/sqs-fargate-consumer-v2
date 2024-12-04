package consumer

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sqs-fargate-consumer-v2/internal/scheduler"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type ConsumerGroup struct {
	config    *config.ConsumerGroupConfig
	client    *sqs.Client
	scheduler *scheduler.Scheduler
	buffer    *EventBuffer
	collector *metrics.Collector

	workers   map[string]*ConsumerWorker
	workersMu sync.RWMutex

	// Worker ID management
	nextWorkerID atomic.Int32
	workerIDs    []int32 // Pool of available worker IDs
	idPoolMu     sync.Mutex

	// Worker status tracking
	workerStatus map[string]*ConsumerWorkerStatus
	statusMu     sync.RWMutex

	// Coordination
	shuttingDown atomic.Bool
	startupDone  atomic.Bool
}

type ConsumerWorkerStatus struct {
	ID             string
	NumericID      int32
	State          int32 // 0: idle, 1: polling, 2: stopped
	LastActiveTime time.Time
	LastIdleTime   time.Time
	MessageCount   atomic.Int64
	StartTime      time.Time
}

func NewConsumer(cfg *config.ConsumerGroupConfig, client *sqs.Client, collector *metrics.Collector, buffer *EventBuffer) *ConsumerGroup {
	return &ConsumerGroup{
		config:       cfg,
		client:       client,
		buffer:       buffer,
		collector:    collector,
		workers:      make(map[string]*ConsumerWorker),
		workerStatus: make(map[string]*ConsumerWorkerStatus),
		scheduler:    scheduler.NewScheduler(cfg.Queues, collector),
	}
}

func (c *ConsumerGroup) Start(ctx context.Context) error {
	log.Printf("[ConsumerGroup] Starting consumer group with configuration - Min workers: %d, Max workers: %d",
		c.config.MinWorkers, c.config.MaxWorkers)

	// Initialize worker ID pool
	for i := int32(0); i < int32(c.config.MinWorkers); i++ {
		c.workerIDs = append(c.workerIDs, i)
	}
	sort.Slice(c.workerIDs, func(i, j int) bool {
		return c.workerIDs[i] < c.workerIDs[j]
	})
	log.Printf("[ConsumerGroup] Initialized worker ID pool with %d IDs", len(c.workerIDs))

	// Start initial workers
	for i := 0; i < c.config.MinWorkers; i++ {
		if err := c.AddWorker(); err != nil {
			log.Printf("[ConsumerGroup] Fatal error starting initial workers: %v", err)
			return fmt.Errorf("fatal error starting initial workers: %w", err)
		}
	}

	c.startupDone.Store(true)
	log.Printf("[ConsumerGroup] Successfully started with %d workers", c.config.MinWorkers)

	// Start monitoring routines
	go c.monitorWorkerStatus(ctx)
	go c.monitorWorkerHealth(ctx)

	return nil
}

func (c *ConsumerGroup) getNextWorkerID() int32 {
	c.idPoolMu.Lock()
	defer c.idPoolMu.Unlock()

	if len(c.workerIDs) > 0 {
		id := c.workerIDs[0]
		c.workerIDs = c.workerIDs[1:]
		return id
	}

	return c.nextWorkerID.Add(1)
}

func (c *ConsumerGroup) returnWorkerID(id int32) {
	c.idPoolMu.Lock()
	defer c.idPoolMu.Unlock()

	c.workerIDs = append(c.workerIDs, id)
	sort.Slice(c.workerIDs, func(i, j int) bool {
		return c.workerIDs[i] < c.workerIDs[j]
	})
}

func (c *ConsumerGroup) AddWorker() error {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	if len(c.workers) >= c.config.MaxWorkers {
		log.Printf("[ConsumerGroup] Cannot add worker: maximum worker count (%d) reached", c.config.MaxWorkers)
		return fmt.Errorf("maximum worker count reached")
	}

	numericID := c.getNextWorkerID()
	workerID := fmt.Sprintf("consumer-worker-%d", numericID)

	log.Printf("[ConsumerGroup] Creating new worker with ID: %s", workerID)

	worker := NewConsumerWorker(workerID, c.buffer, c.scheduler, c.client, c.collector)
	c.workers[workerID] = worker

	status := &ConsumerWorkerStatus{
		ID:             workerID,
		NumericID:      numericID,
		State:          ConsumerWorkerStatusIdle,
		StartTime:      time.Now(),
		LastActiveTime: time.Now(),
		LastIdleTime:   time.Now(),
	}

	c.statusMu.Lock()
	c.workerStatus[workerID] = status
	c.statusMu.Unlock()

	workerCtx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		if err := worker.Start(workerCtx); err != nil {
			if !c.startupDone.Load() {
				log.Printf("[ConsumerGroup] Worker %s failed during startup: %v", workerID, err)
				panic(fmt.Sprintf("worker startup failed: %v", err))
			}
			log.Printf("[ConsumerGroup] Worker %s failed: %v", workerID, err)
			c.handleWorkerError(workerID, err)
		}
	}()

	c.collector.RecordConsumerAdded(workerID)
	log.Printf("[ConsumerGroup] Successfully added and started worker %s. Total workers: %d",
		workerID, len(c.workers))
	return nil
}

func (c *ConsumerGroup) RemoveWorker() error {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	if len(c.workers) <= c.config.MinWorkers {
		log.Printf("[ConsumerGroup] Cannot remove worker: minimum worker count (%d) reached", c.config.MinWorkers)
		return fmt.Errorf("minimum worker count reached")
	}

	var workerToRemove string
	var longestIdleTime time.Duration
	now := time.Now()

	c.statusMu.RLock()
	log.Printf("[ConsumerGroup] Searching for idle worker to remove among %d workers", len(c.workerStatus))
	for id, status := range c.workerStatus {
		if status.State == ConsumerWorkerStatusIdle {
			idleTime := now.Sub(status.LastIdleTime)
			log.Printf("[ConsumerGroup] Worker %s has been idle for %v", id, idleTime)
			if idleTime > longestIdleTime {
				longestIdleTime = idleTime
				workerToRemove = id
			}
		}
	}
	c.statusMu.RUnlock()

	if workerToRemove == "" {
		log.Printf("[ConsumerGroup] No idle workers available for removal")
		return fmt.Errorf("no idle workers available for removal")
	}

	if worker, exists := c.workers[workerToRemove]; exists {
		log.Printf("[ConsumerGroup] Stopping worker %s", workerToRemove)
		worker.Stop()
		delete(c.workers, workerToRemove)

		c.statusMu.Lock()
		status := c.workerStatus[workerToRemove]
		delete(c.workerStatus, workerToRemove)
		c.statusMu.Unlock()

		c.returnWorkerID(status.NumericID)
		c.collector.RecordConsumerRemoved(workerToRemove)
		log.Printf("[ConsumerGroup] Successfully removed worker %s. Total workers: %d",
			workerToRemove, len(c.workers))
		return nil
	}

	return fmt.Errorf("consumer worker not found")
}

func (c *ConsumerGroup) monitorWorkerStatus(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.updateWorkerMetrics()
		}
	}
}

func (c *ConsumerGroup) monitorWorkerHealth(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkWorkerHealth()
		}
	}
}

func (c *ConsumerGroup) updateWorkerMetrics() {
	c.workersMu.RLock()
	defer c.workersMu.RUnlock()

	now := time.Now()

	for id, worker := range c.workers {
		metrics := worker.GetMetrics()

		c.statusMu.Lock()
		if status, exists := c.workerStatus[id]; exists {
			prevState := status.State
			status.State = metrics.Status
			status.MessageCount.Store(metrics.MessageCount)
			status.LastActiveTime = metrics.LastPollTime

			// Update idle time if worker just became idle
			if prevState != ConsumerWorkerStatusIdle && status.State == ConsumerWorkerStatusIdle {
				status.LastIdleTime = now
			}
		}
		c.statusMu.Unlock()
	}
}

func (c *ConsumerGroup) checkWorkerHealth() {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()

	now := time.Now()

	for id, status := range c.workerStatus {
		// Check for stuck workers
		if status.State == ConsumerWorkerStatusPolling &&
			now.Sub(status.LastActiveTime) > 5*time.Minute {
			c.replaceWorker(id)
			continue
		}
	}
}

func (c *ConsumerGroup) handleWorkerError(workerID string, err error) {
	c.collector.RecordError(workerID, "consumer_worker_error")
	fmt.Printf("Worker %s encountered an error: %v\n", workerID, err)
	c.replaceWorker(workerID)
}

func (c *ConsumerGroup) replaceWorker(workerID string) {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	if worker, exists := c.workers[workerID]; exists {
		worker.Stop()

		c.statusMu.RLock()
		numericID := c.workerStatus[workerID].NumericID
		c.statusMu.RUnlock()

		newWorker := NewConsumerWorker(workerID, c.buffer, c.scheduler, c.client, c.collector)
		c.workers[workerID] = newWorker

		c.statusMu.Lock()
		c.workerStatus[workerID] = &ConsumerWorkerStatus{
			ID:             workerID,
			NumericID:      numericID,
			State:          ConsumerWorkerStatusIdle,
			StartTime:      time.Now(),
			LastActiveTime: time.Now(),
			LastIdleTime:   time.Now(),
		}
		c.statusMu.Unlock()

		go func() {
			if err := newWorker.Start(context.Background()); err != nil {
				c.collector.RecordError(workerID, "consumer_worker_error")
				fmt.Printf("Error starting replacement worker: %v\n", err)
			}
		}()

		c.collector.RecordConsumerReplaced(workerID)
	}
}

func (c *ConsumerGroup) Shutdown(ctx context.Context) error {
	log.Printf("[ConsumerGroup] Initiating shutdown sequence")
	c.shuttingDown.Store(true)

	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	workerCount := len(c.workers)
	log.Printf("[ConsumerGroup] Stopping %d workers", workerCount)

	for id, worker := range c.workers {
		log.Printf("[ConsumerGroup] Stopping worker %s", id)
		worker.Stop()

		c.statusMu.Lock()
		delete(c.workerStatus, id)
		c.statusMu.Unlock()
	}

	c.workers = make(map[string]*ConsumerWorker)
	log.Printf("[ConsumerGroup] Shutdown completed successfully")

	return nil
}
