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

	workers   map[string]*Worker
	workersMu sync.RWMutex

	// Worker ID management
	nextWorkerID atomic.Int32
	workerIDs    []int32 // Pool of available worker IDs
	idPoolMu     sync.Mutex

	// Worker status tracking
	workerStatus map[string]*WorkerStatus
	statusMu     sync.RWMutex

	// Coordination
	shuttingDown atomic.Bool
	startupDone  atomic.Bool
}

type WorkerStatus struct {
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
		workers:      make(map[string]*Worker),
		workerStatus: make(map[string]*WorkerStatus),
		scheduler:    scheduler.NewScheduler(cfg.Queues, collector),
	}
}

func (c *ConsumerGroup) Start(ctx context.Context) error {
	// Initialize worker ID pool with sequential IDs
	for i := int32(0); i < int32(c.config.MinWorkers); i++ {
		c.workerIDs = append(c.workerIDs, i)
	}
	sort.Slice(c.workerIDs, func(i, j int) bool {
		return c.workerIDs[i] < c.workerIDs[j]
	})

	// Start initial workers
	for i := 0; i < c.config.MinWorkers; i++ {
		if err := c.AddWorker(); err != nil {
			// Critical failure during initialization
			return fmt.Errorf("fatal error starting initial workers: %w", err)
		}
	}

	c.startupDone.Store(true)
	log.Printf("Consumer group started with %d workers\n", c.config.MinWorkers)

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
		return fmt.Errorf("maximum worker count reached")
	}

	numericID := c.getNextWorkerID()
	workerID := fmt.Sprintf("worker-%d", numericID)

	// Create new worker
	worker := NewWorker(workerID, c.buffer, c.scheduler, c.client, c.collector)
	c.workers[workerID] = worker

	// Initialize worker status
	status := &WorkerStatus{
		ID:             workerID,
		NumericID:      numericID,
		State:          WorkerStatusIdle,
		StartTime:      time.Now(),
		LastActiveTime: time.Now(),
		LastIdleTime:   time.Now(),
	}

	c.statusMu.Lock()
	c.workerStatus[workerID] = status
	c.statusMu.Unlock()

	// Start worker
	workerCtx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		if err := worker.Start(workerCtx); err != nil {
			if !c.startupDone.Load() {
				// During startup, propagate error to main thread
				panic(fmt.Sprintf("worker startup failed: %v", err))
			}
			log.Printf("Worker %s failed to start with an error: %v\n", workerID, err)
			c.handleWorkerError(workerID, err)
		}
	}()

	c.collector.RecordWorkerAdded(workerID)
	return nil
}

func (c *ConsumerGroup) RemoveWorker() error {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	if len(c.workers) <= c.config.MinWorkers {
		return fmt.Errorf("minimum worker count reached")
	}

	// Find the most idle worker to remove
	var workerToRemove string
	var longestIdleTime time.Duration
	now := time.Now()

	c.statusMu.RLock()
	for id, status := range c.workerStatus {
		if status.State == WorkerStatusIdle {
			idleTime := now.Sub(status.LastIdleTime)
			if idleTime > longestIdleTime {
				longestIdleTime = idleTime
				workerToRemove = id
			}
		}
	}
	c.statusMu.RUnlock()

	if workerToRemove == "" {
		// If no idle worker found, don't remove any
		return fmt.Errorf("no idle workers available for removal")
	}

	if worker, exists := c.workers[workerToRemove]; exists {
		worker.Stop()
		delete(c.workers, workerToRemove)

		c.statusMu.Lock()
		status := c.workerStatus[workerToRemove]
		delete(c.workerStatus, workerToRemove)
		c.statusMu.Unlock()

		c.returnWorkerID(status.NumericID)
		c.collector.RecordWorkerRemoved(workerToRemove)
		return nil
	}

	return fmt.Errorf("worker not found")
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
			if prevState != WorkerStatusIdle && status.State == WorkerStatusIdle {
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
		if status.State == WorkerStatusPolling &&
			now.Sub(status.LastActiveTime) > 5*time.Minute {
			c.replaceWorker(id)
			continue
		}
	}
}

func (c *ConsumerGroup) handleWorkerError(workerID string, err error) {
	c.collector.RecordError(workerID, "worker_error")
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

		newWorker := NewWorker(workerID, c.buffer, c.scheduler, c.client, c.collector)
		c.workers[workerID] = newWorker

		c.statusMu.Lock()
		c.workerStatus[workerID] = &WorkerStatus{
			ID:             workerID,
			NumericID:      numericID,
			State:          WorkerStatusIdle,
			StartTime:      time.Now(),
			LastActiveTime: time.Now(),
			LastIdleTime:   time.Now(),
		}
		c.statusMu.Unlock()

		go func() {
			if err := newWorker.Start(context.Background()); err != nil {
				c.collector.RecordError(workerID, "worker_error")
				fmt.Printf("Error starting replacement worker: %v\n", err)
			}
		}()

		c.collector.RecordWorkerReplaced(workerID)
	}
}

func (c *ConsumerGroup) Shutdown(ctx context.Context) error {
	c.shuttingDown.Store(true)

	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	for id, worker := range c.workers {
		worker.Stop()

		c.statusMu.Lock()
		delete(c.workerStatus, id)
		c.statusMu.Unlock()
	}

	c.workers = make(map[string]*Worker)

	return nil
}
