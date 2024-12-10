package scheduler

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/interfaces"
	"sync"
	"time"
)

// Scheduler manages queue selection based on priority and metrics
type SchedulerImpl struct {
	queues    []config.QueueConfig
	collector interfaces.MetricsCollector
	mu        sync.Mutex

	// Track queue selection history
	lastPollTimes map[string]time.Time
	pollCounts    map[string]int

	// Shutdown handling
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	rand *rand.Rand
}

func NewScheduler(queues []config.QueueConfig, collector interfaces.MetricsCollector) interfaces.Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	now := time.Now()
	source := rand.NewPCG(uint64(now.UnixNano()), uint64(now.UnixMicro()))
	r := rand.New(source)

	return &SchedulerImpl{
		queues:        queues,
		collector:     collector,
		lastPollTimes: make(map[string]time.Time),
		pollCounts:    make(map[string]int),
		ctx:           ctx,
		cancelFunc:    cancel,
		rand:          r,
	}
}

func (s *SchedulerImpl) Start(ctx context.Context) error {
	log.Printf("[Scheduler] Starting with %d queues", len(s.queues))

	// Start any background tasks if needed
	s.wg.Add(1)
	go s.cleanupRoutine()

	return nil
}

func (s *SchedulerImpl) cleanupRoutine() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanupStaleMetrics()
		}
	}
}

func (s *SchedulerImpl) cleanupStaleMetrics() {
	s.mu.Lock()
	defer s.mu.Unlock()

	threshold := time.Now().Add(-24 * time.Hour)
	for queueName, lastPoll := range s.lastPollTimes {
		if lastPoll.Before(threshold) {
			delete(s.lastPollTimes, queueName)
			delete(s.pollCounts, queueName)
		}
	}
}

// SelectQueue implements a priority-based queue selection algorithm
func (s *SchedulerImpl) SelectQueue() (*config.QueueConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create slice of queues with their weights
	type weightedQueue struct {
		queue  *config.QueueConfig
		weight float64
	}
	var queuesWithWeights []weightedQueue
	totalWeight := 0.0

	// Calculate basic weights for queues that have messages
	for _, queue := range s.queues {
		metrics := s.collector.GetQueueMetrics(queue.Name)
		if metrics == nil || metrics.MessageCount == 0 {
			continue
		}

		// Simple weight calculation:
		// - Use configured weight
		// - Multiply by priority to give higher priority queues more chance
		weight := queue.Weight * float64(queue.Priority)

		totalWeight += weight
		queuesWithWeights = append(queuesWithWeights, weightedQueue{
			queue:  &queue,
			weight: weight,
		})
	}

	if len(queuesWithWeights) == 0 {
		return nil, nil
	}

	// Generate random number between 0 and totalWeight
	selection := s.rand.Float64() * totalWeight
	currentWeight := 0.0

	// Select queue based on weight ranges
	for _, qw := range queuesWithWeights {
		currentWeight += qw.weight
		if selection <= currentWeight {
			log.Printf("[Scheduler] Selected queue %s (Priority: %d, Weight: %.2f)",
				qw.queue.Name, qw.queue.Priority, qw.weight)
			return qw.queue, nil
		}
	}

	// Fallback: return first queue (shouldn't normally happen)
	return queuesWithWeights[0].queue, nil
}

// updateQueueStats updates tracking information for selected queue
func (s *SchedulerImpl) updateQueueStats(queue *config.QueueConfig, now time.Time) {
	s.lastPollTimes[queue.Name] = now
	s.pollCounts[queue.Name]++

	// Reset poll counts for other queues
	for qName := range s.pollCounts {
		if qName != queue.Name {
			s.pollCounts[qName] = 0
		}
	}
}

func (s *SchedulerImpl) Shutdown(ctx context.Context) error {
	log.Printf("[Scheduler] Initiating shutdown...")
	s.cancelFunc()

	// Wait for cleanup routines with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("scheduler shutdown timed out: %w", ctx.Err())
	case <-done:
		log.Printf("[Scheduler] Shutdown completed successfully")
		return nil
	}
}
