package scheduler

import (
	"context"
	"fmt"
	"log"
	"math"
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
}

func NewScheduler(queues []config.QueueConfig, collector interfaces.MetricsCollector) interfaces.Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerImpl{
		queues:        queues,
		collector:     collector,
		lastPollTimes: make(map[string]time.Time),
		pollCounts:    make(map[string]int),
		ctx:           ctx,
		cancelFunc:    cancel,
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

	now := time.Now()
	var selectedQueue *config.QueueConfig
	var highestScore float64 = -1

	// First pass: check high priority queues with messages
	for _, queue := range s.filterQueuesWithMessages(3) {
		score := s.calculateQueueScore(queue, now)
		if score > highestScore {
			highestScore = score
			selectedQueue = &queue
		}
	}

	// If no high priority messages, check medium priority
	if selectedQueue == nil {
		for _, queue := range s.filterQueuesWithMessages(2) {
			score := s.calculateQueueScore(queue, now)
			if score > highestScore {
				highestScore = score
				selectedQueue = &queue
			}
		}
	}

	// Finally check low priority if still no selection
	if selectedQueue == nil {
		for _, queue := range s.filterQueuesWithMessages(1) {
			score := s.calculateQueueScore(queue, now)
			if score > highestScore {
				highestScore = score
				selectedQueue = &queue
			}
		}
	}

	if selectedQueue != nil {
		s.updateQueueStats(selectedQueue, now)
		log.Printf("[Scheduler] Selected queue %s (Priority: %d, Score: %.2f)", selectedQueue.Name, selectedQueue.Priority, highestScore)
	}

	return selectedQueue, nil
}

// filterQueuesWithMessages returns queues of specified priority that have messages
func (s *SchedulerImpl) filterQueuesWithMessages(priority int) []config.QueueConfig {
	var filtered []config.QueueConfig
	for _, queue := range s.queues {
		if queue.Priority != priority {
			continue
		}

		metrics := s.collector.GetQueueMetrics(queue.Name)
		if metrics != nil && metrics.MessageCount > 0 {
			filtered = append(filtered, queue)
		}
	}
	return filtered
}

// calculateQueueScore determines queue selection priority
func (s *SchedulerImpl) calculateQueueScore(queue config.QueueConfig, now time.Time) float64 {
	metrics := s.collector.GetQueueMetrics(queue.Name)
	if metrics == nil {
		return 0
	}

	// Base score starts with message count and weight
	baseScore := float64(metrics.MessageCount) * queue.Weight

	// Priority multiplier ensures high priority queues maintain precedence
	// Priority 3 (High)   = 1000x
	// Priority 2 (Medium) = 100x
	// Priority 1 (Low)    = 10x
	priorityMultiplier := math.Pow(10, float64(queue.Priority+1))
	score := baseScore * priorityMultiplier

	// Add a capped wait time factor
	lastPoll, exists := s.lastPollTimes[queue.Name]
	if exists {
		// How many seconds since last poll
		waitTime := now.Sub(lastPoll).Seconds()
		// Cap the wait time boost to prevent overwhelming priority
		maxWaitBoost := float64(queue.Priority) * 0.5                 // Higher priority = smaller max boost
		waitFactor := math.Min(1.0+(waitTime/30.0), 1.0+maxWaitBoost) // reach maximum boost after 30 seconds
		score *= waitFactor
	}

	// Apply reduced poll count penalty
	pollCount := s.pollCounts[queue.Name]
	if pollCount > 10 {
		score *= 0.95 // Reduced penalty to 5%
	}

	log.Printf("[Scheduler] Queue %s score details - Priority: %d, Messages: %d, Base Score: %.2f, Final Score: %.2f", queue.Name, queue.Priority, metrics.MessageCount, baseScore, score)

	return score
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
