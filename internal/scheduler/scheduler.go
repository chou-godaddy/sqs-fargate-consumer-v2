package scheduler

import (
	"context"
	"fmt"
	"log"
	"math"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sync"
	"time"
)

type QueueMetrics struct {
	MessageCount   int64
	InFlightCount  int64
	ProcessingTime time.Duration
	LastPollTime   time.Time
	ErrorCount     int64
	Priority       int
	Weight         float64
}

type Scheduler struct {
	queues    []config.QueueConfig
	metrics   map[string]*QueueMetrics
	collector *metrics.Collector
	mu        sync.RWMutex

	priorityWeights    map[int]float64
	lastSelectedQueue  string
	consecutiveSelects map[string]int
}

func NewScheduler(queues []config.QueueConfig, collector *metrics.Collector) *Scheduler {
	priorityWeights := make(map[int]float64)
	for i := 1; i <= 3; i++ {
		priorityWeights[i] = math.Pow(2, float64(i-1))
	}

	log.Printf("[Scheduler] Initializing with %d queues", len(queues))
	for _, q := range queues {
		log.Printf("[Scheduler] Registered queue: %s (Priority: %d, Weight: %.2f)",
			q.Name, q.Priority, q.Weight)
	}

	return &Scheduler{
		queues:             queues,
		metrics:            make(map[string]*QueueMetrics),
		collector:          collector,
		priorityWeights:    priorityWeights,
		consecutiveSelects: make(map[string]int),
	}
}

func (s *Scheduler) UpdateMetrics(queueName string, metrics *QueueMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics[queueName] = metrics
}

func (s *Scheduler) SelectQueue() (*config.QueueConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var selectedQueue *config.QueueConfig
	highestScore := -1.0
	now := time.Now()

	log.Printf("[Scheduler] Starting queue selection")

	for _, queue := range s.queues {
		metrics, exists := s.metrics[queue.Name]
		if metrics == nil || !exists {
			score := s.priorityWeights[queue.Priority] * queue.Weight
			log.Printf("[Scheduler] Queue %s has no metrics, using base score: %.2f",
				queue.Name, score)
			if score > highestScore {
				highestScore = score
				selectedQueue = &queue
			}
			continue
		}

		// Calculate queue score
		priorityScore := s.priorityWeights[queue.Priority]
		backlogFactor := float64(metrics.MessageCount) / float64(queue.MaxBatchSize)
		if backlogFactor > 1.0 {
			backlogFactor = 1.0 + math.Log10(backlogFactor)
		}

		waitTime := now.Sub(metrics.LastPollTime).Seconds()
		timeFactor := math.Log1p(waitTime / 60.0)

		errorPenalty := math.Exp(-float64(metrics.ErrorCount) * 0.1)
		starvationBoost := 1.0
		if consecutive, exists := s.consecutiveSelects[queue.Name]; !exists || consecutive == 0 {
			starvationBoost = 1.2
		}

		score := priorityScore * backlogFactor * timeFactor * errorPenalty * starvationBoost * queue.Weight

		log.Printf("[Scheduler] Queue %s score calculation: Priority: %.2f, Backlog: %.2f (Messages: %d), Time: %.2f (Wait: %.1fs), Error Penalty: %.2f (Errors: %d), Final Score: %.2f",
			queue.Name,
			priorityScore,
			backlogFactor,
			metrics.MessageCount,
			timeFactor,
			waitTime,
			errorPenalty,
			metrics.ErrorCount,
			score)

		if consecutive := s.consecutiveSelects[queue.Name]; consecutive > 3 {
			score *= math.Pow(0.8, float64(consecutive-3))
			log.Printf("[Scheduler] Queue %s adjusted score: %.2f (after consecutive selection penalty)",
				queue.Name, score)
		}

		if score > highestScore {
			highestScore = score
			selectedQueue = &queue
		}
	}

	if selectedQueue == nil {
		log.Printf("[Scheduler] No eligible queues found")
		return nil, fmt.Errorf("no eligible queues")
	}

	// Update consecutive selection tracking
	for queueName := range s.consecutiveSelects {
		if queueName == selectedQueue.Name {
			s.consecutiveSelects[queueName]++
			log.Printf("[Scheduler] Queue %s selected %d times consecutively",
				queueName, s.consecutiveSelects[queueName])
		} else {
			s.consecutiveSelects[queueName] = 0
		}
	}

	s.lastSelectedQueue = selectedQueue.Name
	log.Printf("[Scheduler] Selected queue %s with final score %.2f", selectedQueue.Name, highestScore)

	return selectedQueue, nil
}

func (s *Scheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.updateQueueMetrics()
		}
	}
}

func (s *Scheduler) updateQueueMetrics() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, queue := range s.queues {
		queueMetrics := s.collector.GetQueueMetrics(queue.Name)
		if queueMetrics != nil {
			s.metrics[queue.Name] = &QueueMetrics{
				MessageCount:   queueMetrics.MessageCount,
				InFlightCount:  queueMetrics.InFlightCount,
				ProcessingTime: queueMetrics.ProcessingTime,
				LastPollTime:   queueMetrics.LastPollTime,
				ErrorCount:     queueMetrics.ErrorCount,
				Priority:       queue.Priority,
				Weight:         queue.Weight,
			}
			log.Printf("[Scheduler] Updated metrics for queue %s - Messages: %d, In Flight: %d, Processing Time: %v",
				queue.Name, queueMetrics.MessageCount, queueMetrics.InFlightCount, queueMetrics.ProcessingTime)
		}
	}
}
