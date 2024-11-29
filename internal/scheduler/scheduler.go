package scheduler

import (
	"context"
	"fmt"
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

	// New fields for smarter scheduling
	priorityWeights    map[int]float64
	lastSelectedQueue  string
	consecutiveSelects map[string]int
}

func NewScheduler(queues []config.QueueConfig, collector *metrics.Collector) *Scheduler {
	// Initialize priority weights (exponential scaling)
	priorityWeights := make(map[int]float64)
	for i := 1; i <= 3; i++ {
		priorityWeights[i] = math.Pow(2, float64(i-1))
	}

	return &Scheduler{
		queues:             queues,
		metrics:            make(map[string]*QueueMetrics),
		collector:          collector,
		priorityWeights:    priorityWeights,
		consecutiveSelects: make(map[string]int),
	}
}

func (s *Scheduler) UpdateMetrics(queueURL string, metrics *QueueMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics[queueURL] = metrics
}

func (s *Scheduler) SelectQueue() (*config.QueueConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var selectedQueue *config.QueueConfig
	highestScore := -1.0

	now := time.Now()

	for _, queue := range s.queues {
		metrics, exists := s.metrics[queue.URL]
		if !exists {
			continue
		}

		// Skip if queue was just polled (considering poll interval)
		if timeSinceLastPoll := now.Sub(metrics.LastPollTime); timeSinceLastPoll < queue.PollInterval.Duration {
			continue
		}

		// Calculate base priority score (exponential scaling)
		priorityScore := s.priorityWeights[queue.Priority]

		// Message backlog factor
		backlogFactor := float64(metrics.MessageCount) / float64(queue.MaxBatchSize)
		if backlogFactor > 1.0 {
			backlogFactor = 1.0 + math.Log10(backlogFactor) // Logarithmic scaling for large backlogs
		}

		// Time factor - increases score the longer a queue hasn't been polled
		waitTime := now.Sub(metrics.LastPollTime).Seconds()
		timeFactor := math.Log1p(waitTime / 60.0) // Logarithmic scaling of wait time in minutes

		// Error penalty - exponential decay
		errorPenalty := math.Exp(-float64(metrics.ErrorCount) * 0.1)

		// Starvation prevention - boost score if queue hasn't been selected recently
		starvationBoost := 1.0
		if consecutive, exists := s.consecutiveSelects[queue.URL]; !exists || consecutive == 0 {
			starvationBoost = 1.2
		}

		// Calculate final score
		score := priorityScore * backlogFactor * timeFactor * errorPenalty * starvationBoost * queue.Weight

		// Apply fairness adjustment if this queue has been selected too many times consecutively
		if consecutive := s.consecutiveSelects[queue.URL]; consecutive > 3 {
			score *= math.Pow(0.8, float64(consecutive-3)) // Decay score for repeated selection
		}

		if score > highestScore {
			highestScore = score
			selectedQueue = &queue
		}
	}

	if selectedQueue == nil {
		return nil, fmt.Errorf("no eligible queues")
	}

	// Update consecutive selection tracking
	for url := range s.consecutiveSelects {
		if url == selectedQueue.URL {
			s.consecutiveSelects[url]++
		} else {
			s.consecutiveSelects[url] = 0
		}
	}

	s.lastSelectedQueue = selectedQueue.URL
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
		queueMetrics := s.collector.GetQueueMetrics(queue.URL)
		if queueMetrics != nil {
			s.metrics[queue.URL] = &QueueMetrics{
				MessageCount:   queueMetrics.MessageCount,
				InFlightCount:  queueMetrics.InFlightCount,
				ProcessingTime: queueMetrics.ProcessingTime,
				LastPollTime:   queueMetrics.LastPollTime,
				ErrorCount:     queueMetrics.ErrorCount,
				Priority:       queue.Priority,
				Weight:         queue.Weight,
			}
		}
	}
}
