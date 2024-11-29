package scaler

import (
	"context"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sync"
	"time"
)

type ConsumerController interface {
	addWorker() error
	removeWorker() error
}

type ScalerMetrics struct {
	LoadFactor        float64
	ProcessingTime    time.Duration
	ErrorRate         float64
	BufferUtilization float64
	WorkerCount       int
}

type Scaler struct {
	consumer           ConsumerController
	collector          *metrics.Collector
	config             *config.ConsumerGroupConfig
	scaleUpThreshold   float64
	scaleDownThreshold float64

	lastScaleUp   time.Time
	lastScaleDown time.Time

	// Metrics tracking
	metricsWindow []ScalerMetrics
	metricsMu     sync.RWMutex

	// Scaling state
	consecutiveScaleUps   int
	consecutiveScaleDowns int
	scalingMu             sync.Mutex
}

func NewScaler(
	consumer ConsumerController,
	collector *metrics.Collector,
	config *config.ConsumerGroupConfig,
) *Scaler {
	return &Scaler{
		consumer:           consumer,
		collector:          collector,
		config:             config,
		scaleUpThreshold:   config.ScaleUpThreshold,
		scaleDownThreshold: config.ScaleDownThreshold,
		metricsWindow:      make([]ScalerMetrics, 0, 100),
	}
}

func (s *Scaler) Start(ctx context.Context) error {
	// Fast evaluation ticker for quick response to spikes
	fastTicker := time.NewTicker(5 * time.Second)
	// Slower evaluation ticker for scale-down decisions
	slowTicker := time.NewTicker(30 * time.Second)

	defer fastTicker.Stop()
	defer slowTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fastTicker.C:
			s.evaluateScaleUp()
		case <-slowTicker.C:
			s.evaluateScaleDown()
		}
	}
}

func (s *Scaler) evaluateScaleUp() {
	s.updateMetrics()

	s.scalingMu.Lock()
	defer s.scalingMu.Unlock()

	// Don't scale up if we've scaled up very recently
	if time.Since(s.lastScaleUp) < s.config.ScaleUpCooldown.Duration {
		return
	}

	metrics := s.getCurrentMetrics()
	if s.shouldScaleUp(metrics) {
		scaleFactor := s.calculateScaleUpFactor(metrics)
		s.scaleUpWorkers(scaleFactor)
	}
}

func (s *Scaler) evaluateScaleDown() {
	s.updateMetrics()

	s.scalingMu.Lock()
	defer s.scalingMu.Unlock()

	// Don't scale down if we've scaled down very recently
	if time.Since(s.lastScaleDown) < s.config.ScaleDownCooldown.Duration {
		return
	}

	metrics := s.getCurrentMetrics()
	if s.shouldScaleDown(metrics) {
		s.scaleDownWorkers()
	}
}

func (s *Scaler) shouldScaleUp(metrics ScalerMetrics) bool {
	// Check various factors that might indicate need to scale up
	if metrics.LoadFactor > s.scaleUpThreshold {
		return true
	}

	if metrics.BufferUtilization > 0.8 {
		return true
	}

	// Check for rapid increase in processing time
	if s.isProcessingTimeIncreasing() {
		return true
	}

	return false
}

func (s *Scaler) shouldScaleDown(metrics ScalerMetrics) bool {
	// Must be below threshold for an extended period
	if !s.isConsistentlyBelowThreshold() {
		return false
	}

	// Don't scale down if buffer utilization is high
	if metrics.BufferUtilization > 0.5 {
		return false
	}

	// Don't scale down if processing times are high
	if metrics.ProcessingTime > 5*time.Second {
		return false
	}

	// Ensure we're not at minimum workers
	return metrics.WorkerCount > s.config.MinWorkers
}

func (s *Scaler) calculateScaleUpFactor(metrics ScalerMetrics) int {
	// Base scale factor on how far we are above threshold
	loadDelta := metrics.LoadFactor - s.scaleUpThreshold
	scaleFactor := 1

	// Quick response to severe overload
	if loadDelta > 0.5 {
		scaleFactor = 3
	} else if loadDelta > 0.3 {
		scaleFactor = 2
	}

	// Consider buffer utilization
	if metrics.BufferUtilization > 0.9 {
		scaleFactor++
	}

	// Consider processing time trends
	if s.isProcessingTimeIncreasing() {
		scaleFactor++
	}

	// Limit scale factor based on consecutive scale ups
	if s.consecutiveScaleUps > 0 {
		maxFactor := 4 - s.consecutiveScaleUps
		if maxFactor < 1 {
			maxFactor = 1
		}
		if scaleFactor > maxFactor {
			scaleFactor = maxFactor
		}
	}

	return scaleFactor
}

func (s *Scaler) scaleUpWorkers(scaleFactor int) {
	currentMetrics := s.getCurrentMetrics()
	maxNewWorkers := s.config.MaxWorkers - currentMetrics.WorkerCount

	if maxNewWorkers <= 0 {
		return
	}

	if scaleFactor > maxNewWorkers {
		scaleFactor = maxNewWorkers
	}

	success := true
	for i := 0; i < scaleFactor; i++ {
		if err := s.consumer.addWorker(); err != nil {
			success = false
			break
		}
	}

	if success {
		s.consecutiveScaleUps++
		s.consecutiveScaleDowns = 0
		s.lastScaleUp = time.Now()
	}
}

func (s *Scaler) scaleDownWorkers() {
	// Scale down one worker at a time
	if err := s.consumer.removeWorker(); err == nil {
		s.consecutiveScaleDowns++
		s.consecutiveScaleUps = 0
		s.lastScaleDown = time.Now()
	}
}

func (s *Scaler) updateMetrics() {
	metrics := s.calculateCurrentMetrics()

	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()

	s.metricsWindow = append(s.metricsWindow, metrics)

	// Keep only last 5 minutes of metrics
	i := 0
	for ; i < len(s.metricsWindow); i++ {
		if time.Now().Sub(time.Unix(int64(i), 0)) < 5*time.Minute {
			break
		}
	}
	if i > 0 {
		s.metricsWindow = s.metricsWindow[i:]
	}
}

func (s *Scaler) calculateCurrentMetrics() ScalerMetrics {
	queueMetrics := s.collector.GetAllQueueMetrics()

	var totalLoad float64
	var totalErrors int64
	var maxProcessingTime time.Duration
	var totalMessages int64

	// Aggregate metrics across all queues
	for _, qm := range queueMetrics {
		weight := float64(qm.Priority) / 3.0
		totalLoad += float64(qm.MessageCount) * weight
		totalErrors += qm.ErrorCount
		totalMessages += qm.MessageCount
		if qm.ProcessingTime > maxProcessingTime {
			maxProcessingTime = qm.ProcessingTime
		}
	}

	workerCount := s.collector.GetWorkerCount()
	bufferMetrics := s.collector.GetBufferMetrics()

	errorRate := 0.0
	if totalMessages > 0 {
		errorRate = float64(totalErrors) / float64(totalMessages)
	}

	return ScalerMetrics{
		LoadFactor:        totalLoad / float64(workerCount*100),
		ProcessingTime:    maxProcessingTime,
		ErrorRate:         errorRate,
		BufferUtilization: bufferMetrics.GetAverageUtilization(),
		WorkerCount:       workerCount,
	}
}

func (s *Scaler) getCurrentMetrics() ScalerMetrics {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	if len(s.metricsWindow) == 0 {
		return ScalerMetrics{}
	}
	return s.metricsWindow[len(s.metricsWindow)-1]
}

func (s *Scaler) isProcessingTimeIncreasing() bool {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	if len(s.metricsWindow) < 3 {
		return false
	}

	// Look at last 3 data points
	window := s.metricsWindow[len(s.metricsWindow)-3:]

	// Check if processing time is consistently increasing
	return window[0].ProcessingTime < window[1].ProcessingTime &&
		window[1].ProcessingTime < window[2].ProcessingTime
}

func (s *Scaler) isConsistentlyBelowThreshold() bool {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	if len(s.metricsWindow) < 6 {
		return false
	}

	// Check last minute of metrics (6 samples at 10-second intervals)
	window := s.metricsWindow[len(s.metricsWindow)-6:]

	for _, metrics := range window {
		if metrics.LoadFactor > s.scaleDownThreshold {
			return false
		}
	}

	return true
}
