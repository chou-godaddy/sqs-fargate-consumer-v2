package scaler

import (
	"context"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sync"
	"sync/atomic"
	"time"
)

type ConsumerGroupController interface {
	AddWorker() error
	RemoveWorker() error
}

type ScalerMetrics struct {
	LoadFactor        float64
	ProcessingTime    time.Duration
	ErrorRate         float64
	BufferUtilization float64
	ConsumerCount     int32
	MaxConsumers      int
	MinConsumers      int
	TimestampUTC      time.Time
}

type Scaler struct {
	consumerGroup  ConsumerGroupController
	collector      *metrics.Collector
	config         *config.ScalerConfig
	consumerConfig *config.ConsumerGroupConfig

	lastScaleUp   time.Time
	lastScaleDown time.Time

	metricsWindow []ScalerMetrics
	metricsMu     sync.RWMutex

	consecutiveScaleUps   int
	consecutiveScaleDowns int
	scalingMu             sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	currentConsumerCount atomic.Int32
}

func NewScaler(
	consumerGroup ConsumerGroupController,
	collector *metrics.Collector,
	config *config.ScalerConfig,
	consumerConfig *config.ConsumerGroupConfig,
) *Scaler {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Scaler{
		consumerGroup:  consumerGroup,
		collector:      collector,
		config:         config,
		consumerConfig: consumerConfig,
		metricsWindow:  make([]ScalerMetrics, 0, 100),
		ctx:            ctx,
		cancel:         cancel,
	}

	// Initialize with minimum workers count
	s.currentConsumerCount.Store(int32(consumerConfig.MinWorkers))

	return s
}

func (s *Scaler) Start(ctx context.Context) error {
	log.Printf("[Scaler] Starting with configuration - Scale Up Threshold: %.2f%%, Scale Down Threshold: %.2f%%",
		s.config.ScaleUpThreshold*100, s.config.ScaleDownThreshold*100)

	fastTicker := time.NewTicker(s.config.ScalingUpTicker.Duration)
	slowTicker := time.NewTicker(s.config.ScalingDownTicker.Duration)

	log.Printf("[Scaler] Configured with fast interval %v and slow interval %v",
		s.config.ScalingUpTicker.Duration, s.config.ScalingDownTicker.Duration)

	defer fastTicker.Stop()
	defer slowTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Scaler] Stopping due to context cancellation")
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

	if time.Since(s.lastScaleUp) < s.config.ScaleUpCooldown.Duration {
		return
	}

	currentCount := s.currentConsumerCount.Load()
	metrics := s.getCurrentMetrics()

	log.Printf("[Scaler] Evaluating scale up - Load Factor: %.2f%%, Buffer Utilization: %.2f%%, Current Consumers: %d/%d",
		metrics.LoadFactor*100,
		metrics.BufferUtilization*100,
		currentCount,
		s.consumerConfig.MaxWorkers)

	if currentCount >= int32(s.consumerConfig.MaxWorkers) {
		log.Printf("[Scaler] Cannot scale up: already at maximum consumers (%d)", s.consumerConfig.MaxWorkers)
		return
	}

	if s.shouldScaleUp(metrics) {
		scaleFactor := s.calculateScaleUpFactor(metrics)
		s.scaleUpConsumers(scaleFactor)
	}
}

func (s *Scaler) evaluateScaleDown() {
	s.updateMetrics()

	s.scalingMu.Lock()
	defer s.scalingMu.Unlock()

	if time.Since(s.lastScaleDown) < s.config.ScaleDownCooldown.Duration {
		return
	}

	currentCount := s.currentConsumerCount.Load()
	metrics := s.getCurrentMetrics()

	log.Printf("[Scaler] Evaluating scale down - Load Factor: %.2f%%, Buffer Utilization: %.2f%%, Current Consumers: %d/%d",
		metrics.LoadFactor*100,
		metrics.BufferUtilization*100,
		currentCount,
		s.consumerConfig.MinWorkers)

	if currentCount <= int32(s.consumerConfig.MinWorkers) {
		log.Printf("[Scaler] Cannot scale down: already at minimum consumers (%d)", s.consumerConfig.MinWorkers)
		return
	}

	if s.shouldScaleDown(metrics) {
		s.scaleDownConsumers()
	}
}

func (s *Scaler) shouldScaleUp(metrics ScalerMetrics) bool {
	if metrics.ConsumerCount >= int32(s.consumerConfig.MaxWorkers) {
		return false
	}

	if metrics.LoadFactor > s.config.ScaleUpThreshold {
		log.Printf("[Scaler] Scale up condition met: Load factor %.2f%% exceeds threshold %.2f%%",
			metrics.LoadFactor*100, s.config.ScaleUpThreshold*100)
		return true
	}

	if metrics.BufferUtilization > s.config.ScaleUpThreshold {
		log.Printf("[Scaler] Scale up condition met: Buffer utilization %.2f%% exceeds threshold %.2f%%",
			metrics.BufferUtilization*100, s.config.ScaleUpThreshold*100)
		return true
	}

	if s.isProcessingTimeIncreasing() {
		log.Printf("[Scaler] Scale up condition met: Processing time showing increasing trend")
		return true
	}

	return false
}

func (s *Scaler) shouldScaleDown(metrics ScalerMetrics) bool {
	if metrics.ConsumerCount <= int32(s.consumerConfig.MinWorkers) {
		return false
	}

	if !s.isConsistentlyBelowThreshold() {
		return false
	}

	if metrics.BufferUtilization > s.config.ScaleDownThreshold {
		log.Printf("[Scaler] Scale down prevented: Buffer utilization %.2f%% above threshold %.2f%%",
			metrics.BufferUtilization*100, s.config.ScaleDownThreshold*100)
		return false
	}

	if metrics.LoadFactor > s.config.ScaleDownThreshold {
		log.Printf("[Scaler] Scale down prevented: Load factor %.2f%% above threshold %.2f%%",
			metrics.LoadFactor*100, s.config.ScaleDownThreshold*100)
		return false
	}

	log.Printf("[Scaler] Scale down condition met: Buffer utilization %.2f%% and load factor %.2f%% below threshold %.2f%%",
		metrics.BufferUtilization*100, metrics.LoadFactor*100, s.config.ScaleDownThreshold*100)
	return true
}

func (s *Scaler) calculateScaleUpFactor(metrics ScalerMetrics) int {
	// Base scale factor on how far we are above threshold
	loadDelta := metrics.LoadFactor - s.config.ScaleUpThreshold
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

func (s *Scaler) scaleUpConsumers(scaleFactor int) {
	currentCount := s.currentConsumerCount.Load()
	maxNewConsumers := int32(s.consumerConfig.MaxWorkers) - currentCount

	if maxNewConsumers <= 0 {
		return
	}

	if int32(scaleFactor) > maxNewConsumers {
		scaleFactor = int(maxNewConsumers)
	}

	success := true
	added := 0
	for i := 0; i < scaleFactor; i++ {
		if err := s.consumerGroup.AddWorker(); err != nil {
			success = false
			break
		}
		s.currentConsumerCount.Add(1)
		added++
	}

	if success {
		s.consecutiveScaleUps++
		s.consecutiveScaleDowns = 0
		s.lastScaleUp = time.Now()
		log.Printf("[Scaler] Successfully scaled up by adding %d consumers. Total consumers: %d",
			added, s.currentConsumerCount.Load())
	}
}

func (s *Scaler) scaleDownConsumers() {
	if err := s.consumerGroup.RemoveWorker(); err == nil {
		s.currentConsumerCount.Add(-1)
		s.consecutiveScaleDowns++
		s.consecutiveScaleUps = 0
		s.lastScaleDown = time.Now()
		log.Printf("[Scaler] Successfully scaled down by removing one consumer. Total consumers: %d",
			s.currentConsumerCount.Load())
	}
}

func (s *Scaler) updateMetrics() {
	metrics := s.calculateCurrentMetrics()

	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()

	s.metricsWindow = append(s.metricsWindow, metrics)

	// Keep only metrics within the configured window
	cutoffTime := time.Now().Add(-s.config.MetricsWindow.Duration)
	startIdx := 0
	for i, metric := range s.metricsWindow {
		if metric.TimestampUTC.After(cutoffTime) {
			startIdx = i
			break
		}
	}
	s.metricsWindow = s.metricsWindow[startIdx:]

	// Enforce maximum data points limit
	if len(s.metricsWindow) > 100 {
		s.metricsWindow = s.metricsWindow[len(s.metricsWindow)-100:]
	}

	log.Printf("[Scaler] Updated metrics - Load: %.2f%%, Buffer: %.2f%%, Consumers: %d/%d/%d (Current/Min/Max)",
		metrics.LoadFactor*100,
		metrics.BufferUtilization*100,
		metrics.ConsumerCount,
		metrics.MinConsumers,
		metrics.MaxConsumers)
}

func (s *Scaler) calculateCurrentMetrics() ScalerMetrics {
	queueMetrics := s.collector.GetAllQueueMetrics()
	bufferMetrics := s.collector.GetBufferMetrics()

	var totalLoad float64
	var totalErrors int64
	var maxProcessingTime time.Duration
	var totalMessages int64

	for _, qm := range queueMetrics {
		weight := float64(qm.Priority) / 3.0
		totalLoad += float64(qm.MessageCount) * weight
		totalErrors += qm.ErrorCount
		totalMessages += qm.MessageCount
		if qm.ProcessingTime > maxProcessingTime {
			maxProcessingTime = qm.ProcessingTime
		}
	}

	currentConsumers := s.currentConsumerCount.Load()
	errorRate := 0.0
	if totalMessages > 0 {
		errorRate = float64(totalErrors) / float64(totalMessages)
	}

	return ScalerMetrics{
		LoadFactor:        totalLoad / float64(currentConsumers*100),
		ProcessingTime:    maxProcessingTime,
		ErrorRate:         errorRate,
		BufferUtilization: bufferMetrics.GetAverageUtilization(),
		ConsumerCount:     currentConsumers,
		MinConsumers:      s.consumerConfig.MinWorkers,
		MaxConsumers:      s.consumerConfig.MaxWorkers,
		TimestampUTC:      time.Now().UTC(),
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
		if metrics.LoadFactor > s.config.ScaleDownThreshold ||
			metrics.BufferUtilization > s.config.ScaleDownThreshold {
			return false
		}
	}

	return true
}

func (s *Scaler) Shutdown(ctx context.Context) {
	log.Printf("[Scaler] Initiating shutdown")
	s.cancel()
	log.Printf("[Scaler] Shutdown completed")
}
