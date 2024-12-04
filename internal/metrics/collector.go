package metrics

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"sqs-fargate-consumer-v2/internal/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type MetricType string
type MetricUnit string

const (
	MetricMessagesReceived  MetricType = "MessagesReceived"
	MetricMessagesProcessed MetricType = "MessagesProcessed"
	MetricProcessingTime    MetricType = "ProcessingTime"
	MetricProcessingErrors  MetricType = "ProcessingErrors"
	MetricConsumerCount     MetricType = "ConsumerCount"
	MetricBufferUtilization MetricType = "BufferUtilization"
	MetricQueueDepth        MetricType = "QueueDepth"
	MetricConsumerAdditions MetricType = "ConsumerAdditions"
	MetricConsumerRemovals  MetricType = "ConsumerRemovals"
	MetricMemoryUsage       MetricType = "MemoryUsage"
	MetricCPUUsage          MetricType = "CPUUsage"
	MetricsInFlightMessages MetricType = "InFlightMessages"
)

const (
	UnitCount        MetricUnit = "Count"
	UnitMilliseconds MetricUnit = "Milliseconds"
	UnitBytes        MetricUnit = "Bytes"
	UnitPercent      MetricUnit = "Percent"
	UnitBytesPerSec  MetricUnit = "Bytes/Second"
)

type MetricDataPoint struct {
	Value     float64
	Timestamp time.Time
	Unit      MetricUnit
}

type MetricSeries struct {
	Points    []MetricDataPoint
	LastWrite time.Time
}

type Collector struct {
	client  *cloudwatch.Client
	config  *config.MetricsConfig
	sqs     *sqs.Client
	metrics map[string]map[MetricType]*MetricSeries
	mu      sync.RWMutex
	done    chan struct{}

	metricUnits map[MetricType]MetricUnit

	queues   map[string]string // map[QueueName]QueueURL
	queuesMu sync.RWMutex
}

type QueueMetrics struct {
	MessageCount   int64
	InFlightCount  int64
	ProcessingTime time.Duration
	LastPollTime   time.Time
	ErrorCount     int64
	Priority       int
	Weight         float64
}

type BufferMetrics struct {
	HighPriorityUtilization   float64
	MediumPriorityUtilization float64
	LowPriorityUtilization    float64
	TotalMemoryUsage          int64
}

func (b *BufferMetrics) GetAverageUtilization() float64 {
	return (b.HighPriorityUtilization +
		b.MediumPriorityUtilization +
		b.LowPriorityUtilization) / 3.0
}

func NewCollector(cloudwatchClient *cloudwatch.Client, sqsClient *sqs.Client, metricsConfig *config.MetricsConfig, queues []config.QueueConfig) *Collector {
	// Initialize metric units map first
	metricUnits := map[MetricType]MetricUnit{
		MetricMessagesReceived:  UnitCount,
		MetricMessagesProcessed: UnitCount,
		MetricProcessingTime:    UnitMilliseconds,
		MetricProcessingErrors:  UnitCount,
		MetricConsumerCount:     UnitCount,
		MetricBufferUtilization: UnitPercent,
		MetricQueueDepth:        UnitCount,
		MetricsInFlightMessages: UnitCount,
		MetricMemoryUsage:       UnitBytes,
		MetricCPUUsage:          UnitPercent,
	}

	queueMap := make(map[string]string, len(queues))
	metricsMap := make(map[string]map[MetricType]*MetricSeries)

	// Initialize maps for each queue
	for _, q := range queues {
		queueMap[q.Name] = q.URL
		metricsMap[q.Name] = make(map[MetricType]*MetricSeries)
		log.Printf("[Metrics] Mapped queue %s to URL %s", q.Name, q.URL)
	}

	return &Collector{
		client:      cloudwatchClient,
		sqs:         sqsClient,
		config:      metricsConfig,
		metrics:     metricsMap,
		queues:      queueMap,
		metricUnits: metricUnits,
		done:        make(chan struct{}),
	}
}

func (c *Collector) Start(ctx context.Context) error {
	log.Printf("[Metrics] Starting collector with %d registered queues", len(c.queues))

	// Start metrics collection routines
	go c.collectSQSMetrics(ctx)
	go c.publish(ctx)

	// Perform initial metrics collection
	c.updateSQSMetrics(ctx)

	return nil
}

func (c *Collector) collectSQSMetrics(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-ticker.C:
			c.updateSQSMetrics(ctx)
		}
	}
}

func (c *Collector) updateSQSMetrics(ctx context.Context) {
	c.queuesMu.RLock()
	queueURLs := make(map[string]string, len(c.queues))
	for name, url := range c.queues {
		queueURLs[name] = url
	}
	c.queuesMu.RUnlock()

	log.Printf("[Metrics] Starting metrics collection for %d queues", len(queueURLs))

	for queueName, queueURL := range queueURLs {
		input := &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(queueURL),
			AttributeNames: []sqstypes.QueueAttributeName{
				sqstypes.QueueAttributeNameApproximateNumberOfMessages,
				sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		}

		result, err := c.sqs.GetQueueAttributes(ctx, input)
		if err != nil {
			log.Printf("[Metrics] Error getting attributes for queue %s (%s): %v", queueName, queueURL, err)
			continue
		}

		messageBacklog, _ := strconv.Atoi(result.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)])
		inFlightMessages, _ := strconv.Atoi(result.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])

		c.mu.Lock()
		if c.metrics[queueName] == nil {
			c.metrics[queueName] = make(map[MetricType]*MetricSeries)
		}

		// Update queue metrics
		c.recordMetricLocked(queueName, MetricQueueDepth, float64(messageBacklog))
		c.recordMetricLocked(queueName, "InFlightMessages", float64(inFlightMessages))
		c.mu.Unlock()

		log.Printf("[Metrics] Queue %s - Backlog: %d, In Flight: %d",
			queueName, messageBacklog, inFlightMessages)
	}
}

func (c *Collector) recordMetricLocked(queueName string, metricType MetricType, value float64) {
	if c.metrics[queueName] == nil {
		c.metrics[queueName] = make(map[MetricType]*MetricSeries)
	}

	unit, exists := c.metricUnits[metricType]
	if !exists {
		unit = UnitCount // Default to Count if unit not specified
		log.Printf("[Metrics] Warning: No unit defined for metric type %s, using Count", metricType)
	}

	if c.metrics[queueName][metricType] == nil {
		c.metrics[queueName][metricType] = &MetricSeries{
			Points: make([]MetricDataPoint, 0, c.config.MaxDataPoints),
		}
	}

	point := MetricDataPoint{
		Value:     value,
		Timestamp: time.Now(),
		Unit:      unit,
	}

	series := c.metrics[queueName][metricType]
	series.Points = append(series.Points, point)
	series.LastWrite = time.Now()

	log.Printf("[Metrics] Recorded %s = %.2f for queue %s", metricType, value, queueName)
}

func (c *Collector) cleanupRoutine(ctx context.Context) {
	cleanupTicker := time.NewTicker(time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-cleanupTicker.C:
			c.cleanup()
		}
	}
}

func (c *Collector) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.metrics == nil {
		c.metrics = make(map[string]map[MetricType]*MetricSeries)
		return
	}

	now := time.Now()
	retentionCutoff := now.Add(-c.config.RetentionPeriod.Duration)

	for queueName, metrics := range c.metrics {
		if metrics == nil {
			delete(c.metrics, queueName)
			continue
		}

		for metricType, series := range metrics {
			if series == nil || series.Points == nil {
				delete(metrics, metricType)
				continue
			}

			// Remove old data points
			i := 0
			for ; i < len(series.Points); i++ {
				if series.Points[i].Timestamp.After(retentionCutoff) {
					break
				}
			}
			if i > 0 {
				series.Points = series.Points[i:]
			}

			// Remove metrics that haven't been updated recently
			if series.LastWrite.Before(retentionCutoff) {
				delete(metrics, metricType)
				continue
			}

			// Enforce maximum number of data points
			if len(series.Points) > c.config.MaxDataPoints {
				excess := len(series.Points) - c.config.MaxDataPoints
				series.Points = series.Points[excess:]
			}
		}

		// Remove empty queue metrics
		if len(metrics) == 0 {
			delete(c.metrics, queueName)
		}
	}
}

func (c *Collector) RecordMetric(queueName string, metricType MetricType, value float64) {
	if c == nil || c.metrics == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.metricUnits[metricType]; !exists {
		return
	}

	normalizedName := c.normalizeQueueName(queueName)
	if _, exists := c.metrics[normalizedName]; !exists {
		c.metrics[normalizedName] = make(map[MetricType]*MetricSeries)
	}

	if _, exists := c.metrics[normalizedName][metricType]; !exists {
		c.metrics[normalizedName][metricType] = &MetricSeries{
			Points: make([]MetricDataPoint, 0, c.config.MaxDataPoints),
		}
	}

	series := c.metrics[normalizedName][metricType]
	series.Points = append(series.Points, MetricDataPoint{
		Value:     value,
		Timestamp: time.Now(),
		Unit:      c.metricUnits[metricType],
	})
	series.LastWrite = time.Now()

	log.Printf("[Metrics] Recorded %s = %.2f for queue %s", metricType, value, queueName)

	if len(series.Points) > c.config.MaxDataPoints {
		series.Points = series.Points[1:]
	}
}

func (c *Collector) RecordProcessingStarted(queueName string, priority int) {
	c.RecordMetric(queueName, MetricMessagesReceived, 1)
}

func (c *Collector) RecordProcessingComplete(queueName string, priority int, duration time.Duration) {
	c.RecordMetric(queueName, MetricMessagesProcessed, 1)
	c.RecordMetric(queueName, MetricProcessingTime, float64(duration.Milliseconds()))
}

func (c *Collector) RecordError(queueName string, errorType string) {
	c.RecordMetric(queueName, MetricProcessingErrors, 1)
}

func (c *Collector) RecordConsumerAdded(consumerID string) {
	log.Printf("[Metrics] Recording new consumer addition: %s", consumerID)
	normalizedName := c.normalizeQueueName("")
	c.RecordMetric(normalizedName, MetricConsumerAdditions, 1)
	currentCount := float64(c.getConsumerCount() + 1)
	c.RecordMetric(normalizedName, MetricConsumerCount, currentCount)
}

func (c *Collector) RecordConsumerRemoved(consumerID string) {
	log.Printf("[Metrics] Recording consumer removal: %s", consumerID)
	normalizedName := c.normalizeQueueName("")
	c.RecordMetric(normalizedName, MetricConsumerRemovals, 1)
	currentCount := float64(c.getConsumerCount() - 1)
	c.RecordMetric(normalizedName, MetricConsumerCount, currentCount)
}

func (c *Collector) RecordConsumerReplaced(consumerID string) {
	log.Printf("[Metrics] Recording consumer replacement: %s", consumerID)
	normalizedName := c.normalizeQueueName("")
	c.RecordMetric(normalizedName, MetricConsumerRemovals, 1)
	c.RecordMetric(normalizedName, MetricConsumerAdditions, 1)
}

func (c *Collector) publish(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.PublishInterval.Duration)
	defer cancel()

	c.mu.RLock()
	defer c.mu.RUnlock()

	var metricData []types.MetricDatum

	for queueName, metrics := range c.metrics {
		for metricType, series := range metrics {
			if len(series.Points) == 0 {
				continue
			}

			// Get the latest point
			point := series.Points[len(series.Points)-1]

			datum := types.MetricDatum{
				MetricName: aws.String(string(metricType)),
				Value:      aws.Float64(point.Value),
				Timestamp:  aws.Time(point.Timestamp),
				Unit:       types.StandardUnit(point.Unit),
				Dimensions: []types.Dimension{
					{
						Name:  aws.String("QueueName"),
						Value: aws.String(queueName),
					},
				},
			}
			metricData = append(metricData, datum)
		}
	}

	// Publish in batches of 20 (CloudWatch limit)
	for i := 0; i < len(metricData); i += 20 {
		end := i + 20
		if end > len(metricData) {
			end = len(metricData)
		}

		batch := metricData[i:end]
		input := &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(c.config.Namespace),
			MetricData: batch,
		}

		if _, err := c.client.PutMetricData(ctx, input); err != nil {
			return fmt.Errorf("publishing metrics: %w", err)
		}
	}

	return nil
}

func (c *Collector) GetRawMetrics(queueName string) map[MetricType]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Use normalized queue name
	normalizedName := c.normalizeQueueName(queueName)
	result := make(map[MetricType]float64)
	if queueMetrics, exists := c.metrics[normalizedName]; exists {
		for metricType, series := range queueMetrics {
			// Create a copy of points under lock
			points := make([]MetricDataPoint, len(series.Points))
			copy(points, series.Points)

			if len(points) > 0 {
				result[metricType] = points[len(points)-1].Value
			}
		}
	}
	return result
}

func (c *Collector) GetQueueMetrics(queueName string) *QueueMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	normalizedName := c.normalizeQueueName(queueName)
	if queueMetrics, exists := c.metrics[normalizedName]; exists {
		qm := &QueueMetrics{}

		// Get MessageCount
		if msgCount, exists := queueMetrics[MetricMessagesReceived]; exists && len(msgCount.Points) > 0 {
			qm.MessageCount = int64(msgCount.Points[len(msgCount.Points)-1].Value)
		}

		// Get InFlightCount
		if inFlight, exists := queueMetrics[MetricQueueDepth]; exists && len(inFlight.Points) > 0 {
			qm.InFlightCount = int64(inFlight.Points[len(inFlight.Points)-1].Value)
		}

		// Get ProcessingTime
		if procTime, exists := queueMetrics[MetricProcessingTime]; exists && len(procTime.Points) > 0 {
			qm.ProcessingTime = time.Duration(procTime.Points[len(procTime.Points)-1].Value) * time.Millisecond
		}

		// Get LastPollTime
		if series, exists := queueMetrics[MetricMessagesReceived]; exists && len(series.Points) > 0 {
			qm.LastPollTime = series.LastWrite
		}

		// Get ErrorCount
		if errors, exists := queueMetrics[MetricProcessingErrors]; exists && len(errors.Points) > 0 {
			qm.ErrorCount = int64(errors.Points[len(errors.Points)-1].Value)
		}

		return qm
	}
	return nil
}

func (c *Collector) getConsumerCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	normalizedName := c.normalizeQueueName("")
	if metrics, exists := c.metrics[normalizedName]; exists {
		if consumerCount, exists := metrics[MetricConsumerCount]; exists && len(consumerCount.Points) > 0 {
			return int(consumerCount.Points[len(consumerCount.Points)-1].Value)
		}
	}
	return 0
}

// GetAllQueueMetrics returns all queue metrics
func (c *Collector) GetAllQueueMetrics() map[string]*QueueMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*QueueMetrics)
	for queueName := range c.metrics {
		if metrics := c.GetQueueMetrics(queueName); metrics != nil {
			result[queueName] = metrics
		}
	}
	return result
}

func (c *Collector) GetConsumerCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	normalizedName := c.normalizeQueueName("")
	if metrics, exists := c.metrics[normalizedName]; exists {
		if workerCount, exists := metrics[MetricConsumerCount]; exists && len(workerCount.Points) > 0 {
			return int(workerCount.Points[len(workerCount.Points)-1].Value)
		}
	}
	return 0
}

func (c *Collector) GetBufferMetrics() *BufferMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	normalizedName := c.normalizeQueueName("")
	result := &BufferMetrics{}

	if metrics, exists := c.metrics[normalizedName]; exists {
		// Get utilization metrics directly under lock
		if high, exists := metrics[MetricBufferUtilization+"High"]; exists && len(high.Points) > 0 {
			result.HighPriorityUtilization = high.Points[len(high.Points)-1].Value
		}
		if med, exists := metrics[MetricBufferUtilization+"Medium"]; exists && len(med.Points) > 0 {
			result.MediumPriorityUtilization = med.Points[len(med.Points)-1].Value
		}
		if low, exists := metrics[MetricBufferUtilization+"Low"]; exists && len(low.Points) > 0 {
			result.LowPriorityUtilization = low.Points[len(low.Points)-1].Value
		}
		if memory, exists := metrics[MetricMemoryUsage]; exists && len(memory.Points) > 0 {
			result.TotalMemoryUsage = int64(memory.Points[len(memory.Points)-1].Value)
		}
	}

	return result
}

func (c *Collector) Shutdown(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		// Already shutting down
		return nil
	default:
		close(c.done)
		return nil
	}
}

func (c *Collector) normalizeQueueName(queueName string) string {
	if queueName == "" {
		return "default"
	}
	return queueName
}
