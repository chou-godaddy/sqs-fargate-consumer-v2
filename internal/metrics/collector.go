package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"sqs-fargate-consumer-v2/internal/config"
)

type MetricType string
type MetricUnit string

const (
	MetricMessagesReceived  MetricType = "MessagesReceived"
	MetricMessagesProcessed MetricType = "MessagesProcessed"
	MetricProcessingTime    MetricType = "ProcessingTime"
	MetricProcessingErrors  MetricType = "ProcessingErrors"
	MetricWorkerCount       MetricType = "WorkerCount"
	MetricBufferUtilization MetricType = "BufferUtilization"
	MetricQueueDepth        MetricType = "QueueDepth"
	MetricWorkerAdditions   MetricType = "WorkerAdditions"
	MetricWorkerRemovals    MetricType = "WorkerRemovals"
	MetricMemoryUsage       MetricType = "MemoryUsage"
	MetricCPUUsage          MetricType = "CPUUsage"
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
	metrics map[string]map[MetricType]*MetricSeries
	mu      sync.RWMutex

	// Metric metadata
	metricUnits map[MetricType]MetricUnit
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

func NewCollector(client *cloudwatch.Client, config *config.MetricsConfig) *Collector {
	units := map[MetricType]MetricUnit{
		MetricMessagesReceived:  UnitCount,
		MetricMessagesProcessed: UnitCount,
		MetricProcessingTime:    UnitMilliseconds,
		MetricProcessingErrors:  UnitCount,
		MetricWorkerCount:       UnitCount,
		MetricBufferUtilization: UnitPercent,
		MetricQueueDepth:        UnitCount,
		MetricWorkerAdditions:   UnitCount,
		MetricWorkerRemovals:    UnitCount,
		MetricMemoryUsage:       UnitBytes,
		MetricCPUUsage:          UnitPercent,
	}

	return &Collector{
		client:      client,
		config:      config,
		metrics:     make(map[string]map[MetricType]*MetricSeries),
		metricUnits: units,
	}
}

func (c *Collector) Start(ctx context.Context) error {
	// Start metrics cleanup routine
	go c.cleanupRoutine(ctx)

	// Start metrics publishing routine
	publishTicker := time.NewTicker(c.config.PublishInterval.Duration)
	defer publishTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-publishTicker.C:
			if err := c.publish(ctx); err != nil {
				// Log error but continue collecting
				fmt.Printf("Error publishing metrics: %v\n", err)
				continue
			}
		}
	}
}

func (c *Collector) cleanupRoutine(ctx context.Context) {
	cleanupTicker := time.NewTicker(time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			c.cleanup()
		}
	}
}

func (c *Collector) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	retentionCutoff := now.Add(-c.config.RetentionPeriod.Duration)

	for queueURL, metrics := range c.metrics {
		for metricType, series := range metrics {
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
			}

			// Enforce maximum number of data points
			if len(series.Points) > c.config.MaxDataPoints {
				excess := len(series.Points) - c.config.MaxDataPoints
				series.Points = series.Points[excess:]
			}
		}

		// Remove empty queue metrics
		if len(metrics) == 0 {
			delete(c.metrics, queueURL)
		}
	}
}

func (c *Collector) RecordMetric(queueURL string, metricType MetricType, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.metrics[queueURL]; !exists {
		c.metrics[queueURL] = make(map[MetricType]*MetricSeries)
	}

	if _, exists := c.metrics[queueURL][metricType]; !exists {
		c.metrics[queueURL][metricType] = &MetricSeries{
			Points: make([]MetricDataPoint, 0, c.config.MaxDataPoints),
		}
	}

	series := c.metrics[queueURL][metricType]
	series.Points = append(series.Points, MetricDataPoint{
		Value:     value,
		Timestamp: time.Now(),
		Unit:      c.metricUnits[metricType],
	})
	series.LastWrite = time.Now()

	// Trim if exceeding max points
	if len(series.Points) > c.config.MaxDataPoints {
		series.Points = series.Points[1:]
	}
}

func (c *Collector) RecordProcessingStarted(queueURL string, priority int) {
	c.RecordMetric(queueURL, MetricMessagesReceived, 1)
}

func (c *Collector) RecordProcessingComplete(queueURL string, priority int, duration time.Duration) {
	c.RecordMetric(queueURL, MetricMessagesProcessed, 1)
	c.RecordMetric(queueURL, MetricProcessingTime, float64(duration.Milliseconds()))
}

func (c *Collector) RecordError(queueURL string, errorType string) {
	c.RecordMetric(queueURL, MetricProcessingErrors, 1)
}

func (c *Collector) RecordWorkerAdded(workerID string) {
	c.RecordMetric("", MetricWorkerAdditions, 1)
	c.RecordMetric("", MetricWorkerCount, float64(c.getWorkerCount()+1))
}

func (c *Collector) RecordWorkerRemoved(workerID string) {
	c.RecordMetric("", MetricWorkerRemovals, 1)
	c.RecordMetric("", MetricWorkerCount, float64(c.getWorkerCount()-1))
}

func (c *Collector) RecordWorkerReplaced(workerID string) {
	// Worker count stays the same for replacement
	c.RecordMetric("", MetricWorkerRemovals, 1)
	c.RecordMetric("", MetricWorkerAdditions, 1)
}

func (c *Collector) publish(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var metricData []types.MetricDatum

	for queueURL, metrics := range c.metrics {
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
						Name:  aws.String("QueueURL"),
						Value: aws.String(queueURL),
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

func (c *Collector) GetRawMetrics(queueURL string) map[MetricType]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[MetricType]float64)
	if queueMetrics, exists := c.metrics[queueURL]; exists {
		for metricType, series := range queueMetrics {
			if len(series.Points) > 0 {
				result[metricType] = series.Points[len(series.Points)-1].Value
			}
		}
	}
	return result
}

func (c *Collector) GetQueueMetrics(queueURL string) *QueueMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if queueMetrics, exists := c.metrics[queueURL]; exists {
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

func (c *Collector) getWorkerCount() int {
	metrics := c.GetRawMetrics("") // Use GetRawMetrics instead of GetQueueMetrics
	if count, exists := metrics[MetricWorkerCount]; exists {
		return int(count)
	}
	return 0
}

// GetAllQueueMetrics returns all queue metrics
func (c *Collector) GetAllQueueMetrics() map[string]*QueueMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*QueueMetrics)
	for queueURL := range c.metrics {
		if metrics := c.GetQueueMetrics(queueURL); metrics != nil {
			result[queueURL] = metrics
		}
	}
	return result
}

func (c *Collector) GetWorkerCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if metrics, exists := c.metrics[""]; exists {
		if workerCount, exists := metrics[MetricWorkerCount]; exists && len(workerCount.Points) > 0 {
			return int(workerCount.Points[len(workerCount.Points)-1].Value)
		}
	}
	return 0
}

func (c *Collector) GetBufferMetrics() *BufferMetrics {
	rawMetrics := c.GetRawMetrics("") // Use empty string for global metrics

	result := &BufferMetrics{}

	// Get utilization metrics
	if high, exists := rawMetrics[MetricBufferUtilization+"High"]; exists {
		result.HighPriorityUtilization = high
	}
	if med, exists := rawMetrics[MetricBufferUtilization+"Medium"]; exists {
		result.MediumPriorityUtilization = med
	}
	if low, exists := rawMetrics[MetricBufferUtilization+"Low"]; exists {
		result.LowPriorityUtilization = low
	}

	// Get memory usage
	if memory, exists := rawMetrics[MetricMemoryUsage]; exists {
		result.TotalMemoryUsage = int64(memory)
	}

	return result
}
