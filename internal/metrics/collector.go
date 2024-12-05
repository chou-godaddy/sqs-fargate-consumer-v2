package metrics

import (
	"context"
	"fmt"
	"log"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/interfaces"
	"sqs-fargate-consumer-v2/internal/models"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Collector struct {
	sqsClient        *sqs.Client
	cloudwatchClient *cloudwatch.Client
	bufferMetrics    *BufferMetricsCollector
	queues           map[string]string // map[QueueName]QueueURL
	metrics          sync.Map          // map[string]*QueueMetrics
	updateInterval   time.Duration
	namespace        string
	region           string
	maxDataPoints    int

	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewCollector(sqsClient *sqs.Client, cloudwatchClient *cloudwatch.Client, queues []config.QueueConfig) interfaces.MetricsCollector {
	if sqsClient == nil {
		log.Fatal("[Collector] SQS client cannot be nil")
	}
	if cloudwatchClient == nil {
		log.Fatal("[Collector] CloudWatch client cannot be nil")
	}
	if len(queues) == 0 {
		log.Fatal("[Collector] Queue configuration cannot be empty")
	}

	queueMap := make(map[string]string)
	for _, queue := range queues {
		if queue.Name == "" || queue.URL == "" {
			log.Fatal("[Collector] Queue name and URL cannot be empty")
		}
		queueMap[queue.Name] = queue.URL
	}

	return &Collector{
		sqsClient:        sqsClient,
		cloudwatchClient: cloudwatchClient,
		bufferMetrics:    NewBufferMetricsCollector(),
		queues:           queueMap,
		updateInterval:   time.Second,
		namespace:        "SQS-FARGATE-CONSUMER-V2/SQSConsumer",
		region:           "us-west-2",
		maxDataPoints:    1000,
		stopChan:         make(chan struct{}),
	}
}

func (c *Collector) Start(ctx context.Context) error {
	log.Printf("[Collector] Starting metrics collection for %d queues", len(c.queues))

	// Initialize metrics for each queue
	for queueName := range c.queues {
		c.metrics.Store(queueName, &models.QueueMetrics{
			LastUpdateTime: time.Now(),
		})
	}

	// Start metrics collection for each queue
	for queueName, queueURL := range c.queues {
		c.wg.Add(1)
		go c.collectQueueMetrics(ctx, queueName, queueURL)
	}

	// Start CloudWatch metrics publishing
	c.wg.Add(1)
	go c.publishMetrics(ctx)

	return nil
}

func (c *Collector) collectQueueMetrics(ctx context.Context, queueName, queueURL string) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			metrics, err := c.fetchQueueMetrics(ctx, queueURL)
			if err != nil {
				log.Printf("[Collector] Error fetching metrics for queue %s: %v", queueName, err)
				continue
			}

			if existingMetrics, ok := c.metrics.Load(queueName); ok {
				existing := existingMetrics.(*models.QueueMetrics)

				// Update counters while preserving history
				metrics.ErrorCount.Store(existing.ErrorCount.Load())
				metrics.ProcessedCount.Store(existing.ProcessedCount.Load())
				metrics.ProcessingTime.Store(existing.ProcessingTime.Load())
				metrics.MaxLatency.Store(existing.MaxLatency.Load())

				// Manage historical data points
				metrics.HistoricalPoints = append(existing.HistoricalPoints, models.MetricPoint{
					Timestamp:     time.Now(),
					MessageCount:  metrics.MessageCount,
					InFlightCount: metrics.InFlightCount,
				})

				// Trim historical points if exceeding maxDataPoints
				if len(metrics.HistoricalPoints) > c.maxDataPoints {
					metrics.HistoricalPoints = metrics.HistoricalPoints[len(metrics.HistoricalPoints)-c.maxDataPoints:]
				}
			}

			c.metrics.Store(queueName, metrics)
			// log.Printf("[Collector] Updated metrics for queue %s - Messages: %d, InFlight: %d, Historical points: %d",
			// 	queueName, metrics.MessageCount, metrics.InFlightCount, len(metrics.HistoricalPoints))
		}
	}
}

func (c *Collector) fetchQueueMetrics(ctx context.Context, queueURL string) (*models.QueueMetrics, error) {
	input := &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	}

	result, err := c.sqsClient.GetQueueAttributes(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("getting queue attributes: %w", err)
	}

	return &models.QueueMetrics{
		MessageCount:   getIntAttribute(result.Attributes, string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)),
		InFlightCount:  getIntAttribute(result.Attributes, string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)),
		LastUpdateTime: time.Now(),
	}, nil
}

func (c *Collector) publishMetrics(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			if err := c.publishToCloudWatch(ctx); err != nil {
				log.Printf("[Collector] Error publishing metrics: %v", err)
			}
		}
	}
}

func (c *Collector) publishToCloudWatch(ctx context.Context) error {
	var metricData []types.MetricDatum

	// Collect SQS queue metrics
	c.metrics.Range(func(key, value interface{}) bool {
		queueName := key.(string)
		metrics := value.(*models.QueueMetrics)

		// Queue depth metrics
		metricData = append(metricData,
			c.createMetricDatum("VisibleMessages", float64(metrics.MessageCount), types.StandardUnitCount, queueName, time.Now()),
			c.createMetricDatum("InFlightMessages", float64(metrics.InFlightCount), types.StandardUnitCount, queueName, time.Now()),
		)

		// Performance metrics
		if metrics.ProcessedCount.Load() > 0 {
			avgProcessingTime := float64(metrics.ProcessingTime.Load()) / float64(metrics.ProcessedCount.Load())
			metricData = append(metricData,
				c.createMetricDatum("ProcessedMessages", float64(metrics.ProcessedCount.Load()), types.StandardUnitCount, queueName, time.Now()),
				c.createMetricDatum("Errors", float64(metrics.ErrorCount.Load()), types.StandardUnitCount, queueName, time.Now()),
				c.createMetricDatum("AverageProcessingTime", avgProcessingTime, types.StandardUnitMilliseconds, queueName, time.Now()),
				c.createMetricDatum("MaxLatency", float64(metrics.MaxLatency.Load()), types.StandardUnitMilliseconds, queueName, time.Now()),
			)
		}
		return true
	})

	// Collect buffer metrics
	bufferMetrics := c.GetBufferMetrics()

	// Queue usage metrics
	metricData = append(metricData,
		c.createMetricDatum("BufferHighPriorityUsage", bufferMetrics.HighPriorityUsage*100, types.StandardUnitPercent, "Buffer", time.Now()),
		c.createMetricDatum("BufferMediumPriorityUsage", bufferMetrics.MediumPriorityUsage*100, types.StandardUnitPercent, "Buffer", time.Now()),
		c.createMetricDatum("BufferLowPriorityUsage", bufferMetrics.LowPriorityUsage*100, types.StandardUnitPercent, "Buffer", time.Now()),
	)

	// Message counts
	metricData = append(metricData,
		c.createMetricDatum("BufferTotalSize", float64(bufferMetrics.TotalSize), types.StandardUnitBytes, "Buffer", time.Now()),
		c.createMetricDatum("BufferMessagesIn", float64(bufferMetrics.TotalMessagesIn), types.StandardUnitCount, "Buffer", time.Now()),
		c.createMetricDatum("BufferMessagesOut", float64(bufferMetrics.TotalMessagesOut), types.StandardUnitCount, "Buffer", time.Now()),
		c.createMetricDatum("BufferOverflows", float64(bufferMetrics.OverflowCount), types.StandardUnitCount, "Buffer", time.Now()),
	)

	// Wait time metrics
	metricData = append(metricData,
		c.createMetricDatum("BufferAverageWaitTime", float64(bufferMetrics.AverageWaitTime.Milliseconds()), types.StandardUnitMilliseconds, "Buffer", time.Now()),
		c.createMetricDatum("BufferMaxWaitTime", float64(bufferMetrics.MaxWaitTime.Milliseconds()), types.StandardUnitMilliseconds, "Buffer", time.Now()),
	)

	// Wait time histogram
	for bucket, count := range bufferMetrics.WaitTimeHistogram {
		metricData = append(metricData,
			c.createMetricDatum("BufferWaitTimeDistribution", float64(count), types.StandardUnitCount, "Buffer", time.Now(),
				types.Dimension{
					Name:  aws.String("TimeBucket"),
					Value: aws.String(bucket),
				},
			),
		)
	}

	// Processing rate
	metricData = append(metricData,
		c.createMetricDatum("BufferProcessingRate", bufferMetrics.MessageProcessingRate, types.StandardUnitCountSecond, "Buffer", time.Now()),
	)

	// Publish metrics in batches of 20 (CloudWatch limit)
	for i := 0; i < len(metricData); i += 20 {
		end := i + 20
		if end > len(metricData) {
			end = len(metricData)
		}

		input := &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(c.namespace),
			MetricData: metricData[i:end],
		}

		if _, err := c.cloudwatchClient.PutMetricData(ctx, input); err != nil {
			return fmt.Errorf("publishing metrics batch: %w", err)
		}
	}

	return nil
}

func (c *Collector) createMetricDatum(name string, value float64, unit types.StandardUnit, dimension string, timestamp time.Time, extraDims ...types.Dimension) types.MetricDatum {
	dimensions := append([]types.Dimension{
		{
			Name:  aws.String("Component"),
			Value: aws.String(dimension),
		},
		{
			Name:  aws.String("Region"),
			Value: aws.String(c.region),
		},
	}, extraDims...)

	return types.MetricDatum{
		MetricName: aws.String(name),
		Value:      aws.Float64(value),
		Unit:       unit,
		Timestamp:  aws.Time(timestamp),
		Dimensions: dimensions,
	}
}

func (c *Collector) RecordError(queueName string) {
	if metrics, ok := c.metrics.Load(queueName); ok {
		metrics.(*models.QueueMetrics).ErrorCount.Add(1)
	}
}

func (c *Collector) RecordProcessed(queueName string, duration time.Duration) {
	if metrics, ok := c.metrics.Load(queueName); ok {
		m := metrics.(*models.QueueMetrics)
		m.ProcessedCount.Add(1)
		processingTime := duration.Milliseconds()
		m.ProcessingTime.Add(processingTime)

		// Update max latency if current duration is higher
		for {
			currentMax := m.MaxLatency.Load()
			if processingTime <= currentMax {
				break
			}
			if m.MaxLatency.CompareAndSwap(currentMax, processingTime) {
				break
			}
		}
	}
}

func (c *Collector) GetQueueMetrics(queueName string) *models.QueueMetrics {
	if metrics, ok := c.metrics.Load(queueName); ok {
		return metrics.(*models.QueueMetrics)
	}
	return nil
}

func (c *Collector) GetAllQueueMetrics() map[string]*models.QueueMetrics {
	result := make(map[string]*models.QueueMetrics)
	c.metrics.Range(func(key, value interface{}) bool {
		result[key.(string)] = value.(*models.QueueMetrics)
		return true
	})
	return result
}

func (c *Collector) Shutdown(ctx context.Context) error {
	close(c.stopChan)

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func getIntAttribute(attrs map[string]string, key string) int64 {
	if val, ok := attrs[key]; ok {
		var result int64
		fmt.Sscanf(val, "%d", &result)
		return result
	}
	return 0
}

// BufferMetricsEmitter interface implementation
func (c *Collector) OnMessageEnqueued(message *models.Message) {
	if c.bufferMetrics != nil {
		c.bufferMetrics.OnMessageEnqueued(message)
	}
}

func (c *Collector) OnMessageDequeued(message *models.Message) {
	if c.bufferMetrics != nil {
		c.bufferMetrics.OnMessageDequeued(message)
	}
}

func (c *Collector) OnBufferOverflow(priority models.Priority) {
	if c.bufferMetrics != nil {
		c.bufferMetrics.OnBufferOverflow(priority)
	}
}

func (c *Collector) OnQueueSizeChanged(priority models.Priority, currentSize, capacity int) {
	if c.bufferMetrics != nil {
		c.bufferMetrics.OnQueueSizeChanged(priority, currentSize, capacity)
	}
}

// BufferMetricsProvider interface implementation
func (c *Collector) GetBufferMetrics() models.BufferMetrics {
	if c.bufferMetrics != nil {
		return c.bufferMetrics.GetMetrics()
	}
	return models.BufferMetrics{}
}

func (c *Collector) GetBufferMetricsCollector() interfaces.BufferMetricsCollector {
	return c.bufferMetrics
}
