# sqs-fargate-consumer-v2

## Overview

A high-performance, scalable SQS message processing system with priority-based queueing and dynamic resource management.

## Architecture

### Components
- ConsumerGroup: Manages SQS message consumption
- MessageProcessor: Handles message processing
- MessageBuffer: Priority-based message buffering
- Scheduler: Smart queue selection and management
- Metrics: Buffer metrics, queue metrics collection and monitoring

## Message Buffer

- Maintains 3 priority queues/channel (high, medium, low)
- Push operations respect message priority and size limits
- Pop operations follow strict priority (high → medium → low)
- Emits metrics during push/pop operations
- Monitors queue/channel sizes periodically

Separate channels allow:
- Different buffer sizes per priority
- Prevent low priority messages from flooding the buffer
- Guarantee space for high priority messages
- No need for message sorting or priority queues
- Constant time O(1) operations for both push and pop
- Lock-free operations using Go channels
- Reduced contention between priorities            


## Consumer Group

- Maintains pool of SQS polling workers
- Workers poll messages when SQS queues have messages
- Scales based on queue depth, inflight messages, and buffer capacity
- Uses backpressure when buffer is near full
- Long polling (20s) to reduce SQS API calls

### Scaling Factors
1. Buffer Pressure:
```go
maxBufferUsage := max(highUsage, mediumUsage, lowUsage)
bufferPressure := maxBufferUsage > config.ScaleThreshold
```
2. Message Backlog:
```go
backlogThreshold := currentWorkers * 10
backlogPressure := (messagesIn - messagesOut) > backlogThreshold
```
3. Cooldown Periods:
```go
if time.Since(lastScaleUpTime) < config.ScaleUpCoolDown {
    return // Skip scaling
}
if time.Since(lastScaleDownTime) < config.ScaleDownCoolDown {
    return // Skip scaling
}
```

## Message Processor

- Processes messages from buffer
- Maintains worker pool for message processing
- Simulates processing with random duration
- Deletes messages from SQS after successful processing

### Scaling Factors
1. Worker Utilization Check:
```go
utilizationRate := float64(activeWorkers) / float64(currentWorkers)
workerPressure := utilizationRate > config.ScaleThreshold
```
2. Backlog Assessment:
```go
backlogThreshold := config.MaxWorkers * 10
backlogPressure := backlog > backlogThreshold
```
3. Scale Up Decision:
```go
shouldScaleUp := (bufferPressure || workerPressure || backlogPressure) &&
                 currentWorkers < config.MaxWorkers
```
4. Scale Down Decision:
```go
lowUtilization := utilizationRate < config.ScaleThreshold/2
lowBacklog := backlog < config.MinWorkers*5
shouldScaleDown := lowUtilization && !bufferPressure && lowBacklog &&
                   currentWorkers > config.MinWorkers
```


## Metrics Collection

### Buffer Metrics
- Queue utilization per priority level
- Message counts (in/out/total)
- Publishes to CloudWatch
- Wait time statistics
    - Average and maximum wait times
    - Wait time distribution histogram
- Processing rate monitoring

### Queue Metrics
- Message counts (visible/in-flight)
- Processing statistics

### CloudWatch Integration
- Queue depth metrics
- Processing performance
- Buffer utilization
- Wait time distributions
- Error rates


## Scheduler
- Priority order is strictly maintained
- Queues don't get starved
- Recent polling history influences selection
- Higher message counts get proportionally more attention

### Queue Selection Factors
1. Priority Levels:
```go
// Priority multipliers to ensure higher priority queues are favored
priorityMultiplier := math.Pow(10, float64(queue.Priority+1))
// Priority 3 (High)   = 10000x
// Priority 2 (Medium) = 1000x
// Priority 1 (Low)    = 100x
```
2. Wait Time Consideration:
```go
// Add wait time factor to prevent starvation
waitTime := now.Sub(lastPollTime).Seconds()
maxWaitBoost := float64(queue.Priority) * 0.5
// 30 means max boost at 30s
waitFactor := math.Min(1.0 + (waitTime/30.0), 1.0 + maxWaitBoost)
```
3. Queue Weights:
```go
// Apply configured queue weight
score := baseScore * priorityMultiplier * queue.Weight
```

Example Scenarios:

1. High Priority Queue Selection:
```
Queue Configuration:
- High Priority Queue:
  * Messages: 10
  * Weight: 1.0
  * Last Poll: 5 seconds ago
  * Poll Count: 5
Score Calculation:
  * Base Score = 10 * 1.0 = 10
  * Priority Multiplier = 10^4 = 10000
  * Wait Factor = 1 + (5/30) = 1.167
  * No poll count penalty
Final Score = 10 * 10000 * 1.167 = 116,700
```

2. Medium Priority Queue with Wait Time:
```
Queue Configuration:
- Medium Priority Queue:
  * Messages: 20
  * Weight: 0.75
  * Last Poll: 60 seconds ago
  * Poll Count: 3
Score Calculation:
  * Base Score = 20 * 0.75 = 15
  * Priority Multiplier = 10^3 = 1000
  * Wait Factor = 1 + (60/30) = 3 (capped at 1.5 for medium priority)
  * No poll count penalty
Final Score = 15 * 1000 * 1.5 = 22,500
```

3. Low Priority Queue with Poll Penalty:
```
Queue Configuration:
- Low Priority Queue:
  * Messages: 50
  * Weight: 0.5
  * Last Poll: 2 seconds ago
  * Poll Count: 15
Score Calculation:
  * Base Score = 50 * 0.5 = 25
  * Priority Multiplier = 10^2 = 100
  * Wait Factor = 1 + (2/30) = 1.067
  * Poll Count Penalty = 0.95
Final Score = 25 * 100 * 1.067 * 0.95 = 2,534
```

Selection Results:
- High Priority Queue (Score: 116,700) would be selected over
- Medium Priority Queue (Score: 22,500) and
- Low Priority Queue (Score: 2,534)
