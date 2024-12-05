# sqs-fargate-consumer-v2


## Message Buffer

Logic:
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

Logic:
- Maintains pool of SQS polling workers
- Workers poll messages when SQS queues have messages
- Scales based on queue depth, inflight messages, and buffer capacity
- Uses backpressure when buffer is near full
- Long polling (20s) to reduce SQS API calls

Scaling:
- Monitors total messages, inflight count, and buffer utilization
- Scales down when buffer usage high (>90%)
- Considers available buffer capacity for scaling up
- Respects min/max worker bounds

Scaling example scenarios:
1. High buffer usage (90%), MaxWorkers=100, MinWorkers=5:
    - Returns ~10 workers to reduce polling

2. Empty queues, no inflight:
    - Returns MinWorkers (5)

3. Queue depth=1000, avgProcessingTime=200ms, buffer at 50%:
    - messagesPerWorkerPerSecond = 5 (1000/200)
    - For 5s interval: desiredWorkers = 1000/(5*5) = 40 workers


## Message Processor

Logic:
- Processes messages from buffer
- Maintains worker pool for message processing
- Simulates processing with random duration
- Deletes messages from SQS after successful processing

Scaling:
- Scales based on buffer usage and worker utilization
- Uses worker status (idle/processing) for scaling decisions
- Scales down when utilization < threshold/2
- Respects min/max worker configuration

Scaling example scenarios:
1. High utilization scenario:
    - Current workers: 20
    - Active workers: 16
    - Utilization: 16/20 = 0.8
    - Buffer high priority usage: 0.75
    - ScaleThreshold: 0.7
    - Result: Scales up by adding worker

2. Low utilization scenario:
    - Current workers: 30
    - Active workers: 9
    - Utilization: 9/30 = 0.3
    - ScaleThreshold: 0.7
    - Result: Scales down by stopping one worker

3. Steady state scenario:
    - Current workers: 25
    - Active workers: 15
    - Utilization: 15/25 = 0.6
    - Result: Maintains current worker count


## Metrics Collection

Logic:
- Collects metrics for queues and buffer
- Maintains historical data points
- Publishes to CloudWatch
- Tracks processing rates and latencies


## Scheduler

Logic:
- Selects queues based on priority and metrics
- Uses weighted scoring system for queue selection
- Tracks queue poll history
- Cleans up stale metrics

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

Key Factors in Queue Selection:
1. Priority Level:
   - Exponential multiplier ensures higher priority queues are preferred
   - High = 10000x, Medium = 1000x, Low = 100x

2. Message Count & Weight:
   - More messages increase score linearly
   - Weight allows fine-tuning of queue importance

3. Wait Time:
   - Increases score for queues not polled recently
   - Capped based on priority to maintain priority hierarchy
   ```
   30 means "reach maximum boost after 30 seconds"
   If lastPoll was:
    - 2 seconds ago:  1 + (2/30)  = 1.067  (6.7% boost)
    - 15 seconds ago: 1 + (15/30) = 1.5    (50% boost)
    - 30 seconds ago: 1 + (30/30) = 2.0    (100% boost)
    - 60 seconds ago: Would be capped by maxWaitBoost
   ```

4. Poll Count Penalty:
   - Small penalty for frequently polled queues
   - Helps prevent queue starvation

The scheduler ensures:
- Priority order is strictly maintained
- Queues don't get starved
- Recent polling history influences selection
- Higher message counts get proportionally more attention