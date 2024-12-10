# sqs-fargate-consumer-v2

## Overview

A high-performance, scalable SQS message processing system with priority-based queueing and dynamic resource management.

## Architecture

### Components
- ConsumerGroup: Manages SQS message consumption
- MessageProcessor: Handles message processing
- MessageBuffer: Single channel message buffering
- Scheduler: Weighted random queue selection
- Metrics: Buffer metrics, queue metrics collection and monitoring

## Message Buffer

- Single channel implementation for simplified message handling
- Emits metrics during push/pop operations
- Blocking push/pop operations when buffer is full      

## Consumer Group

- Maintains pool of SQS polling workers
- Workers poll messages when SQS queues have messages and push to message buffer channel
- Long polling (20s) to reduce SQS API calls
- Scales based on message backlog

## Message Processor

- Maintains worker pool for message processing
- Processes messages from buffer
- Deletes messages from SQS after successful processing
- Scaling based on Worker utilization, Buffer pressure, Message backlog

## Metrics Collection

### Buffer Metrics
- Buffer utilization
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
- Implements weighted random queue selection
- Probabilistic fairness based on weights