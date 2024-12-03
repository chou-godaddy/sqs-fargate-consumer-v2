package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/consumer"
	"sqs-fargate-consumer-v2/internal/dependencies"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sqs-fargate-consumer-v2/internal/processor"
	"sqs-fargate-consumer-v2/internal/scaler"

	goapi "github.com/gdcorp-domains/fulfillment-go-api"
)

func health(deps interface{}) error {
	return nil
}

func main() {
	configLocation := flag.String("config", "internal/config/config.json", "path to the config file")
	flag.Parse()

	// Load configuration
	cfg := &config.Config{}

	if *configLocation == "" {
		panic("config file path cannot be empty")
	}

	if err := cfg.Load(*configLocation); err != nil {
		panic(fmt.Errorf("failed to load config from %s: %w", *configLocation, err))
	}

	cfg.Config.Server.HealthChecks = []func(interface{}) error{health}

	dep := dependencies.New(cfg)

	svr, err := goapi.New(cfg.Config, dep)
	if err != nil {
		panic(fmt.Errorf("failed to create goapi server: %w", err))
	}

	// Create metrics collector
	collector := metrics.NewCollector(dep.CloudwatchClient, &cfg.MetricsConfig)

	// Create event buffer
	buffer := consumer.NewEventBuffer(cfg.BufferConfig, cfg.ConsumerGroupConfig.MinWorkers)

	// Create consumer group
	consumerGroup := consumer.NewConsumer(&cfg.ConsumerGroupConfig, dep.SQSClient, collector, buffer)

	// Create message processor
	processorCfg := config.ProcessorConfig{
		MaxConcurrency:     cfg.ProcessorConfig.MaxConcurrency,
		MinConcurrency:     cfg.ProcessorConfig.MinConcurrency,
		ProcessTimeout:     cfg.ProcessorConfig.ProcessTimeout,
		ScaleUpThreshold:   cfg.ProcessorConfig.ScaleUpThreshold,
		ScaleDownThreshold: cfg.ProcessorConfig.ScaleDownThreshold,
		ScaleInterval:      cfg.ProcessorConfig.ScaleInterval,
	}

	messageProcessor := processor.NewMessageProcessor(
		processorCfg,
		buffer,
		handleMessage,
		collector,
		dep.SQSClient,
	)

	// Create auto-scaler
	autoScaler := scaler.NewScaler(
		consumerGroup,
		collector,
		&cfg.ScalerConfig,
		&cfg.ConsumerGroupConfig,
	)

	// Start health, ready check server
	go func() {
		log.Println("Starting health check server...")
		if err := svr.Start(); err != nil {
			log.Printf("failed to start server: %v", err)
		}
	}()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create error channel for component errors
	errChan := make(chan error, 4)

	// Start components
	go func() {
		log.Println("Starting metrics collector...")
		if err := collector.Start(ctx); err != nil {
			errChan <- fmt.Errorf("Failed to start metrics collector: %v", err)
		}
		log.Println("Metrics collector started")
	}()

	go func() {
		log.Println("Starting consumer group...")
		if err := consumerGroup.Start(ctx); err != nil {
			errChan <- fmt.Errorf("Failed to start consumer group: %v", err)
		}
	}()

	go func() {
		log.Println("Starting message processor...")
		if err := messageProcessor.Start(ctx); err != nil {
			errChan <- fmt.Errorf("Failed to start message processor: %v", err)
		}
	}()

	go func() {
		log.Println("Starting auto-scaler...")
		if err := autoScaler.Start(ctx); err != nil {
			errChan <- fmt.Errorf("Failed to start auto-scaler: %v", err)
		}
	}()

	// Create shutdown signal channel
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err := <-errChan:
		log.Printf("Fatal error: %v", err)
		// Initiate shutdown on fatal error
		shutdown <- syscall.SIGTERM

	case sig := <-shutdown:
		log.Printf("Shutdown signal (%v) received, initiating graceful shutdown...", sig)

		// Create shutdown context with timeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		log.Println("Stopping scaler...")
		autoScaler.Shutdown(shutdownCtx)

		log.Println("Stopping message processor...")
		if err := messageProcessor.Shutdown(shutdownCtx); err != nil {
			log.Printf("Error shutting down message processor: %v", err)
		}

		log.Println("Stopping consumer group...")
		if err := consumerGroup.Shutdown(shutdownCtx); err != nil {
			log.Printf("Error shutting down consumer group: %v", err)
		}

		log.Println("Stopping metrics collector...")
		collector.Shutdown(shutdownCtx)

		log.Println("Stopping health check server...")
		svr.Stop()

		select {
		case <-shutdownCtx.Done():
			log.Println("Shutdown timeout reached, forcing exit")
		default:
			log.Println("Graceful shutdown completed")
		}

	}
}

func handleMessage(ctx context.Context, event *consumer.Event) error {
	msg := event.Message
	fmt.Printf("Processing message: %s from queue: %s\n", *msg.MessageId, event.QueueURL)

	// Create a random source using crypto/rand for better randomization
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Errorf("failed to generate random number: %v", err)
	}

	// Convert to uint64 and calculate random duration between 2s and 20s
	randomNum := binary.BigEndian.Uint64(b[:])
	durationMs := 2000 + (randomNum % 18001) // 2000ms to 20000ms

	// Simulate work with context cancellation support
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(durationMs) * time.Millisecond):
		// Processing completed
		log.Printf("Processed message %s in %dms", *msg.MessageId, durationMs)
	}

	return nil
}
