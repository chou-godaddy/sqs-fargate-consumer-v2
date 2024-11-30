package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	goapi "github.com/gdcorp-domains/fulfillment-go-api"
	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/consumer"
	"sqs-fargate-consumer-v2/internal/dependencies"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sqs-fargate-consumer-v2/internal/processor"
	"sqs-fargate-consumer-v2/internal/scaler"
)

func health(deps interface{}) error {
	return nil
}

func main() {
	configLocation := flag.String("config", "internal/config/config.json", "path to the config file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configLocation)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
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
	)

	// Create auto-scaler
	autoScaler := scaler.NewScaler(
		consumerGroup,
		collector,
		&cfg.ConsumerGroupConfig,
	)

	// Start health, ready check server
	go func() {
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
		if err := collector.Start(ctx); err != nil {
			errChan <- fmt.Errorf("Failed to start metrics collector: %v", err)
		}
	}()

	go func() {
		if err := consumerGroup.Start(ctx); err != nil {
			errChan <- fmt.Errorf("Failed to start consumer group: %v", err)
		}
	}()

	go func() {
		if err := messageProcessor.Start(ctx); err != nil {
			errChan <- fmt.Errorf("Failed to start message processor: %v", err)
		}
	}()

	go func() {
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

	// Simulate some work
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(100+time.Now().UnixNano()%900) * time.Millisecond):
		// Random processing time between 100ms and 1s
	}

	return nil
}
