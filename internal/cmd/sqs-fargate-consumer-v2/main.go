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

	"sqs-fargate-consumer-v2/internal/config"
	"sqs-fargate-consumer-v2/internal/consumer"
	"sqs-fargate-consumer-v2/internal/dependencies"
	"sqs-fargate-consumer-v2/internal/interfaces"
	"sqs-fargate-consumer-v2/internal/metrics"
	"sqs-fargate-consumer-v2/internal/processor"
	"sqs-fargate-consumer-v2/internal/scheduler"

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

	// Start health, ready check server
	go func() {
		log.Println("Starting health check server...")
		if err := svr.Start(); err != nil {
			log.Printf("failed to start server: %v", err)
		}
	}()

	// Create metrics collector
	collector := metrics.NewCollector(dep.SQSClient, dep.CloudwatchClient, cfg.Consumer.Queues)

	// Create message buffer
	buffer := consumer.NewMessageBuffer(cfg.Buffer)

	bufferMetrcisCollector := collector.GetBufferMetricsCollector()

	buffer.SetMetricsEmitter(bufferMetrcisCollector)

	// Create scheduler
	scheduler := scheduler.NewScheduler(cfg.Consumer.Queues, collector)

	// Create consumer group
	consumerGroup := consumer.NewConsumerGroup(cfg.Consumer, dep.SQSClient, scheduler, buffer, collector)

	// Create message processor
	processor := processor.NewMessageProcessor(cfg.Processor, buffer, dep.SQSClient, collector, bufferMetrcisCollector)

	// Create root context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start components
	startComponents(ctx, collector, consumerGroup, processor)

	// Wait for shutdown signal
	handleGracefulShutdown(cancel, collector, consumerGroup, processor)
}

func startComponents(ctx context.Context, components ...interfaces.Component) {
	for _, comp := range components {
		if err := comp.Start(ctx); err != nil {
			log.Fatalf("Failed to start component: %v", err)
		}
	}
}

func handleGracefulShutdown(cancel context.CancelFunc, components ...interfaces.Component) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalChan
	log.Printf("Received shutdown signal: %v", sig)
	cancel()

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Shutdown components in reverse order
	for i := len(components) - 1; i >= 0; i-- {
		componentName := fmt.Sprintf("%T", components[i])
		log.Printf("Shutting down %s...", componentName)

		if err := components[i].Shutdown(shutdownCtx); err != nil {
			log.Printf("Error shutting down %s: %v", componentName, err)
		} else {
			log.Printf("Successfully shut down %s", componentName)
		}
	}
}
