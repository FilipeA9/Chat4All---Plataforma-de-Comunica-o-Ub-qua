package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/chat4all/chat4all-v2/internal/cfg"
	"github.com/chat4all/chat4all-v2/internal/kafka"
	"github.com/chat4all/chat4all-v2/internal/storage"
	"github.com/chat4all/chat4all-v2/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(slog.String("component", "worker"))

	config, err := cfg.Load()
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx := context.Background()
	store, err := storage.New(ctx, config.PostgresURL)
	if err != nil {
		logger.Error("failed to initialize storage", slog.Any("error", err))
		os.Exit(1)
	}
	defer store.Close()

	consumer, err := kafka.NewConsumer(config.KafkaBrokers, config.KafkaGroupID, []string{config.KafkaTopic})
	if err != nil {
		logger.Error("failed to create kafka consumer", slog.Any("error", err))
		os.Exit(1)
	}
	defer consumer.Close()

	service := worker.NewService(consumer, store, logger)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := <-sigCh
		logger.Info("shutdown signal received", slog.String("signal", sig.String()))
		cancel()
	}()

	if err := service.Run(ctx); err != nil {
		logger.Error("worker stopped with error", slog.Any("error", err))
	}

	logger.Info("worker stopped")
}
