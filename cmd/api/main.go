package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chat4all/chat4all-v2/api"
	"github.com/chat4all/chat4all-v2/internal/cfg"
	"github.com/chat4all/chat4all-v2/internal/kafka"
	"github.com/chat4all/chat4all-v2/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	config, err := cfg.Load()
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	slogger := logger.With(slog.String("component", "api"))

	ctx := context.Background()
	store, err := storage.New(ctx, config.PostgresURL)
	if err != nil {
		slogger.Error("failed to initialize storage", slog.Any("error", err))
		os.Exit(1)
	}
	defer store.Close()

	producer, err := kafka.NewProducer(config.KafkaBrokers)
	if err != nil {
		slogger.Error("failed to create kafka producer", slog.Any("error", err))
		os.Exit(1)
	}
	defer producer.Close()

	server := api.NewServer(config, producer, store, slogger)

	httpServer := &http.Server{
		Addr:    config.HTTPAddr(),
		Handler: server.Router(),
	}

	go func() {
		slogger.Info("api server starting", slog.String("addr", config.HTTPAddr()))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slogger.Error("http server error", slog.Any("error", err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slogger.Info("received shutdown signal", slog.String("signal", sig.String()))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(config.ShutdownTimeoutS)*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slogger.Error("graceful shutdown failed", slog.Any("error", err))
	}

	slogger.Info("api server stopped")
}
