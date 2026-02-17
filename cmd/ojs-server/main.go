package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	ojsgrpc "github.com/openjobspec/ojs-backend-redis/internal/grpc"
	"github.com/openjobspec/ojs-backend-redis/internal/core"
	"github.com/openjobspec/ojs-backend-redis/internal/metrics"
	"github.com/openjobspec/ojs-backend-redis/internal/redis"
	"github.com/openjobspec/ojs-backend-redis/internal/scheduler"
	"github.com/openjobspec/ojs-backend-redis/internal/server"
	"google.golang.org/grpc"
)

func main() {
	cfg := server.LoadConfig()

	// Connect to Redis
	backend, err := redis.New(cfg.RedisURL)
	if err != nil {
		slog.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer backend.Close()

	redisHost := cfg.RedisURL
	if u, err := url.Parse(cfg.RedisURL); err == nil {
		redisHost = u.Host
	}
	slog.Info("connected to Redis", "host", redisHost)

	// Initialize Prometheus server info metric
	metrics.Init(core.OJSVersion, "redis")

	// Start background scheduler
	sched := scheduler.New(backend)
	sched.Start()
	defer sched.Stop()

	// Initialize real-time Pub/Sub broker
	broker := redis.NewPubSubBroker(backend.Client())
	defer broker.Close()

	// Create HTTP server with real-time support
	router := server.NewRouterWithRealtime(backend, cfg, broker, broker)
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// Start HTTP server
	go func() {
		slog.Info("OJS HTTP server listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Start gRPC server
	grpcServer := grpc.NewServer()
	ojsgrpc.Register(grpcServer, backend)

	go func() {
		lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
		if err != nil {
			slog.Error("failed to listen for gRPC", "port", cfg.GRPCPort, "error", err)
			os.Exit(1)
		}
		slog.Info("OJS gRPC server listening", "port", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	grpcServer.GracefulStop()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	slog.Info("server stopped")
}
