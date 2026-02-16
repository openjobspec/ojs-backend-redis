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
	"time"

	ojsgrpc "github.com/openjobspec/ojs-backend-redis/internal/grpc"
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

	// Start background scheduler
	sched := scheduler.New(backend)
	sched.Start()
	defer sched.Stop()

	// Create HTTP server
	router := server.NewRouter(backend)
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	grpcServer.GracefulStop()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	slog.Info("server stopped")
}
