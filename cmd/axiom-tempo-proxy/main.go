// Command axiom-tempo-proxy serves the Grafana Tempo query API backed by an
// Axiom trace dataset.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/config"
	"github.com/velddev/axiom-tempo-proxy/internal/server"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	server.Version = version

	client, err := axiom.New(axiom.Config{
		BaseURL:    cfg.AxiomURL,
		Token:      cfg.AxiomToken,
		OrgID:      cfg.AxiomOrgID,
		QueryPath:  cfg.AxiomQueryPath,
		UserAgent:  "axiom-tempo-proxy/" + version,
		HTTPClient: &http.Client{Timeout: cfg.QueryTimeout + 10*time.Second},
	})
	if err != nil {
		return err
	}

	srv := server.New(cfg, client, log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	warmCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	srv.Warm(warmCtx)
	cancel()

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.ListenAddr, "default_dataset", cfg.Dataset, "axiom", cfg.AxiomURL)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
