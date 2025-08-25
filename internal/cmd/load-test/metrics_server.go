package main

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

type MetricsServer struct {
	server *http.Server
	webUI  *WebUI
}

func NewMetricsServer(addr string) *MetricsServer {
	mux := http.NewServeMux()

	// Create WebUI instance
	webUI := NewWebUI()

	// Add routes
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/", webUI.handleIndex)
	mux.HandleFunc("/ws", webUI.handleWebSocket)

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return &MetricsServer{
		server: server,
		webUI:  webUI,
	}
}

func (s *MetricsServer) Start(ctx context.Context) error {
	go func() {
		log.Info().Str("addr", s.server.Addr).Msg("Starting metrics server")
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("Error shutting down metrics server")
		}
	}()

	return nil
}

func (s *MetricsServer) BroadcastStats(report *Report) {
	if s.webUI != nil {
		s.webUI.broadcastStats(report)
	}
}
