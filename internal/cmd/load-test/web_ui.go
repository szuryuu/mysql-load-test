package main

import (
	"embed"
	"html/template"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

//go:embed web/*
var webFiles embed.FS

type WebUI struct {
	upgrader websocket.Upgrader
	clients  map[*websocket.Conn]bool
	mu       sync.Mutex
}

func NewWebUI() *WebUI {
	return &WebUI{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for local development
			},
		},
		clients: make(map[*websocket.Conn]bool),
	}
}

func (ui *WebUI) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := ui.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to upgrade websocket connection")
		return
	}

	ui.mu.Lock()
	ui.clients[conn] = true
	ui.mu.Unlock()

	defer func() {
		ui.mu.Lock()
		delete(ui.clients, conn)
		ui.mu.Unlock()
		conn.Close()
	}()

	// Keep connection alive
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (ui *WebUI) broadcastStats(stats *PerformanceStats) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	for client := range ui.clients {
		err := client.WriteJSON(stats)
		if err != nil {
			log.Error().Err(err).Msg("Failed to send stats to client")
			client.Close()
			delete(ui.clients, client)
		}
	}
}

func (ui *WebUI) handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(webFiles, "web/index.html")
	if err != nil {
		http.Error(w, "Error loading template", http.StatusInternalServerError)
		return
	}

	if err := tmpl.Execute(w, nil); err != nil {
		http.Error(w, "Error rendering template", http.StatusInternalServerError)
		return
	}
}

type PerformanceStats struct {
	Timestamp         time.Time `json:"timestamp"`
	Runtime           string    `json:"runtime"`
	QueriesFetched    int64     `json:"queries_fetched"`
	CacheHits         int64     `json:"cache_hits"`
	CacheMisses       int64     `json:"cache_misses"`
	CacheHitRate      float64   `json:"cache_hit_rate"`
	CacheEvictions    int64     `json:"cache_evictions"`
	CacheNewItems     int64     `json:"cache_new_items"`
	FetchWeightsLat   string    `json:"fetch_weights_lat"`
	QueryLatencyP50   string    `json:"query_latency_p50"`
	QueryLatencyP95   string    `json:"query_latency_p95"`
	QueryLatencyP99   string    `json:"query_latency_p99"`
	QueriesPerSecond  float64   `json:"queries_per_second"`
	ActiveConnections int       `json:"active_connections"`
}
