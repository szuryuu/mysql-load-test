package main

import (
	"embed"
	"encoding/json"
	"html/template"
	"math"
	"net/http"
	"sync"

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

func sanitizeReport(report *Report) (sanitized *Report, changed bool) {
	// Deep copy the report to avoid mutating the original
	copy := *report
	changed = false

	// Sanitize Aggregates

	// Sanitize InternalStats
	if copy.InternalStats != nil {
		if math.IsNaN(copy.InternalStats.CacheHitRate) || math.IsInf(copy.InternalStats.CacheHitRate, 0) {
			copy.InternalStats.CacheHitRate = 0
			changed = true
		}
		// Lats is a slice of float64
		for i, v := range copy.InternalStats.Lats {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				copy.InternalStats.Lats[i] = 0
				changed = true
			}
		}
	}
	// Sanitize Lats in Report
	for i, v := range copy.Lats {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			copy.Lats[i] = 0
			changed = true
		}
	}
	return &copy, changed
}

func (ui *WebUI) broadcastStats(stats *Report) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	sanitized, changed := sanitizeReport(stats)

	for client := range ui.clients {
		// Try marshaling to JSON first to catch errors
		data, err := json.Marshal(sanitized)
		if err != nil {
			log.Error().Err(err).Msg("Failed to marshal stats to JSON (possible non-finite float value)")
			if changed {
				log.Warn().Msg("Sanitized non-finite float values in stats before sending to client")
			}
			client.Close()
			delete(ui.clients, client)
			continue
		}
		err = client.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			log.Error().Err(err).Msg("Failed to send stats to client (WebSocket write error)")
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
