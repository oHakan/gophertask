package main

import (
	"context"
	"encoding/json"
	"fmt"
	"gophertask/internal/broker"
	"gophertask/internal/tasks"
	"gophertask/internal/watcher"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

func main() {
	// 1. Initialize Redis Broker
	redisAddr := "localhost:6379"
	b := broker.NewRedisBroker(redisAddr, "", 0)

	// 2. Initialize Watcher Manager
	wm := watcher.NewWatcherManager(b)
	if err := wm.LoadAndStart(context.Background()); err != nil {
		log.Printf("⚠️ Failed to load networks: %v", err)
	}

	mux := http.NewServeMux()

	// Serve Static UI
	mux.Handle("/", http.FileServer(http.Dir("./cmd/server/static")))

	// Task APIs
	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			var t struct {
				Type    string          `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			task := &tasks.Task{
				ID:        uuid.New().String(),
				Type:      t.Type,
				Payload:   t.Payload,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				State:     tasks.StatePending,
			}
			if err := b.Enqueue(task); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.Write([]byte(`{"status":"ok", "id":"` + task.ID + `"}`))
		} else {
			tasksList, _ := b.ListTasks()
			json.NewEncoder(w).Encode(tasksList)
		}
	})

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, _ := b.GetQueueStats()

		// Get Active Workers count
		ctx := context.Background()
		client := b.InternalClient()

		// Clean old heartbeats (older than 10s)
		minScore := fmt.Sprintf("%d", time.Now().Add(-10*time.Second).Unix())
		client.ZRemRangeByScore(ctx, "gophertask:workers:heartbeat", "-inf", minScore)

		val, _ := client.ZCard(ctx, "gophertask:workers:heartbeat").Result()

		resp := map[string]interface{}{
			"pending":        stats["pending"],
			"processing":     stats["processing"],
			"failed":         stats["failed"],
			"active_workers": val,
		}
		json.NewEncoder(w).Encode(resp)
	})

	// --- Network Management APIs ---

	mux.HandleFunc("/api/networks", func(w http.ResponseWriter, r *http.Request) {
		list := wm.ListNetworks()
		json.NewEncoder(w).Encode(list)
	})

	mux.HandleFunc("/api/networks/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		var req struct {
			Name string `json:"name"`
			URL  string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		cfg, err := wm.AddNetwork(r.Context(), req.Name, req.URL)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(cfg)
	})

	mux.HandleFunc("/api/networks/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		var req struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		if err := wm.RemoveNetwork(r.Context(), req.ID); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte(`{"status":"ok"}`))
	})

	// --- Address Management APIs ---

	mux.HandleFunc("/api/watcher/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		var req struct {
			NetworkID string `json:"network_id"`
			Address   string `json:"address"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		if req.NetworkID == "" {
			http.Error(w, "network_id required", 400)
			return
		}

		if err := wm.AddAddressToNetwork(req.NetworkID, req.Address); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/api/watcher/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		var req struct {
			NetworkID string `json:"network_id"`
			Address   string `json:"address"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		if req.NetworkID == "" {
			http.Error(w, "network_id required", 400)
			return
		}

		if err := wm.RemoveAddressFromNetwork(r.Context(), req.NetworkID, req.Address); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/api/watcher/list", func(w http.ResponseWriter, r *http.Request) {
		networkID := r.URL.Query().Get("network_id")
		if networkID == "" {
			http.Error(w, "network_id required", 400)
			return
		}

		list, err := wm.ListAddresses(r.Context(), networkID)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(list)
	})

	mux.HandleFunc("/api/watcher/logs", func(w http.ResponseWriter, r *http.Request) {
		client := b.InternalClient()
		logs, _ := client.LRange(context.Background(), "gophertask:logs", 0, 50).Result()
		json.NewEncoder(w).Encode(logs)
	})

	log.Println("Server running on http://localhost:8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
