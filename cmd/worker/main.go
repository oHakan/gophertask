package main

import (
	"context"
	"encoding/json"
	"gophertask/internal/broker"
	"gophertask/internal/modules"
	"gophertask/internal/worker"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {
	brk := broker.NewRedisBroker("localhost:6379", "", 0)

	// Start Worker with 10 concurrent threads
	proc := worker.NewProcessor(brk, 10)

	// --- Register Modules ---

	// 1. Email Module
	emailMod := &modules.EmailModule{}
	proc.Register(emailMod.ID(), emailMod.Handle)

	// 2. Ethereum Module
	ethMod := modules.NewEthereumModule()
	proc.Register(ethMod.ID(), ethMod.Handle)

	// 3. Ethereum Notification Module
	// Instead of watching, we now LISTENING for tasks created by the Watcher (Server)
	proc.Register("ethereum:transaction_found", func(ctx context.Context, payload json.RawMessage) (interface{}, error) {
		log.Printf("🔔 Worker received Alert: %s", string(payload))
		// Return payload as result to show in UI
		var details map[string]interface{}
		json.Unmarshal(payload, &details)
		return details, nil
	})

	// 4. Crypto Price Module
	priceMod := modules.NewPriceModule()
	proc.Register(priceMod.ID(), priceMod.Handle)

	log.Printf("🚀 Worker started with modules: %s, %s, %s", emailMod.ID(), ethMod.ID(), priceMod.ID())

	proc.Start()

	// --- Worker Heartbeat ---
	go func() {
		workerID := uuid.New().String()
		log.Printf("💓 Heartbeat started for worker %s", workerID)

		client := brk.InternalClient()
		ctx := context.Background()

		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// ZADD gophertask:workers:heartbeat <timestamp> <workerID>
				// Expires after 10 seconds effectively if we query nicely, but we should clear old ones manually or use SET with EX.
				// However, ZADD is better for counting range. We'll rely on Server to count.
				err := client.ZAdd(ctx, "gophertask:workers:heartbeat", redis.Z{
					Score:  float64(time.Now().Unix()),
					Member: workerID,
				}).Err()
				if err != nil {
					log.Printf("Heartbeat failed: %v", err)
				}
			}
		}
	}()

	// Wait for SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	proc.Shutdown()
}
