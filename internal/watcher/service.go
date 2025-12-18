package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"gophertask/internal/tasks"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	KeyWatchedAddrs = "gophertask:eth:watched"
	KeyLogs         = "gophertask:logs"
)

type RedisClientProvider interface {
	InternalClient() *redis.Client
}

type EthWatcher struct {
	ID           string
	Name         string
	client       *ethclient.Client
	broker       tasks.Broker
	watchedAddrs map[string]bool
	mu           sync.RWMutex
	wssURL       string
	redis        *redis.Client

	cancelFunc context.CancelFunc
	running    bool
	statusMu   sync.Mutex
}

func NewEthWatcher(id, name, wssURL string, b tasks.Broker) *EthWatcher {
	w := &EthWatcher{
		ID:           id,
		Name:         name,
		wssURL:       wssURL,
		broker:       b,
		watchedAddrs: make(map[string]bool),
	}

	if provider, ok := b.(RedisClientProvider); ok {
		w.redis = provider.InternalClient()
	} else {
		log.Println("⚠️ Broker does not support direct Redis access. Watch list persistence disabled.")
	}

	return w
}

func (w *EthWatcher) Start(ctx context.Context) {
	w.statusMu.Lock()
	if w.running {
		w.statusMu.Unlock()
		log.Printf("⚠️ Watcher '%s' already running.", w.Name)
		return
	}

	// Create cancelable context derived from parent
	ctx, cancel := context.WithCancel(ctx)
	w.cancelFunc = cancel
	w.running = true
	w.statusMu.Unlock()

	log.Printf("🚀 Starting Ethereum Watcher: %s (%s)...", w.Name, w.wssURL)

	// 1. Start Sync Loop
	go w.syncLoop(ctx)

	// 2. Start Listener
	go func() {
		defer func() {
			w.statusMu.Lock()
			w.running = false
			w.statusMu.Unlock()
			log.Printf("🛑 Watcher '%s' Stopped.", w.Name)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := w.connectAndListen(ctx); err != nil {
					// Only log and retry if not cancelled
					if ctx.Err() == nil {
						log.Printf("⚠️ [%s] Watcher disconnected: %v. Retrying in 5s...", w.Name, err)
						time.Sleep(5 * time.Second)
					}
				}
			}
		}
	}()
}

func (w *EthWatcher) Stop() {
	w.statusMu.Lock()
	defer w.statusMu.Unlock()
	if w.running && w.cancelFunc != nil {
		log.Printf("🛑 Stopping Watcher '%s' requested...", w.Name)
		w.cancelFunc()
		w.running = false
	}
}

func (w *EthWatcher) IsRunning() bool {
	w.statusMu.Lock()
	defer w.statusMu.Unlock()
	return w.running
}

func (w *EthWatcher) syncLoop(ctx context.Context) {
	if w.redis == nil {
		return
	}
	// Initial Sync
	w.syncFromRedis(ctx)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.syncFromRedis(ctx)
		}
	}
}

func (w *EthWatcher) syncFromRedis(ctx context.Context) {
	// Namespace the key with Network ID
	key := fmt.Sprintf("%s:%s", KeyWatchedAddrs, w.ID)
	addrs, err := w.redis.SMembers(ctx, key).Result()
	if err != nil {
		log.Printf("⚠️ [%s] Failed to sync watched addresses: %v", w.Name, err)
		return
	}

	newMap := make(map[string]bool)
	for _, a := range addrs {
		newMap[strings.ToLower(a)] = true
	}

	w.mu.Lock()
	w.watchedAddrs = newMap
	w.mu.Unlock()
}

// AddAddress adds to local and Redis
func (w *EthWatcher) AddAddress(addr string) {
	w.mu.Lock()
	w.watchedAddrs[strings.ToLower(addr)] = true
	w.mu.Unlock()

	if w.redis != nil {
		key := fmt.Sprintf("%s:%s", KeyWatchedAddrs, w.ID)
		w.redis.SAdd(context.Background(), key, addr)
	}
	log.Printf("👀 [%s] Started watching address: %s", w.Name, addr)
}

func (w *EthWatcher) connectAndListen(ctx context.Context) error {
	log.Printf("🔌 [%s] Connecting to Ethereum WSS: %s", w.Name, w.wssURL)
	client, err := ethclient.Dial(w.wssURL)
	if err != nil {
		return err
	}
	defer client.Close()
	w.client = client

	headers := make(chan *types.Header)
	sub, err := client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	log.Printf("🟢 [%s] Connected & Listening for Blocks...", w.Name)

	for {
		select {
		case err := <-sub.Err():
			return err
		case <-ctx.Done():
			return nil
		case header := <-headers:
			go w.processBlock(header)
		}
	}
}

func (w *EthWatcher) processBlock(header *types.Header) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("⚠️ [%s] Recovered from panic in processBlock: %v", w.Name, r)
		}
	}()

	// fetching full block
	block, err := w.client.BlockByNumber(context.Background(), header.Number)
	if err != nil {
		log.Printf("[%s] Failed to fetch block %d: %v", w.Name, header.Number, err)
		return
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	for _, tx := range block.Transactions() {
		// Safe Sender Derivation
		// Some transactions might fail signer derivation, we skip them silently to avoid crashes
		if tx.ChainId() == nil || tx.ChainId().Sign() == 0 {
			continue
		}

		signer := types.LatestSignerForChainID(tx.ChainId())
		from, err := types.Sender(signer, tx)
		if err != nil {
			continue
		}

		fromStr := strings.ToLower(from.Hex())
		toStr := ""
		if tx.To() != nil {
			toStr = strings.ToLower(tx.To().Hex())
		}

		matched := false
		direction := ""

		if w.watchedAddrs[fromStr] {
			matched = true
			direction = "OUT"
		} else if w.watchedAddrs[toStr] {
			matched = true
			direction = "IN"
		}

		if matched {
			msg := fmt.Sprintf("🚨 Match %s Block %d: %s -> %s", direction, block.NumberU64(), fromStr, toStr)
			log.Printf("[%s] %s", w.Name, msg)
			w.logMatch(msg)

			// Enqueue Task for Workers
			payload := map[string]string{
				"network":    w.Name,
				"network_id": w.ID,
				"message":    msg,
				"block":      fmt.Sprintf("%d", block.NumberU64()),
				"from":       fromStr,
				"to":         toStr,
				"direction":  direction,
			}
			payloadBytes, _ := json.Marshal(payload)

			task := &tasks.Task{
				ID:        uuid.New().String(),
				Type:      "ethereum:transaction_found",
				Payload:   payloadBytes,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				State:     tasks.StatePending,
			}

			if err := w.broker.Enqueue(task); err != nil {
				log.Printf("[%s] Failed to enqueue notification task: %v", w.Name, err)
			} else {
				log.Printf("[%s] Sent 'ethereum:transaction_found' task to queue", w.Name)
			}
		}
	}
}

func (w *EthWatcher) logMatch(msg string) {
	if w.redis != nil {
		ctx := context.Background()
		pipe := w.redis.Pipeline()
		pipe.LPush(ctx, KeyLogs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg))
		pipe.LTrim(ctx, KeyLogs, 0, 99)
		pipe.Exec(ctx)
	}
}
