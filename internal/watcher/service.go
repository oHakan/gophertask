package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"gophertask/internal/tasks"
	"log"
	"math"
	"math/big"
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
	KeyTokens       = "gophertask:eth:tokens"
	KeyLogs         = "gophertask:logs"
)

type TokenConfig struct {
	Symbol   string `json:"symbol"`
	Address  string `json:"address"`
	Decimals int    `json:"decimals"`
}

type RedisClientProvider interface {
	InternalClient() *redis.Client
}

type EthWatcher struct {
	ID           string
	Name         string
	client       *ethclient.Client
	broker       tasks.Broker
	watchedAddrs map[string]bool
	tokens       map[string]TokenConfig // Address -> Config
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
		tokens:       make(map[string]TokenConfig),
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
	w.syncTokensFromRedis(ctx)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.syncFromRedis(ctx)
			w.syncTokensFromRedis(ctx)
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

func (w *EthWatcher) syncTokensFromRedis(ctx context.Context) {
	key := fmt.Sprintf("%s:%s", KeyTokens, w.ID)
	// Tokens are stored as Hash: ContractAddress -> JSON(TokenConfig)
	vals, err := w.redis.HGetAll(ctx, key).Result()
	if err != nil {
		log.Printf("⚠️ [%s] Failed to sync tokens: %v", w.Name, err)
		return
	}

	newTokens := make(map[string]TokenConfig)
	for _, v := range vals {
		var config TokenConfig
		if err := json.Unmarshal([]byte(v), &config); err == nil {
			newTokens[strings.ToLower(config.Address)] = config
		}
	}

	w.mu.Lock()
	w.tokens = newTokens
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

// AddToken adds to local and Redis
func (w *EthWatcher) AddToken(symbol, address string, decimals int) {
	cfg := TokenConfig{Symbol: symbol, Address: address, Decimals: decimals}

	w.mu.Lock()
	w.tokens[strings.ToLower(address)] = cfg
	w.mu.Unlock()

	if w.redis != nil {
		key := fmt.Sprintf("%s:%s", KeyTokens, w.ID)
		payload, _ := json.Marshal(cfg)
		// Store by address
		w.redis.HSet(context.Background(), key, strings.ToLower(address), payload)
	}
	log.Printf("🪙 [%s] Added Token: %s (%s)", w.Name, symbol, address)
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
		if tx.ChainId() == nil || tx.ChainId().Sign() == 0 {
			continue
		}

		signer := types.LatestSignerForChainID(tx.ChainId())
		from, err := types.Sender(signer, tx)
		if err != nil {
			continue
		}

		fromAddr := strings.ToLower(from.Hex())
		toAddr := ""
		if tx.To() != nil {
			toAddr = strings.ToLower(tx.To().Hex())
		}

		// 1. NATIVE ETH CHECK
		matched := false
		direction := ""
		asset := "ETH"
		symbol := "ETH"
		value := tx.Value().String()
		decimals := 18

		// Check Native
		if w.watchedAddrs[fromAddr] {
			matched = true
			direction = "OUT"
		} else if w.watchedAddrs[toAddr] {
			matched = true
			direction = "IN"
		}

		// 2. ERC-20 TOKEN CHECK
		// If it's a call TO a watched Token Contract
		if !matched {
			if tokenCfg, ok := w.tokens[toAddr]; ok {
				// Decode ERC-20 Transfer
				// MethodID: 0xa9059cbb
				data := tx.Data()
				if len(data) >= 68 && strings.HasPrefix(fmt.Sprintf("%x", data[:4]), "a9059cbb") {
					// Param 1: To Address (bytes 16-36)
					recipientBytes := data[4:36]
					// Convert 32-byte padded address to 20-byte address
					recipientAddr := fmt.Sprintf("0x%x", recipientBytes[12:])
					// Check if Recipient is Watched
					if w.watchedAddrs[strings.ToLower(recipientAddr)] {
						matched = true
						direction = "IN"
						asset = "ERC20"
						symbol = tokenCfg.Symbol
						decimals = tokenCfg.Decimals

						// Param 2: Amount (bytes 36-68)
						amountBytes := data[36:68]
						amountInt := new(big.Int).SetBytes(amountBytes)

						// Convert to float for display: amount / 10^decimals
						amountFloat := new(big.Float).SetInt(amountInt)
						div := new(big.Float).SetFloat64(math.Pow(10, float64(decimals)))
						finalVal := new(big.Float).Quo(amountFloat, div)

						value = finalVal.Text('f', 6) // Display up to 6 decimal places
						value = strings.TrimRight(strings.TrimRight(value, "0"), ".")
						log.Printf("Token Value: %s", value)
						// Update involved addresses for log
						toAddr = strings.ToLower(recipientAddr) // The actual recipient (not contract)
					}
				}
			}
		}

		if matched {
			if value == "0" {
				continue
			}

			msg := fmt.Sprintf("🚨 [%s] %s Alert: %s %s -> %s | Val: %s", w.Name, symbol, direction, fromAddr, toAddr, value)
			log.Printf("[%s] %s", w.Name, msg)
			w.logMatch(msg)

			// Enqueue Task for Workers
			payload := map[string]interface{}{
				"network":    w.Name,
				"network_id": w.ID,
				"message":    msg,
				"block":      fmt.Sprintf("%d", block.NumberU64()),
				"from":       fromAddr,
				"to":         toAddr,
				"direction":  direction,
				"asset_type": asset,
				"symbol":     symbol,
				"value":      value,
				"decimals":   decimals,
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
