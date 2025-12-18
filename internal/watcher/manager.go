package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"gophertask/internal/tasks"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const KeyNetworks = "gophertask:networks"

type WatcherManager struct {
	watchers map[string]*EthWatcher
	broker   tasks.Broker
	redis    *redis.Client
	mu       sync.RWMutex
}

type NetworkConfig struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	URL  string `json:"url"`
}

func NewWatcherManager(b tasks.Broker) *WatcherManager {
	wm := &WatcherManager{
		watchers: make(map[string]*EthWatcher),
		broker:   b,
	}

	if provider, ok := b.(RedisClientProvider); ok {
		wm.redis = provider.InternalClient()
	}

	return wm
}

func (wm *WatcherManager) LoadAndStart(ctx context.Context) error {
	if wm.redis == nil {
		return fmt.Errorf("redis not available for loading networks")
	}

	// 1. Load networks from Redis
	vals, err := wm.redis.HGetAll(ctx, KeyNetworks).Result()
	if err != nil {
		return err
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	count := 0
	for _, val := range vals {
		var config NetworkConfig
		if err := json.Unmarshal([]byte(val), &config); err != nil {
			log.Printf("⚠️ Failed to unmarshal network config: %v", err)
			continue
		}

		// Avoid duplicates if already running
		if _, exists := wm.watchers[config.ID]; exists {
			continue
		}

		w := NewEthWatcher(config.ID, config.Name, config.URL, wm.broker)
		wm.watchers[config.ID] = w
		w.Start(ctx)
		count++
	}

	// If no networks, add default Mainnet
	if len(vals) == 0 {
		defaultID := uuid.New().String()
		defaultConfig := NetworkConfig{
			ID:   defaultID,
			Name: "Ethereum Mainnet",
			URL:  "wss://ethereum-rpc.publicnode.com",
		}

		// Save to Redis
		payload, _ := json.Marshal(defaultConfig)
		wm.redis.HSet(ctx, KeyNetworks, defaultID, payload)

		w := NewEthWatcher(defaultConfig.ID, defaultConfig.Name, defaultConfig.URL, wm.broker)
		wm.watchers[defaultID] = w
		w.Start(ctx)
		count++
	}

	log.Printf("📥 WatcherManager loaded %d networks.", count)
	return nil
}

func (wm *WatcherManager) AddNetwork(ctx context.Context, name, url string) (*NetworkConfig, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Check for duplicates by URL (simple check)
	for _, w := range wm.watchers {
		if w.wssURL == url {
			return nil, fmt.Errorf("network with URL %s already exists", url)
		}
	}

	id := uuid.New().String()
	config := NetworkConfig{ID: id, Name: name, URL: url}

	// 1. Create and Start Watcher
	w := NewEthWatcher(id, name, url, wm.broker)
	w.Start(context.Background()) // Runs in background
	wm.watchers[id] = w

	// 2. Persist to Redis
	if wm.redis != nil {
		payload, _ := json.Marshal(config)
		if err := wm.redis.HSet(ctx, KeyNetworks, id, payload).Err(); err != nil {
			log.Printf("⚠️ Failed to persist network: %v", err)
		}
	}

	return &config, nil
}

func (wm *WatcherManager) RemoveNetwork(ctx context.Context, id string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	w, exists := wm.watchers[id]
	if !exists {
		return fmt.Errorf("network not found")
	}

	// 1. Stop Watcher
	w.Stop()
	delete(wm.watchers, id)

	// 2. Remove from Redis
	if wm.redis != nil {
		wm.redis.HDel(ctx, KeyNetworks, id)
		// Clean up watched addresses key
		wm.redis.Del(ctx, fmt.Sprintf("%s:%s", KeyWatchedAddrs, id))
	}

	return nil
}

func (wm *WatcherManager) ListNetworks() []map[string]interface{} {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	list := make([]map[string]interface{}, 0, len(wm.watchers))
	for id, w := range wm.watchers {
		list = append(list, map[string]interface{}{
			"id":      id,
			"name":    w.Name,
			"url":     w.wssURL,
			"running": w.IsRunning(),
		})
	}
	return list
}

func (wm *WatcherManager) GetWatcher(id string) (*EthWatcher, bool) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	w, ok := wm.watchers[id]
	return w, ok
}

// AddAddressToNetwork adds an address to a specific network's watch list
func (wm *WatcherManager) AddAddressToNetwork(networkID, address string) error {
	w, ok := wm.GetWatcher(networkID)
	if !ok {
		return fmt.Errorf("network not found")
	}
	w.AddAddress(address)
	return nil
}

// RemoveAddressFromNetwork removes an address (Only from Redis for now, fully implemented via sync loop)
func (wm *WatcherManager) RemoveAddressFromNetwork(ctx context.Context, networkID, address string) error {
	_, ok := wm.GetWatcher(networkID)
	if !ok {
		return fmt.Errorf("network not found")
	}

	if wm.redis != nil {
		key := fmt.Sprintf("%s:%s", KeyWatchedAddrs, networkID)
		return wm.redis.SRem(ctx, key, address).Err()
	}
	// For local in-memory update, we rely on the sync loop to eventually catch up or we could expose RemoveAddress on EthWatcher
	return nil
}

// ListAddresses for a network
func (wm *WatcherManager) ListAddresses(ctx context.Context, networkID string) ([]string, error) {
	if wm.redis == nil {
		return []string{}, nil
	}
	key := fmt.Sprintf("%s:%s", KeyWatchedAddrs, networkID)
	return wm.redis.SMembers(ctx, key).Result()
}

// AddTokenToNetwork registers an ERC-20 token to watch
func (wm *WatcherManager) AddTokenToNetwork(networkID, symbol, address string, decimals int) error {
	w, ok := wm.GetWatcher(networkID)
	if !ok {
		return fmt.Errorf("network not found")
	}
	w.AddToken(symbol, address, decimals)
	return nil
}

// ListTokens returns the list of watched tokens for a network
func (wm *WatcherManager) ListTokens(ctx context.Context, networkID string) ([]TokenConfig, error) {
	if wm.redis == nil {
		return []TokenConfig{}, nil
	}
	key := fmt.Sprintf("%s:%s", KeyTokens, networkID)
	vals, err := wm.redis.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	tokens := make([]TokenConfig, 0, len(vals))
	for _, v := range vals {
		var config TokenConfig
		if err := json.Unmarshal([]byte(v), &config); err == nil {
			tokens = append(tokens, config)
		}
	}
	return tokens, nil
}
