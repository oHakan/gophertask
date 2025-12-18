package modules

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type EthereumModule struct {
	// Public RPC Endpoint (Cloudflare)
	rpcURL string
}

func NewEthereumModule() *EthereumModule {
	return &EthereumModule{
		rpcURL: "https://ethereum-rpc.publicnode.com",
	}
}

func (m *EthereumModule) ID() string {
	return "ethereum:check_balance"
}

type EthPayload struct {
	Address string `json:"address"`
}

func (m *EthereumModule) Handle(ctx context.Context, payload json.RawMessage) (interface{}, error) {
	var p EthPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %v", err)
	}

	log.Printf("💎 [EthModule] Checking balance for %s...", p.Address)

	// JSON-RPC Request
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getBalance",
		"params":  []interface{}{p.Address, "latest"},
		"id":      1,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, _ := http.NewRequestWithContext(ctx, "POST", m.rpcURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("RPC request failed: %v", err)
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decoding RPC response failed: %v", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC Error: %s", rpcResp.Error.Message)
	}

	log.Printf("💎 [EthModule] Balance: %s (Wei)", rpcResp.Result)
	return map[string]string{"balance_wei": rpcResp.Result, "address": p.Address}, nil
}
