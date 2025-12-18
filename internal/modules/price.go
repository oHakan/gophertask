package modules

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

type PriceModule struct {
	apiURL string
}

func NewPriceModule() *PriceModule {
	return &PriceModule{
		apiURL: "https://api.binance.com/api/v3/ticker/price",
	}
}

func (m *PriceModule) ID() string {
	return "crypto:get_price"
}

type PricePayload struct {
	Symbol string `json:"symbol"`
}

func (m *PriceModule) Handle(ctx context.Context, payload json.RawMessage) (interface{}, error) {
	var p PricePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %v", err)
	}

	symbol := strings.ToUpper(p.Symbol)
	// Default to USDT if no pair specified
	if !strings.Contains(symbol, "USDT") && !strings.Contains(symbol, "BTC") {
		symbol += "USDT"
	}

	log.Printf("💰 [PriceModule] Fetching price for %s...", symbol)

	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s?symbol=%s", m.apiURL, symbol), nil)
	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status: %s", resp.Status)
	}

	var apiResp struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decoding response failed: %v", err)
	}

	log.Printf("💰 [PriceModule] %s: %s", apiResp.Symbol, apiResp.Price)

	return apiResp, nil
}
