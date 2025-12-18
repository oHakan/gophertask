package modules

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"
)

type EmailModule struct{}

func (m *EmailModule) ID() string {
	return "email:send"
}

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func (m *EmailModule) Handle(ctx context.Context, payload json.RawMessage) (interface{}, error) {
	var p EmailPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %v", err)
	}

	log.Printf("📧 [EmailModule] Preparing to send to %s...", p.To)

	// Simulation: Network delay
	select {
	case <-time.After(1500 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Simulation: Random Failure
	if rand.Float32() < 0.1 {
		return nil, fmt.Errorf("upstream SMTP timeout")
	}

	log.Printf("📧 [EmailModule] Sent: '%s'", p.Subject)
	return map[string]string{"status": "sent", "message_id": fmt.Sprintf("msg_%d", time.Now().Unix())}, nil
}
