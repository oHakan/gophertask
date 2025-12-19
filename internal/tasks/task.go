package tasks

import (
	"encoding/json"
	"time"
)

type WebhookConfig struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
}

type State string

const (
	StatePending    State = "pending"
	StateProcessing State = "processing"
	StateCompleted  State = "completed"
	StateFailed     State = "failed"
)

type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	State     State           `json:"state"`
	Err       string          `json:"error,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Webhooks  []WebhookConfig `json:"webhooks,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type Broker interface {
	Enqueue(task *Task) error
	Dequeue(timeout time.Duration) (*Task, error)
	MarkProcessing(taskID string) error
	MarkCompleted(taskID string, result interface{}) error
	MarkFailed(taskID, err string) error
}
