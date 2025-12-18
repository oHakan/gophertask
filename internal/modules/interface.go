package modules

import (
	"context"
	"encoding/json"
)

// Module defines the contract for pluggable job processors.
type Module interface {
	// ID returns the unique identifier for this module (e.g. "email:send")
	ID() string

	// Handle executes the business logic for the job.
	Handle(ctx context.Context, payload json.RawMessage) error
}
