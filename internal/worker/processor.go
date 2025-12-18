package worker

import (
	"context"
	"encoding/json"
	"gophertask/internal/tasks"
	"log"
	"sync"
	"time"
)

// Handler is a function that processes a specific task type.
type Handler func(ctx context.Context, payload json.RawMessage) (interface{}, error)

type Processor struct {
	broker      tasks.Broker
	handlers    map[string]Handler
	concurrency int
	quit        chan bool
	wg          sync.WaitGroup
}

func NewProcessor(broker tasks.Broker, concurrency int) *Processor {
	return &Processor{
		broker:      broker,
		handlers:    make(map[string]Handler),
		concurrency: concurrency,
		quit:        make(chan bool),
	}
}

func (p *Processor) Register(taskType string, handler Handler) {
	p.handlers[taskType] = handler
}

func (p *Processor) Start() {
	log.Printf("Starting worker with concurrency %d...", p.concurrency)

	// Create a semaphore to limit concurrency
	sem := make(chan struct{}, p.concurrency)

	go func() {
		for {
			select {
			case <-p.quit:
				return
			default:
				// Acquire semaphore slot
				sem <- struct{}{}

				// Fetch task with timeout
				task, err := p.broker.Dequeue(2 * time.Second)
				if err != nil {
					log.Printf("Error pulling task: %v", err)
					<-sem                       // Release slot
					time.Sleep(1 * time.Second) // Backoff
					continue
				}
				if task == nil {
					<-sem // Release slot if timeout (no task)
					continue
				}

				p.wg.Add(1)
				go func(t *tasks.Task) {
					defer p.wg.Done()
					defer func() { <-sem }() // Release slot

					p.processTask(t)
				}(task)
			}
		}
	}()
}

func (p *Processor) Shutdown() {
	log.Println("Shutting down worker...")
	close(p.quit)
	p.wg.Wait()
	log.Println("Worker stopped cleanly.")
}

func (p *Processor) processTask(t *tasks.Task) {
	handler, exists := p.handlers[t.Type]
	if !exists {
		log.Printf("No handler registered for task type: %s", t.Type)
		p.broker.MarkFailed(t.ID, "No handler registered")
		return
	}

	// Update state to Processing
	if err := p.broker.MarkProcessing(t.ID); err != nil {
		log.Printf("Failed to mark task processing: %v", err)
	}

	log.Printf("[Worker] Processing Task %s (%s)", t.ID, t.Type)

	// Execute the handler
	start := time.Now()
	res, err := handler(context.Background(), t.Payload)
	duration := time.Since(start)

	if err != nil {
		log.Printf("❌ Task %s failed after %v: %v", t.ID, duration, err)
		p.broker.MarkFailed(t.ID, err.Error())
	} else {
		log.Printf("✅ Task %s completed in %v", t.ID, duration)
		p.broker.MarkCompleted(t.ID, res)
	}
}
