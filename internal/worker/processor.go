package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"gophertask/internal/tasks"
	"log"
	"net/http"
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

	// Trigger Webhooks
	if len(t.Webhooks) > 0 {
		go p.triggerWebhooks(t, res, err)
	}
}

func (p *Processor) triggerWebhooks(t *tasks.Task, result interface{}, taskErr error) {
	payload := map[string]interface{}{
		"task_id":   t.ID,
		"task_type": t.Type,
		"status":    "completed",
		"result":    result,
		"timestamp": time.Now(),
	}

	if taskErr != nil {
		payload["status"] = "failed"
		payload["error"] = taskErr.Error()
	}

	body, _ := json.Marshal(payload)

	for _, wh := range t.Webhooks {
		go func(w tasks.WebhookConfig) {
			log.Printf("🔗 Triggering webhook: %s %s", w.Method, w.URL)

			req, err := http.NewRequest(w.Method, w.URL, bytes.NewBuffer(body))
			if err != nil {
				log.Printf("⚠️ Failed to create webhook request: %v", err)
				return
			}

			for k, v := range w.Headers {
				req.Header.Set(k, v)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("User-Agent", "GopherTask-Worker/1.0")

			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("❌ Webhook failed (%s): %v", w.URL, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode >= 400 {
				log.Printf("⚠️ Webhook returned error status (%s): %s", w.URL, resp.Status)
			} else {
				log.Printf("✅ Webhook sent successfully (%s)", w.URL)
			}
		}(wh)
	}
}
