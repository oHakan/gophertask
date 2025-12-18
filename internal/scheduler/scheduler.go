package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"gophertask/internal/tasks"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const KeySchedules = "gophertask:schedules"

type ScheduledJob struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Interval  string          `json:"interval"` // e.g. "1m", "5m"
	NextRun   int64           `json:"next_run"` // Unix Timestamp
	LastRun   int64           `json:"last_run"`
	CreatedAt time.Time       `json:"created_at"`
}

type RedisClientProvider interface {
	InternalClient() *redis.Client
}

type Scheduler struct {
	broker tasks.Broker
	redis  *redis.Client
	mu     sync.RWMutex
	jobs   map[string]*ScheduledJob
}

func NewScheduler(b tasks.Broker) *Scheduler {
	s := &Scheduler{
		broker: b,
		jobs:   make(map[string]*ScheduledJob),
	}

	if provider, ok := b.(RedisClientProvider); ok {
		s.redis = provider.InternalClient()
	}
	return s
}

func (s *Scheduler) Start(ctx context.Context) {
	if s.redis == nil {
		log.Println("⚠️ Scheduler disabled: Redis not available in Broker")
		return
	}

	log.Println("⏰ Scheduler started...")
	s.syncFromRedis(ctx)

	ticker := time.NewTicker(2 * time.Second) // Check every 2s
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	now := time.Now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, job := range s.jobs {
		if now >= job.NextRun {
			// Trigger Job
			// Enqueue Task
			task := &tasks.Task{
				ID:        uuid.New().String(),
				Type:      job.Type,
				Payload:   job.Payload,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				State:     tasks.StatePending,
			}

			if err := s.broker.Enqueue(task); err != nil {
				log.Printf("⚠️ Scheduler failed to enqueue job %s: %v", job.ID, err)
				continue
			}

			// Calculate Next Run
			duration, err := time.ParseDuration(job.Interval)
			if err != nil {
				log.Printf("⚠️ Invalid interval for job %s: %v", job.ID, err)
				// Disable or default? Let's default to 1m to avoid tight loop spam
				duration = time.Minute
			}

			job.LastRun = now
			job.NextRun = now + int64(duration.Seconds())

			// Save Update
			s.saveJob(ctx, job)
			log.Printf("⏰ Triggered Recurring Job: %s (Type: %s)", job.ID, job.Type)
		}
	}
}

func (s *Scheduler) AddJob(ctx context.Context, jobType string, payload json.RawMessage, interval string) (*ScheduledJob, error) {
	// Validate interval
	if _, err := time.ParseDuration(interval); err != nil {
		return nil, fmt.Errorf("invalid interval format (e.g. 1m, 1h): %v", err)
	}

	job := &ScheduledJob{
		ID:        uuid.New().String(),
		Type:      jobType,
		Payload:   payload,
		Interval:  interval,
		CreatedAt: time.Now(),
		NextRun:   time.Now().Add(1 * time.Second).Unix(), // Run almost immediately
	}

	s.mu.Lock()
	s.jobs[job.ID] = job
	s.saveJob(ctx, job)
	s.mu.Unlock()

	return job, nil
}

func (s *Scheduler) RemoveJob(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[id]; !ok {
		return fmt.Errorf("job not found")
	}

	delete(s.jobs, id)
	return s.redis.HDel(ctx, KeySchedules, id).Err()
}

func (s *Scheduler) ListJobs() []*ScheduledJob {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]*ScheduledJob, 0, len(s.jobs))
	for _, job := range s.jobs {
		list = append(list, job)
	}
	return list
}

func (s *Scheduler) syncFromRedis(ctx context.Context) {
	vals, err := s.redis.HGetAll(ctx, KeySchedules).Result()
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, val := range vals {
		var job ScheduledJob
		if err := json.Unmarshal([]byte(val), &job); err == nil {
			s.jobs[job.ID] = &job
		}
	}
}

func (s *Scheduler) saveJob(ctx context.Context, job *ScheduledJob) {
	data, _ := json.Marshal(job)
	s.redis.HSet(ctx, KeySchedules, job.ID, data)
}
