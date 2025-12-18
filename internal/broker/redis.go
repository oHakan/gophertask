package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"gophertask/internal/tasks"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	QueueKeyPrefix  = "gophertask:queue"
	TaskKeyPrefix   = "gophertask:task:"
	RecentTasksList = "gophertask:recent"
)

type RedisBroker struct {
	client *redis.Client
}

func NewRedisBroker(addr string, password string, db int) *RedisBroker {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisBroker{client: rdb}
}

func (b *RedisBroker) Enqueue(task *tasks.Task) error {
	ctx := context.Background()
	task.State = tasks.StatePending
	task.CreatedAt = time.Now()
	task.UpdatedAt = time.Now()

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	pipe := b.client.Pipeline()
	// 1. Save task details
	pipe.Set(ctx, TaskKeyPrefix+task.ID, data, 24*time.Hour)
	// 2. Push to Queue
	pipe.RPush(ctx, QueueKeyPrefix, task.ID)
	// 3. Keep track of recent tasks (cap at 100)
	pipe.LPush(ctx, RecentTasksList, task.ID)
	pipe.LTrim(ctx, RecentTasksList, 0, 99)

	_, err = pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) Dequeue(timeout time.Duration) (*tasks.Task, error) {
	ctx := context.Background()

	result, err := b.client.BLPop(ctx, timeout, QueueKeyPrefix).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	taskID := result[1]
	data, err := b.client.Get(ctx, TaskKeyPrefix+taskID).Result()
	if err != nil {
		// Task ID in queue but data gone? rare race condition or expiry
		return nil, fmt.Errorf("task popped but missing details: %v", err)
	}

	var task tasks.Task
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (b *RedisBroker) InternalClient() *redis.Client {
	return b.client
}

func (b *RedisBroker) ListTasks() ([]*tasks.Task, error) {
	ctx := context.Background()

	// Get recent IDs
	ids, err := b.client.LRange(ctx, RecentTasksList, 0, 50).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []*tasks.Task{}, nil
	}

	// MGet all details
	var keys []string
	for _, id := range ids {
		keys = append(keys, TaskKeyPrefix+id)
	}

	result, err := b.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	var taskList []*tasks.Task
	for _, val := range result {
		// val is interface{}, can be nil if expired
		if val == nil {
			continue
		}
		var t tasks.Task
		if strVal, ok := val.(string); ok {
			if err := json.Unmarshal([]byte(strVal), &t); err == nil {
				taskList = append(taskList, &t)
			}
		}
	}
	return taskList, nil
}

func (b *RedisBroker) MarkProcessing(taskID string) error {
	return b.updateState(taskID, tasks.StateProcessing, "", nil)
}

func (b *RedisBroker) MarkCompleted(taskID string, result interface{}) error {
	return b.updateState(taskID, tasks.StateCompleted, "", result)
}

func (b *RedisBroker) MarkFailed(taskID, err string) error {
	return b.updateState(taskID, tasks.StateFailed, err, nil)
}

func (b *RedisBroker) updateState(taskID string, state tasks.State, errMsg string, result interface{}) error {
	ctx := context.Background()
	key := TaskKeyPrefix + taskID

	data, err := b.client.Get(ctx, key).Result()
	if err != nil {
		return err
	}

	var task tasks.Task
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		return err
	}

	task.State = state
	task.UpdatedAt = time.Now()
	// Preserving original creation time from loaded task
	if errMsg != "" {
		task.Err = errMsg
	}
	if result != nil {
		if rb, err := json.Marshal(result); err == nil {
			task.Result = rb
		}
	}

	newData, err := json.Marshal(task)
	if err != nil {
		return err
	}

	return b.client.Set(ctx, key, newData, 24*time.Hour).Err()
}

func (b *RedisBroker) GetQueueStats() (map[string]int64, error) {
	ctx := context.Background()
	pipe := b.client.Pipeline()

	pending := pipe.LLen(ctx, QueueKeyPrefix)
	// We don't track processing active count in a set, so we can't easily count "processing" state without scanning.
	// For now, we'll just return Pending length.

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]int64{
		"pending":    pending.Val(),
		"processing": 0, // Not tracked
		"failed":     0, // Not tracked separately as aggregate
	}, nil
}
