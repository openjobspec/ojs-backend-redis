package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

const (
	eventChannelPrefix      = "ojs:events:"
	eventQueueChannelPrefix = "ojs:events:queue:"
	eventAllChannel         = "ojs:events:all"
)

func eventJobChannel(jobID string) string  { return eventChannelPrefix + jobID }
func eventQueueChannel(queue string) string { return eventQueueChannelPrefix + queue }

// PubSubBroker implements core.EventPublisher and core.EventSubscriber
// using Redis Pub/Sub.
type PubSubBroker struct {
	client *goredis.Client
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPubSubBroker creates a new PubSubBroker.
func NewPubSubBroker(client *goredis.Client) *PubSubBroker {
	ctx, cancel := context.WithCancel(context.Background())
	return &PubSubBroker{
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
}

// PublishJobEvent publishes a job event to all relevant Redis Pub/Sub channels.
func (b *PubSubBroker) PublishJobEvent(event *core.JobEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	pipe := b.client.Pipeline()
	msg := string(data)

	// Publish to job-specific channel
	pipe.Publish(b.ctx, eventJobChannel(event.JobID), msg)

	// Publish to queue channel
	if event.Queue != "" {
		pipe.Publish(b.ctx, eventQueueChannel(event.Queue), msg)
	}

	// Publish to global channel
	pipe.Publish(b.ctx, eventAllChannel, msg)

	_, err = pipe.Exec(b.ctx)
	if err != nil {
		slog.Error("failed to publish event", "error", err, "job_id", event.JobID)
		return fmt.Errorf("publish event: %w", err)
	}

	return nil
}

// SubscribeJob subscribes to events for a specific job.
func (b *PubSubBroker) SubscribeJob(jobID string) (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(eventJobChannel(jobID))
}

// SubscribeQueue subscribes to events for all jobs in a queue.
func (b *PubSubBroker) SubscribeQueue(queue string) (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(eventQueueChannel(queue))
}

// SubscribeAll subscribes to all events.
func (b *PubSubBroker) SubscribeAll() (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(eventAllChannel)
}

func (b *PubSubBroker) subscribe(channel string) (<-chan *core.JobEvent, func(), error) {
	sub := b.client.Subscribe(b.ctx, channel)

	// Verify subscription is active
	_, err := sub.Receive(b.ctx)
	if err != nil {
		_ = sub.Close()
		return nil, nil, fmt.Errorf("subscribe to %s: %w", channel, err)
	}

	ch := make(chan *core.JobEvent, 64)

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer close(ch)
		redisCh := sub.Channel()
		for {
			select {
			case <-b.ctx.Done():
				return
			case msg, ok := <-redisCh:
				if !ok {
					return
				}
				var event core.JobEvent
				if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
					slog.Error("failed to unmarshal event", "error", err)
					continue
				}
				select {
				case ch <- &event:
				default:
					// Drop event if channel is full to avoid blocking
					slog.Warn("dropping event, subscriber channel full", "channel", channel)
				}
			}
		}
	}()

	unsubscribe := func() {
		_ = sub.Close()
	}

	return ch, unsubscribe, nil
}

// Close shuts down the broker and waits for all goroutines to finish.
func (b *PubSubBroker) Close() error {
	b.cancel()
	b.wg.Wait()
	return nil
}
