package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/Eri-stay/practice-kafka/messenger"
	"github.com/IBM/sarama"
)

var _ messenger.Consumer = (*saramaConsumer)(nil)

type saramaConsumer struct {
	cfg    *config.Config
	client sarama.ConsumerGroup
}

func NewConsumer(cfg *config.Config, groupID string) (*saramaConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("start Sarama consumer group: %w", err)
	}

	return &saramaConsumer{
		cfg:    cfg,
		client: client,
	}, nil
}

func (c *saramaConsumer) consumeRaw(ctx context.Context, topic string) <-chan []byte {
	out := make(chan []byte)

	go func() {
		defer close(out)

		handler := consumerGroupHandler{ch: out}

		for {
			if err := c.client.Consume(ctx, []string{topic}, &handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return out
}

func (c *saramaConsumer) Close() error {
	return c.client.Close()
}

type consumerGroupHandler struct {
	handler messenger.MessageHandler
	ch      chan []byte
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil

		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			h.ch <- msg.Value

			session.MarkMessage(msg, "")
		}
	}
}

// Entities-based methods that return read-only channel

func (c *saramaConsumer) RequestsStream(ctx context.Context) <-chan entities.Request {
	out := make(chan entities.Request)

	rawChan := c.consumeRaw(ctx, c.cfg.TopicEmailRequests)

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case bytes, ok := <-rawChan:
				if !ok {
					return
				}

				var payload emailRequest
				if err := json.Unmarshal(bytes, &payload); err != nil {
					log.Printf("Failed to unmarshal request: %v", err)
				}

				req := entities.Request{
					Recipient:    payload.Recipient,
					Subject:      payload.Subject,
					Body:         payload.Body,
					ScheduleTime: payload.ScheduleTime,
				}

				out <- req
			}
		}
	}()
	return out
}

func (c *saramaConsumer) EmailsStream(ctx context.Context) <-chan entities.Email {
	out := make(chan entities.Email)

	rawChan := c.consumeRaw(ctx, c.cfg.TopicEmailExecute)

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case bytes, ok := <-rawChan:
				if !ok {
					return
				}

				var payload email
				if err := json.Unmarshal(bytes, &payload); err != nil {
					log.Printf("Failed to unmarshal email: %v", err)
				}

				email := entities.Email{
					Id:        payload.Id,
					Recipient: payload.Recipient,
					Subject:   payload.Subject,
					Body:      payload.Body,
				}

				out <- email
			}
		}
	}()
	return out
}

func (c *saramaConsumer) ResultsStream(ctx context.Context) <-chan entities.Result {
	out := make(chan entities.Result)

	rawChan := c.consumeRaw(ctx, c.cfg.TopicEmailResults)

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case bytes, ok := <-rawChan:
				if !ok {
					return
				}

				var payload result
				if err := json.Unmarshal(bytes, &payload); err != nil {
					log.Printf("Failed to unmarshal result: %v", err)
				}

				res := entities.Result{
					EmailId:    payload.EmailId,
					Status:     string(payload.Status),
					ErrorMsg:   payload.ErrorMsg,
					Created_at: payload.Created_at,
				}

				out <- res
			}
		}
	}()
	return out
}
