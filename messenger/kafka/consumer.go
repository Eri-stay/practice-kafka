package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/Eri-stay/practice-kafka/messenger"
	"github.com/IBM/sarama"
)

type saramaConsumer struct {
	client sarama.ConsumerGroup
}

func NewConsumer(brokers []string, groupID string) (messenger.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("start Sarama consumer group: %w", err)
	}

	return &saramaConsumer{client: client}, nil
}

func (c *saramaConsumer) Consume(ctx context.Context, topic string, handler messenger.MessageHandler) error {
	consumer := &consumerGroupHandler{handler: handler}

	for {
		// client.Consume blocks until the session is rebalanced, or context is cancelled
		if err := c.client.Consume(ctx, []string{topic}, consumer); err != nil {
			log.Printf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (c *saramaConsumer) Close() error {
	return c.client.Close()
}

type consumerGroupHandler struct {
	handler messenger.MessageHandler
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Read messages one by one
	for msg := range claim.Messages() {
		// Pass to our business logic
		err := h.handler(session.Context(), msg.Value)

		if err == nil {
			// Mark message as processed ONLY if no error occurred
			session.MarkMessage(msg, "")
		} else {
			log.Printf("Failed to process message: %v", err)
		}
	}
	return nil
}
