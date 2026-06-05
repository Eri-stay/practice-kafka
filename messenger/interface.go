package messenger

import (
	"context"

	"github.com/Eri-stay/practice-kafka/entities"
)

type Producer interface {
	// Produce(topic string, message []byte) error
	EmailRequestEvent(event entities.Request) error
	ExecutionRequestEvent(event entities.Email) error
	ExecutionResultEvent(event entities.Result) error
	// Close() error
}

type MessageHandler func(ctx context.Context, message []byte) error

type Consumer interface {
	// Consume(ctx context.Context, topic string, handler MessageHandler) error
	// Close() error

	RequestsStream(ctx context.Context) <-chan entities.Request
	EmailsStream(ctx context.Context) <-chan entities.Email
	ResultsStream(ctx context.Context) <-chan entities.Result
}
