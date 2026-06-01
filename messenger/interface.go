package messenger

import "context"

type Producer interface {
	Produce(topic string, message []byte) error
	Close() error
}

type MessageHandler func(ctx context.Context, message []byte) error

type Consumer interface {
	Consume(ctx context.Context, topic string, handler MessageHandler) error
	Close() error
}
