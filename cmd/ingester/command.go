package ingester

import (
	"fmt"
	"os/signal"
	"syscall"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/messenger/kafka"
	"github.com/urfave/cli/v2"
)

func Command(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:    "ingester",
		Aliases: []string{"ing"},
		Usage:   "Read outer messages from Kafka and write to DB",
		Action:  func(c *cli.Context) error { return runIngester(c, cfg) },
	}
}

func runIngester(c *cli.Context, cfg *config.Config) error {
	storage, err := db.NewStorage(cfg.DbURL)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	defer storage.Close()

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup)
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	emails_st := &db.Emails{DB: storage.DB}
	handler := &Handler{db: emails_st}
	ctx, cancel := signal.NotifyContext(c.Context, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Use a channel to synchronize consumer shutdown
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		if err := consumer.Consume(ctx, cfg.TopicEmailRequests, handler.SaveEmailRequest); err != nil {
			fmt.Printf("consume error: %v\n", err)
		}
	}()

	<-ctx.Done() // Wait for termination signal

	<-doneCh

	return nil
}
