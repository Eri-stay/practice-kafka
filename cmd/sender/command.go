package sender

import (
	"fmt"
	"os/signal"
	"syscall"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/messenger/kafka"
	"github.com/urfave/cli/v2"
)

func Command(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:    "sender",
		Aliases: []string{"send"},
		Usage:   "recieve messages from kafka (dispatcher and send to recipients)",
		Action:  func(c *cli.Context) error { return runSender(c, cfg) },
	}
}

func runSender(c *cli.Context, cfg *config.Config) error {
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup)
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	handler := Handler{Cfg: cfg}
	ctx, cancel := signal.NotifyContext(c.Context, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		if err := consumer.Consume(ctx, cfg.TopicEmailExecute, handler.SendEmail); err != nil {
			fmt.Printf("consume error: %v\n", err)
		}
	}()

	<-ctx.Done() // Wait for termination signal

	<-doneCh

	return nil
}
