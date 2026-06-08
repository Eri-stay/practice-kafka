package ingester

import (
	"fmt"

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

	consumer, err := kafka.NewConsumer(cfg, cfg.KafkaConsumerIngester)
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	requestCh := consumer.RequestsStream(c.Context)

	emails_st := &db.Emails{DB: storage.DB}
	ingester := &ingester{db: emails_st}

	for {
		select {
		case <-c.Done():
			return nil
		case email, ok := <-requestCh:
			if !ok {
				return nil
			}

			if err := ingester.SaveEmailRequest(c.Context, email); err != nil {
				return fmt.Errorf("save email request: %w", err)
			}

		}
	}
}
