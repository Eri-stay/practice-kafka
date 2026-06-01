package dispatcher

import (
	"fmt"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/Eri-stay/practice-kafka/messenger/kafka"
	"github.com/urfave/cli/v2"
)

const (
	EmailLimitPending = 2
	EmailLimitFailed  = 100000
)

func Command(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:    "dispatcher",
		Aliases: []string{"dsp"},
		Usage:   "Read pending email requests from DB and dispatch to Kafka",
		Action:  func(c *cli.Context) error { return runDispatcher(c, cfg) },
	}
}

func runDispatcher(ctx *cli.Context, cfg *config.Config) error {
	storage, err := db.NewStorage(cfg.DbURL)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	defer storage.Close()

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("create Kafka producer: %w", err)
	}
	defer producer.Close()

	emails_ch := make(chan entities.Email)

	dispatcher := Dispatcher{
		EmailsDB: db.Emails{DB: storage.DB},
		Produser: producer,
		Channel:  emails_ch,
		Context:  ctx.Context,
		Config:   cfg,
	}

	// retrieve all emails for sending
	go dispatcher.RetrieveEmailsToSend()

	for email := range dispatcher.Channel {
		if err := dispatcher.DispatchToSender(email); err != nil {
			return fmt.Errorf("dispatch email: %v", err)
		}
	}
	return nil
}
