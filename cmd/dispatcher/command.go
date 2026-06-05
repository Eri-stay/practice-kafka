package dispatcher

import (
	"fmt"
	"strconv"

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

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("create Kafka producer: %w", err)
	}
	defer producer.Close()

	emails_ch := make(chan entities.Email)
	timeInterval, err := strconv.Atoi(cfg.DispatcherInterval)
	if err != nil {
		return fmt.Errorf("convert time interval: %v", err)
	}

	dispatcher := dispatcher{
		emailsDB:     db.Emails{DB: storage.DB},
		channel:      emails_ch,
		context:      ctx.Context,
		timeInterval: timeInterval,
	}

	// retrieve all emails for sending
	go dispatcher.RetrieveEmailsToSend()

	for email := range dispatcher.channel {
		if err := producer.ExecutionRequestEvent(email); err != nil {
		}
	}
	return nil
}
