package sender

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/Eri-stay/practice-kafka/messenger/kafka"
	"github.com/Eri-stay/practice-kafka/pkg/metrics"
	"github.com/urfave/cli/v2"
	"github.com/wneessen/go-mail"
)

func Command(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:    "sender",
		Aliases: []string{"send"},
		Usage:   "Receive messages from kafka (dispatcher and send to recipients)",
		Action:  func(c *cli.Context) error { return runSender(c, cfg) },
	}
}

func runSender(c *cli.Context, cfg *config.Config) error {
	consumer, err := kafka.NewConsumer(cfg, cfg.KafkaConsumerSender)
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	emailsCh := consumer.EmailsStream(c.Context)

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("create Kafka producer: %w", err)
	}

	port, err := strconv.Atoi(cfg.SMTPPort)
	if err != nil {
		return fmt.Errorf("convert port to int: %w", err)
	}
	client, err := mail.NewClient(cfg.SMTPHost,
		mail.WithPort(port),
		mail.WithSMTPAuth(mail.SMTPAuthPlain),
		mail.WithUsername(cfg.SMTPUser),
		mail.WithPassword(cfg.SMTPPass))
	if err != nil {
		return fmt.Errorf("create smtp client: %w", err)
	}

	go metrics.StartMetricsServer(cfg.MetricsPort)

	sender := sender{
		cfg:      cfg,
		producer: producer,
		client:   client,
	}

	for {
		select {
		case <-c.Done():
			return nil
		case email, ok := <-emailsCh:
			if !ok {
				return nil
			}

			err := sender.SendEmail(c.Context, email)
			switch {
			case err == nil:
				sender.ProduceResult(email.Id, entities.StatusSuccess, "")
			case errors.Is(err, ErrInvalidParameter):
				sender.ProduceResult(email.Id, entities.StatusPermFail, err.Error())
			case errors.Is(err, ErrDeliveryFailed):
				sender.ProduceResult(email.Id, entities.StatusTempFail, err.Error())
			}
		}
	}
}
