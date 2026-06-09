package result_recorder

import (
	"fmt"
	"log"
	"strconv"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/messenger/kafka"
	"github.com/Eri-stay/practice-kafka/pkg/metrics"
	"github.com/urfave/cli/v2"
)

func Command(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:    "recorder",
		Aliases: []string{"rec"},
		Usage:   "Receive messages from kafka (sender) and write result to db",
		Action:  func(c *cli.Context) error { return runRecorder(c, cfg) },
	}
}

func runRecorder(c *cli.Context, cfg *config.Config) error {
	storage, err := db.NewStorage(cfg.DbURL)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	defer storage.Close()

	maxRetries, err := strconv.Atoi(cfg.MaxRetries)
	if err != nil {
		return fmt.Errorf("convert max retries to int: %w", err)
	}

	consumer, err := kafka.NewConsumer(cfg, cfg.KafkaConsumerWriter)
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer consumer.Close()

	resultsCh := consumer.ResultsStream(c.Context)
	executionsDB := &db.Executions{DB: storage.DB}
	emailsDB := &db.Emails{DB: storage.DB}

	go metrics.StartMetricsServer(cfg.MetricsPort)

	recorder := recorder{
		dbEmails:     emailsDB,
		dbExecutions: executionsDB,
		maxRetries:   maxRetries,
	}

	for {
		select {
		case <-c.Done():
			return nil
		case res, ok := <-resultsCh:
			if !ok {
				return nil
			}

			err := recorder.SaveExecResult(c.Context, res)
			if err != nil {
				log.Printf("Failed to record result for email ID %d: %v", res.EmailId, err)
			}
		}
	}
}
