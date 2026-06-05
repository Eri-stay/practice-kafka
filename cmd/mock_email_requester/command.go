package mock_email_requester

import (
	"log"
	"time"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/messenger/kafka"
	"github.com/urfave/cli/v2"
)

func Command(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:    "request",
		Aliases: []string{"req"},
		Usage:   "Generate and send mock email requests to Kafka",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "count",
				Aliases: []string{"c"},
				Value:   1,
				Usage:   "Number of email requests to generate",
			},
			&cli.BoolFlag{
				Name:    "valid",
				Aliases: []string{"v"},
				Value:   true,
				Usage:   "Generate valid email addresses",
			},
			&cli.IntFlag{
				Name:    "schedule",
				Aliases: []string{"m"},
				Value:   0,
				Usage:   "Schedule email request to be sent after specified minutes",
			},
		},
		Action: func(c *cli.Context) error { return runMockRequester(c, cfg) },
	}
}

func runMockRequester(c *cli.Context, cfg *config.Config) error {
	count := c.Int("count")
	valid := c.Bool("valid")
	schedule := c.Int("schedule")

	// Initialize Kafka producer
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Printf("Failed to run a program: ", err)
	}
	defer producer.Close()

	// Generate and send mock email requests
	for i := 0; i < count; i++ {
		request := generateRandomEmailRequest(valid)

		if schedule > 0 {
			scheduleTime := time.Now().Add(time.Duration(schedule) * time.Minute)
			request.ScheduleTime = &scheduleTime
		}

		if err := producer.EmailRequestEvent(request); err != nil {
			log.Printf("Failed to produce message: %v", err)
		} else {
			log.Printf("Produced email request %d: \"%s\"", i+1, request.Subject)
		}
	}
	return nil
}
