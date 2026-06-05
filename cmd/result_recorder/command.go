package result_recorder

import (
	"github.com/Eri-stay/practice-kafka/config"
	"github.com/urfave/cli/v2"
)

func Command(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:    "recorder",
		Aliases: []string{"rec"},
		Usage:   "Receive messages from kafka (sender) and write result to db",
	}
}
