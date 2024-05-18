package cmd

import (
	"github.com/urfave/cli/v2"
)

func App() *cli.App {
	app := cli.NewApp()
	app.Name = "rdb-seeder-stresser"
	app.Usage = "Utility seeding and stress testing DB"

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "log-level",
			EnvVars: []string{"LOG_LEVEL"},
			Value:   "INFO",
			Usage:   "Set log level",
		},
	}

	app.Commands = []*cli.Command{
		Seed,
		Stress,
	}

	return app
}
