package cmd

import (
	"github.com/urfave/cli/v2"
	// https://stackoverflow.com/a/68877864
	// https://go.dev/doc/code
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/seed"
)

func App() *cli.App {
	app := cli.NewApp()
	app.Name = "rdb-seeder-stresser"
	app.Usage = "Utiliyu seeding and stress testing DB"

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "log-level",
			EnvVars: []string{"LOG_LEVEL"},
			Value:   "INFO",
			Usage:   "Set log level",
		},
	}

	app.Commands = []*cli.Command{
		seed.Seed,
		seed.Stress,
	}

	return app
}
