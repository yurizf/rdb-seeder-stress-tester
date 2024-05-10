package cmd

import (
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/seed"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stress"
)

var Seed = &cli.Command{
	Name:        "seed",
	Description: "Collect aws cost metrics.",
	Usage:       "seeder-tester seed",
	Action:      seed.Seed, //function
	Flags: []cli.Flag{
		&cli.PathFlag{
			Name:    "config",
			Value:   "./config.json",
			EnvVars: []string{"CONFIG_JSON"},
			Usage:   "Job Configuration JSON file.",
		},
		&cli.StringFlag{
			Name:     "db-url",
			EnvVars:  []string{"DB_URL"},
			Usage:    "The connection string used to connect to the database.",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "db-type",
			EnvVars:  []string{"DB_TYPE"},
			Usage:    "The DB type: MySQL or PostgreSQL.",
			Required: true,
		},
	},
}

var Stress = &cli.Command{
	Name:        "stress",
	Description: "Collect aws cost metrics.",
	Usage:       "seeder-tester test",
	Action:      stress.Stress, //function
	Flags: []cli.Flag{
		&cli.PathFlag{
			Name:    "input-file",
			Value:   "./input.txt",
			EnvVars: []string{"INPUT_FILE"},
			Usage:   "Job Configuration JSON file.",
		},
		&cli.StringFlag{
			Name:     "db-url",
			EnvVars:  []string{"DB_URL"},
			Usage:    "The connection string used to connect to the database.",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "db-type",
			EnvVars:  []string{"DB_TYPE"},
			Usage:    "The DB type: MySQL or PostgreSQL.",
			Required: true,
		},
	},
}