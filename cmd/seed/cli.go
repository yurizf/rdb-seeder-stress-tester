package seed

import (
	"github.com/urfave/cli/v2"
)

var Command = &cli.Command{
	Name:        "seed",
	Description: "Collect aws cost metrics.",
	Usage:       "seeder-tester seed",
	Action:      seed, //function
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
