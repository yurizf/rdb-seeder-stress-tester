package main

import (
	"github.com/yurizf/rdb-seeder-stress-tester/cmd"
	"log"
	"os"
)

func main() {
	if err := cmd.App().Run(os.Args); err != nil {
		log.Printf("[ERROR] command exited with error %s", err)
	}
}
