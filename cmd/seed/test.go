package seed

import (
	"bufio"
	"fmt"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

func test(cc *cli.Context) error {
	path := cc.Path("input-file")
	if len(path) == 0 {
		return fmt.Errorf("No input file path given")
	}

	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	const maxCapacity int = 4194304
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		s := scanner.Text()

	}


