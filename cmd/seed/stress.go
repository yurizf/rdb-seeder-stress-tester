package seed

import (
	"bufio"
	"fmt"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/syncmap"
	"log"
	"os"
	"strings"
)

const THREADS string = "threads = "
const POISON_PILL = "POISON_PILL"

type run interface {
	runSQLs(cc *cli.Context, threadID string, statsMap *syncmap.Map, sql chan string)
}

// to allow DB mocking
func stress(cc *cli.Context) error {
	doStress(cc, newDB())
	return nil
}

func doStress(cc *cli.Context, db run) error {
	path := cc.Path("input-file")
	if len(path) == 0 {
		return fmt.Errorf("no input file path given")
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

	sqls := make(chan string, 1)
	var statsMap syncmap.Map

	threads := 0
	poisonPills := 0
	for scanner.Scan() {
		// get a line
		s := scanner.Text()
		if strings.HasPrefix(s, THREADS) {
			fmt.Sscanf(s, THREADS+"%d", &threads)
			if poisonPills > 0 {
				for i := 0; i < poisonPills; i++ {
					sqls <- POISON_PILL
				}
				poisonPills = 0
			}
			continue
		}

		// we get here on a non "threads = " line
		if threads > 0 {
			for i := 0; i < threads; i++ {
				go db.runSQLs(cc, fmt.Sprintf("thread-%d", i), &statsMap, sqls)
			}
			poisonPills = threads
			threads = 0
		}

		sqls <- s
	}

	return nil
}
