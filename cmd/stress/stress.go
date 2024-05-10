package stress

import (
	"bufio"
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/db"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"log"
	"os"
	"strings"
	"sync"
)

const THREADS string = "threads = "

type run interface {
	RunSQLs(cc *cli.Context, threadID string, statsMap stats.StatsMAP, wg *sync.WaitGroup, sql chan string)
}

// to allow DB mocking
func Stress(cc *cli.Context) error {

	doStress(cc, db.New(), stats.New())
	return nil
}

func doStress(cc *cli.Context, r run, statsMap stats.StatsMAP) error {
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

	threadsCnt := 0
	poisonPillsCnt := 0
	var wg sync.WaitGroup
	batch := -1
	for scanner.Scan() {
		// get a line
		s := scanner.Text()
		if strings.HasPrefix(s, THREADS) {
			batch++
			fmt.Sscanf(s, THREADS+"%d", &threadsCnt)
			if poisonPillsCnt > 0 {
				for i := 0; i < poisonPillsCnt; i++ {
					sqls <- db.POISON_PILL
				}
				poisonPillsCnt = 0
				// wait till all threadsCnt swallow poison pills, one pill per thread
				wg.Wait()
			}
			continue
		}

		// we get here on a non "threadsCnt = " line
		if threadsCnt > 0 {
			for i := 0; i < threadsCnt; i++ {
				wg.Add(1)
				go r.RunSQLs(cc,
					fmt.Sprintf("batch-%d-thread-%d", batch, i),
					statsMap,
					&wg,
					sqls)
				poisonPillsCnt++
			}
			batch++
			threadsCnt = 0
		}

		sqls <- s
	}

	// input file has been completely read
	for i := 0; i < poisonPillsCnt; i++ {
		sqls <- db.POISON_PILL
	}
	wg.Wait()

	statsMap.Print()

	return nil
}
