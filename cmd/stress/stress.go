package stress

import (
	"bufio"
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/db"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
)

const THREADS string = "THREADS = "
const ID string = "ID = "

type run interface {
	RunSQLs(cc *cli.Context, threadID string, statsMap stats.StatsMAP, wg *sync.WaitGroup, sql chan db.Task)
}

// to allow DB mocking
func Stress(cc *cli.Context) error {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logLevel := cc.String("log-level")
	switch logLevel {
	case "debug":
		opts = &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}
	case "error":
		opts = &slog.HandlerOptions{
			Level: slog.LevelError,
		}
	case "warn":
		opts = &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, opts)))

	doStress(cc,
		func(dbType string, dbUrl string) run {
			return db.New(dbType, dbUrl)
		},
		stats.New())
	return nil
}

func doStress(cc *cli.Context,
	new func(dbtype string, dburl string) run,
	statsMap stats.StatsMAP) error {
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

	sqls := make(chan db.Task, 1)

	threadsCnt := 0
	var wg sync.WaitGroup
	count := 0
	id := ""
	for scanner.Scan() {
		// get a line
		s := scanner.Text()

		if strings.HasPrefix(s, ID) {
			fmt.Sscanf(s, ID+"%s", &id)
			scanner.Scan()
			fmt.Sscanf(s, THREADS+"%d", &threadsCnt)
			// batch == 0 means beginning of the file, nothing is running yet
			if count > 0 {
				for i := 0; i < threadsCnt; i++ {
					sqls <- db.POISON_TASK
				}
				// wait till all threadsCnt swallow poison pills, one pill per thread
				wg.Wait()
			}

			if threadsCnt > 0 {
				for i := 0; i < threadsCnt; i++ {
					r := new(cc.String("db-type"), cc.String("db-url"))
					wg.Add(1)
					go r.RunSQLs(cc,
						fmt.Sprintf("thread-%d", i),
						statsMap,
						&wg,
						sqls)
				}
			}
			continue
		}

		// we get here on a non "threadsCnt = " line
		// feed sql to the channel
		sqls <- db.Task{
			id,
			s,
		}
		count++
		if count%100 == 0 {
			slog.Info("sqls fed", "ID", id, "count", count)
		}
	}

	// input file has been completely read
	for i := 0; i < threadsCnt; i++ {
		sqls <- db.POISON_TASK
	}
	wg.Wait()

	statsMap.Print(cc)

	return nil
}
