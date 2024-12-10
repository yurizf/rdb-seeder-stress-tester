package stress

import (
	"flag"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/db"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type mockSelect struct{}

func (s *mockSelect) PoisonPill() string {
	return stats.POISON_PILL
}

// type run interface {
// RunSQLs(cc *cli.Context, threadID string, statsChan chan stats.OneStatement, wg *sync.WaitGroup, sql chan db.Task)

func (s *mockSelect) RunSQLs(cc *cli.Context,
	threadID string,
	// statsMap stats.StatsMAP,
	statsChan chan stats.OneStatement,
	wg *sync.WaitGroup,
	sql chan db.Task) {

	defer wg.Done()
	count := 0

	for {
		select {
		case task := <-sql:
			if task.SQL == stats.POISON_PILL {
				log.Print(threadID, "   poison pill read from the channel")
				count = 0
				return
			} else {
				log.Print(threadID, "   ", task.SQL)
				statsChan <- stats.OneStatement{
					ID:       task.SQLID,
					ThreadID: threadID,
					SQL:      task.SQL,
					Duration: time.Duration(rand.IntN(10)) * time.Second,
				}
				count++
			}
		case <-time.After(10 * time.Second):
			log.Print("no sql arrived into go function for 10 second")
		}
	}

}

func outDir() string {
	_, fname, _, _ := runtime.Caller(0)
	top := filepath.Dir(filepath.Dir(filepath.Dir(fname)))
	ret := filepath.Join(top, "test", "stress-test-out-dir")
	if err := os.MkdirAll(ret, 0755); err != nil {
		log.Fatalf("failed to create directory %s: %v", ret, err)
		return ""
	}
	return ret
}

func mockCLIConetext() *cli.Context {
	app := cli.NewApp()
	fs := flag.NewFlagSet("", flag.ExitOnError)

	fl := flag.Flag{
		Name: "input-file",
	}
	fs.String(fl.Name, "", "")
	// set a new value
	outDir := outDir()
	fs.Set(fl.Name, filepath.Join(filepath.Dir(outDir), "assets", "sqls.txt"))

	fl = flag.Flag{
		Name: "out-dir",
	}
	fs.String(fl.Name, "", "")
	fs.Set(fl.Name, outDir)

	cc := cli.NewContext(app, fs, nil)
	cc.Command.Name = "test"
	return cc
}

func Test_doStress(t *testing.T) {
	type args struct {
		cc *cli.Context
		db run
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test-stress",
			args: args{
				cc: mockCLIConetext(),
				db: &mockSelect{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := doStress(tt.args.cc,
				func(dbType string, dbUrl string) run {
					return tt.args.db
				}); (err != nil) != tt.wantErr {
				t.Errorf("doStress() error = %v, wantErr %v", err, tt.wantErr)
			}

		})
	}
}
