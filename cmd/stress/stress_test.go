package stress

import (
	"flag"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/db"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"log"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type mockSelect struct{}

func (s *mockSelect) PoisonPill() string {
	return db.POISON_PILL
}

func (s *mockSelect) RunSQLs(cc *cli.Context,
	threadID string,
	statsMap stats.StatsMAP,
	wg *sync.WaitGroup,
	sql chan db.Task) {

	defer wg.Done()
	count := 0

	for {
		select {
		case task := <-sql:
			if task.SQL == db.POISON_PILL {
				log.Print(threadID, "   poison pill read from the channel")
				count = 0
				return
			} else {
				log.Print(threadID, "   ", task.SQL)
				statsMap.StoreSingleSQL(task.SQLID, threadID, 10*time.Second, task.SQL)
				count++
			}
		case <-time.After(10 * time.Second):
			log.Print("no sql arrived into go function for 10 second")
		}
	}

}

func mockCLIConetext() *cli.Context {
	fl := flag.Flag{
		Name: "input-file",
	}
	fs := flag.NewFlagSet("", flag.ExitOnError)
	app := cli.NewApp()
	// define a flag with default value
	fs.String(fl.Name, "", "")
	_, fname, _, _ := runtime.Caller(0)
	top := filepath.Dir(filepath.Dir(filepath.Dir(fname)))
	// set a new value
	fs.Set(fl.Name, top+"/test/assets/sqls.txt")
	return cli.NewContext(app, fs, nil)
}

func Test_doStress(t *testing.T) {
	type args struct {
		cc    *cli.Context
		db    run
		stats stats.StatsMAP
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test-stress",
			args: args{
				cc:    mockCLIConetext(),
				db:    &mockSelect{},
				stats: stats.New(),
				// TODO: Add test cases.
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := doStress(tt.args.cc,
				func(dbType string, dbUrl string) run {
					return tt.args.db
				},
				tt.args.stats); (err != nil) != tt.wantErr {
				t.Errorf("doStress() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
