package seed

import (
	"flag"
	"fmt"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/syncmap"
	"log"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type mockSelect struct{}

func (s *mockSelect) runSQLs(cc *cli.Context,
	threadID string,
	statsMap *syncmap.Map,
	wg *sync.WaitGroup,
	sql chan string) {

	defer wg.Done()
	count := 0
	shortSQL := ""
	longSQL := ""

	for {
		select {
		case statement := <-sql:
			if statement == POISON_PILL {
				log.Print(threadID, "   poison pill read from the channel")
				statsMap.Store(threadID, stats{0,
					0,
					0,
					count,
					0,
					shortSQL,
					longSQL,
				})
				count = 0
				return
			} else {
				log.Print(threadID, "   ", statement)
				shortSQL = statement
				longSQL = statement
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
		stats []syncmap.Map
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
				stats: make([]syncmap.Map, 10),
				// TODO: Add test cases.
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := doStress(tt.args.cc, tt.args.db, tt.args.stats); (err != nil) != tt.wantErr {
				t.Errorf("doStress() error = %v, wantErr %v", err, tt.wantErr)
			}
			for i := 0; i < 2; i++ {
				tt.args.stats[i].Range(func(k, v any) bool {
					stats := v.(stats)
					fmt.Println(k.(string), stats.min, stats.avg, stats.max, stats.count, stats.shortestSQL, stats.longestSQL)
					return true
				})
			}
		})
	}
}
