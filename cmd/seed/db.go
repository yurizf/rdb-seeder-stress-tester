package seed

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/syncmap"
	"strings"
	"sync"
	"time"
)

type database struct{}

type stats struct {
	min   time.Duration
	max   time.Duration
	count int
	avg   time.Duration
}

func (db *database) seedTable(cc *cli.Context, wg *sync.WaitGroup, fields []string, sqlStem string, fieldValues []map[string]any) {

	var pgConn *pgx.Conn
	var mySqlConn *sqlx.DB
	sql := sqlStem

	switch cc.String("db-type") {
	case "postgres":
		// have to have it here b/c of defer call. to avoid type assertions etc
		pgConn, _ = pgx.Connect(cc.Context, cc.String("db-url"))
		defer func() { pgConn.Close(cc.Context); wg.Done() }()

		for i := range fields {
			sql += "$" + fmt.Sprint(i+1) + ","
		}
		sql = sql[:len(sql)-1] + ")"

	case "mysql":
		mySqlConn, _ := sqlx.Connect("mysql", cc.String("db-url"))
		defer func() { mySqlConn.Close(); wg.Done() }()

		sql += strings.Repeat("?,", len(fields))
		sql = sql[:len(sql)-1] + ")"
	}

	for _, m := range fieldValues {
		values := make([]any, len(m))
		for j, f := range fields {
			values[j] = m[f]
		}

		switch cc.String("db-type") {
		case "postgres":
			pgConn.Exec(cc.Context, sql, values...)
		case "mysql":
			mySqlConn.ExecContext(cc.Context, sql, values)
		}
	}
}

// reading SQL from the channel until it reads the poison pill
func (db *database) runSQLs(cc *cli.Context, threadID string, statsMap *syncmap.Map, sql chan string) {

	var pgConn *pgx.Conn
	var mySqlConn *sqlx.DB
	switch cc.String("db-type") {
	case "postgres":
		pgConn, _ = pgx.Connect(cc.Context, cc.String("db-url"))
		defer pgConn.Close(cc.Context)
	case "mysql":
		mySqlConn, _ = sqlx.Connect("mysql", cc.String("db-url"))
		defer mySqlConn.Close()
	}

	max := 0 * time.Second
	min := 999 * time.Hour
	// var avg time.Duration
	count := 0
	totalDuration := 0 * time.Second

	select {
	case statement := <-sql:
		if statement == POISON_PILL {
			statsMap.Store(threadID, stats{min,
				max,
				count,
				//https://go.dev/play/p/6Pbqrz8ZZ3t
				totalDuration / time.Duration(count),
			})
		}
		start := time.Now()

		switch cc.String("db-type") {
		case "postgres":

			pgConn.Exec(cc.Context, statement)
		case "mysql":
			mySqlConn.ExecContext(cc.Context, statement)
		}

		count++
		duration := time.Since(start)
		if duration < min {
			min = duration
		}
		if duration > max {
			max = duration
		}
		totalDuration += duration

	case <-time.After(1 * time.Second):
		log.Print("no sql arrived into go function for 1 second")
	}

}

func newDB() *database {
	return &database{}
}
