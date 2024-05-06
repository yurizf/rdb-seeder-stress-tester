package seed

import (
	sqlite "database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/syncmap"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type database struct{}

type stats struct {
	min   time.Duration
	max   time.Duration
	total time.Duration
	count int
	avg   time.Duration
}

var re = regexp.MustCompile(`{[a-zA-Z_\-0-9":, ]+}`)

func (db *database) seedTable(cc *cli.Context,
	threadID string,
	statsMap *syncmap.Map,
	wg *sync.WaitGroup,
	fields []string,
	sqlStem string,
	fieldValues []map[string]any) {

	var pgConn *pgx.Conn
	var mySqlConn *sqlx.DB
	var liteDBConn *sqlite.DB
	sqlStatement := sqlStem

	switch cc.String("db-type") {
	case "postgres":
		// have to have it here b/c of defer call. to avoid type assertions etc
		pgConn, _ = pgx.Connect(cc.Context, cc.String("db-url"))
		defer func() { pgConn.Close(cc.Context); wg.Done() }()

		for i := range fields {
			sqlStatement += "$" + fmt.Sprint(i+1) + ","
		}
		sqlStatement = sqlStatement[:len(sqlStatement)-1] + ")"

	case "mysql":
		mySqlConn, _ = sqlx.Connect("mysql", cc.String("db-url"))
		defer func() { mySqlConn.Close(); wg.Done() }()

		sqlStatement += strings.Repeat("?,", len(fields))
		sqlStatement = sqlStatement[:len(sqlStatement)-1] + ")"

	case "sqlite":
		liteDBConn, _ = sqlite.Open("sqlite3", cc.String("db-url"))
		defer func() { liteDBConn.Close(); wg.Done() }()

		sqlStatement += strings.Repeat("?,", len(fields))
		sqlStatement = sqlStatement[:len(sqlStatement)-1] + ")"
	}

	maxDuration := 0 * time.Second
	minDuration := 999 * time.Hour
	// var avg time.Duration
	count := 0
	totalDuration := 0 * time.Second

	for _, m := range fieldValues {
		vals := make([]any, len(m))
		for j, f := range fields {
			vals[j] = m[f]
		}

		start := time.Now()
		// log.Print(sqlStatement, vals, "\n")
		switch cc.String("db-type") {
		case "postgres":
			pgConn.Exec(cc.Context, sqlStatement, vals...)
		case "mysql":
			mySqlConn.ExecContext(cc.Context, sqlStatement, vals...)
		case "sqlite":
			_, err := liteDBConn.ExecContext(cc.Context, sqlStatement, vals...)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to insert")
			}
			liteDBConn.Begin()
		}

		count++

		duration := time.Since(start)
		if duration < minDuration {
			minDuration = duration
		}
		if duration > maxDuration {
			maxDuration = duration
		}
		totalDuration += duration
	}

	statsMap.Store(threadID, stats{minDuration,
		maxDuration,
		totalDuration,
		count,
		//https://go.dev/play/p/6Pbqrz8ZZ3t
		totalDuration / time.Duration(count),
	})
}

func (db *database) writeSQLSelect(f *os.File, sqlStatement string, jsonStrings []string, tokens []string) error {
	outSQL := sqlStatement
	for j, js := range jsonStrings {
		outSQL = strings.ReplaceAll(outSQL, js, tokens[j])
	}

	_, err := f.WriteString(outSQL + "\n")
	return err
}

// reading SQL from the channel until it reads the poison pill
func (db *database) runSQLs(cc *cli.Context, threadID string, statsMap *syncmap.Map, wg *sync.WaitGroup, sql chan string) {

	var pgConn *pgx.Conn
	var mySqlConn *sqlx.DB
	var liteDBConn *sqlite.DB

	switch cc.String("db-type") {
	case "postgres":
		pgConn, _ = pgx.Connect(cc.Context, cc.String("db-url"))
		defer func() { pgConn.Close(cc.Context); wg.Done() }()
	case "mysql":
		mySqlConn, _ = sqlx.Connect("mysql", cc.String("db-url"))
		defer func() { mySqlConn.Close(); wg.Done() }()
	case "sqlite":
		liteDBConn, _ = sqlite.Open("sqlite3", cc.String("db-url"))
		defer func() { liteDBConn.Close(); wg.Done() }()
	}

	max := 0 * time.Second
	min := 999 * time.Hour
	count := 0
	totalDuration := 0 * time.Second

	select {
	case statement := <-sql:
		if statement == POISON_PILL {
			statsMap.Store(threadID, stats{min,
				max,
				totalDuration,
				count,
				//https://go.dev/play/p/6Pbqrz8ZZ3t
				totalDuration / time.Duration(count),
			})
			return
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
