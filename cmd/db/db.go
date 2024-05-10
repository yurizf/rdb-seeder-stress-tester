package db

import (
	sqlite "database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"os"
	"strings"
	"sync"
	"time"
)

const POISON_PILL = "POISON_PILL"

type Database struct{}

func (db *Database) SeedTable(cc *cli.Context,
	threadID string,
	statsMap stats.StatsMAP,
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

	count := 0

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
		if count%100 == 0 {
			fmt.Printf("%s made %d inserts\n", threadID, count)
		}

		duration := time.Since(start)
		statsMap.Store(threadID, duration, sqlStatement)
	}
}

func (db *Database) WriteSQLSelect(f *os.File,
	sqlStatement string,
	jsonStrings []string,
	tokens []string) error {

	outSQL := sqlStatement
	for j, js := range jsonStrings {
		outSQL = strings.ReplaceAll(outSQL, js, tokens[j])
	}

	_, err := f.WriteString(outSQL + "\n")
	return err
}

// reading SQL from the channel until it reads the poison pill
func (db *Database) RunSQLs(cc *cli.Context,
	threadID string,
	statsMap stats.StatsMAP,
	wg *sync.WaitGroup,
	sql chan string) {

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

	count := 0
	f, _ := os.Create("./" + threadID + ".txt")
	defer f.Close()

	for {
		select {
		case statement := <-sql:
			if statement == POISON_PILL {
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
			if count%100 == 0 {
				fmt.Printf("%s made %d queries\n", threadID, count)
			}
			duration := time.Since(start)
			statsMap.Store(threadID, duration, statement)
			f.WriteString(fmt.Sprintf("%s %s\n", duration, statement))

		case <-time.After(1 * time.Second):
			log.Print("no sql arrived into go function for 1 second")
		}
	}

}

func New() *Database {
	return &Database{}
}
