package db

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

type Database struct {
	dbUrl  string
	dbType string
	dbi    database
}

type Task struct {
	SQLID string
	SQL   string
}

var POISON_TASK = Task{
	"NONE",
	stats.POISON_PILL,
}

type database interface {
	connect(cc *cli.Context) error
	close(cc *cli.Context) error
	buildInsert(table string, fields []string) string
	exec(cc *cli.Context, sql string, arguments []any) error
	execLiteral(cc *cli.Context, sql string) error
}

func (db *Database) SeedTable(cc *cli.Context,
	threadID string,
	table string,
	fields []string,
	fieldValues []map[string]any,
	statsChan chan stats.OneStatement,
	wg *sync.WaitGroup,
) {
	sqlStatement := db.dbi.buildInsert(table, fields)
	err := db.dbi.connect(cc)
	if err != nil {
		log.Fatalf("failed to connect to db %s error %s\n", db.dbUrl, err.Error())
	}
	defer func() { db.dbi.close(cc); wg.Done() }()

	count := 0

	for _, m := range fieldValues {
		vals := make([]any, len(m))
		for j, f := range fields {
			vals[j] = m[f]
		}

		start := time.Now()
		// log.Print(sqlStatement, vals, "\n")
		err := db.dbi.exec(cc, sqlStatement, vals)
		if err != nil {
			log.Fatalf("failed to insert %s %v\n", sqlStatement, err)
		}

		count++
		if count%1000 == 0 {
			slog.Info("inserts", "table", table, "thread", threadID, "count", count)
		}

		duration := time.Since(start)
		sqlWithValues := sqlStatement + "   "
		for _, v := range vals {
			switch v := v.(type) {
			case string:
				sqlWithValues += v + "; "
			case int:
				sqlWithValues += fmt.Sprintf("%d ;", v)
			}
		}

		statsChan <- stats.OneStatement{
			ID:       "insert-in-table" + table,
			ThreadID: threadID,
			SQL:      sqlWithValues,
			Duration: duration,
		}
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

type countFile struct {
	count int
	f     *os.File
}

// reading SQL from the channel until it reads the poison pill
func (db *Database) RunSQLs(cc *cli.Context,
	threadID string,
	statsChan chan stats.OneStatement,
	wg *sync.WaitGroup,
	sql chan Task) {

	err := db.dbi.connect(cc)
	if err != nil {
		log.Fatalf("failed to connect to db %s %s", db.dbUrl, err.Error())
	}
	defer func() { db.dbi.close(cc); wg.Done() }()

	count := 0
	for {
		select {
		case task := <-sql:
			if task.SQL == stats.POISON_PILL {
				return
			}

			start := time.Now()
			db.dbi.execLiteral(cc, task.SQL)

			count++
			// fmt.Printf("Count %d Thread %s\n", count, threadID)
			if count%100 == 0 {
				slog.Info(fmt.Sprintf("%s made %d queries. Current SQLID is %s", threadID, count, task.SQLID))
			}
			duration := time.Since(start)
			statsChan <- stats.OneStatement{
				ID:       task.SQLID,
				ThreadID: threadID,
				SQL:      task.SQL,
				Duration: duration,
			}

		case <-time.After(1 * time.Second):
			log.Print("no sql arrived into go function for 1 second")
		}
	}
}

func New(dbType string, dbUrl string) *Database {
	var db database
	switch dbType {
	case "postgres":
		db = newPg()
	case "mysql":
		db = newMYSQL()
	}

	return &Database{
		dbUrl:  dbUrl,
		dbType: dbType,
		dbi:    db,
	}
}
