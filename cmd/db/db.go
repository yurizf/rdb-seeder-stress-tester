package db

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
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

const POISON_PILL string = "POISON PILL"

var POISON_TASK = Task{
	"NONE",
	"POISON_PILL",
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
	statsMap stats.StatsMAP,
	wg *sync.WaitGroup,
) {
	sqlStatement := db.dbi.buildInsert(table, fields)
	err := db.dbi.connect(cc)
	if err != nil {
		log.Fatal().Err(err).Msg(fmt.Sprintf("failed to connect to db %s", db.dbUrl))
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
			log.Fatal().Err(err).Msg(fmt.Sprintf("failed to insert %s", sqlStatement))
		}

		count++
		if count%100 == 0 {
			slog.Info(fmt.Sprintf("inserts", "thread", threadID, "table", table, "count", count))
		}

		duration := time.Since(start)
		// statsMap.StoreSingleSQL(task.SQLID, threadID, duration, task.SQL)
		statsMap.StoreSingleSQL("insert-in-"+table, threadID, duration, sqlStatement)
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
	statsMap stats.StatsMAP,
	wg *sync.WaitGroup,
	sql chan Task) {

	err := db.dbi.connect(cc)
	if err != nil {
		log.Fatal().Err(err).Msg(fmt.Sprintf("failed to connect to db %s", db.dbUrl))
	}
	defer func() { db.dbi.close(cc); wg.Done() }()

	files := make(map[string]countFile)
	defer func() {
		for k, v := range files {
			v.f.WriteString(fmt.Sprintf("thread %s executed %d queries for ID %s\n", threadID, v.count, k))
			v.f.Close()
		}
	}()

	for {
		select {
		case task := <-sql:
			if task.SQL == POISON_PILL {
				return
			}

			i, ok := files[task.SQLID]
			if !ok {
				dir := cc.String("out-dir") + "/" + task.SQLID
				err := os.MkdirAll(dir, os.ModePerm)
				if err != nil {
					log.Fatal().Err(err).Msg("Failed to mkdir" + dir)
				}
				f, err := os.Create(dir + "/" + threadID + ".txt")
				if err != nil {
					log.Fatal().Err(err).Msg("Failed to os.create" + dir + "/" + threadID + ".txt")
				}
				i = countFile{0, f}
				files[task.SQLID] = i
			}

			start := time.Now()
			db.dbi.execLiteral(cc, task.SQL)

			i.count++
			if i.count%100 == 0 {
				slog.Info(fmt.Sprintf("%s made %d queries for %s", threadID, i.count, task.SQLID))
			}
			duration := time.Since(start)
			statsMap.StoreSingleSQL(task.SQLID, threadID, duration, task.SQL)
			i.f.WriteString(fmt.Sprintf("%s %s", duration, task.SQL))

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
