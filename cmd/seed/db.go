package seed

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx"
	"github.com/urfave/cli/v2"
	"strings"
	"sync"
)

type database struct{}

func (db *database) seedTable(cc *cli.Context, wg *sync.WaitGroup, fields []string, sqlStem string, s *tableSeed, fieldValues []map[string]any) {
	switch cc.String("db-type") {
	case "postgres":
		seedTablePg(cc, wg, fields, sqlStem, s, fieldValues)
	case "mysql":
		seedTableMySQL(cc, wg, fields, sqlStem, s, fieldValues)
	}
}

func seedTablePg(cc *cli.Context, wg *sync.WaitGroup, fields []string, sql string, s *tableSeed, fieldValues []map[string]any) {
	for i := range fields {
		sql += "$" + fmt.Sprint(i+1) + ", "
	}
	sql = sql[:len(sql)-1] + ")"

	conn, _ := pgx.Connect(cc.Context, cc.String("db-url"))
	defer func() { conn.Close(cc.Context); wg.Done() }()

	for _, m := range fieldValues {
		values := make([]any, len(m))
		for j, f := range fields {
			values[j] = m[f]
		}
		conn.Exec(cc.Context, sql, values...)
	}
}

func seedTableMySQL(cc *cli.Context, wg *sync.WaitGroup, fields []string, sql string, s *tableSeed, fieldValues []map[string]any) {
	sql += strings.Repeat("?", len(fields))
	sql = sql[:len(sql)-1] + ")"

	conn, _ := sqlx.Connect("mysql", cc.String("db-url"))
	defer func() { conn.Close(); wg.Done() }()

	for _, m := range fieldValues {
		values := make([]any, len(m))
		for j, f := range fields {
			values[j] = m[f]
		}
		// ExecContext executes a query without returning any rows.
		// The args are for any placeholder parameters in the query.
		conn.ExecContext(cc.Context, sql, values...)
	}

}

func newDB() *database {
	return &database{}
}
