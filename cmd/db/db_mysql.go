package db

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/urfave/cli/v2"
	"strings"
)

type mySQL struct {
	mySqlConn *sqlx.DB
}

func (db *mySQL) connect(cc *cli.Context) error {
	var err error
	db.mySqlConn, err = sqlx.Connect("mysql", cc.String("db-url"))
	return err
}

func (db *mySQL) close(cc *cli.Context) error {
	if db.mySqlConn != nil {
		return db.mySqlConn.Close()
	}
	return fmt.Errorf("db connection is nil")
}

func (db *mySQL) buildInsert(table string, fields []string) string {
	sqlStatement := "INSERT INTO " + table + " ("
	for _, f := range fields {
		sqlStatement += f + ","
	}
	sqlStatement = sqlStatement[:len(sqlStatement)-1] + ") VALUES ("

	sqlStatement += strings.Repeat("?,", len(fields))
	return sqlStatement[:len(sqlStatement)-1] + ")"
}

func (db *mySQL) exec(cc *cli.Context, query string, args []any) error {
	_, err := db.mySqlConn.ExecContext(cc.Context, query, args...)
	return err
}

func (db *mySQL) execLiteral(cc *cli.Context, query string) error {
	_, err := db.mySqlConn.ExecContext(cc.Context, query)
	return err
}

func newMYSQL() *mySQL {
	return &mySQL{}
}
