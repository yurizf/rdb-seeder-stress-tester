package db

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli/v2"
)

type pg struct {
	//pgConn *pgx.Conn
	pgConn *pgxpool.Pool
}

// https://github.com/jackc/pgx/wiki/Getting-started-with-pgx#using-a-connection-pool

func (db *pg) connect(cc *cli.Context) error {
	var err error
	// getting
	// {"error":"conn busy","message":"failed to insert INSERT INTO table_1 (a,b) VALUES ($1,$2)"}
	// pgx.Connect() represents a single connection and is not concurrency safe.
	// however, we don't use this connection concurrently. Each threads does open
	// so it's unclear to me why this does not work
	//db.pgConn, err = pgx.Connect(cc.Context, cc.String("db-url"))
	db.pgConn, err = pgxpool.New(cc.Context, cc.String("db-url"))
	return err
}

func (db *pg) close(cc *cli.Context) error {
	if db.pgConn != nil {
		// return db.pgConn.Close(cc.Context)
		db.pgConn.Close()
		return nil
	}
	return fmt.Errorf("db connection is nil")
}

func (db *pg) buildInsert(table string, fields []string) string {
	sqlStatement := "INSERT INTO " + table + " ("
	for _, f := range fields {
		sqlStatement += f + ","
	}
	sqlStatement = sqlStatement[:len(sqlStatement)-1] + ") VALUES ("
	for i := range fields {
		sqlStatement += "$" + fmt.Sprint(i+1) + ","
	}
	return sqlStatement[:len(sqlStatement)-1] + ")"
}

func (db *pg) exec(cc *cli.Context, query string, args []any) error {
	_, err := db.pgConn.Exec(cc.Context, query, args...)
	return err
}

func (db *pg) execLiteral(cc *cli.Context, query string) error {
	_, err := db.pgConn.Exec(cc.Context, query)
	return err
}

func newPg() *pg {
	return &pg{}
}
