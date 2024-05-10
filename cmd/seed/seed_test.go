package seed

import (
	"encoding/json"
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockDB struct{}

func (db *mockDB) PoisonPill() string {
	return db.PoisonPill()
}

func (db *mockDB) SeedTable(cc *cli.Context,
	threadID string,
	statsMap stats.StatsMAP,
	wg *sync.WaitGroup,
	fields []string,
	sql string,
	fieldValues []map[string]any) {

	defer wg.Done()
	fmt.Println("seeding table", threadID, sql, fields, fieldValues)
	statsMap.Store(threadID, 0*time.Second, sql)
}

func (db *mockDB) WriteSQLSelect(f *os.File, sqlStatement string, jsonStrings []string, tokens []string) error {

	for i, js := range jsonStrings {
		var def whereListDef
		json.Unmarshal([]byte(js), &def)
		commasCount := strings.Count(tokens[i], ",")
		if commasCount+1 < def.MinLen || commasCount+1 > def.MaxLen {
			return fmt.Errorf("wrong where list size %d", commasCount+1)
		}
	}

	return nil
}

func Test_doSeed(t *testing.T) {
	// mockApp := cli.NewApp()
	// cli.StringFlag{ Name:"a", Value: "v"}

	type args struct {
		cc       *cli.Context
		dbSeeder db
		config   config
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test-seed",
			args: args{
				cc:       &cli.Context{},
				dbSeeder: &mockDB{},
				config: config{
					Seed: []tableSeed{
						{
							Table:   "Table_1",
							Records: 1000,
							Threads: 5,
							Fields: []fieldSeed{
								{
									ID:          "field-1",
									Field:       "Field_1_of_Table_1",
									FieldType:   "int",
									Min:         10,
									Max:         100,
									Cardinality: 15,
								},
								{
									ID:          "field-2",
									Field:       "Field_2_of_Table_1",
									FieldType:   "string",
									Min:         10,
									Max:         90,
									Cardinality: 50,
								},
							},
						},
						{
							Table:   "Table_2",
							Records: 1000,
							Threads: 5,
							Fields: []fieldSeed{
								{
									ID:          "field-1",
									Field:       "Field_1_of_Table_2",
									FieldType:   "int",
									Min:         10,
									Max:         100,
									Cardinality: 15,
								},
								{
									ID:          "field-2",
									Field:       "Field_2_of_Table_2",
									FieldType:   "string",
									Min:         10,
									Max:         90,
									Cardinality: 50,
								},
							},
						},
					}, // tableseed
					Stress: stressConfig{
						SaveSQLsToFile: "./sqls-" + time.Now().Format("20060102150405") + ".sql",
						Sql: []sql{
							{
								ID:        "sql-query-1",
								Statement: `SELECT * FROM Table_1 WHERE Field_1_of_Table_1 in ( {"table":"Table_1", "field":"Field_1_of_Table_1", "minlen": 10, "maxlen": 30})`,
								Repeat:    30,
								Threads:   5,
								Comment:   "blah",
							},
							{
								ID:        "sql-query-1",
								Statement: `SELECT * FROM Table-2 WHERE Field_2_of_Table_2 in ({"table":"Table_2", "field": "Field_2_of_Table_2", "minlen": 20, "maxlen": 40})`,
								Repeat:    25,
								Threads:   7,
								Comment:   "blah",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := doSeed(tt.args.cc, tt.args.dbSeeder, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("doSeed() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
