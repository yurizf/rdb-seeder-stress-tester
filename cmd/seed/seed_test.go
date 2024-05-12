package seed

import (
	"encoding/json"
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockDB struct{}

func (db *mockDB) SeedTable(cc *cli.Context,
	threadID string,
	table string,
	fields []string,
	fieldValues []map[string]any,
	statsMap stats.StatsMAP,
	wg *sync.WaitGroup) {

	defer wg.Done()
	slog.Debug("test seeding table", "threadID", threadID, "sql", "test-sql with"+strings.Join(fields, ","), "fieldValues", fieldValues)
	for _, _ = range fieldValues {
		statsMap.StoreSingleSQL(threadID, 1*time.Second, "test-sql with"+strings.Join(fields, ","))
	}
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
		stats    stats.StatsMAP
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
				stats:    stats.New(),
				config: config{
					Seed: []tableSeed{
						{
							Table:   "Table_1",
							Records: 1000,
							Threads: 3,
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
						SaveSQLsToFile: "./test-sqls.sql",
						Sql: []sql{
							{
								ID:        "sql-query-1",
								Statement: `SELECT * FROM Table_1 WHERE Field_1_of_Table_1 in ( {"table":"Table_1", "field":"Field_1_of_Table_1", "minlen": 10, "maxlen": 30})`,
								Repeat:    10,
								Threads:   5,
								Comment:   "blah",
							},
							{
								ID:        "sql-query-1",
								Statement: `SELECT * FROM Table-2 WHERE Field_2_of_Table_2 in ({"table":"Table_2", "field": "Field_2_of_Table_2", "minlen": 20, "maxlen": 40})`,
								Repeat:    5,
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
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, opts)))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := doSeed(tt.args.cc, tt.args.dbSeeder, tt.args.config, tt.args.stats); (err != nil) != tt.wantErr {
				t.Errorf("doSeed() error = %v, wantErr %v", err, tt.wantErr)
			}

			stats := tt.args.stats.Dump()
			expectedCnt := 0
			for _, s := range tt.args.config.Seed {
				expectedCnt += s.Records * s.Threads
			}

			actualCnt := 0
			for _, v := range stats {
				actualCnt += v.Count
			}
			if expectedCnt != actualCnt {
				t.Errorf("expected count %d does not match actual count %d", expectedCnt, actualCnt)
			}

			tt.args.stats.Print()
			slog.Info(tt.name)
		})
	}
}
