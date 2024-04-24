package seed

import (
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/syncmap"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type fieldSeed struct {
	ID          string `json:"ID" binding:"required"`
	Field       string `json:"field" binding:"required"`
	FieldType   string `json:"field_type" binding:"required"`
	Encoding    string `json:"encoding"`
	Min         int    `json:"min" binding:"required"`
	Max         int    `json:"max" binding:"required"`
	Cardinality int    `json:"cardinality" binding:"required"`
}

type tableSeed struct {
	Table   string      `json:"table" binding:"required"`
	Records int         `json:"records" binding:"required"`
	Threads int         `json:"insertThreads" binding:"required"`
	Fields  []fieldSeed `json:"fields" binding:"required"`
}

type sql struct {
	ID        string `json:"ID" binding:"required"`
	Statement string `json:"statement" binding:"required"`
	Repeat    int    `json:"repeat" binding:"required"`
	Threads   int    `json:"threads" binding:"required"`
	Comment   string `json:"comment" binding:"required"`
}

type config struct {
	Seed []tableSeed
	Test struct {
		Save_to_file string `json:"save-to-file"`
		Sql          []sql  `json:"sql"`
	}
}

type whereListDef struct {
	Table  string `json:"table"`
	Field  string `json:"field"`
	MinLen int    `json:"minLen"`
	MaxLen int    `json:"maxLen"`
}

func seed(cc *cli.Context) error {
	path := cc.Path("config")
	if len(path) == 0 {
		return fmt.Errorf("No config path given")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("[ERROR] failed to open config file %s %s", path, err)
	}

	var config config
	if err = json.Unmarshal(b, &config); err != nil {
		log.Fatal("Failed to load from json `%s`: %s", path, err)
	}

	// table -> []map[fieldName]any: int | string
	var seedMap syncmap.Map
	// table -> map[filedName[->fieldType. Need it to know if we need to quote strings
	tableFieldTypes := make(map[string](map[string]string), len(config.Seed))
	for _, seed := range config.Seed {
		genOneTable(seedMap, &seed)
		tableFieldTypes[seed.Table] = make(map[string]string, len(seed.Fields))
		for _, field := range seed.Fields {
			tableFieldTypes[seed.Table][field.Field] = field.FieldType
		}
	}

	for _, seed := range config.Seed {
		for i := 0; i < seed.Records; i++ {
		}
	}

	for _, seed := range config.Seed {
		sql := "INSERT INTO " + seed.Table + " ("
		fields := make([]string, len(seed.Fields))

		for i, f := range seed.Fields {
			sql += f.Field + ", "
			fields[i] = f.Field
		}
		sql = sql[:len(sql)-1] + ") VALUES ("

		slice, _ := seedMap.Load(seed.Table)
		typedSlice := slice.([]map[string]any)

		var wg sync.WaitGroup
		switch cc.String("db-type") {
		case "mysql":
			for i := 0; i < seed.Threads; i++ {
				wg.Add(1)
				go seedTableMySQL(cc, wg, fields, sql, &seed, typedSlice[i+seed.Records/seed.Threads:])
			}

		case "postgres":
			for i := 0; i < seed.Threads; i++ {
				wg.Add(1)
				go seedTablePg(cc, wg, fields, sql, &seed, typedSlice[i+seed.Records/seed.Threads:])
			}
		}
	}

	// generate tests
	f, err := os.Create(config.Test.Save_to_file)
	re := regexp.MustCompile(`{[a-zA-Z_\-0-9":, ]+}`)
	for _, sql := range config.Test.Sql {
		f.WriteString("threads=" + strconv.Itoa(sql.Threads) + "\n")

		jsonStrings := re.FindAllString(sql.Statement, -1)
		defs := make([]whereListDef, len(jsonStrings))

		for i, sql := range jsonStrings {
			json.Unmarshal([]byte(sql), &defs[i])
		}

		// build the list of queries
		for i := 0; i < sql.Repeat; i++ {
			for j, def := range defs {
				// get the length
				l := rand.Intn(def.MaxLen-def.MinLen+1) + def.MinLen
				// generate list picking l random elements
				var sb strings.Builder
				for i := 0; i < l; i++ {
					lst, _ := seedMap.Load(def.Table)
					lstTyped := lst.([]map[string]any)
					idx := rand.Intn(len(lstTyped))
					fieldType := tableFieldTypes[def.Table][def.Field]
					if fieldType == "string" {
						sb.WriteString("\"")
						value := lstTyped[idx][def.Field]
						valueStr := value.(string)
						sb.WriteString(valueStr)
						sb.WriteString("\", ")
						continue
					}

					value := lstTyped[idx][def.Field]
					valueInt := value.(int)
					sb.WriteString(string(valueInt))
					sb.WriteString("\", ")
				}

				token := sb.String()[:-2]
				sql.Statement = strings.ReplaceAll(sql.Statement, jsonStrings[j], token)
				f.WriteString(sql.Statement + "\n")
			}
		}
	}

	return nil
}

func seedTablePg(cc *cli.Context, wg sync.WaitGroup, fields []string, sql string, s *tableSeed, fieldValues []map[string]any) {
	for i, _ := range fields {
		sql += "$" + fmt.Sprint(i+1) + ", "
	}
	sql = sql[:len(sql)-1] + ")"

	db, err := pgx.Connect(cc.Context, cc.String("db-url"))
	defer func() { db.Close(cc.Context); wg.Done() }()

	for _, m := range fieldValues {
		values := make([]any, len(m))
		for j, f := range fields {
			values[j] = m[f]
		}
		db.Exec(cc.Context, sql, values...)
	}

}

func seedTableMySQL(cc *cli.Context, wg sync.WaitGroup, fields []string, sql string, s *tableSeed, fieldValues []map[string]any) {
	sql += strings.Repeat("?", len(fields))
	sql = sql[:len(sql)-1] + ")"

	db, err := sqlx.Connect("mysql", cc.String("db-url"))
	defer func() { db.Close(); wg.Done() }()

	for _, m := range fieldValues {
		values := make([]any, len(m))
		for j, f := range fields {
			values[j] = m[f]
		}
		// ExecContext executes a query without returning any rows.
		// The args are for any placeholder parameters in the query.
		res, err := db.ExecContext(cc.Context, sql, values...)
	}

}

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
const allChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_-/+?!@#$%^&*()[]"

func randString(minLen int, maxLen int) string {
	// inclusive
	n := rand.Intn(maxLen-minLen+1) + minLen
	b := make([]byte, n)
	for i := range b {
		b[i] = allChars[rand.Int63()%int64(len(allChars))]
	}
	return string(b)
}

// seedMap: table->[]map[field]any: string|int
func genOneTable(seedMap syncmap.Map, s *tableSeed) {
	var records []map[string]any

	slice, ok := seedMap.Load(s.Table)
	if ok {
		records = slice.([]map[string]any)
	} else {
		records = make([]map[string]any, 0, s.Records)
	}

	for i := 0; i < s.Records; i++ {
		m := make(map[string]any)
		for _, f := range s.Fields {
			if len(records) >= f.Cardinality {
				m[f.Field] = records[rand.Intn(len(records))][f.Field]
			} else {
				switch f.FieldType {
				case "string":
					m[f.Field] = randString(f.Min, f.Max)
				case "int":
					m[f.Field] = rand.Intn(f.Max-f.Min+1) + f.Min
				}
			}
		}

		records = append(records, m)
	}
	seedMap.Store(s.Table, records)
}
