package seed

import (
	"encoding/json"
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/db"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stats"
	"github.com/yurizf/rdb-seeder-stress-tester/cmd/stress"
	"golang.org/x/sync/syncmap"
	"log"
	"log/slog"
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
	Unique      bool   `json:"unique" binding:"required"`
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

type stressConfig struct {
	SaveSQLsToFile string `json:"sqls_to_file"`
	Sql            []sql  `json:"sql"`
}

type config struct {
	Seed   []tableSeed  `json:"seed"`
	Stress stressConfig `json:"stressConfig"`
}

type whereListDef struct {
	Table  string `json:"table"`
	Field  string `json:"field"`
	MinLen int    `json:"minLen"`
	MaxLen int    `json:"maxLen"`
}

type dbseeder interface {
	SeedTable(cc *cli.Context,
		threadID string,
		table string,
		fields []string,
		fieldValues []map[string]any,
		statsChan chan stats.OneStatement,
		wg *sync.WaitGroup)

	WriteSQLSelect(f *os.File, sqlStatement string, jsonStrings []string, tokens []string) error
}

var re = regexp.MustCompile(`{[a-zA-Z_\-0-9":, ]+}`)

// to allow DB mocking
func Seed(cc *cli.Context) error {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logLevel := cc.String("log-level")
	switch logLevel {
	case "debug":
		opts = &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}
	case "error":
		opts = &slog.HandlerOptions{
			Level: slog.LevelError,
		}
	case "warn":
		opts = &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, opts)))

	path := cc.Path("config")
	if len(path) == 0 {
		return fmt.Errorf("no config path given")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("[ERROR] failed to open config file %s %s", path, err)
	}

	var config config
	if err = json.Unmarshal(b, &config); err != nil {
		log.Fatalf("Failed to load from json `%s`: %s\n", path, err)
		return err
	}

	// dbSeeder := db.New(cc.String("dbseeder-type"), cc.String("dbseeder-url"))
	statsChan := make(chan stats.OneStatement, 1)
	err = doSeed(cc,
		// had to wrap it in a func. Passing db.New() directly gives a syntax error
		func(dbType string, dbUrl string) dbseeder {
			return db.New(dbType, dbUrl)
		},
		config,
		statsChan)
	if err != nil {
		log.Fatalf("Failed to seed %s\n", err)
		return err
	}

	return nil
}

func doSeed(cc *cli.Context,
	new func(dbtype string, dburl string) dbseeder,
	config config,
	statsChan chan stats.OneStatement) error {
	// initialize rand
	// Deprecated: As of Go 1.20 there is no reason to call Seed with a random value.
	// rand.Seed(time.Now().UTC().UnixNano())

	// table -> []map[fieldName]any: int | string. To keep all generated values per table
	var seedMap syncmap.Map
	// table -> map[filedName]->fieldType. Need it to know if we need to do quotes: string or not
	tableFieldTypes := make(map[string]map[string]string, len(config.Seed))

	// loop by tables
	for _, seed := range config.Seed {
		// generate "this table" -> []map[fieldName]-> value of type any(int or string)
		genOneTable(&seedMap, &seed)
		tableFieldTypes[seed.Table] = make(map[string]string, len(seed.Fields))
		// populate tableFieldTypes for the current table
		for _, field := range seed.Fields {
			tableFieldTypes[seed.Table][field.Field] = field.FieldType
		}
	}

	// start stats collector
	var wgStats sync.WaitGroup
	wgStats.Add(1)
	go stats.Collect(cc, statsChan, &wgStats)
	// by tables
	for _, seed := range config.Seed {
		fields := make([]string, len(seed.Fields))
		for i, f := range seed.Fields {
			fields[i] = f.Field
		}

		slice, _ := seedMap.Load(seed.Table)
		typedSlice := slice.([]map[string]any)

		var wg sync.WaitGroup
		// spawn seed.Threads, each to insert seed.Records records from typedSlice.
		for i := 0; i < seed.Threads; i++ {
			s := typedSlice
			perThread := seed.Records / seed.Threads
			if i == seed.Threads-1 {
				// pick all, incl the reminder
				s = typedSlice[i*perThread:]
				fmt.Println("SEEDING", i, i*perThread, seed.Records)
			} else {
				s = typedSlice[i*perThread : (i+1)*perThread]
				fmt.Println("SEEDING", i, i*perThread, (i+1)*perThread)
			}

			dbSeeder := new(cc.String("db-type"), cc.String("db-url"))
			wg.Add(1)
			go dbSeeder.SeedTable(cc,
				fmt.Sprintf("thread-%d", i),
				seed.Table,
				fields,
				// make each thread insert "different" values
				s,
				statsChan,
				&wg)
		}

		wg.Wait()
	}

	statsChan <- stats.OneStatement{
		ID: stats.POISON_PILL,
	}
	wgStats.Wait()

	return saveSQLSelect(&config, new(cc.String("db-type"), cc.String("db-url")), &seedMap, tableFieldTypes)
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
func genOneTable(seedMap *syncmap.Map, s *tableSeed) {
	var records []map[string]any

	slice, ok := seedMap.Load(s.Table)
	if ok {
		records = slice.([]map[string]any)
	} else {
		// first time for this table
		records = make([]map[string]any, 0, s.Records)
	}

	unique := make(map[string]bool)
	// for the number of records specified for this table
	for i := 0; i < s.Records; i++ {
		m := make(map[string]any)
		// for each field
		for _, f := range s.Fields {
			if len(records) >= f.Cardinality {
				// if the current record > cardinality, pick a random value already generated
				m[f.Field] = records[rand.Intn(len(records))][f.Field]
			} else {
				// else, generate a new random value
				switch f.FieldType {
				case "string":
					m[f.Field] = randString(f.Min, f.Max)
					if f.Unique {
						s := m[f.Field].(string)
						//sooner or later we will hit a new one
						for _, ok = unique[s]; ok; {
							m[f.Field] = rand.Intn(f.Max-f.Min+1) + f.Min
							s = m[f.Field].(string)
						}
						unique[s] = true
					}
				case "int":

					m[f.Field] = rand.Intn(f.Max-f.Min+1) + f.Min
					if f.Unique {
						s := strconv.Itoa(m[f.Field].(int))
						for _, ok = unique[s]; ok; {
							m[f.Field] = rand.Intn(f.Max-f.Min+1) + f.Min
							s = strconv.Itoa(m[f.Field].(int))
						}
						unique[s] = true
					}
				default:
					log.Fatalf("Invalid %s field type: %s", f.Field, f.FieldType)

				}
			}
		}

		records = append(records, m)
	}

	seedMap.Store(s.Table, records)
}

func saveSQLSelect(config *config, dbSeeder dbseeder, seedMap *syncmap.Map, tableFieldTypes map[string]map[string]string) error {
	// generate tests
	// the output file will look like
	// threads = sql.Threads
	// sql.Repeat sql.Statement statements with IN () lists built of previously generated random values
	f, _ := os.Create(config.Stress.SaveSQLsToFile)

	for _, sql := range config.Stress.Sql {
		if _, err := f.WriteString(stress.ID + strings.Join(strings.Fields(sql.ID), "+") + "\n"); err != nil {
			return err
		}
		if _, err := f.WriteString(stress.THREADS + strconv.Itoa(sql.Threads) + "\n"); err != nil {
			return err
		}

		jsonStrings := re.FindAllString(sql.Statement, -1)
		defs := make([]whereListDef, len(jsonStrings))

		for i, sql := range jsonStrings {
			if err := json.Unmarshal([]byte(sql), &defs[i]); err != nil {
				return err
			}
		}

		// build the list of queries
		for i := 0; i < sql.Repeat; i++ {
			tokens := make([]string, len(jsonStrings))
			for j, def := range defs {
				tokens[j] = generateOneINList(def, seedMap, tableFieldTypes)
			}
			if err := dbSeeder.WriteSQLSelect(f, sql.Statement, jsonStrings, tokens); err != nil {
				return err
			}

		}
	}
	return nil
}

// whereListDef is from in ( {"table":"table_1", "field":"a", "minlen": 30, "maxlen": 100})
func generateOneINList(def whereListDef, seedMap *syncmap.Map, tableFieldTypes map[string]map[string]string) string {

	// get the IN list random length
	l := rand.Intn(def.MaxLen-def.MinLen+1) + def.MinLen
	// generate list picking l random elements from the values we generated
	var sb strings.Builder
	for i := 0; i < l; i++ {
		lst, _ := seedMap.Load(def.Table)
		lstTyped := lst.([]map[string]any)
		idx := rand.Intn(len(lstTyped))
		fieldType := tableFieldTypes[def.Table][def.Field]
		if fieldType == "string" {
			sb.WriteString(`"`)
			value := lstTyped[idx][def.Field]
			valueStr := value.(string)
			sb.WriteString(valueStr)
			sb.WriteString(`", `)
			continue
		}

		value := lstTyped[idx][def.Field]
		valueInt := value.(int)
		sb.WriteString(strconv.Itoa(valueInt))
		sb.WriteString(", ")
	}

	token := sb.String()
	return token[:len(token)-2]
}
