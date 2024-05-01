package seed

import (
	"encoding/json"
	"fmt"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/syncmap"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
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

type stressConfig struct {
	SaveToFile string `json:"save_to_file"`
	RunNow     bool   `json:"run_now"`
	Sql        []sql  `json:"sql"`
}

type config struct {
	Seed   []tableSeed
	Stress stressConfig
}

type whereListDef struct {
	Table  string `json:"table"`
	Field  string `json:"field"`
	MinLen int    `json:"minLen"`
	MaxLen int    `json:"maxLen"`
}

type db interface {
	seedTable(cc *cli.Context, wg *sync.WaitGroup, fields []string, sql string, fieldValues []map[string]any)
}

// to allow DB mocking
func seed(cc *cli.Context) error {
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

	dbSeeder := newDB()
	err = doSeed(cc, dbSeeder, config)
	if err != nil {
		log.Fatalf("Failed to seed %s\n", err)
		return err
	}

	return nil
}

func doSeed(cc *cli.Context, dbSeeder db, config config) error {

	rand.Seed(time.Now().UTC().UnixNano())
	// table -> []map[fieldName]any: int | string
	var seedMap syncmap.Map
	// table -> map[filedName[->fieldType. Need it to know if we need to quote strings
	tableFieldTypes := make(map[string]map[string]string, len(config.Seed))
	for _, seed := range config.Seed {
		genOneTable(&seedMap, &seed)
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
		sqlStem := "INSERT INTO " + seed.Table + " ("
		fields := make([]string, len(seed.Fields))

		for i, f := range seed.Fields {
			sqlStem += f.Field + ", "
			fields[i] = f.Field
		}
		sqlStem = sqlStem[:len(sqlStem)-1] + ") VALUES ("

		slice, _ := seedMap.Load(seed.Table)
		typedSlice := slice.([]map[string]any)

		var wg sync.WaitGroup
		for i := 0; i < seed.Threads; i++ {
			wg.Add(1)
			go dbSeeder.seedTable(cc, &wg, fields, sqlStem, typedSlice[i+seed.Records/seed.Threads:])
		}

		wg.Wait()
	}

	// generate tests
	f, _ := os.Create(config.Stress.SaveToFile)
	re := regexp.MustCompile(`{[a-zA-Z_\-0-9":, ]+}`)
	for _, sql := range config.Stress.Sql {
		f.WriteString(THREADS + strconv.Itoa(sql.Threads) + "\n")

		jsonStrings := re.FindAllString(sql.Statement, -1)
		defs := make([]whereListDef, len(jsonStrings))

		for i, sql := range jsonStrings {
			json.Unmarshal([]byte(sql), &defs[i])
		}

		// build the list of queries
		for i := 0; i < sql.Repeat; i++ {
			/*
				sanity check: assignment is deep copy. Despite what's said here
				https://stackoverflow.com/questions/65419268/how-to-deep-copy-a-string-in-go
				x := "abcd"
				y := x
				p := &y
				y = strings.ReplaceAll(y, "bc", "xy")
				fmt.Printf("%P %P %P %s %s", &x, p, &y, x, y)
				%!P(*string=0xc0000140b0) %!P(*string=0xc0000140c0) %!P(*string=0xc0000140c0) abcd axyd

			*/
			outSQL := sql.Statement
			for j, def := range defs {
				token := generateOneINList(def, seedMap, tableFieldTypes)
				outSQL = strings.ReplaceAll(outSQL, jsonStrings[j], token)
			}

			f.WriteString(outSQL + "\n")
		}
	}

	return nil
}

func generateOneINList(def whereListDef, seedMap syncmap.Map, tableFieldTypes map[string]map[string]string) string {

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
