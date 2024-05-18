package stats

import (
	"bytes"
	"fmt"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/syncmap"
	"io"
	"log/slog"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

type Stats struct {
	shortest    time.Duration
	longest     time.Duration
	total       time.Duration
	Count       int
	shortestSQL string
	longestSQL  string
	// 100milis, 200milis,
	Histogram        []int
	histoDescription []string
}

type StatsMAP struct {
	m *syncmap.Map
}

func (m *StatsMAP) StoreSingleSQL(key1 string, key2 string, d time.Duration, sql string) {
	key := key1 + "\n" + key2
	s, ok := m.m.Load(key)
	if !ok {
		s = Stats{
			999 * time.Hour,
			0 * time.Second,
			0 * time.Second,
			0,
			"",
			"",
			make([]int, 1000),
			make([]string, 1000),
		}
	}

	stats := s.(Stats)
	stats.Count++

	if d < stats.shortest {
		stats.shortest = d
		stats.shortestSQL = sql
	}

	if d > stats.longest {
		stats.longest = d
		stats.longestSQL = sql
	}
	stats.total += d
	stats.Histogram[slot(d)]++

	m.m.Store(key, stats)
}

func slot(t time.Duration) int {
	return int(t / (100 * time.Millisecond))
}

// print Stats for all threads
func (m *StatsMAP) Print(cc *cli.Context) {
	slog.Info("**********************************************************************")
	slog.Info("*                              PRINTING STATS                        *")
	slog.Info("**********************************************************************")
	keyStats := make(map[string]Stats)
	cummulativeHistohram := make([]int, 20)

	m.m.Range(func(k, v any) bool {
		fullKey := k.(string)
		key := strings.Split(fullKey, "\n")[0]
		stats := v.(Stats)
		s, ok := keyStats[key]
		if !ok {
			s = Stats{
				999 * time.Hour,
				0 * time.Second,
				0 * time.Second,
				0,
				"",
				"",
				make([]int, 1000),
				make([]string, 1000),
			}
		}

		slog.Debug("sql id and thread id", "id", fullKey)
		slog.Debug("total", "count", stats.Count)
		slog.Debug("shortest run", "time", stats.shortest, "sql", stats.shortestSQL)
		slog.Debug("longest run", "time", stats.longest, "sql", stats.longestSQL)

		if s.longest < stats.longest {
			s.longest = stats.longest
			s.longestSQL = stats.longestSQL
		}
		if s.shortest > stats.shortest {
			s.shortest = stats.shortest
			s.shortestSQL = stats.shortestSQL
		}
		s.Count += stats.Count
		s.total += stats.total

		for i := 0; i < 20; i++ {
			s.Histogram[i] += stats.Histogram[i]
			cummulativeHistohram[i] += stats.Histogram[i]
		}
		keyStats[key] = s
		return true
	})

	slog.Info("**********************************************************************")
	slog.Info("*                         Aggregated stats                           *")
	slog.Info("**********************************************************************")

	for k, v := range keyStats {
		dir := cc.String("out-dir") + "/" + k
		f, _ := os.Create(dir + "/stats.txt")
		slog.Info("Stats for", "id", k)
		f.WriteString(fmt.Sprintf("Count %d\n", v.Count))
		slog.Info("Total", "count", v.Count)
		f.WriteString(fmt.Sprintf("Total duration %s\n", v.total))
		slog.Info("Total", "duration", v.total)
		f.WriteString(fmt.Sprintf("Shortest sql %s %s\n", v.shortest, v.shortestSQL))
		slog.Info("Shortest", "duration", v.shortest, "sql", v.shortestSQL)
		f.WriteString(fmt.Sprintf("Longest sql %s %s\n", v.longest, v.longestSQL))
		slog.Info("Longest", "duration", v.longest, "sql", v.longestSQL)

		ioWriter := bytes.NewBufferString("")
		printHistohram(ioWriter, k, v.Histogram)
		f.WriteString(ioWriter.String())
	}

	slog.Info("**********************************************************************")
	slog.Info("*   cummulative sql execution histohram by time elapsed              *")
	slog.Info("**********************************************************************")
	printHistohram(os.Stdout, "cummulative", cummulativeHistohram)
	slog.Info("**********************************************************************")
}

func printHistohram(ioWriter io.Writer, id string, histo []int) {
	slog.Info("ID and Thread", "id-thread", id)
	w := tabwriter.NewWriter(ioWriter, 1, 1, 1, ' ', 0)

	s := "under\t"
	for i := 0; i < 20; i++ {
		s += fmt.Sprintf("%d ms\t", 100*(i+1))
	}
	fmt.Fprintln(w, s)
	s = "     \t"
	for i := 0; i < 20; i++ {
		s += fmt.Sprintf("%d\t", histo[i])
	}
	fmt.Fprintln(w, s)
	w.Flush()
}

func (m *StatsMAP) Dump() map[string]Stats {
	retVal := make(map[string]Stats)
	m.m.Range(func(k, v any) bool {
		threadID := k.(string)
		stats := v.(Stats)
		retVal[threadID] = stats
		return true
	})
	return retVal
}

func New() StatsMAP {
	return StatsMAP{
		m: &syncmap.Map{},
	}
}
