package stats

import (
	"fmt"
	"golang.org/x/sync/syncmap"
	"log/slog"
	"os"
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

func (m *StatsMAP) StoreSingleSQL(threadID string, d time.Duration, sql string) {
	s, ok := m.m.Load(threadID)
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

	m.m.Store(threadID, stats)
}

func slot(t time.Duration) int {
	return int(t / (100 * time.Millisecond))
}

// print Stats for all threads
func (m *StatsMAP) Print() {
	slog.Info("**********************************************************************")
	slog.Info("*                              PRINTING STATS                        *")
	slog.Info("**********************************************************************")
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	histo := make([]int, 20)

	m.m.Range(func(k, v any) bool {
		threadID := k.(string)
		stats := v.(Stats)
		slog.Debug("thread ID", threadID)
		slog.Debug("Count", stats.Count)
		slog.Debug("shortest", stats.shortest, stats.shortestSQL)
		slog.Debug("longest", stats.longest, stats.longestSQL)
		for i := 0; i < 20; i++ {
			histo[i] += stats.Histogram[i]
		}
		return true
	})

	slog.Info("**********************************************************************")
	slog.Info("*   cummulative sql execution histohram by time elapsed              *")
	slog.Info("**********************************************************************")
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
	slog.Info("**********************************************************************")
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
