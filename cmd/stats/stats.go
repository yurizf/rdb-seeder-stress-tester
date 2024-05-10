package stats

import (
	"fmt"
	"golang.org/x/sync/syncmap"
	"os"
	"text/tabwriter"
	"time"
)

type stats struct {
	shortest    time.Duration
	longest     time.Duration
	total       time.Duration
	count       int
	shortestSQL string
	longestSQL  string
	// 100milis, 200milis,
	histogram        []int
	histoDescription []string
}

type StatsMAP struct {
	m *syncmap.Map
}

func (m *StatsMAP) Store(threadID string, d time.Duration, sql string) {
	s, ok := m.m.Load(threadID)
	if !ok {
		s = stats{
			999 * time.Hour,
			0 * time.Second,
			0 * time.Second,
			0,
			"",
			"",
			make([]int, 30),
			make([]string, 30),
		}
	}

	stats := s.(stats)
	stats.count++

	if d < stats.shortest {
		stats.shortest = d
		stats.shortestSQL = sql
	}

	if d > stats.longest {
		stats.longest = d
		stats.longestSQL = sql
	}
	stats.total += d
	stats.histogram[slot(d)]++

	m.m.Store(threadID, stats)
}

func slot(t time.Duration) int {
	return int(t / (100 * time.Millisecond))
}

// print stats for all threads
func (m *StatsMAP) Print() {
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	histo := make([]int, 20)

	m.m.Range(func(k, v any) bool {
		threadID := k.(string)
		stats := v.(stats)
		fmt.Println(threadID, stats.count, stats.shortest, stats.shortestSQL, stats.longest, stats.longestSQL)
		for i := 0; i < 20; i++ {
			histo[i] += stats.histogram[i]
		}

		return true
	})

	fmt.Println("**********************************************************************")
	fmt.Println("*   cummulative sql execution histohram by time elapsed              *")
	fmt.Println("**********************************************************************")
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
	fmt.Println("**********************************************************************")
}

func New() StatsMAP {
	return StatsMAP{
		m: &syncmap.Map{},
	}
}
