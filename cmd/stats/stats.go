package stats

import (
	"bytes"
	"fmt"
	"github.com/urfave/cli/v2"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"text/tabwriter"
	"time"
)

const POISON_PILL string = "POISON PILL"

type OneStatement struct {
	ID       string
	ThreadID string
	Duration time.Duration
	SQL      string
}

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

func slot(t time.Duration) int {
	return min(int(t/(100*time.Millisecond)), 999)
}

func printStats(w io.Writer, id string, v *Stats) {
	fmt.Fprintln(w, "**********************************************************************")
	fmt.Fprintln(w, "                "+id+" stats")
	fmt.Fprintln(w, "**********************************************************************")
	fmt.Fprintf(w, "Count %d\n", v.Count)
	fmt.Fprintf(w, "Total duration %s\n", v.total)
	fmt.Fprintf(w, "Shortest sql %s %s\n", v.shortest, v.shortestSQL)
	fmt.Fprintf(w, "Longest sql %s %s\n", v.longest, v.longestSQL)

	tw := tabwriter.NewWriter(w, 1, 1, 1, ' ', 0)

	s := "under\t"
	for i := 0; i < 500; i++ {
		s += fmt.Sprintf("%d ms\t", 100*(i+1))
	}
	fmt.Fprintln(w, s)
	s = "     \t"
	for i := 0; i < 500; i++ {
		s += fmt.Sprintf("%d\t", v.Histogram[i])
	}
	fmt.Fprintln(w, s)
	tw.Flush()
}

// put this in a func to ease unit tests
func DurationsFileName(cc *cli.Context) string {
	return filepath.Join(cc.String("out-dir"), cc.Command.Name+"-durations.txt")
}

func Collect(cc *cli.Context,
	statsChan chan OneStatement,
	wg *sync.WaitGroup) {

	fname := DurationsFileName(cc)
	f, err := os.Create(fname)
	if err != nil {
		log.Fatalf("failed to create  stats file %s: %v", fname, err)
	}
	defer f.Close()

	aggregate := make(map[string]Stats)

	for {
		select {
		case stats := <-statsChan:
			// fmt.Println("********* Stats read in from channel " + stats.ID)
			if stats.ID == POISON_PILL {
				fname = cc.String("out-dir") + "/" + cc.Command.Name + "stats.txt"
				fs, err := os.Create(fname)
				if err != nil {
					log.Fatalf("failed to create  stats file %s: %v", fname, err)
				}
				defer fs.Close()
				for k, v := range aggregate {
					ioWriter := bytes.NewBufferString("")
					printStats(ioWriter, k, &v)
					s := ioWriter.String()
					fs.WriteString(s)
					// slog ignores line breaks
					fmt.Println(s)
				}
				wg.Done()
				return
			}

			s, ok := aggregate[stats.ID]
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

			if s.longest < stats.Duration {
				s.longest = stats.Duration
				s.longestSQL = stats.SQL
			}

			if s.shortest > stats.Duration {
				s.shortest = stats.Duration
				s.shortestSQL = stats.SQL
			}
			s.Count += 1
			s.total += stats.Duration
			s.Histogram[slot(stats.Duration)]++
			aggregate[stats.ID] = s
			f.WriteString(fmt.Sprintf("ID=%s Thread=%s Duration=%s SQL=%s\n", stats.ID, stats.ThreadID, stats.Duration, stats.SQL))

		case <-time.After(1 * time.Second):
			log.Print("no stats arrived into go stats collect function for 1 second")
		}
	}

}
