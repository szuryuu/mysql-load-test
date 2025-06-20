package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// ANSI color codes for terminal output
const (
	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Purple = "\033[35m"
	Cyan   = "\033[36m"
	White  = "\033[37m"
	Bold   = "\033[1m"
	Dim    = "\033[2m"
)

type queryCount struct {
	query string
	count int
}

func main() {
	qsdb, err := NewQuerySourceDB(QuerySourceDBConfig{
		DSN:               "root:root@tcp(127.0.0.1:13306)/MySQLLoadTester?parseTime=true",
		QueriesFetchQuery: "select Query from Query where Hash = {{.Hash}}",
		FingerprintWeightsQuery: `
with queryFingerprintTotal as (
  select 
    count(*) as c
  from QueryFingerprint qf
)
select
  qf2.Fingerprint as Fingerprint,
  qf2.Hash as Hash,
  count(*) as Count,
  qft.c as Total,
  (count(*)/qft.c)*100 as Weight
from QueryFingerprint qf2
cross join queryFingerprintTotal qft on 1=1
group by qf2.Hash
order by Weight desc
		`,
	})
	if err != nil {
		panic(err)
	}
	_ = qsdb

	counts := map[string]int{}
	var mu sync.Mutex
	startTime := time.Now()

	// Pretty reporter goroutine
	go func() {
		for {
			time.Sleep(time.Second)

			// Clear screen
			fmt.Print("\033[2J\033[H")

			// Header
			printHeader()

			// Get sorted data
			sorted := make([]queryCount, 0, len(counts))
			totalQueries := 0
			mu.Lock()
			lenCount := len(counts)
			for query, count := range counts {
				sorted = append(sorted, queryCount{query: query, count: count})
				totalQueries += count
			}
			mu.Unlock()

			sort.Slice(sorted, func(i, j int) bool {
				return sorted[i].count > sorted[j].count
			})

			// Stats summary
			printStats(totalQueries, lenCount, startTime)

			// Top queries table
			printTopQueries(sorted, totalQueries)

			// Footer
			printFooter()
		}
	}()

	// Main query execution loop
	for {
		query := qsdb.weights.GetRandomWeighted()
		mu.Lock()
		counts[query.Query]++
		mu.Unlock()
	}
}

func printHeader() {
	fmt.Printf("%s%s╔══════════════════════════════════════════════════════════════════════════════╗%s\n", Bold, Cyan, Reset)
	fmt.Printf("%s%s║                           MySQL Load Tester Report                          ║%s\n", Bold, Cyan, Reset)
	fmt.Printf("%s%s╚══════════════════════════════════════════════════════════════════════════════╝%s\n", Bold, Cyan, Reset)
	fmt.Println()
}

func printStats(totalQueries, uniqueQueries int, startTime time.Time) {
	duration := time.Since(startTime)
	qps := float64(totalQueries) / duration.Seconds()

	fmt.Printf("%s%s📊 Statistics:%s\n", Bold, Yellow, Reset)
	fmt.Printf("┌─────────────────────────────────────────────────────────────────────────────┐\n")
	fmt.Printf("│ %s%-20s%s │ %s%-50s%s │\n", Green, "Total Queries", Reset, Bold, fmt.Sprintf("%d", totalQueries), Reset)
	fmt.Printf("│ %s%-20s%s │ %s%-50s%s │\n", Green, "Unique Queries", Reset, Bold, fmt.Sprintf("%d", uniqueQueries), Reset)
	fmt.Printf("│ %s%-20s%s │ %s%-50s%s │\n", Green, "Runtime", Reset, Bold, duration.Round(time.Second).String(), Reset)
	fmt.Printf("│ %s%-20s%s │ %s%-50s%s │\n", Green, "Queries/Second", Reset, Bold, fmt.Sprintf("%.2f", qps), Reset)
	fmt.Printf("└─────────────────────────────────────────────────────────────────────────────┘\n")
	fmt.Println()
}

func printTopQueries(sorted []queryCount, totalQueries int) {
	fmt.Printf("%s%s🔥 Top 10 Most Executed Queries:%s\n", Bold, Purple, Reset)
	fmt.Printf("┌─────┬─────────────────────────────────────────────────────────────┬─────────┬─────────┐\n")
	fmt.Printf("│ %s%-3s%s │ %s%-59s%s │ %s%-7s%s │ %s%-7s%s │\n",
		Bold, "Rank", Reset,
		Bold, "Query Preview", Reset,
		Bold, "Count", Reset,
		Bold, "Percent", Reset)
	fmt.Printf("├─────┼─────────────────────────────────────────────────────────────┼─────────┼─────────┤\n")

	maxDisplay := min(10, len(sorted))
	for i, qc := range sorted[:maxDisplay] {
		query := truncateQuery(qc.query, 59)
		percentage := float64(qc.count) / float64(totalQueries) * 100

		// Color coding based on rank
		var rankColor string
		switch {
		case i == 0:
			rankColor = Red + Bold // Gold for #1
		case i < 3:
			rankColor = Yellow + Bold // Silver for top 3
		case i < 5:
			rankColor = Green // Bronze for top 5
		default:
			rankColor = White
		}

		fmt.Printf("│ %s%-3d%s │ %s%-59s%s │ %s%7d%s │ %s%6.2f%%%s │\n",
			rankColor, i+1, Reset,
			Dim, query, Reset,
			Blue+Bold, qc.count, Reset,
			Green+Bold, percentage, Reset)
	}

	fmt.Printf("└─────┴─────────────────────────────────────────────────────────────┴─────────┴─────────┘\n")
	fmt.Println()
}

func printFooter() {
	fmt.Printf("%s%s💡 Live updating every second... Press Ctrl+C to stop%s\n", Dim, Yellow, Reset)
	fmt.Printf("%s%s⚡ Report generated at: %s%s\n", Dim, Cyan, time.Now().Format("2006-01-02 15:04:05"), Reset)
}

func truncateQuery(query string, maxLen int) string {
	// Clean up the query - remove extra whitespace and newlines
	cleaned := strings.ReplaceAll(strings.TrimSpace(query), "\n", " ")
	cleaned = strings.ReplaceAll(cleaned, "\t", " ")

	// Remove multiple spaces
	for strings.Contains(cleaned, "  ") {
		cleaned = strings.ReplaceAll(cleaned, "  ", " ")
	}

	if len(cleaned) <= maxLen {
		return cleaned
	}
	return cleaned[:maxLen-3] + "..."
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
