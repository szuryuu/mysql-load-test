package main

import (
	"context"
	"fmt"
	"mysql-load-test/pkg/query"
	"sort"
	"strings"
)

type OutputStats struct {
	queryCounts       map[string]int
	fingerprintCounts map[string]int
}

func NewOutputStats() *OutputStats {
	return &OutputStats{
		queryCounts:       make(map[string]int),
		fingerprintCounts: make(map[string]int),
	}
}

func (o *OutputStats) StartOutput(ctx context.Context, inQueryChan <-chan *query.Query) error {
	for q := range inQueryChan {
		o.queryCounts[string(q.Raw)]++
		o.fingerprintCounts[string(q.Fingerprint)]++
	}
	o.printStats()
	return nil
}

type queryCount struct {
	query string
	count int
}

func (o *OutputStats) printStats() {
	// Convert maps to slices for sorting
	qcSlice := make([]queryCount, 0, len(o.queryCounts))
	for k, v := range o.queryCounts {
		qcSlice = append(qcSlice, queryCount{query: k, count: v})
	}

	fcSlice := make([]queryCount, 0, len(o.fingerprintCounts))
	for k, v := range o.fingerprintCounts {
		fcSlice = append(fcSlice, queryCount{query: k, count: v})
	}

	// Sort by count (descending)
	sort.Slice(qcSlice, func(i, j int) bool {
		return qcSlice[i].count > qcSlice[j].count
	})

	sort.Slice(fcSlice, func(i, j int) bool {
		return fcSlice[i].count > fcSlice[j].count
	})

	// Print Query Counts table
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("TOP QUERY COUNTS")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("%-8s | %-65s\n", "COUNT", "QUERY")
	fmt.Println(strings.Repeat("-", 80))

	limit := 10
	if len(qcSlice) < limit {
		limit = len(qcSlice)
	}

	for i := 0; i < limit; i++ {
		query := qcSlice[i].query
		// Truncate long queries and clean up whitespace
		query = strings.ReplaceAll(query, "\n", " ")
		query = strings.ReplaceAll(query, "\t", " ")
		// Replace multiple spaces with single space
		for strings.Contains(query, "  ") {
			query = strings.ReplaceAll(query, "  ", " ")
		}
		query = strings.TrimSpace(query)

		if len(query) > 120 {
			query = query[:120] + "..."
		}

		fmt.Printf("%-8d | %-65s\n", qcSlice[i].count, query)
	}

	// Print Fingerprint Counts table
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("TOP FINGERPRINT COUNTS")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("%-8s | %-65s\n", "COUNT", "FINGERPRINT")
	fmt.Println(strings.Repeat("-", 80))

	limit = 10
	if len(fcSlice) < limit {
		limit = len(fcSlice)
	}

	for i := 0; i < limit; i++ {
		fingerprint := fcSlice[i].query
		// Clean up whitespace
		fingerprint = strings.ReplaceAll(fingerprint, "\n", " ")
		fingerprint = strings.ReplaceAll(fingerprint, "\t", " ")
		// Replace multiple spaces with single space
		for strings.Contains(fingerprint, "  ") {
			fingerprint = strings.ReplaceAll(fingerprint, "  ", " ")
		}
		fingerprint = strings.TrimSpace(fingerprint)

		if len(fingerprint) > 120 {
			fingerprint = fingerprint[:120] + "..."
		}

		fmt.Printf("%-8d | %-65s\n", fcSlice[i].count, fingerprint)
	}

	// Print summary
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("SUMMARY: %d unique queries, %d unique fingerprints\n", len(qcSlice), len(fcSlice))
	fmt.Println(strings.Repeat("=", 80))
}

func (o *OutputStats) Concurrency() OutputConcurrencyInfo {
	return OutputConcurrencyInfo{
		MaxConcurrency:     0,
		CurrentConcurrency: 0,
	}
}

func (o *OutputStats) Destroy() error {
	return nil
}
