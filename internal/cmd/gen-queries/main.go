package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
)

const (
	outputFile = "queries.txt"
	numQueries = 1000000
)

func main() {
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	fmt.Printf("Creating %d unique queries into file %s...\n", numQueries, outputFile)

	categories := []string{"electronics", "books", "clothing", "home", "toys", "sports", "automotive"}
	statuses := []string{"shipped", "pending", "delivered", "cancelled", "returned"}
	logLevels := []string{"INFO", "WARN", "ERROR", "DEBUG", "CRITICAL"}

	startTime := time.Now()

	for i := 1; i <= numQueries; i++ {
		now := time.Now().UTC()
		nanoseconds := rand.Intn(1_000_000_000)
		fullTimestamp := fmt.Sprintf("%s.%09d UTC", now.Format("Jan 02, 2006 15:04:05"), nanoseconds)

		var query string
		queryType := i % 5

		switch queryType {
		case 0:
			query = fmt.Sprintf("SELECT user_name, email, last_login FROM users WHERE id = %d;", i)
		case 1:
			category := categories[i%len(categories)]
			query = fmt.Sprintf("SELECT product_name, price FROM products WHERE category = '%s' AND product_sku = 'SKU-%d-A';", category, i)
		case 2:
			status := statuses[i%len(statuses)]
			query = fmt.Sprintf("UPDATE orders SET status = '%s', updated_at = NOW() WHERE id = %d;", status, i)
		case 3:
			level := logLevels[i%len(logLevels)]
			eventID := fmt.Sprintf("evt-%d-%09d", i, nanoseconds)
			query = fmt.Sprintf("SELECT COUNT(*) FROM logs WHERE level = '%s' AND event_id = '%s';", level, eventID)
		case 4:
			token := uuid.New()
			query = fmt.Sprintf("INSERT INTO sessions (user_id, session_token, ip_address) VALUES (%d, '%s', '192.168.1.1');", i, token)
		}

		fmt.Fprintf(writer, "%s\t%s\n", fullTimestamp, query)
	}

	fmt.Printf("Done! File %s with %d lines created in %v.\n", outputFile, numQueries, time.Since(startTime))
}
