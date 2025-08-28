package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"mysql-load-test/internal/cmd/dbloader"
	"mysql-load-test/pkg/query"
)

func main() {
	dbCfg := dbloader.OutputDBConfig{
		Host:      "127.0.0.1",
		Port:      13306,
		User:      "root",
		Password:  "root",
		DBName:    "MySQLLoadTester",
		Truncate:  true,
		BatchSize: 10000,
	}

	outputDB, err := dbloader.NewDBOutput(dbCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating DB output: %v\n", err)
		os.Exit(1)
	}
	defer outputDB.Destroy()

	file, err := os.Open("queries.bin")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening cache file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	inQueryChan := make(chan *query.Query, 10000)

	go func() {
		defer close(inQueryChan)
		reader := bufio.NewReader(file)
		for {
			var q query.Query

			if err := binary.Read(reader, binary.LittleEndian, &q.Hash); err != nil {
				if err != io.EOF {
					fmt.Fprintf(os.Stderr, "Error reading hash from cache: %v\n", err)
				}
				break
			}
			if err := binary.Read(reader, binary.LittleEndian, &q.FingerprintHash); err != nil {
				break
			}
			if err := binary.Read(reader, binary.LittleEndian, &q.Offset); err != nil {
				break
			}
			if err := binary.Read(reader, binary.LittleEndian, &q.Length); err != nil {
				break
			}

			inQueryChan <- &q
		}
	}()

	fmt.Println("Starting to load queries from cache into database...")
	if err := outputDB.StartOutput(context.Background(), inQueryChan); err != nil {
		fmt.Fprintf(os.Stderr, "Error loading data: %v\n", err)
		os.Exit(1)
	}
}
