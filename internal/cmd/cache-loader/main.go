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
			var queryLength uint32
			if err := binary.Read(reader, binary.LittleEndian, &queryLength); err != nil {
				if err != io.EOF {
					fmt.Fprintf(os.Stderr, "Error reading length: %v\n", err)
				}
				break
			}

			rawQuery := make([]byte, queryLength)
			if _, err := io.ReadFull(reader, rawQuery); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading query: %v\n", err)
				break
			}

			var fingerprintHash uint64
			if err := binary.Read(reader, binary.LittleEndian, &fingerprintHash); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading hash: %v\n", err)
				break
			}

			inQueryChan <- &query.Query{
				Raw:             rawQuery,
				FingerprintHash: fingerprintHash,
				Hash:            fingerprintHash,
				Offset:          0,
				Length:          0,
			}
		}
	}()

	fmt.Println("Starting to load queries from cache into database...")
	if err := outputDB.StartOutput(context.Background(), inQueryChan); err != nil {
		fmt.Fprintf(os.Stderr, "Error loading data: %v\n", err)
		os.Exit(1)
	}
}
