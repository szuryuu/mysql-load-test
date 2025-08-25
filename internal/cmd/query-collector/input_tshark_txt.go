package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"mysql-load-test/pkg/query"
)

type InputTsharkTxtConfig struct {
	File string
}

type InputTsharkTxt struct {
	cfg     InputTsharkTxtConfig
	reader  io.Reader
	closers []io.Closer
	common  *InputCommon
}

func NewInputTsharkTxt(cfg InputTsharkTxtConfig, common *InputCommon) (*InputTsharkTxt, error) {
	file, err := os.Open(cfg.File)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}

	closers := []io.Closer{file}

	r, err := common.WrapReader(file)
	if err != nil {
		return nil, fmt.Errorf("error wrapping reader: %w", err)
	}

	return &InputTsharkTxt{
		cfg:     cfg,
		reader:  r,
		closers: closers,
		common:  common,
	}, nil
}

func (i *InputTsharkTxt) StartExtractor(ctx context.Context, outChan chan<- *query.Query) error {
	return i.extractQueries(ctx, outChan)
}

func (i *InputTsharkTxt) Destroy() error {
	var errs []error

	for _, closer := range i.closers {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("error closing input pcap: %w", errs[0])
	}

	return nil
}

func (i *InputTsharkTxt) extractQueries(ctx context.Context, outChan chan<- *query.Query) error {
	var r = bufio.NewScanner(i.reader)
	buf := make([]byte, 5*1024*1024)
	r.Buffer(buf, cap(buf))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if !r.Scan() {
				if err := r.Err(); err != nil {
					return fmt.Errorf("error scanning file: %w", err)
				}
				return nil
			}

			line := r.Bytes()
			lineCopy := make([]byte, len(line))
			copy(lineCopy, line)

			q, parseErr := i.parseTsharkTxtLine(lineCopy)
			if parseErr != nil {
				return fmt.Errorf("error parsing line: %w", parseErr)
			}
			outChan <- q
		}
	}
}

var (
	tsharkTimestampLayouts = []string{
		"Jan 2, 2006 15:04:05.000000000 MST",
		"jan 2, 2006 15:04:05.000000000 mst",
	}
)

func (i *InputTsharkTxt) parseTsharkTxtLine(line []byte) (*query.Query, error) {
	// Example line:
	// Jun 23, 2025 10:20:26.262728119 UTC     set session sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'
	tabsSeparated := bytes.Split(line, []byte("\t"))
	if len(tabsSeparated) != 2 {
		return nil, fmt.Errorf("invalid line format: %s", line)
	}

	timestamp := string(tabsSeparated[0])

	var parsedTime time.Time
	var parseErr error
	for _, layout := range tsharkTimestampLayouts {
		parsedTime, parseErr = time.Parse(layout, timestamp)
		if parseErr == nil {
			break
		}
	}
	if parseErr != nil {
		return nil, fmt.Errorf("error parsing timestamp: %w", parseErr)
	}

	q := tabsSeparated[1]

	return &query.Query{
		Raw:       q,
		Timestamp: uint64(parsedTime.Unix()),
	}, nil
}
