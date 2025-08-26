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
	file, ok := i.reader.(io.ReadSeeker)
	if !ok {
		return fmt.Errorf("reader must be io.ReadSeeker to track offset")
	}

	br := bufio.NewReader(file)
	var offset int64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			lineStart := offset

			line, err := br.ReadBytes('\n')
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("error reading file: %w", err)
			}

			lineLen := len(line)
			offset += int64(lineLen)

			q, parseErr := i.parseTsharkTxtLine(line)
			if parseErr != nil {
				fmt.Fprintf(os.Stderr, "error parsing line, skipping: %v\n", parseErr)
				continue
			}

			// q.Raw = nil
			q.Offset = uint64(lineStart)
			q.Length = uint64(lineLen)

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
	rawQuery := bytes.TrimSpace(tabsSeparated[1])

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

	return &query.Query{
		Timestamp: uint64(parsedTime.Unix()),
		Raw:       rawQuery,
	}, nil
}
