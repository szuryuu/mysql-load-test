package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"mysql-load-test/pkg/query"
)

type OutputCacheConfig struct {
	File           string `json:"file"`
	BatchSize      int    `json:"batch_size"`
	MaxConcurrency int
}

type OutputCache struct {
	cfg     OutputCacheConfig
	closers []io.Closer
	writer  *bufio.Writer
}

func NewCacheOutput(cfg OutputCacheConfig, common *OutputCommon) (*OutputCache, error) {
	file, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}

	writer, err := common.WrapWriter(file)
	if err != nil {
		return nil, fmt.Errorf("error wrapping writer: %w", err)
	}
	closers := []io.Closer{file}

	bufioWriter := bufio.NewWriter(writer)

	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}

	return &OutputCache{
		cfg:     cfg,
		writer:  bufioWriter,
		closers: closers,
	}, nil
}

func (o *OutputCache) Destroy() error {
	for _, closer := range o.closers {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("error closing output cache: %w", err)
		}
	}
	return nil
}

func (o *OutputCache) Concurrency() OutputConcurrencyInfo {
	return OutputConcurrencyInfo{
		MaxConcurrency:     0,
		CurrentConcurrency: 0,
	}
}

func (o *OutputCache) StartOutput(ctx context.Context, inQueryChan <-chan *query.Query) error {
	var buf []byte
	for q := range inQueryChan {
		querySize := q.GetSize()
		if buf == nil || len(buf) < querySize {
			buf = make([]byte, querySize)
		}
		var err error
		var n int
		n, err = q.MarshalBinary(buf)
		if err != nil {
			return fmt.Errorf("error marshaling query: %w", err)
		}
		// buf[n] = '\n'
		if _, err := o.writer.Write(buf[:n]); err != nil {
			return fmt.Errorf("error writing lines: %w", err)
		}
		// fmt.Println(string(buf[:n]))
	}
	return nil
}
