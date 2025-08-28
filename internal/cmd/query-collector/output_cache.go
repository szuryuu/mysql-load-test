package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"mysql-load-test/pkg/query"
	"os"
	"sync"
)

type OutputCacheConfig struct {
	File           string `json:"file"`
	BatchSize      int    `json:"batch_size"`
	MaxConcurrency int
}

type OutputCache struct {
	cfg        OutputCacheConfig
	closers    []io.Closer
	writer     *bufio.Writer
	bufferPool *sync.Pool
}

func NewCacheOutput(cfg OutputCacheConfig, common *OutputCommon) (*OutputCache, error) {
	file, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}

	writer := file
	closers := []io.Closer{file}

	bufioWriter := bufio.NewWriterSize(writer, 1024*1024)

	return &OutputCache{
		cfg:     cfg,
		writer:  bufioWriter,
		closers: closers,
		bufferPool: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, 32)
				return &b
			},
		},
	}, nil
}

func (o *OutputCache) Destroy() error {
	if err := o.writer.Flush(); err != nil {
		for _, closer := range o.closers {
			closer.Close()
		}
		return fmt.Errorf("error flushing buffer: %w", err)
	}

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
	defer o.writer.Flush()

	for q := range inQueryChan {
		if q == nil {
			continue
		}

		totalSize := 32

		bufPtr := o.bufferPool.Get().(*[]byte)
		buf := *bufPtr
		defer o.bufferPool.Put(bufPtr)

		binary.LittleEndian.PutUint64(buf[0:8], q.Hash)
		binary.LittleEndian.PutUint64(buf[8:16], q.FingerprintHash)
		binary.LittleEndian.PutUint64(buf[16:24], q.Offset)
		binary.LittleEndian.PutUint64(buf[24:32], q.Length)

		if _, err := o.writer.Write(buf[:totalSize]); err != nil {
			return fmt.Errorf("error writing query data: %w", err)
		}
	}

	return nil
}
