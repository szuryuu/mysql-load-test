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

	writer, err := common.WrapWriter(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("error wrapping writer: %w", err)
	}
	closers := []io.Closer{file}

	bufioWriter := bufio.NewWriterSize(writer, 1024*1024)

	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}

	return &OutputCache{
		cfg:     cfg,
		writer:  bufioWriter,
		closers: closers,
		bufferPool: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, 4096)
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
		if q == nil || len(q.Raw) == 0 {
			continue
		}

		queryLength := uint32(len(q.Raw))
		totalSize := 4 + len(q.Raw) + 8

		bufPtr := o.bufferPool.Get().(*[]byte)
		buf := *bufPtr
		defer o.bufferPool.Put(bufPtr)

		if cap(buf) < totalSize {
			buf = make([]byte, totalSize)
			*bufPtr = buf
		}
		buf = buf[:totalSize]

		binary.LittleEndian.PutUint32(buf[0:4], queryLength)

		copy(buf[4:], q.Raw)

		offset := 4 + len(q.Raw)
		binary.LittleEndian.PutUint64(buf[offset:], q.FingerprintHash)

		if _, err := o.writer.Write(buf); err != nil {
			return fmt.Errorf("error writing query data: %w", err)
		}
	}

	return nil
}
