package main

// package main

// import (
// 	"context"
// 	"fmt"
// 	"io"
// 	"os"

// 	"mysql-load-test/pkg/query"
// )

// type InputCacheConfig struct {
// 	File       string
// 	ImportName string
// }

// type InputCache struct {
// 	cfg     InputCacheConfig
// 	reader  io.Reader
// 	closers []io.Closer
// 	common  *InputCommon
// }

// func NewInputCache(cfg InputCacheConfig, common *InputCommon) (*InputCache, error) {
// 	file, err := os.Open(cfg.File)
// 	if err != nil {
// 		return nil, fmt.Errorf("error opening file: %w", err)
// 	}
// 	closers := []io.Closer{file}

// 	r, err := common.WrapReader(file)
// 	if err != nil {
// 		return nil, fmt.Errorf("error wrapping reader: %w", err)
// 	}

// 	return &InputCache{
// 		cfg:     cfg,
// 		reader:  r,
// 		common:  common,
// 		closers: closers,
// 	}, nil
// }

// func (i *InputCache) StartExtractor(ctx context.Context, outChan chan<- *query.Query) error {
// 	return i.extractQueriesFromCache(ctx, outChan)
// }

// func (i *InputCache) Destroy() error {
// 	var errs []error

// 	for _, closer := range i.closers {
// 		if err := closer.Close(); err != nil {
// 			errs = append(errs, err)
// 		}
// 	}

// 	if len(errs) > 0 {
// 		return fmt.Errorf("error closing input cache: %w", errs[0])
// 	}

// 	return nil
// }

// func (i *InputCache) extractQueriesFromCache(ctx context.Context, outChan chan<- *query.Query) error {
// 	i := 0
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		default:
// 			n, q := query.UnmarshalBinary()
// 			outChan <- &q
// 		}
// 	}

// 	return nil
// }
