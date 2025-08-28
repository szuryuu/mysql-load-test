package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"

	"mysql-load-test/pkg/query"

	"github.com/DataDog/zstd"
)

type Input interface {
	StartExtractor(context.Context, chan<- *query.Query) error
	Destroy() error
}

type InputCommonConfig struct {
	Encoding string
	Type     string
}

type InputCommon struct {
	cfg InputCommonConfig
}

func NewInputCommon(cfg InputCommonConfig) *InputCommon {
	return &InputCommon{
		cfg: cfg,
	}
}

func (i *InputCommon) WrapReader(r io.Reader) (io.Reader, error) {
	var reader io.Reader

	switch i.cfg.Encoding {
	case "gzip":
		gzReader, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("error creating gzip reader: %w", err)
		}
		reader = gzReader
	case "zstd":
		zstdReader := zstd.NewReader(r)
		reader = zstdReader
	case "raw", "plain":
		reader = r
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", i.cfg.Encoding)
	}

	return reader, nil
}
