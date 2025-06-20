package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"

	"mysql-load-test/pkg/query"

	"github.com/DataDog/zstd"
)

type OutputConcurrencyInfo struct {
	MaxConcurrency     int
	CurrentConcurrency int
}

type Output interface {
	StartOutput(ctx context.Context, _ <-chan *query.Query) error
	Destroy() error
	Concurrency() OutputConcurrencyInfo
}

type OutputCommonConfig struct {
	Encoding string
	Type     string
}

type OutputCommon struct {
	cfg OutputCommonConfig
}

func NewOutputCommon(cfg OutputCommonConfig) *OutputCommon {
	return &OutputCommon{
		cfg: cfg,
	}
}

func (o *OutputCommon) WrapWriter(w io.Writer) (io.Writer, error) {

	var writer io.Writer

	switch o.cfg.Encoding {
	case "gzip":
		writer = gzip.NewWriter(w)
	case "zstd":
		writer = zstd.NewWriter(w)
	case "plain":
		writer = w
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", o.cfg.Encoding)
	}

	return writer, nil
}
