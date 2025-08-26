package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"mysql-load-test/pkg/query"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
)

type InputPcapConfig struct {
	File string
}

type InputPcap struct {
	cfg     InputPcapConfig
	reader  io.Reader
	closers []io.Closer
	common  *InputCommon
}

func NewInputPcap(cfg InputPcapConfig, common *InputCommon) (*InputPcap, error) {
	file, err := os.Open(cfg.File)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}

	closers := []io.Closer{file}

	r, err := common.WrapReader(file)
	if err != nil {
		return nil, fmt.Errorf("error wrapping reader: %w", err)
	}

	return &InputPcap{
		cfg:     cfg,
		reader:  r,
		closers: closers,
		common:  common,
	}, nil
}

func (i *InputPcap) StartExtractor(ctx context.Context, outChan chan<- *query.Query) error {
	return i.extractQueriesFromPcap(ctx, outChan)
}

func (i *InputPcap) Destroy() error {
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

func (i *InputPcap) extractQueriesFromPcap(ctx context.Context, outChan chan<- *query.Query) error {
	file, ok := i.reader.(io.ReadSeeker)
	if !ok {
		return fmt.Errorf("reader must be io.ReadSeeker to get offsets")
	}

	pcapReader, err := pcapgo.NewReader(i.reader)
	if err != nil {
		return fmt.Errorf("error creating pcapgo reader: %w", err)
	}

	var offset int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			posBefore, _ := file.Seek(0, io.SeekCurrent)

			pktBytes, ci, err := pcapReader.ReadPacketData()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("error reading packet: %w", err)
			}

			posAfter, _ := file.Seek(0, io.SeekCurrent)
			offset = posBefore
			length := posAfter - posBefore

			if ci.CaptureLength < ci.Length {
				continue
			}

			newPkt := gopacket.NewPacket(pktBytes, pcapReader.LinkType(), gopacket.Default)
			appLayer := newPkt.ApplicationLayer()
			if appLayer == nil {
				continue
			}

			payload := appLayer.Payload()
			if len(payload) < 5 {
				continue
			}
			if payload[4] != 0x03 {
				continue
			}

			outChan <- &query.Query{
				Offset:    uint64(offset),
				Length:    uint64(length),
				Timestamp: uint64(ci.Timestamp.Unix()),
			}
		}
	}
}
