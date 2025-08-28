package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"
)

type QuerySourceFileConfig struct {
	InputFile string `mapstructure:"input_file" yaml:"input_file" validate:"required"`
}

type queryInfo struct {
	offset int
	length int
}

type QuerySourceFile struct {
	cfg *QuerySourceFileConfig

	dataBuffer []byte

	queryInfos []queryInfo

	fingerprintIndex map[uint64][]int

	fingerprintWeights *QueryFingerprintWeights

	perfStats QuerySourceFileInternalPerfStats
	mu        sync.RWMutex
	initOnce  func() error
}

type QuerySourceFileInternalPerfStats struct {
	InitLatency        time.Duration
	QueriesLoaded      int
	UniqueFingerprints int
}

func NewQuerySourceFile(cfg *QuerySourceFileConfig) (*QuerySourceFile, error) {
	qsf := &QuerySourceFile{
		cfg:                cfg,
		fingerprintIndex:   make(map[uint64][]int),
		queryInfos:         make([]queryInfo, 0, 1000000),
		fingerprintWeights: NewQueryFingerprintWeights(),
	}
	return qsf, nil
}

func (qsf *QuerySourceFile) Init(ctx context.Context) error {
	qsf.initOnce = sync.OnceValue(func() error {
		startTime := time.Now()
		logger.Info().Str("file", qsf.cfg.InputFile).Msg("Initializing QuerySourceFile: loading and indexing binary cache...")

		file, err := os.Open(qsf.cfg.InputFile)
		if err != nil {
			return fmt.Errorf("failed to open binary cache file: %w", err)
		}
		defer file.Close()

		qsf.dataBuffer, err = io.ReadAll(file)
		if err != nil {
			return fmt.Errorf("failed to read binary cache file into memory: %w", err)
		}

		cursor := 0
		fingerprintCounts := make(map[uint64]int)
		totalQueries := 0

		for cursor < len(qsf.dataBuffer) {
			if cursor+4 > len(qsf.dataBuffer) {
				return fmt.Errorf("incomplete data at offset %d: not enough bytes for query length", cursor)
			}
			queryLength := int(binary.LittleEndian.Uint32(qsf.dataBuffer[cursor : cursor+4]))
			cursor += 4

			queryOffset := cursor
			hashOffset := queryOffset + queryLength

			if hashOffset+8 > len(qsf.dataBuffer) {
				return fmt.Errorf("incomplete data at offset %d: not enough bytes for query and hash", cursor-4)
			}
			fingerprintHash := binary.LittleEndian.Uint64(qsf.dataBuffer[hashOffset : hashOffset+8])

			if queryLength > 0 {
				queryIndex := len(qsf.queryInfos)
				qsf.queryInfos = append(qsf.queryInfos, queryInfo{
					offset: queryOffset,
					length: queryLength,
				})

				qsf.fingerprintIndex[fingerprintHash] = append(qsf.fingerprintIndex[fingerprintHash], queryIndex)
				fingerprintCounts[fingerprintHash]++
				totalQueries++
			}

			cursor = hashOffset + 8
		}

		if totalQueries == 0 {
			return fmt.Errorf("no valid queries found in the binary cache file")
		}

		for hash, count := range fingerprintCounts {
			weight := float64(count) / float64(totalQueries)
			qsf.fingerprintWeights.Add(
				weight,
				&QueryFingerprintData{Hash: hash},
			)
		}

		qsf.perfStats.InitLatency = time.Since(startTime)
		qsf.perfStats.QueriesLoaded = totalQueries
		qsf.perfStats.UniqueFingerprints = len(qsf.fingerprintIndex)

		logger.Info().
			Dur("duration", qsf.perfStats.InitLatency).
			Int("queries_loaded", qsf.perfStats.QueriesLoaded).
			Int("unique_fingerprints", qsf.perfStats.UniqueFingerprints).
			Msg("Binary cache loaded and indexed successfully.")

		return nil
	})
	return qsf.initOnce()
}

func (qsf *QuerySourceFile) Destroy() error {
	return nil
}

func (qsf *QuerySourceFile) PerfStats() any {
	qsf.mu.RLock()
	defer qsf.mu.RUnlock()
	return qsf.perfStats
}

func (qsf *QuerySourceFile) GetRandomWeightedQuery(ctx context.Context) (*QueryDataSourceResult, error) {
	fingerprintData := qsf.fingerprintWeights.GetRandomWeighted()
	if fingerprintData == nil {
		return nil, fmt.Errorf("failed to get random weighted fingerprint")
	}
	fingerprintHash := fingerprintData.Hash

	queryIndices, ok := qsf.fingerprintIndex[fingerprintHash]
	if !ok || len(queryIndices) == 0 {
		return nil, fmt.Errorf("no query indices found for fingerprint hash: %d", fingerprintHash)
	}

	randomIndex := queryIndices[rand.Intn(len(queryIndices))]
	info := qsf.queryInfos[randomIndex]

	if info.offset+info.length > len(qsf.dataBuffer) || info.length <= 0 {
		return nil, fmt.Errorf("invalid query info: offset=%d, length=%d, buffer_size=%d", info.offset, info.length, len(qsf.dataBuffer))
	}

	queryBytes := qsf.dataBuffer[info.offset : info.offset+info.length]

	return &QueryDataSourceResult{
		Query: string(queryBytes),
		// Fingerprint: "",
	}, nil
}
