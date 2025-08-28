package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"mysql-load-test/internal/lrucache"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"golang.org/x/exp/mmap"
)

type QuerySourceDBConfig struct {
	DSN                     string `mapstructure:"dsn" yaml:"dsn" validate:"required"`
	FingerprintWeightsQuery string `mapstructure:"fingerprint_weights_query" yaml:"fingerprint_weights_query" validate:"omitempty"`
	QueriesFetchQuery       string `mapstructure:"queries_fetch_query" yaml:"queries_fetch_query" validate:"omitempty"`
	QueriesIdsFetchQuery    string `mapstructure:"queries_ids_fetch_query" yaml:"queries_ids_fetch_query" validate:"omitempty"`
	InputFile               string `mapstructure:"input_file" yaml:"input_file" validate:"required"`
}

type QuerySourceDB struct {
	cfg *QuerySourceDBConfig

	// Each fingerprint hash has cache to cache query by offset.
	queriesCaches map[uint64]*lrucache.LRUCache[int, *QueryDataSourceResult]

	fingerprintWeights *QueryFingerprintWeights

	// Map of fingerprint hash to query id
	queryIds map[uint64][]int

	queriesCountTotal uint64
	db                *DBConn
	perfStats         *QuerySourceDBInternalPerfStats
	mu                sync.RWMutex

	initOnce         func() error
	fetchWeightsOnce func() error
	fetchIdsOnce     func() error

	concurrency int

	mmapReader *mmap.ReaderAt
	mmapData   []byte
}

type FileOffsetResult struct {
	FileOffset uint64
	FileLength uint64
}

func executeTemplate(tmpl string, data any, templateName string) (string, error) {
	tmplParsed, err := template.New(templateName).Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	if err := tmplParsed.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func NewQuerySourceDB(cfg *QuerySourceDBConfig, concurrency int, fingerprintWeights *QueryFingerprintWeights) (*QuerySourceDB, error) {
	qsdb := &QuerySourceDB{
		fingerprintWeights: fingerprintWeights,
		cfg:                cfg,
		perfStats: &QuerySourceDBInternalPerfStats{
			QueriesFetchTotal: 0,
		},
		concurrency: concurrency,
	}

	queriesCaches := make(map[uint64]*lrucache.LRUCache[int, *QueryDataSourceResult])
	qsdb.queriesCaches = queriesCaches

	return qsdb, nil
}

func (qsdb *QuerySourceDB) fetchWeights(ctx context.Context) error {
	if qsdb.fingerprintWeights != nil {
		return nil
	}

	qsdb.fingerprintWeights = NewQueryFingerprintWeights()

	fetchFingerprintWeightsQuery := qsdb.cfg.FingerprintWeightsQuery

	if fetchFingerprintWeightsQuery == "" {
		return fmt.Errorf("no fetch fingerprint weight query specified")
	}

	rows, err := qsdb.db.QueryContext(ctx, fetchFingerprintWeightsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var hash uint64
		var count, total int64
		var weight float64

		// expect: `Hash`, `Count`, Total, Weight
		if err := rows.Scan(&hash, &count, &total, &weight); err != nil {
			return err
		}
		qsdb.fingerprintWeights.Add(
			fmt.Sprintf("%d", hash),
			weight,
			&QueryFingerprintData{
				Hash:      hash,
				FreqTotal: count,
			},
		)

	}

	if qsdb.fingerprintWeights.totalWeight == 0 {
		return fmt.Errorf("no query weights were loaded from the database, ensure the QueryFingerprint table is populated")
	}

	return nil

}

func (qsdb *QuerySourceDB) fetchQueryIds(ctx context.Context) error {
	if qsdb.queryIds != nil {
		return nil
	}

	qsdb.queryIds = make(map[uint64][]int)

	fetchIdsQuery := qsdb.cfg.QueriesIdsFetchQuery

	if fetchIdsQuery == "" {
		return fmt.Errorf("no fetch query ids query specified")
	}

	rows, err := qsdb.db.QueryContext(ctx, fetchIdsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var hash uint64
		// expect: ID, Hash
		if err := rows.Scan(&id, &hash); err != nil {
			return err
		}
		_, ok := qsdb.queryIds[hash]
		if !ok {
			qsdb.queryIds[hash] = make([]int, 0, 1)
		}
		qsdb.queryIds[hash] = append(qsdb.queryIds[hash], id)
	}

	return nil

}

func (qsdb *QuerySourceDB) Init(ctx context.Context) error {
	qsdb.fetchWeightsOnce = sync.OnceValue(func() error {
		return qsdb.fetchWeights(ctx)
	})
	qsdb.fetchIdsOnce = sync.OnceValue(func() error {
		return qsdb.fetchQueryIds(ctx)
	})

	qsdb.initOnce = sync.OnceValue(func() error {
		logger.Info().Str("file", qsdb.cfg.InputFile).Msg("Memory mapping the input file")
		reader, err := mmap.Open(qsdb.cfg.InputFile)
		if err != nil {
			return fmt.Errorf("failed to memory-map input file: %w", err)
		}
		qsdb.mmapReader = reader

		_, err = os.Stat(qsdb.cfg.InputFile)
		if err != nil {
			return fmt.Errorf("failed to get file info: %w", err)
		}
		// qsdb.mmapData = make([]byte, fileInfo.Size())
		// _, err = qsdb.mmapReader.ReadAt(qsdb.mmapData, 0)
		// if err != nil {
		// 	return fmt.Errorf("failed to read mmap data into slice: %w", err)
		// }
		logger.Info().Msg("Input file successfully memory-mapped")

		logger.Info().Msg("Opening database connection for query data source DB")
		db := NewDBConn(RetryConfig{
			MaxRetries:    3,
			InitialDelay:  100 * time.Millisecond,
			MaxDelay:      5 * time.Second,
			BackoffFactor: 2.0,
		})
		err = db.Open(qsdb.cfg.DSN, qsdb.concurrency)
		if err != nil {
			return fmt.Errorf("error opening database: %w", err)
		}
		qsdb.db = db

		logger.Info().Msg("Fetching query weights from the database")
		a := time.Now()
		err = qsdb.fetchWeightsOnce()
		if err != nil {
			err = fmt.Errorf("error fetching weights: %w", err)
		}
		fetchLat := time.Since(a)
		qsdb.perfStats.FetchWeightsLat = fetchLat

		logger.Info().Msg("Fetching query ids from the database")
		a = time.Now()
		err = qsdb.fetchIdsOnce()
		if err != nil {
			err = fmt.Errorf("error fetching ids: %w", err)
		}
		fetchIdsLat := time.Since(a)
		qsdb.perfStats.FetchIdsLat = fetchIdsLat

		return nil
	})
	return qsdb.initOnce()
}

func (qsdb *QuerySourceDB) Destroy() error {
	if qsdb.mmapReader != nil {
		qsdb.mmapReader.Close()
	}
	if qsdb.db != nil {
		return qsdb.db.Close()
	}
	return nil
}

func (qsdb *QuerySourceDB) PerfStats() any {
	qsdb.mu.RLock()
	defer qsdb.mu.RUnlock()
	cacheStats := lrucache.LRUCacheStats{}
	for _, queriesCache := range qsdb.queriesCaches {
		cacheStats.HitsTotal += queriesCache.Stats().HitsTotal
		cacheStats.MissesTotal += queriesCache.Stats().MissesTotal
		cacheStats.EvictionsTotal += queriesCache.Stats().EvictionsTotal
		cacheStats.MoveToFrontTotal += queriesCache.Stats().MoveToFrontTotal
		cacheStats.NewItemsTotal += queriesCache.Stats().NewItemsTotal
	}
	qsdb.perfStats.CacheStats = cacheStats
	return *qsdb.perfStats
}

func (qsdb *QuerySourceDB) GetRandomWeightedQuery(ctx context.Context) (*QueryDataSourceResult, error) {
	fingerprintData := qsdb.fingerprintWeights.GetRandomWeighted()
	if fingerprintData == nil {
		return nil, fmt.Errorf("failed to get random weighted fingerprint")
	}

	fingerprintHash := fingerprintData.Hash

	queryIds, ok := qsdb.queryIds[fingerprintHash]
	if !ok || len(queryIds) == 0 {
		return nil, fmt.Errorf("no query IDs found for fingerprint hash: %d", fingerprintHash)
	}

	queryId := queryIds[rand.Intn(len(queryIds))]

	qsdb.mu.RLock()
	queryCache, cacheExists := qsdb.queriesCaches[fingerprintHash]
	qsdb.mu.RUnlock()

	if cacheExists {
		if queryResult, found := queryCache.Get(queryId); found {
			qsdb.perfStats.QueriesFetchTotal++
			return queryResult, nil
		}
	}

	var offset, length uint64

	fetchQuery, err := executeTemplate(qsdb.cfg.QueriesFetchQuery, map[string]any{"ID": queryId}, "queries_fetch_query")
	if err != nil {
		return nil, fmt.Errorf("failed to create query fetch template: %w", err)
	}

	row, err := qsdb.db.QueryRowContext(ctx, fetchQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute fetch query for ID %d: %w", queryId, err)
	}
	if err := row.Scan(&offset, &length); err != nil {
		return nil, fmt.Errorf("failed to scan offset/length from DB for ID %d: %w", queryId, err)
	}

	if offset+length > uint64(qsdb.mmapReader.Len()) {
		return nil, fmt.Errorf("offset + length (%d) exceeds mmap size (%d)", offset+length, qsdb.mmapReader.Len())
	}

	lineBytes := make([]byte, length)
	_, err = qsdb.mmapReader.ReadAt(lineBytes, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("failed to read segment data from mmap: %w", err)
	}

	parts := bytes.SplitN(lineBytes, []byte("\t"), 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid query format in file at offset %d", offset)
	}

	rawQuery := bytes.TrimSpace(parts[1])

	if len(rawQuery) == 0 {
		return nil, fmt.Errorf("read empty query from file at offset %d", offset)
	}

	queryResult := &QueryDataSourceResult{
		Query: string(rawQuery),
	}

	qsdb.mu.Lock()
	if !cacheExists {
		queryCache = lrucache.New[int, *QueryDataSourceResult](1000)
		qsdb.queriesCaches[fingerprintHash] = queryCache
	}
	queryCache.Set(queryId, queryResult)
	qsdb.mu.Unlock()

	return queryResult, nil
}

type QuerySourceDBInternalPerfStats struct {
	QueriesFetchTotal int
	CacheStats        lrucache.LRUCacheStats
	FetchWeightsLat   time.Duration
	FetchIdsLat       time.Duration
}
