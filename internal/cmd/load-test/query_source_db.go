package main

import (
	"context"
	"fmt"
	"math/rand"
	"mysql-load-test/internal/lrucache"
	"strings"
	"sync"
	"text/template"
	"time"
)

type QuerySourceDBConfig struct {
	DSN string `mapstructure:"dsn" yaml:"dsn" validate:"required"`

	FingerprintWeightsQuery string `mapstructure:"fingerprint_weights_query" yaml:"fingerprint_weights_query" validate:"omitempty"`

	QueriesFetchQuery string `mapstructure:"queries_fetch_query" yaml:"queries_fetch_query" validate:"omitempty"`

	QueriesIdsFetchQuery string `mapstructure:"queries_ids_fetch_query" yaml:"queries_ids_fetch_query" validate:"omitempty"`
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
		var fingerprint string
		var hash uint64
		var freqTotal int64
		var weight float64
		// expect: Fingerprint, `Hash`, `Count`, Total, Weight
		if err := rows.Scan(&fingerprint, &hash, &freqTotal, &qsdb.queriesCountTotal, &weight); err != nil {
			return err
		}
		qsdb.fingerprintWeights.Add(fingerprint, weight, &QueryFingerprintData{
			Fingerprint: fingerprint,
			Hash:        hash,
			FreqTotal:   freqTotal,
		})
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
		logger.Info().Msg("Opening database connection for query data source DB")
		db := NewDBConn(RetryConfig{
			MaxRetries:    3,                      // Retry up to 3 times
			InitialDelay:  100 * time.Millisecond, // Start with 100ms delay
			MaxDelay:      5 * time.Second,        // Cap at 5 seconds
			BackoffFactor: 2.0,                    // Double delay each retry
		})
		err := db.Open(qsdb.cfg.DSN, qsdb.concurrency)
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
	return qsdb.db.Close()
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
	fingerprintHash := fingerprintData.Hash

	queryIds := qsdb.queryIds[fingerprintHash]

	qsdb.mu.RLock()
	queriesCache, ok := qsdb.queriesCaches[fingerprintHash]
	if !ok {
		qsdb.mu.RUnlock()
		queriesCache = lrucache.New[int, *QueryDataSourceResult](len(queryIds))
		qsdb.mu.Lock()
		_queriesCache, ok := qsdb.queriesCaches[fingerprintHash]
		if !ok {
			qsdb.queriesCaches[fingerprintHash] = queriesCache
		} else {
			queriesCache = _queriesCache
		}
		qsdb.mu.Unlock()
	} else {
		qsdb.mu.RUnlock()
	}

	queriesFetchQueryTmpl := qsdb.cfg.QueriesFetchQuery
	if queriesFetchQueryTmpl == "" {
		return nil, fmt.Errorf("no query fetch query specified")
	}

	queryIdIdx := rand.Intn(len(queryIds))
	queryId := queryIds[queryIdIdx]

	if val, ok := queriesCache.Get(queryId); ok {
		return val, nil
	}

	data := map[string]any{
		"FingerprintHash": fingerprintHash,
		"ID":              queryId,
	}

	queriesFetchQuery, templateErr := executeTemplate(queriesFetchQueryTmpl, data, "queries_fetch_query")
	if templateErr != nil {
		return nil, fmt.Errorf("error executing query fetch query template: %w", templateErr)
	}

	rows, err := qsdb.db.QueryContext(ctx, queriesFetchQuery)
	if err != nil {
		return nil, fmt.Errorf("error executing query fetch query: %w", err)
	}
	defer rows.Close()

	var queryResult *QueryDataSourceResult
	resultCount := 0
	for rows.Next() {
		resultCount++
		if resultCount > 1 {
			return nil, fmt.Errorf("query fetch query returned more than one result")
		}
		var queryStr string
		var fingerprintStr string
		// expect: Query, Fingerprint
		if err := rows.Scan(&queryStr, &fingerprintStr); err != nil {
			return nil, fmt.Errorf("error scanning query fetch query: %w", err)
		}
		queryResult = &QueryDataSourceResult{
			Query:       queryStr,
			Fingerprint: fingerprintStr,
		}
	}

	if queryResult == nil {
		return nil, fmt.Errorf("query fetch query returned no result for query\n%s", queriesFetchQuery)
	}

	queriesCache.Set(queryId, queryResult)
	return queryResult, nil
}

type QuerySourceDBInternalPerfStats struct {
	QueriesFetchTotal int
	CacheStats        lrucache.LRUCacheStats
	FetchWeightsLat   time.Duration
	FetchIdsLat       time.Duration
}
