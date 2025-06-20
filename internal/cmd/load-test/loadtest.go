package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

func createDataSource(cfg *Config) (QueryDataSource, error) {
	switch cfg.QueriesDataSource.Type {
	case "db":
		return NewQuerySourceDB(&cfg.QueriesDataSource.QueryDataSourceDB, cfg.Concurrency, nil)
	// case "inline":
	// 	return NewQuerySourceInline(cfg.QueryDataSourceDB)
	default:
		return nil, fmt.Errorf("unsupported query data source type: %s", cfg.QueriesDataSource.Type)
	}
}

func performLoadTest() error {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	logger.Info().Str("data_source_type", config.QueriesDataSource.Type).Msg("Creating query data source")
	qds, qdsCreateErr := createDataSource(&config)
	if qdsCreateErr != nil {
		return fmt.Errorf("error creating query data source: %w", qdsCreateErr)
	}
	qdsInitErr := qds.Init(ctx)
	if qdsInitErr != nil {
		return fmt.Errorf("error initializing query data source: %w", qdsInitErr)
	}
	defer qds.Destroy()
	logger.Info().Msg("Query data source ready")

	// Start metrics server if enabled
	var metricsServer *MetricsServer
	if config.Metrics.Enabled {
		metricsServer = NewMetricsServer(config.Metrics.Addr)
		if err := metricsServer.Start(ctx); err != nil {
			return fmt.Errorf("error starting metrics server: %w", err)
		}
		logger.Info().Str("addr", config.Metrics.Addr).Msg("Metrics server started - visit the dashboard at http://" + config.Metrics.Addr)
	}

	var qpsTicker *time.Ticker
	if config.QPS > 0 {
		qpsTicker = time.NewTicker(time.Second / time.Duration(config.QPS))
		defer qpsTicker.Stop()
	}

	resultsChan := make(chan *QueryResult, config.Concurrency*100)
	fatalErrsChan := make(chan error)

	go func() {
		for err := range fatalErrsChan {
			logger.Error().Err(err).Msg("Fatal error")
			cancel(err)
			return
		}
	}()

	var wg sync.WaitGroup

	dbConn := NewDBConn(RetryConfig{
		MaxRetries:      3,                      // Retry up to 3 times
		InitialDelay:    100 * time.Millisecond, // Start with 100ms delay
		MaxDelay:        5 * time.Second,        // Cap at 5 seconds
		BackoffFactor:   2.0,                    // Double delay each retry
		ConnectionCheck: true,                   // Ping before queries
	})
	logger.Info().Msg("Opening connection to target database")
	if err := dbConn.Open(config.DBDSN, config.Concurrency); err != nil {
		return fmt.Errorf("error opening database connection: %w", err)
	}
	defer dbConn.Close()
	logger.Info().Msg("Connection to target database opened")

	querier := NewQuerier(qds, qpsTicker, &logger, dbConn, resultsChan)

	wg.Add(config.Concurrency)
	for i := 0; i < config.Concurrency; i++ {
		go func() {
			defer wg.Done()
			logger.Info().Int("goroutine_id", i).Msg("Starting querier goroutine")
			if err := querier.Run(ctx); err != nil {
				fatalErrsChan <- fmt.Errorf("error running querier: %w", err)
				return
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range resultsChan {
			// Results are now handled by the web UI
		}
	}()

	wg.Add(1)
	// Stats reporter
	go func() {
		defer wg.Done()

		startTime := time.Now()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// collect stats from components
				qdsPerfStats := qds.PerfStats().(QuerySourceDBInternalPerfStats)
				querierPerfStats := querier.PerfStats()

				// Calculate latency percentiles
				lats := querierPerfStats.GetRandomWeightedQueryLats()
				var p50, p95, p99 time.Duration
				if len(lats) > 0 {
					sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
					p50 = lats[len(lats)*50/100]
					p95 = lats[len(lats)*95/100]
					p99 = lats[len(lats)*99/100]
				}

				// Calculate QPS
				qps := float64(querierPerfStats.GetTotalQueries()) / time.Since(startTime).Seconds()

				// Create stats object
				stats := &PerformanceStats{
					Timestamp:         time.Now(),
					Runtime:           time.Since(startTime).Round(time.Second).String(),
					QueriesFetched:    int64(qdsPerfStats.QueriesFetchTotal),
					CacheHits:         int64(qdsPerfStats.CacheStats.HitsTotal),
					CacheMisses:       int64(qdsPerfStats.CacheStats.MissesTotal),
					CacheHitRate:      float64(qdsPerfStats.CacheStats.HitsTotal) / float64(qdsPerfStats.CacheStats.HitsTotal+qdsPerfStats.CacheStats.MissesTotal) * 100,
					CacheEvictions:    int64(qdsPerfStats.CacheStats.EvictionsTotal),
					CacheNewItems:     int64(qdsPerfStats.CacheStats.NewItemsTotal),
					FetchWeightsLat:   qdsPerfStats.FetchWeightsLat.Round(time.Millisecond).String(),
					QueryLatencyP50:   p50.Round(time.Millisecond).String(),
					QueryLatencyP95:   p95.Round(time.Millisecond).String(),
					QueryLatencyP99:   p99.Round(time.Millisecond).String(),
					QueriesPerSecond:  qps,
					ActiveConnections: config.Concurrency,
				}

				// Broadcast stats to web UI
				if metricsServer != nil {
					metricsServer.BroadcastStats(stats)
				}
			}
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-ctx.Done():
		if err := context.Cause(ctx); err != nil && err.Error() != "interrupted by user" {
			return err
		}
	case <-signalChan:
		fmt.Println("Received SIGTERM/SIGINT, exiting...")
		return nil
	}

	wg.Wait()

	return nil
}

// Add this method to QuerierInternalPerfStats
func (st *QuerierInternalPerfStats) GetTotalQueries() int {
	return st.getRandomWeightedQueryLats_rb.Count()
}
