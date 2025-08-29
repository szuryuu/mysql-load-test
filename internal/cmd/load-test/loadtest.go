package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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

	dbConn := NewDBConn(RetryConfig{
		MaxRetries:      1,                      // Retry up to 3 times
		InitialDelay:    100 * time.Millisecond, // Start with 100ms delay
		MaxDelay:        5 * time.Second,        // Cap at 5 seconds
		BackoffFactor:   2.0,                    // Double delay each retry
		ConnectionCheck: true,                   // Ping before queries
	})
	logger.Info().Msg("Opening connection to target database")
	if err := dbConn.OpenWithTimeout(ctx, config.DBDSN, config.Concurrency, 5*time.Second); err != nil {
		return fmt.Errorf("error opening database connection: %w", err)
	}
	defer dbConn.Close()
	logger.Info().Msg("Connection to target database opened")

	logger.Info().Str("data_source_type", config.QueriesDataSource.Type).Msg("Creating query data source")
	qds, qdsCreateErr := createDataSource(&config)
	if qdsCreateErr != nil {
		return fmt.Errorf("error creating query data source: %w", qdsCreateErr)
	}
	qdsInitErr := qds.Init(ctx)
	if qdsInitErr != nil {
		return fmt.Errorf("error initializing query data source: %w", qdsInitErr)
	}
	// defer qds.Destroy()
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
		r := newReport(resultsChan)
		logger.Info().Msg("Starting reporter")
		runReporter(r, ctx, qds, querier, metricsServer)
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
	qds.Destroy()

	return nil
}

// Add this method to QuerierInternalPerfStats
func (st *QuerierInternalPerfStats) GetTotalQueries() int {
	return st.getRandomWeightedQueryLats_rb.Count()
}
