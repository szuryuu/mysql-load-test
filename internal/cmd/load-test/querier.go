package main

import (
	"context"
	"database/sql"
	"fmt"
	"mysql-load-test/internal/ringbuffer"
	"time"

	"github.com/rs/zerolog"
)

type ExplainRow struct {
	ID           sql.NullInt64   `json:"id"`
	SelectType   sql.NullString  `json:"select_type"`
	Table        sql.NullString  `json:"table"`
	Partitions   sql.NullString  `json:"partitions"`
	Type         sql.NullString  `json:"type"`
	PossibleKeys sql.NullString  `json:"possible_keys"`
	Key          sql.NullString  `json:"key"`
	KeyLen       sql.NullString  `json:"key_len"`
	Ref          sql.NullString  `json:"ref"`
	Rows         sql.NullInt64   `json:"rows"`
	Filtered     sql.NullFloat64 `json:"filtered"`
	Extra        sql.NullString  `json:"extra"`
}

type ExplainQueryResult struct {
	Rows []ExplainRow `json:"rows"`
}

type QueryResult struct {
	CompletionTimestamp         time.Time
	ExplainLatency, ExecLatency time.Duration
	Err                         error
	Explain                     *ExplainQueryResult
}

type Querier struct {
	qds       QueryDataSource
	qpsTicker *time.Ticker
	results   chan<- *QueryResult
	perfStats *QuerierInternalPerfStats
	logger    *zerolog.Logger
	db        *DBConn
}

type QuerierInternalPerfStats struct {
	getRandomWeightedQueryLats_rb *ringbuffer.RingBuffer[time.Duration]
	getRandomWeightedQueryLats    []time.Duration
}

func NewQuerierInternalPerfStats() *QuerierInternalPerfStats {
	return &QuerierInternalPerfStats{
		getRandomWeightedQueryLats_rb: ringbuffer.NewRingBuffer[time.Duration](maxGetRandomWeightedQueryLats),
		getRandomWeightedQueryLats:    make([]time.Duration, maxGetRandomWeightedQueryLats),
	}
}

func (st *QuerierInternalPerfStats) RecordGetRandomWeightedQueryLat(lat time.Duration) {
	st.getRandomWeightedQueryLats_rb.Append(lat)
}

func (st *QuerierInternalPerfStats) GetRandomWeightedQueryLats() []time.Duration {
	lats := st.getRandomWeightedQueryLats[:0]
	return st.getRandomWeightedQueryLats_rb.GetAll(lats)
}

func (q *Querier) PerfStats() QuerierInternalPerfStats {
	return *q.perfStats
}

const (
	maxGetRandomWeightedQueryLats = 5000 * 8
)

func NewQuerier(qds QueryDataSource, qpsTicker *time.Ticker, logger *zerolog.Logger, db *DBConn, resultsChan chan<- *QueryResult) *Querier {
	return &Querier{
		qds:       qds,
		qpsTicker: qpsTicker,
		results:   resultsChan,
		perfStats: NewQuerierInternalPerfStats(),
		logger:    logger,
		db:        db,
	}
}

func (q *Querier) executeQueryFast(ctx context.Context, queryStr string, args ...any) (*QueryResult, error) {
	start := time.Now()

	_, execErr := q.db.ExecContext(ctx, queryStr, args...)
	execLatency := time.Since(start)

	return &QueryResult{
		Err:                 execErr,
		CompletionTimestamp: time.Now(),
		ExplainLatency:      0,
		ExecLatency:         execLatency,
	}, execErr
}

func (q *Querier) do(ctx context.Context) error {
	query, err := q.qds.GetRandomWeightedQuery(ctx)
	if err != nil {
		return fmt.Errorf("error getting random weighted query: %w", err)
	}

	result, err := q.executeQueryFast(ctx, query.Query)
	if err != nil {
		result.Err = querierError{
			query:       query.Query,
			fingerprint: query.Fingerprint,
			err:         err,
		}
	}

	q.results <- result
	return nil
}

func (q *Querier) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if q.qpsTicker != nil {
				<-q.qpsTicker.C
			}
			if err := q.do(ctx); err != nil {
				// Don't log every error to reduce overhead
				if time.Now().UnixNano()%1000 == 0 { // Log 1 in 1000 errors
					q.logger.Error().Err(err).Msg("Error executing query")
				}
			}
		}
	}
}

type querierError struct {
	query       string
	fingerprint string
	err         error
}

func (qe querierError) Error() string {
	return fmt.Sprintf("error executing query \"%s\" with fingerprint \"%s\": %v", qe.query, qe.fingerprint, qe.err)
}
