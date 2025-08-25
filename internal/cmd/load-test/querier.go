package main

import (
	"context"
	"database/sql"
	"fmt"
	"mysql-load-test/internal/ringbuffer"
	"time"

	"github.com/rs/zerolog"
)

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

const (
	maxGetRandomWeightedQueryLats = 5000 * 8 // 8 bytes since time.Duration is int64
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

func (q *Querier) PerfStats() QuerierInternalPerfStats {
	return *q.perfStats
}

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

func (q *Querier) explainQuery(ctx context.Context, query string, args ...any) (*ExplainQueryResult, error) {
	explainQuery := "EXPLAIN " + query
	rows, err := q.db.QueryContext(ctx, explainQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute explain query: %w", err)
	}
	defer rows.Close()

	var result ExplainQueryResult

	for rows.Next() {
		var row ExplainRow

		err := rows.Scan(
			&row.ID,
			&row.SelectType,
			&row.Table,
			&row.Partitions,
			&row.Type,
			&row.PossibleKeys,
			&row.Key,
			&row.KeyLen,
			&row.Ref,
			&row.Rows,
			&row.Filtered,
			&row.Extra,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return &result, nil
}

func (q *Querier) executeQuery(ctx context.Context, query string, args ...any) (*QueryResult, error) {
	var explainQueryResult *ExplainQueryResult
	var explainLatency time.Duration
	var execErr error

	// var wg sync.WaitGroup
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	start := time.Now()
	// 	var err error
	// 	explainQueryResult, err = q.explainQuery(ctx, query, args...)
	// 	explainLatency = time.Since(start)
	// 	if err != nil {
	// 		logger.Info().Msgf("failed to explain query: %s\n", err)
	// 	}
	// }()

	start := time.Now()
	_, execErr = q.db.ExecContext(ctx, query, args...)
	execLatency := time.Since(start)

	// wg.Wait()

	return &QueryResult{
		Explain:             explainQueryResult,
		Err:                 execErr,
		CompletionTimestamp: time.Now(),
		ExplainLatency:      explainLatency,
		ExecLatency:         execLatency,
	}, execErr
}

func (q *Querier) do(ctx context.Context) error {
	// a := time.Now()
	query, err := q.qds.GetRandomWeightedQuery(ctx)
	// fmt.Println(query.Query, query.Fingerprint)
	// q.perfStats.RecordGetRandomWeightedQueryLat(time.Since(a))
	if err != nil {
		return fmt.Errorf("error getting random weighted query: %w", err)
	}

	// fmt.Println(query.Query, query.Fingerprint)

	execStart := time.Now()
	result, err := q.executeQuery(ctx, query.Query)
	execLat := time.Since(execStart)
	_ = execLat

	// if err != nil {
	// 	return fmt.Errorf("error executing query \"%s\" with fingerprint \"%s\": %w", query.Query, query.Fingerprint, err)
	// }

	var querierErr querierError
	if err != nil {
		querierErr = querierError{
			query:       query.Query,
			fingerprint: query.Fingerprint,
			err:         err,
		}
	}
	result.Err = querierErr

	q.results <- result
	// fmt.Println(result.ExecLatency.Microseconds())
	return nil
}

func (q *Querier) Run(ctx context.Context) error {
	for {
		select {
		default:
			if q.qpsTicker != nil {
				select {
				case <-q.qpsTicker.C:
					err := q.do(ctx)
					if err != nil {
						return err
					}
				}
			} else {
				err := q.do(ctx)
				if err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return nil
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
