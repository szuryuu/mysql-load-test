package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"mysql-load-test/pkg/query"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type OutputDBConfig struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	User      string `json:"user"`
	Password  string `json:"password"`
	DBName    string `json:"name"`
	Truncate  bool   `json:"truncate"`
	BatchSize int    `json:"batch_size"`
}

type OutputDB struct {
	cfg             OutputDBConfig
	db              *DB
	insertedQueries atomic.Uint64
	insertLats      chan time.Duration
}

type DB struct {
	*sqlx.DB
}

func NewDBOutput(cfg OutputDBConfig) (*OutputDB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)

	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	_db := &DB{
		DB: db,
	}

	return &OutputDB{
		cfg:             cfg,
		db:              _db,
		insertedQueries: atomic.Uint64{},
		insertLats:      make(chan time.Duration, 100),
	}, nil
}

func (o *OutputDB) truncateTables(ctx context.Context) error {
	tx, err := o.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Truncate in reverse order of foreign key dependencies
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "TRUNCATE TABLE Query"); err != nil {
		return fmt.Errorf("failed to truncate Query table: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "TRUNCATE TABLE QueryFingerprint"); err != nil {
		return fmt.Errorf("failed to truncate QueryFingerprint table: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return fmt.Errorf("failed to enable foreign key checks: %w", err)
	}

	return tx.Commit()
}

func (o *OutputDB) insertQueryBatch(ctx context.Context, batch []*query.Query) error {
	if len(batch) == 0 {
		return nil
	}

	// Filter valid queries first
	validQueries := make([]*query.Query, 0, len(batch))
	for _, q := range batch {
		if isValidQuery(q.Raw) {
			validQueries = append(validQueries, q)
		}
	}

	if len(validQueries) == 0 {
		return nil
	}

	tx, err := o.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Build fingerprint batch insert
	fingerprintValues := make([]string, 0, len(validQueries))
	fingerprintArgs := make([]interface{}, 0, len(validQueries)*2)

	// Track unique fingerprints to avoid duplicates in the same batch
	seenFingerprints := make(map[uint64]bool)

	for _, q := range validQueries {
		if !seenFingerprints[q.FingerprintHash] {
			seenFingerprints[q.FingerprintHash] = true
			fingerprintValues = append(fingerprintValues, "(?, ?)")
			fingerprintArgs = append(fingerprintArgs, q.FingerprintHash, q.Fingerprint)
		}
	}

	// Insert/update fingerprints in batch
	if len(fingerprintValues) > 0 {
		fingerprintSQL := fmt.Sprintf(`
			INSERT INTO QueryFingerprint (Hash, Fingerprint)
			VALUES %s
			`, strings.Join(fingerprintValues, ", "))

		// ON DUPLICATE KEY UPDATE Count = Count + VALUES(Count)

		if _, err := o.execContext(ctx, tx, fingerprintSQL, fingerprintArgs...); err != nil {
			return fmt.Errorf("failed to batch insert fingerprints: %w", err)
		}
	}

	// Build query batch insert
	queryValues := make([]string, 0, len(validQueries))
	queryArgs := make([]interface{}, 0, len(validQueries)*3)

	// Track unique queries to avoid duplicates in the same batch
	seenQueries := make(map[uint64]bool)

	for _, q := range validQueries {
		if !seenQueries[q.Hash] {
			seenQueries[q.Hash] = true
			queryValues = append(queryValues, "(?, ?, ?)")
			queryArgs = append(queryArgs, q.Hash, q.Raw, q.FingerprintHash)
		}
	}

	// Insert/update queries in batch
	if len(queryValues) > 0 {
		querySQL := fmt.Sprintf(`
			INSERT INTO Query (Hash, Query, FingerprintHash)
			VALUES %s
			`, strings.Join(queryValues, ", "))

		// ON DUPLICATE KEY UPDATE Count = Count + VALUES(Count)
		if _, err := o.execContext(ctx, tx, querySQL, queryArgs...); err != nil {
			return fmt.Errorf("failed to batch insert queries: %w", err)
		}
	}

	return tx.Commit()
}

func (o *OutputDB) insertQueryBatch2(ctx context.Context, batch []*query.Query) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	tx, err := o.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	n := 0

	fingerprintValuesCount := 0
	fingerprintValues := make([]any, 0, len(batch))

	queryValuesCount := 0
	queryValues := make([]any, 0, len(batch))

	for _, q := range batch {
		fingerprintValues = append(fingerprintValues, q.FingerprintHash, q.Fingerprint)
		fingerprintValuesCount++

		queryValues = append(queryValues, q.Hash, q.Raw, q.FingerprintHash)
		queryValuesCount++

		n++
	}

	fingerprintSQL := fmt.Sprintf(`INSERT INTO QueryFingerprint (Hash, Fingerprint)
		VALUES %s`, strings.Repeat("(?, ?), ", fingerprintValuesCount-1))
	fingerprintSQL += "(?, ?)"

	querySQL := fmt.Sprintf(`INSERT INTO Query (Hash, Query, FingerprintHash)
			VALUES %s`, strings.Repeat("(?, ?, ?), ", queryValuesCount-1))
	querySQL += "(?, ?, ?)"

	if _, err := o.execContext(ctx, tx, fingerprintSQL, fingerprintValues...); err != nil {
		return 0, fmt.Errorf("failed to batch insert fingerprints: %w", err)
	}

	if _, err := o.execContext(ctx, tx, querySQL, queryValues...); err != nil {
		return 0, fmt.Errorf("failed to batch insert queries: %w", err)
	}

	return n, tx.Commit()
}

func (o *OutputDB) execContext(ctx context.Context, tx *sqlx.Tx, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := tx.ExecContext(ctx, query, args...)
	o.insertLats <- time.Since(start)
	return res, err
}

func (o *OutputDB) StartOutput(ctx context.Context, inQueryChan <-chan *query.Query) error {
	if o.cfg.Truncate {
		if err := o.truncateTables(ctx); err != nil {
			return fmt.Errorf("error truncating tables: %w", err)
		}
	}

	reporterCtx, reporterStop := context.WithCancel(ctx)
	defer reporterStop()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		lastCount := o.insertedQueries.Load()
		lats := make([]time.Duration, 0, 100)
		for {
			select {
			case <-reporterCtx.Done():
				fmt.Printf("Completed %d queries\n", o.insertedQueries.Load())
				return
			case lat := <-o.insertLats:
				lats = append(lats, lat)
			case <-ticker.C:
				if len(lats) > 0 {
					sum := time.Duration(0)
					for _, lat := range lats {
						sum += lat
					}
					fmt.Printf("avg insert latency: %d ms\n", int(sum.Milliseconds())/len(lats))
					lats = lats[:0]
				}

				count := o.insertedQueries.Load()
				fmt.Printf("Inserted %d queries (%d/s)\n", o.insertedQueries.Load(), count-lastCount)
				lastCount = count
			}
		}
	}()

	batch := make([]*query.Query, 0, o.cfg.BatchSize)
	for q := range inQueryChan {
		batch = append(batch, q)
		if len(batch) >= o.cfg.BatchSize {
			var n int
			var err error
			if n, err = o.insertQueryBatch2(ctx, batch); err != nil {
				return fmt.Errorf("error inserting query: %w", err)
			}
			o.insertedQueries.Add(uint64(n))
			batch = batch[:0]
		}
	}

	// CRITICAL FIX: Process the final partial batch
	if len(batch) > 0 {
		if err := o.insertQueryBatch(ctx, batch); err != nil {
			return fmt.Errorf("error inserting final batch: %w", err)
		}
		o.insertedQueries.Add(uint64(len(batch)))
	}

	return nil
}

func (o *OutputDB) Concurrency() OutputConcurrencyInfo {
	return OutputConcurrencyInfo{
		MaxConcurrency:     0,
		CurrentConcurrency: 0,
	}
}

func (o *OutputDB) Destroy() error {
	return o.db.Close()
}
