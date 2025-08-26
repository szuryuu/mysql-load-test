package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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

func (o *OutputDB) insertBatch(ctx context.Context, batch []*query.Query) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	tx, err := o.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	fingerprintValues := make([]string, 0, len(batch))
	fingerprintArgs := make([]interface{}, 0, len(batch)*2)
	seenFingerprints := make(map[uint64]bool)

	for _, q := range batch {
		if !seenFingerprints[q.FingerprintHash] {
			seenFingerprints[q.FingerprintHash] = true
			fingerprintValues = append(fingerprintValues, "(?, ?)")
			fingerprintArgs = append(fingerprintArgs, q.Fingerprint, q.FingerprintHash)
		}
	}

	if len(fingerprintValues) > 0 {
		fingerprintSQL := fmt.Sprintf(`
			INSERT IGNORE INTO QueryFingerprint (Fingerprint, Hash)
			VALUES %s
		`, strings.Join(fingerprintValues, ", "))
		if _, err := tx.ExecContext(ctx, fingerprintSQL, fingerprintArgs...); err != nil {
			return 0, fmt.Errorf("failed to batch insert fingerprints: %w", err)
		}
	}

	queryValues := make([]string, 0, len(batch))
	queryArgs := make([]interface{}, 0, len(batch)*4)
	seenQueries := make(map[uint64]bool)

	for _, q := range batch {
		if !isValidQuery(q.Raw) {
			continue
		}
		if !seenQueries[q.Hash] {
			seenQueries[q.Hash] = true
			queryValues = append(queryValues, "(?, ?, ?, ?)")
			queryArgs = append(queryArgs, q.Hash, q.Offset, q.Length, q.FingerprintHash)
		}
	}

	if len(queryValues) == 0 {
		return 0, tx.Commit()
	}

	querySQL := fmt.Sprintf(`
    INSERT INTO Query (Hash, Offset, Length, FingerprintHash)
    VALUES %s
    `, strings.Join(queryValues, ", "))

	if _, err := o.execContext(ctx, tx, querySQL, queryArgs...); err != nil {
		return 0, fmt.Errorf("failed to batch insert queries: %w", err)
	}

	return len(seenQueries), tx.Commit()

}

func (o *OutputDB) execContext(ctx context.Context, tx *sqlx.Tx, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := tx.ExecContext(ctx, query, args...)
	o.insertLats <- time.Since(start)
	return res, err
}

func (o *OutputDB) StartOutput(ctx context.Context, inQueryChan <-chan *query.Query) error {
	if o.cfg.Truncate {
		fmt.Println("Truncating tables")
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
			if n, err = o.insertBatch(ctx, batch); err != nil {
				fmt.Fprintf(os.Stderr, "error inserting batch: %v\n", err)
			}
			o.insertedQueries.Add(uint64(n))
			batch = batch[:0]
		}
	}

	// CRITICAL FIX: Process the final partial batch
	if len(batch) > 0 {
		var n int
		var err error
		if n, err = o.insertBatch(ctx, batch); err != nil {
			fmt.Fprintf(os.Stderr, "error inserting final batch: %v\n", err)
		}
		o.insertedQueries.Add(uint64(n))
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
