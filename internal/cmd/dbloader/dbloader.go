package dbloader

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"mysql-load-test/pkg/query"

	"github.com/alitto/pond/v2"
	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

func isValidQuery(q []byte) bool {
	if len(q) == 0 {
		return false
	}
	return true
}

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
	db              *sqlx.DB
	insertedQueries atomic.Uint64
	pool            pond.Pool
}

func NewDBOutput(cfg OutputDBConfig) (*OutputDB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)

	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	pool := pond.NewPool(20)

	return &OutputDB{
		cfg:             cfg,
		db:              db,
		insertedQueries: atomic.Uint64{},
		pool:            pool,
	}, nil
}

func (o *OutputDB) truncateTables(ctx context.Context) error {
	tx, err := o.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

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
	fingerprintArgs := make([]any, 0, len(batch))
	seenFingerprints := make(map[uint64]bool)

	for _, q := range batch {
		if !seenFingerprints[q.FingerprintHash] {
			seenFingerprints[q.FingerprintHash] = true
			fingerprintValues = append(fingerprintValues, "(?)")
			fingerprintArgs = append(fingerprintArgs, q.FingerprintHash)
		}
	}

	if len(fingerprintValues) > 0 {
		fingerprintSQL := fmt.Sprintf(`INSERT IGNORE INTO QueryFingerprint (Hash) VALUES %s`, strings.Join(fingerprintValues, ", "))
		if _, err := tx.ExecContext(ctx, fingerprintSQL, fingerprintArgs...); err != nil {
			return 0, fmt.Errorf("failed to batch insert fingerprints: %w", err)
		}
	}

	queryValues := make([]string, 0, len(batch))
	queryArgs := make([]any, 0, len(batch)*4)
	seenQueries := make(map[uint64]bool)

	for _, q := range batch {
		// if !isValidQuery(q.Raw) {
		// 	continue
		// }
		if !seenQueries[q.Hash] {
			seenQueries[q.Hash] = true
			queryValues = append(queryValues, "(?, ?, ?, ?)")
			queryArgs = append(queryArgs, q.Hash, q.Offset, q.Length, q.FingerprintHash)
		}
	}

	if len(queryValues) == 0 {
		return 0, tx.Commit()
	}

	querySQL := fmt.Sprintf(`INSERT IGNORE INTO Query (Hash, Offset, Length, FingerprintHash) VALUES %s`, strings.Join(queryValues, ", "))
	if _, err := tx.ExecContext(ctx, querySQL, queryArgs...); err != nil {
		return 0, fmt.Errorf("failed to batch insert queries: %w", err)
	}

	return len(seenQueries), tx.Commit()
}

func (o *OutputDB) StartOutput(ctx context.Context, inQueryChan <-chan *query.Query) error {
	if o.cfg.Truncate {
		fmt.Println("Truncating tables")
		if err := o.truncateTables(ctx); err != nil {
			return fmt.Errorf("error truncating tables: %w", err)
		}
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		lastCount := o.insertedQueries.Load()
		for {
			select {
			case <-ctx.Done():
				finalCount := o.insertedQueries.Load()
				fmt.Printf("\nCompleted %d queries\n", finalCount)
				return
			case <-ticker.C:
				count := o.insertedQueries.Load()
				fmt.Printf("\rInserted %d queries (%d/s)", count, count-lastCount)
				lastCount = count
			}
		}
	}()

	batch := make([]*query.Query, 0, o.cfg.BatchSize)
	for q := range inQueryChan {
		batch = append(batch, q)
		if len(batch) >= o.cfg.BatchSize {
			currentBatch := batch
			o.pool.Submit(func() {
				if n, err := o.insertBatch(ctx, currentBatch); err != nil {
					fmt.Fprintf(os.Stderr, "error inserting batch: %v\n", err)
				} else {
					o.insertedQueries.Add(uint64(n))
				}
			})
			batch = make([]*query.Query, 0, o.cfg.BatchSize)
		}
	}

	if len(batch) > 0 {
		currentBatch := batch
		o.pool.Submit(func() {
			if n, err := o.insertBatch(ctx, currentBatch); err != nil {
				fmt.Fprintf(os.Stderr, "error inserting final batch: %v\n", err)
			} else {
				o.insertedQueries.Add(uint64(n))
			}
		})
	}

	o.pool.StopAndWait()
	time.Sleep(100 * time.Millisecond)
	return nil
}

func (o *OutputDB) Destroy() error {
	return o.db.Close()
}
