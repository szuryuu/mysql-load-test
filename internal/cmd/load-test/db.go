package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type RetryConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	BackoffFactor   float64
	ConnectionCheck bool
}

type DBConn struct {
	db          *sql.DB
	dsn         string
	concurrency int
	retryConfig RetryConfig
	mu          sync.RWMutex
}

func NewDBConn(retryConfig RetryConfig) *DBConn {
	if retryConfig.MaxRetries == 0 {
		retryConfig.MaxRetries = 3
	}
	if retryConfig.InitialDelay == 0 {
		retryConfig.InitialDelay = 100 * time.Millisecond
	}
	if retryConfig.MaxDelay == 0 {
		retryConfig.MaxDelay = 5 * time.Second
	}
	if retryConfig.BackoffFactor == 0 {
		retryConfig.BackoffFactor = 2.0
	}

	return &DBConn{
		retryConfig: retryConfig,
	}
}

func (d *DBConn) Open(dsn string, concurrency int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.dsn = dsn
	d.concurrency = concurrency

	return d.connect()
}

func (d *DBConn) connect() error {
	db, err := sql.Open("mysql", d.dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(d.concurrency)
	db.SetMaxIdleConns(d.concurrency / 2)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	if d.db != nil {
		d.db.Close()
	}
	d.db = db

	return nil
}

func (d *DBConn) reconnect() error {
	log.Println("Attempting to reconnect to database...")
	return d.connect()
}

func (d *DBConn) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *DBConn) isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common connection-related errors
	errStr := err.Error()
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"no such host",
		"network is unreachable",
		"connection timed out",
		"driver: bad connection",
		"invalid connection",
		"server has gone away",
		"connection lost",
	}

	for _, connErr := range connectionErrors {
		if contains(errStr, connErr) {
			return true
		}
	}

	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			indexOf(s, substr) >= 0)))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func (d *DBConn) withRetry(ctx context.Context, operation func() error) error {
	var lastErr error
	delay := d.retryConfig.InitialDelay

	for attempt := 0; attempt <= d.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}

			delay = time.Duration(float64(delay) * d.retryConfig.BackoffFactor)
			if delay > d.retryConfig.MaxDelay {
				delay = d.retryConfig.MaxDelay
			}
		}

		err := operation()
		if err == nil {
			return nil
		}

		lastErr = err

		if d.isConnectionError(err) {
			d.mu.Lock()
			reconnectErr := d.reconnect()
			d.mu.Unlock()

			if reconnectErr != nil {
				lastErr = fmt.Errorf("reconnection failed: %w (original error: %v)", reconnectErr, err)
				continue
			}
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", d.retryConfig.MaxRetries+1, lastErr)
}

// Generic database operation wrapper
func (d *DBConn) withDB(ctx context.Context, operation func(*sql.DB) error) error {
	return d.withRetry(ctx, func() error {
		d.mu.RLock()
		db := d.db
		d.mu.RUnlock()

		if db == nil {
			return fmt.Errorf("database connection is nil")
		}

		if d.retryConfig.ConnectionCheck {
			if err := db.PingContext(ctx); err != nil {
				return fmt.Errorf("connection health check failed: %w", err)
			}
		}

		return operation(db)
	})
}

// QueryContext executes a query that returns rows with retry logic
func (d *DBConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	err := d.withDB(ctx, func(db *sql.DB) error {
		var err error
		rows, err = db.QueryContext(ctx, query, args...)
		return err
	})
	return rows, err
}

// QueryRowContext executes a query that returns at most one row with retry logic
func (d *DBConn) QueryRowContext(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	var row *sql.Row
	var err error
	err = d.withDB(ctx, func(db *sql.DB) error {
		row = db.QueryRowContext(ctx, query, args...)
		return row.Err()
	})
	return row, err
}

// ExecContext executes a query without returning any rows with retry logic
func (d *DBConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := d.withDB(ctx, func(db *sql.DB) error {
		var err error
		result, err = db.ExecContext(ctx, query, args...)
		return err
	})
	return result, err
}

// PrepareContext creates a prepared statement with retry logic
func (d *DBConn) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	var stmt *sql.Stmt
	err := d.withDB(ctx, func(db *sql.DB) error {
		var err error
		stmt, err = db.PrepareContext(ctx, query)
		return err
	})
	return stmt, err
}

// BeginTx starts a transaction with retry logic
func (d *DBConn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	var tx *sql.Tx
	err := d.withDB(ctx, func(db *sql.DB) error {
		var err error
		tx, err = db.BeginTx(ctx, opts)
		return err
	})
	return tx, err
}

// Ping verifies a connection to the database with retry logic
func (d *DBConn) Ping() error {
	return d.withRetry(context.Background(), func() error {
		d.mu.RLock()
		db := d.db
		d.mu.RUnlock()

		if db == nil {
			return fmt.Errorf("database connection is nil")
		}

		return db.Ping()
	})
}

// PingContext verifies a connection to the database with retry logic
func (d *DBConn) PingContext(ctx context.Context) error {
	return d.withDB(ctx, func(db *sql.DB) error {
		return db.PingContext(ctx)
	})
}

// Stats returns database statistics
func (d *DBConn) Stats() sql.DBStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.db == nil {
		return sql.DBStats{}
	}
	return d.db.Stats()
}
