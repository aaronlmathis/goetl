//
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Copyright (C) 2025 Aaron Mathis aaron.mathis@gmail.com
//
// This file is part of GoETL.
//
// GoETL is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// GoETL is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with GoETL. If not, see https://www.gnu.org/licenses/.

package writers

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aaronlmathis/goetl/core"
	_ "github.com/lib/pq"
)

// Package writers provides implementations of goetl.DataSink for writing data to various destinations.
//
// This file implements a high-performance, configurable PostgreSQL writer for streaming ETL pipelines.
// It supports batching, connection pooling, conflict resolution, table creation, and statistics.

// PostgresWriterError wraps PostgreSQL-specific write errors with context about the operation.
type PostgresWriterError struct {
	Op  string // The operation being performed (e.g., "write", "connect")
	Err error  // The underlying error
}

// Error returns the error string for PostgresWriterError.
func (e *PostgresWriterError) Error() string {
	return fmt.Sprintf("postgres writer %s: %v", e.Op, e.Err)
}

// Unwrap returns the underlying error for PostgresWriterError.
func (e *PostgresWriterError) Unwrap() error {
	return e.Err
}

// PostgresWriterStats holds PostgreSQL write performance statistics.
type PostgresWriterStats struct {
	RecordsWritten   int64            // Total records written
	BatchesWritten   int64            // Number of batches written
	TransactionCount int64            // Number of transactions committed
	LastWriteTime    time.Time        // Time of last write
	WriteDuration    time.Duration    // Total time spent writing
	ConnectionTime   time.Duration    // Time spent establishing connection
	NullValueCounts  map[string]int64 // Count of null values per column
	ConflictCount    int64            // Number of conflicts encountered
}

// ConflictResolution defines how to handle INSERT conflicts in PostgreSQL.
type ConflictResolution int

const (
	// ConflictError returns an error on conflict (default PostgreSQL behavior).
	ConflictError ConflictResolution = iota
	// ConflictIgnore ignores conflicting rows (ON CONFLICT DO NOTHING).
	ConflictIgnore
	// ConflictUpdate updates conflicting rows (ON CONFLICT DO UPDATE).
	ConflictUpdate
)

// PostgresWriterOptions configures the PostgreSQL writer.
type PostgresWriterOptions struct {
	DSN                string             // PostgreSQL connection string
	TableName          string             // Target table name
	Columns            []string           // Columns to write (order matters)
	BatchSize          int                // Number of records per batch
	CreateTable        bool               // Create table if not exists
	TruncateTable      bool               // Truncate table before writing
	ConflictResolution ConflictResolution // Conflict handling strategy
	ConflictColumns    []string           // Columns that define uniqueness for conflict resolution
	UpdateColumns      []string           // Columns to update on conflict (for ConflictUpdate)
	TransactionMode    bool               // Wrap batches in transactions
	ConnMaxLifetime    time.Duration      // Max connection lifetime
	ConnMaxIdleTime    time.Duration      // Max idle connection time
	MaxOpenConns       int                // Max open connections
	MaxIdleConns       int                // Max idle connections
	QueryTimeout       time.Duration      // Timeout for queries
	Metadata           map[string]string  // Arbitrary metadata for user tracking
	MaxErrors          int64
}

// PostgresWriterOption represents a configuration function for PostgresWriterOptions.
type PostgresWriterOption func(*PostgresWriterOptions)

// WithPostgresDSN sets the PostgreSQL connection string.
func WithPostgresDSN(dsn string) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.DSN = dsn
	}
}

// WithTableName sets the target table name.
func WithTableName(tableName string) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.TableName = tableName
	}
}

// WithColumns sets the columns to write.
func WithColumns(columns []string) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.Columns = append([]string(nil), columns...)
	}
}

// WithPostgresBatchSize sets the batch size for writes.
func WithPostgresBatchSize(size int) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.BatchSize = size
	}
}

// WithCreateTable enables or disables table creation.
func WithCreateTable(create bool) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.CreateTable = create
	}
}

// WithTruncateTable enables or disables table truncation before writing.
func WithTruncateTable(truncate bool) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.TruncateTable = truncate
	}
}

// WithConflictResolution sets the conflict resolution strategy and columns.
func WithConflictResolution(resolution ConflictResolution, conflictCols, updateCols []string) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.ConflictResolution = resolution
		opts.ConflictColumns = append([]string(nil), conflictCols...)
		opts.UpdateColumns = append([]string(nil), updateCols...)
	}
}

// WithTransactionMode enables or disables transaction wrapping for batches.
func WithTransactionMode(enabled bool) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.TransactionMode = enabled
	}
}

// WithPostgresConnectionPool configures the connection pool.
func WithPostgresConnectionPool(maxOpen, maxIdle int, maxLifetime, maxIdleTime time.Duration) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.MaxOpenConns = maxOpen
		opts.MaxIdleConns = maxIdle
		opts.ConnMaxLifetime = maxLifetime
		opts.ConnMaxIdleTime = maxIdleTime
	}
}

// WithPostgresQueryTimeout sets the query timeout.
func WithPostgresQueryTimeout(timeout time.Duration) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.QueryTimeout = timeout
	}
}

// WithPostgresMetadata sets user metadata for the writer.
func WithPostgresMetadata(metadata map[string]string) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		if opts.Metadata == nil {
			opts.Metadata = make(map[string]string)
		}
		for k, v := range metadata {
			opts.Metadata[k] = v
		}
	}
}

// Add error threshold option
func WithMaxErrors(maxErrors int64) PostgresWriterOption {
	return func(opts *PostgresWriterOptions) {
		opts.MaxErrors = maxErrors
	}
}

// PostgresWriter implements goetl.DataSink for PostgreSQL output.
// It supports batching, transactions, conflict resolution, and statistics.
type PostgresWriter struct {
	db          *sql.DB
	options     PostgresWriterOptions
	columns     []string
	recordBuf   []core.Record
	stats       PostgresWriterStats
	prepared    *sql.Stmt
	initialized bool
	mu          sync.Mutex
	valuePool   sync.Pool
	errorState  bool
	errorCount  int64
	maxErrors   int64
}

// NewPostgresWriter creates a new PostgreSQL writer with the given options.
// Accepts functional options for configuration. Returns a ready-to-use writer or an error.
func NewPostgresWriter(opts ...PostgresWriterOption) (*PostgresWriter, error) {
	options := &PostgresWriterOptions{}
	options = options.withDefaults()

	for _, opt := range opts {
		opt(options)
	}

	if err := validateOptions(options); err != nil {
		return nil, &PostgresWriterError{Op: "validate", Err: err}
	}

	writer := &PostgresWriter{
		options:   *options,
		columns:   append([]string(nil), options.Columns...),
		recordBuf: make([]core.Record, 0, options.BatchSize),
		stats:     PostgresWriterStats{NullValueCounts: make(map[string]int64)},
		maxErrors: options.MaxErrors,
		valuePool: sync.Pool{
			New: func() interface{} {
				return make([]interface{}, 0, 20) // Pre-allocate for common case
			},
		},
	}

	if err := writer.connect(); err != nil {
		return nil, &PostgresWriterError{Op: "connect", Err: err}
	}

	return writer, nil
}

// Stats returns a copy of the current write statistics.
func (w *PostgresWriter) Stats() PostgresWriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Return a copy to prevent races
	statsCopy := w.stats
	statsCopy.NullValueCounts = make(map[string]int64)
	for k, v := range w.stats.NullValueCounts {
		statsCopy.NullValueCounts[k] = v
	}
	return statsCopy
}

// Write implements the core.DataSink interface.
// Buffers records and writes in batches. Thread-safe.
func (w *PostgresWriter) Write(ctx context.Context, record core.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.db == nil {
		return &PostgresWriterError{Op: "write", Err: fmt.Errorf("writer is closed")}
	}

	if w.errorState && w.errorCount >= w.options.MaxErrors {
		return &PostgresWriterError{Op: "write", Err: fmt.Errorf("writer exceeded maximum error count: %d", w.maxErrors)}
	}

	// Check context cancellation early
	select {
	case <-ctx.Done():
		return &PostgresWriterError{Op: "write", Err: ctx.Err()}
	default:
	}

	if !w.initialized {
		if err := w.initializeUnsafe(ctx, record); err != nil {
			w.errorState = true
			w.errorCount++
			return &PostgresWriterError{Op: "initialize", Err: err}
		}
	}

	// Track null values
	for k, v := range record {
		if v == nil {
			if w.stats.NullValueCounts == nil {
				w.stats.NullValueCounts = make(map[string]int64)
			}
			w.stats.NullValueCounts[k]++
		}
	}

	w.recordBuf = append(w.recordBuf, record)
	w.stats.RecordsWritten++

	if len(w.recordBuf) >= w.options.BatchSize {
		if err := w.flushBufferUnsafe(ctx); err != nil {
			w.errorState = true
			w.errorCount++
			return &PostgresWriterError{Op: "flush_batch", Err: err}
		}
		// Reset error state on successful flush
		w.errorState = false
	}

	return nil
}

// Flush implements the core.DataSink interface.
// Forces any buffered records to be written to PostgreSQL.
func (w *PostgresWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), w.options.QueryTimeout)
	defer cancel()

	return w.flushBufferUnsafe(ctx)
}

// Close implements the core.DataSink interface.
// Flushes and closes all resources.
func (w *PostgresWriter) Close() error {
	flushErr := w.Flush()

	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error
	if flushErr != nil {
		errs = append(errs, fmt.Errorf("flush failed: %w", flushErr))
	}

	if w.prepared != nil {
		if err := w.prepared.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close prepared statement: %w", err))
		}
		w.prepared = nil
	}

	if w.db != nil {
		if err := w.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close database: %w", err))
		}
		w.db = nil
	}

	w.errorState = true

	if len(errs) > 0 {
		return &PostgresWriterError{Op: "close", Err: fmt.Errorf("multiple errors: %v", errs)}
	}

	return nil
}

// withDefaults applies default values to PostgresWriterOptions.
func (opts *PostgresWriterOptions) withDefaults() *PostgresWriterOptions {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if opts.QueryTimeout == 0 {
		opts.QueryTimeout = 30 * time.Second
	}
	if opts.ConnMaxLifetime == 0 {
		opts.ConnMaxLifetime = 5 * time.Minute
	}
	if opts.ConnMaxIdleTime == 0 {
		opts.ConnMaxIdleTime = 1 * time.Minute
	}
	if opts.MaxOpenConns <= 0 {
		opts.MaxOpenConns = 10
	}
	if opts.MaxIdleConns <= 0 {
		opts.MaxIdleConns = 5
	}
	if opts.MaxErrors <= 0 {
		opts.MaxErrors = 100 // Default error threshold
	}
	if opts.Metadata == nil {
		opts.Metadata = make(map[string]string)
	}
	return opts
}

// validateOptions validates the PostgreSQL writer options.
func validateOptions(opts *PostgresWriterOptions) error {
	if opts.DSN == "" {
		return fmt.Errorf("dsn is required")
	}
	if opts.TableName == "" {
		return fmt.Errorf("table name is required")
	}
	if opts.ConflictResolution == ConflictUpdate && len(opts.UpdateColumns) == 0 {
		return fmt.Errorf("update columns required for conflict update resolution")
	}
	if opts.ConflictResolution != ConflictError && len(opts.ConflictColumns) == 0 {
		return fmt.Errorf("conflict columns required for conflict resolution")
	}
	return nil
}

// connect establishes the database connection and configures the connection pool.
func (w *PostgresWriter) connect() error {
	start := time.Now()

	db, err := sql.Open("postgres", w.options.DSN)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(w.options.MaxOpenConns)
	db.SetMaxIdleConns(w.options.MaxIdleConns)
	db.SetConnMaxLifetime(w.options.ConnMaxLifetime)
	db.SetConnMaxIdleTime(w.options.ConnMaxIdleTime)

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), w.options.QueryTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	w.db = db
	w.stats.ConnectionTime = time.Since(start)

	return nil
}

// initializeUnsafe performs one-time initialization (must hold mutex).
func (w *PostgresWriter) initializeUnsafe(ctx context.Context, firstRecord core.Record) error {
	// Determine columns from first record if not specified
	if len(w.columns) == 0 {
		for key := range firstRecord {
			w.columns = append(w.columns, key)
		}
		sort.Strings(w.columns)
	}

	// Create table if requested
	if w.options.CreateTable {
		if err := w.createTableUnsafe(ctx, firstRecord); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Truncate table if requested
	if w.options.TruncateTable {
		if err := w.truncateTableUnsafe(ctx); err != nil {
			return fmt.Errorf("failed to truncate table: %w", err)
		}
	}

	// Prepare insert statement
	if err := w.prepareStatementUnsafe(ctx); err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	w.initialized = true
	return nil
}

// createTableUnsafe creates the target table based on the first record (must hold mutex).
func (w *PostgresWriter) createTableUnsafe(ctx context.Context, record core.Record) error {
	var columns []string
	for _, col := range w.columns {
		value := record[col]
		sqlType := w.inferSQLType(value)
		if !isValidIdentifier(col) {
			return &PostgresWriterError{Op: "validate_column", Err: fmt.Errorf("invalid column name: %s", col)}
		}
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col, sqlType))
	}
	if !isValidIdentifier(w.options.TableName) {
		return &PostgresWriterError{Op: "validate_table", Err: fmt.Errorf("invalid table name: %s", w.options.TableName)}
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s)`, w.options.TableName, strings.Join(columns, ", "))
	_, err := w.db.ExecContext(ctx, query)
	if err != nil {
		return &PostgresWriterError{Op: "create_table", Err: err}
	}
	return nil
}

// isValidIdentifier validates PostgreSQL identifiers for security
func isValidIdentifier(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	// Must start with letter or underscore
	if !((name[0] >= 'a' && name[0] <= 'z') || (name[0] >= 'A' && name[0] <= 'Z') || name[0] == '_') {
		return false
	}
	// Rest must be alphanumeric or underscore
	for _, r := range name[1:] {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// truncateTableUnsafe truncates the target table (must hold mutex).
func (w *PostgresWriter) truncateTableUnsafe(ctx context.Context) error {
	// Use quoted identifier for safety
	query := fmt.Sprintf(`TRUNCATE TABLE "%s"`, w.options.TableName)
	_, err := w.db.ExecContext(ctx, query)
	if err != nil {
		return &PostgresWriterError{Op: "truncate_table", Err: err}
	}
	return err
}

// prepareStatementUnsafe prepares the INSERT statement (must hold mutex).
func (w *PostgresWriter) prepareStatementUnsafe(ctx context.Context) error {
	placeholders := make([]string, len(w.columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	quotedColumns := make([]string, len(w.columns))
	for i, col := range w.columns {
		quotedColumns[i] = fmt.Sprintf(`"%s"`, col)
	}

	quotedConflictColumns := make([]string, len(w.options.ConflictColumns))
	for i, col := range w.options.ConflictColumns {
		quotedConflictColumns[i] = fmt.Sprintf(`"%s"`, col)
	}
	var query string
	switch w.options.ConflictResolution {
	case ConflictIgnore:
		query = fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING`,
			w.options.TableName,
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(quotedConflictColumns, ", "))
	case ConflictUpdate:
		updateClauses := make([]string, len(w.options.UpdateColumns))
		for i, col := range w.options.UpdateColumns {
			updateClauses[i] = fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col, col)
		}
		query = fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s`,
			w.options.TableName,
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(quotedConflictColumns, ", "),
			strings.Join(updateClauses, ", "))
	default:
		query = fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`,
			w.options.TableName,
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", "))
	}

	stmt, err := w.db.PrepareContext(ctx, query)
	if err != nil {
		return &PostgresWriterError{Op: "prepare_statement", Err: err}
	}

	w.prepared = stmt
	return nil
}

// flushBufferUnsafe writes buffered records to PostgreSQL (must hold mutex).
func (w *PostgresWriter) flushBufferUnsafe(ctx context.Context) error {
	if len(w.recordBuf) == 0 {
		return nil
	}

	start := time.Now()

	var tx *sql.Tx
	var err error

	if w.options.TransactionMode {
		tx, err = w.db.BeginTx(ctx, nil)
		if err != nil {
			return &PostgresWriterError{Op: "begin_transaction", Err: err}
		}
		defer func() {
			if err != nil {
				tx.Rollback()
			}
		}()
	}

	var stmt *sql.Stmt
	if tx != nil {
		stmt = tx.StmtContext(ctx, w.prepared)
		defer stmt.Close()
	} else {
		stmt = w.prepared
	}

	for _, record := range w.recordBuf {
		values := w.valuePool.Get().([]interface{})
		if cap(values) < len(w.columns) {
			values = make([]interface{}, len(w.columns))
		} else {
			values = values[:len(w.columns)]
		}

		for i, col := range w.columns {
			if val, ok := record[col]; ok {
				values[i] = w.convertValue(val)
			} else {
				values[i] = nil
			}
		}

		result, err := stmt.ExecContext(ctx, values...)

		for i := range values {
			values[i] = nil
		}

		w.valuePool.Put(values[:0])

		if err != nil {
			return &PostgresWriterError{Op: "execute_insert", Err: err}
		}

		// Check for conflicts
		if rowsAffected, err := result.RowsAffected(); err == nil && rowsAffected == 0 {
			w.stats.ConflictCount++
		}
	}

	if tx != nil {
		if err = tx.Commit(); err != nil {
			return &PostgresWriterError{Op: "commit_transaction", Err: err}
		}
		w.stats.TransactionCount++
	}

	// Update statistics
	writeDuration := time.Since(start)
	w.stats.BatchesWritten++
	w.stats.LastWriteTime = time.Now()
	w.stats.WriteDuration += writeDuration
	w.recordBuf = w.recordBuf[:0]

	// Even better: Periodically reset capacity if it grows too large
	if cap(w.recordBuf) > w.options.BatchSize*4 {
		w.recordBuf = make([]core.Record, 0, w.options.BatchSize)
	}

	return nil
}

// inferSQLType infers PostgreSQL column type from Go value.
func (w *PostgresWriter) inferSQLType(value interface{}) string {
	if value == nil {
		return "TEXT"
	}

	switch value.(type) {
	case bool:
		return "BOOLEAN"
	case int, int8, int16, int32, int64:
		return "BIGINT"
	case uint, uint8, uint16, uint32, uint64:
		return "BIGINT"
	case float32, float64:
		return "DOUBLE PRECISION"
	case time.Time:
		return "TIMESTAMP"
	case []byte:
		return "BYTEA"
	default:
		return "TEXT"
	}
}

// convertValue converts Go values to PostgreSQL-compatible types.
func (w *PostgresWriter) convertValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case time.Time, bool, int64, float64, string, []byte:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float32:
		return float64(v)
	default:
		// Use reflection for remaining types
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16:
			return rv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// Check for overflow
			uval := rv.Uint()
			if uval > 9223372036854775807 { // math.MaxInt64
				return fmt.Sprintf("%v", v) // Convert large uints to string
			}
			return int64(uval)
		case reflect.Float32:
			return float64(rv.Float())
		case reflect.Slice:
			if rv.Type().Elem().Kind() == reflect.Uint8 {
				return rv.Bytes() // Handle []uint8 as []byte
			}
			fallthrough
		default:
			// Safe fallback to string representation
			return fmt.Sprintf("%v", v)
		}
	}
}
