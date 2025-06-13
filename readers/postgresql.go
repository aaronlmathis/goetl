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

package readers

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/aaronlmathis/goetl/core"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// Package readers provides implementations of core.DataSource for reading data from various sources.
//
// This file implements a high-performance, configurable PostgreSQL reader for streaming ETL pipelines.
// It supports batching, connection pooling, cursor-based streaming, query parameterization, and statistics.

// PostgresReaderError provides structured error information for Postgres reader operations
type PostgresReaderError struct {
	Op  string // Operation that failed (e.g., "connect", "query", "scan", "read")
	Err error  // Underlying error
}

func (e *PostgresReaderError) Error() string {
	return fmt.Sprintf("postgres reader %s: %v", e.Op, e.Err)
}

func (e *PostgresReaderError) Unwrap() error {
	return e.Err
}

// PostgresReader implements core.DataSource for PostgreSQL databases.
// Supports streaming query results with configurable batch processing, connection pooling, and cursor-based streaming.
type PostgresReader struct {
	mu                  sync.Mutex
	db                  *sql.DB
	tx                  *sql.Tx
	rows                *sql.Rows
	columnNames         []string
	columnTypes         []*sql.ColumnType
	scanBuffer          []interface{}
	values              []interface{}
	currentRow          int64
	batchSize           int
	query               string
	params              []interface{}
	stats               PostgresReaderStats
	opts                *PostgresReaderOptions
	isFinished          bool
	bufferPool          sync.Pool
	lastHealthCheck     time.Time
	healthCheckInterval time.Duration
}

// PostgresReaderStats holds statistics about the Postgres reader's performance
type PostgresReaderStats struct {
	RecordsRead     int64
	QueryDuration   time.Duration
	ReadDuration    time.Duration
	LastReadTime    time.Time
	NullValueCounts map[string]int64
	ConnectionTime  time.Duration
}

// PostgresReaderOptions configures the Postgres reader
type PostgresReaderOptions struct {
	DSN                 string            // Database connection string
	Query               string            // SQL query to execute
	Params              []interface{}     // Optional query parameters
	BatchSize           int               // Records to fetch per batch (used for cursor queries)
	ConnMaxLifetime     time.Duration     // Maximum connection lifetime
	ConnMaxIdleTime     time.Duration     // Maximum connection idle time
	MaxOpenConns        int               // Maximum open connections
	MaxIdleConns        int               // Maximum idle connections
	QueryTimeout        time.Duration     // Query execution timeout
	Metadata            map[string]string // Custom metadata
	UseCursor           bool              // Use server-side cursor for large results
	CursorName          string            // Name for the cursor (if UseCursor is true)
	HealthCheckInterval time.Duration
}

// PostgresReaderOption represents a configuration function for PostgresReaderOptions
type PostgresReaderOption func(*PostgresReaderOptions)

// Functional option functions

// WithPostgresDSN sets the PostgreSQL connection string.
func WithPostgresDSN(dsn string) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		opts.DSN = dsn
	}
}

// WithPostgresQuery sets the SQL query and optional parameters.
func WithPostgresQuery(query string, params ...interface{}) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		opts.Query = query
		if len(params) > 0 {
			opts.Params = make([]interface{}, len(params))
			copy(opts.Params, params)
		}
	}
}

// WithPostgresBatchSize sets the batch size for query results.
func WithPostgresBatchSize(size int) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		opts.BatchSize = size
	}
}

// WithPostgresConnectionPool configures the connection pool.
func WithPostgresConnectionPool(maxOpen, maxIdle int) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		opts.MaxOpenConns = maxOpen
		opts.MaxIdleConns = maxIdle
	}
}

// WithPostgresConnectionTimeout sets connection and idle timeouts.
func WithPostgresConnectionTimeout(lifetime, idleTime time.Duration) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		opts.ConnMaxLifetime = lifetime
		opts.ConnMaxIdleTime = idleTime
	}
}

// Add functional option for health check interval
func WithPostgresHealthCheckInterval(interval time.Duration) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		opts.HealthCheckInterval = interval
	}
}

// WithPostgresQueryTimeout sets the query execution timeout.
func WithPostgresQueryTimeout(timeout time.Duration) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		opts.QueryTimeout = timeout
	}
}

// WithPostgresMetadata sets user metadata for the reader.
func WithPostgresMetadata(metadata map[string]string) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		if opts.Metadata == nil {
			opts.Metadata = make(map[string]string)
		}
		for k, v := range metadata {
			opts.Metadata[k] = v
		}
	}
}

// WithPostgresCursor enables or disables server-side cursor usage for large results.
func WithPostgresCursor(useCursor bool, cursorName string) PostgresReaderOption {
	return func(opts *PostgresReaderOptions) {
		opts.UseCursor = useCursor
		opts.CursorName = cursorName
	}
}

// NewPostgresReader creates a new PostgreSQL reader with the given options.
// Accepts functional options for configuration. Returns a ready-to-use reader or an error.
func NewPostgresReader(options ...PostgresReaderOption) (*PostgresReader, error) {
	// Start with defaults
	opts := (&PostgresReaderOptions{}).withDefaults()

	// Apply functional options
	for _, option := range options {
		option(opts)
	}

	return createPostgresReader(opts)
}

// createPostgresReader initializes the PostgreSQL reader with the given options
func createPostgresReader(opts *PostgresReaderOptions) (*PostgresReader, error) {
	if opts.DSN == "" {
		return nil, &PostgresReaderError{Op: "validate", Err: fmt.Errorf("dsn is required")}
	}
	if opts.Query == "" {
		return nil, &PostgresReaderError{Op: "validate", Err: fmt.Errorf("query is required")}
	}

	// Connect to database
	startTime := time.Now()
	db, err := sql.Open("postgres", opts.DSN)
	if err != nil {
		return nil, &PostgresReaderError{Op: "connect", Err: err}
	}

	// Configure connection pool
	if opts.MaxOpenConns > 0 {
		db.SetMaxOpenConns(opts.MaxOpenConns)
	}
	if opts.MaxIdleConns > 0 {
		db.SetMaxIdleConns(opts.MaxIdleConns)
	}
	if opts.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	}
	if opts.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(opts.ConnMaxIdleTime)
	}

	// Test connection
	ctx := context.Background()
	if opts.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.QueryTimeout)
		defer cancel()
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, &PostgresReaderError{Op: "ping", Err: err}
	}

	connectionTime := time.Since(startTime)

	reader := &PostgresReader{
		db:                  db,
		query:               opts.Query,
		params:              opts.Params,
		batchSize:           opts.BatchSize,
		opts:                opts,
		healthCheckInterval: opts.HealthCheckInterval,
		lastHealthCheck:     time.Now(),
		stats: PostgresReaderStats{
			NullValueCounts: make(map[string]int64),
			ConnectionTime:  connectionTime,
		},
		isFinished: false,
	}

	// Execute query and prepare for reading
	if err := reader.executeQuery(ctx); err != nil {
		reader.Close()
		return nil, err
	}

	return reader, nil
}

// Stats returns statistics about the PostgreSQL reader's performance
func (p *PostgresReader) Stats() PostgresReaderStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create a deep copy to prevent race conditions
	statsCopy := PostgresReaderStats{
		RecordsRead:     p.stats.RecordsRead,
		QueryDuration:   p.stats.QueryDuration,
		ReadDuration:    p.stats.ReadDuration,
		LastReadTime:    p.stats.LastReadTime,
		ConnectionTime:  p.stats.ConnectionTime,
		NullValueCounts: make(map[string]int64),
	}

	// Deep copy the map
	for k, v := range p.stats.NullValueCounts {
		statsCopy.NullValueCounts[k] = v
	}

	return statsCopy
}

// Read implements the core.DataSource interface.
// Reads the next record from the PostgreSQL query result. Thread-safe.
func (p *PostgresReader) Read(ctx context.Context) (core.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	startTime := time.Now()
	defer func() {
		p.stats.ReadDuration += time.Since(startTime)
		p.stats.LastReadTime = time.Now()
	}()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, &PostgresReaderError{Op: "read", Err: ctx.Err()}
	default:
	}

	if p.db == nil {
		return nil, &PostgresReaderError{Op: "read", Err: fmt.Errorf("reader is closed")}
	}

	// Add connection health check
	if p.db != nil && time.Since(p.lastHealthCheck) > p.healthCheckInterval {
		if err := p.db.PingContext(ctx); err != nil {
			return nil, &PostgresReaderError{Op: "ping", Err: err}
		}
		p.lastHealthCheck = time.Now()
	}

	if p.isFinished || p.rows == nil {
		return nil, io.EOF
	}

	// Check if there are more rows
	if !p.rows.Next() {
		// Check for errors
		if err := p.rows.Err(); err != nil {
			return nil, &PostgresReaderError{Op: "read", Err: err}
		}
		p.isFinished = true
		return nil, io.EOF
	}

	// Scan the row
	if err := p.rows.Scan(p.scanBuffer...); err != nil {
		return nil, &PostgresReaderError{Op: "scan", Err: err}
	}

	// Convert to core.Record
	record := p.convertRowToRecord()
	p.currentRow++
	p.stats.RecordsRead++

	return record, nil
}

// Close releases all resources held by the PostgreSQL reader
func (p *PostgresReader) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error

	if p.scanBuffer != nil {
		p.bufferPool.Put(&p.scanBuffer)
		p.scanBuffer = nil
		p.values = nil
	}

	if p.rows != nil {
		if err := p.rows.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing rows: %w", err))
		}
		p.rows = nil
	}

	if p.tx != nil {
		if err := p.tx.Rollback(); err != nil {
			errs = append(errs, fmt.Errorf("rolling back transaction: %w", err))
		}
		p.tx = nil
	}

	if p.db != nil {
		if err := p.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing database: %w", err))
		}
		p.db = nil
	}

	if len(errs) > 0 {
		return &PostgresReaderError{Op: "close", Err: fmt.Errorf("multiple errors: %v", errs)}
	}

	return nil
}

// Schema returns information about the columns in the query result.
// Returns a map of column name to database type name.
func (p *PostgresReader) Schema() map[string]string {
	schema := make(map[string]string)
	for i, name := range p.columnNames {
		if i < len(p.columnTypes) {
			schema[name] = p.columnTypes[i].DatabaseTypeName()
		}
	}
	return schema
}

// withDefaults applies default values to PostgresReaderOptions
func (opts *PostgresReaderOptions) withDefaults() *PostgresReaderOptions {
	result := &PostgresReaderOptions{}

	// Copy existing values if opts is not nil
	if opts != nil {
		*result = *opts
	}

	// Apply defaults for zero values
	if result.BatchSize <= 0 {
		result.BatchSize = 1000
	}
	if result.QueryTimeout <= 0 {
		result.QueryTimeout = 30 * time.Second
	}
	if result.ConnMaxLifetime <= 0 {
		result.ConnMaxLifetime = 5 * time.Minute
	}
	if result.ConnMaxIdleTime <= 0 {
		result.ConnMaxIdleTime = 1 * time.Minute
	}
	if result.MaxOpenConns <= 0 {
		result.MaxOpenConns = 10
	}
	if result.MaxIdleConns <= 0 {
		result.MaxIdleConns = 5
	}
	if result.HealthCheckInterval <= 0 {
		result.HealthCheckInterval = 30 * time.Second // Default health check interval
	}
	// Initialize maps if nil
	if result.Metadata == nil {
		result.Metadata = make(map[string]string)
	}

	return result
}

// executeQuery executes the SQL query and prepares the reader for streaming results
func (p *PostgresReader) executeQuery(ctx context.Context) error {
	startTime := time.Now()

	var err error

	// Use cursor for large result sets if enabled
	if p.opts.UseCursor {
		err = p.executeWithCursor(ctx)
	} else {
		// Direct query execution
		p.rows, err = p.db.QueryContext(ctx, p.query, p.params...)
	}

	if err != nil {
		return &PostgresReaderError{Op: "query", Err: err}
	}

	p.stats.QueryDuration = time.Since(startTime)

	// Get column information
	columnNames, err := p.rows.Columns()
	if err != nil {
		return &PostgresReaderError{Op: "columns", Err: err}
	}
	p.columnNames = columnNames

	columnTypes, err := p.rows.ColumnTypes()
	if err != nil {
		return &PostgresReaderError{Op: "column_types", Err: err}
	}
	p.columnTypes = columnTypes

	// Prepare scan buffers
	p.prepareScanBuffers()

	return nil
}

// executeWithCursor executes the query using a server-side cursor for memory efficiency
func (p *PostgresReader) executeWithCursor(ctx context.Context) error {
	// Begin transaction for cursor
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return &PostgresReaderError{Op: "begin_transaction", Err: err}
	}

	p.tx = tx

	cursorName := p.opts.CursorName
	if cursorName == "" {
		cursorName = "goetl_cursor"
	}

	// Validate cursor name to prevent SQL injection
	if !isValidCursorName(cursorName) {
		p.tx.Rollback()
		p.tx = nil
		return &PostgresReaderError{Op: "validate_cursor",
			Err: fmt.Errorf("invalid cursor name: %s", cursorName)}
	}

	// Declare cursor
	declareSQL := fmt.Sprintf("DECLARE %s CURSOR FOR %s", cursorName, p.query)
	if _, err := tx.ExecContext(ctx, declareSQL, p.params...); err != nil {
		tx.Rollback()
		return &PostgresReaderError{Op: "declare_cursor", Err: err}
	}

	// Fetch initial batch
	fetchSQL := fmt.Sprintf("FETCH %d FROM %s", p.batchSize, cursorName)
	p.rows, err = tx.QueryContext(ctx, fetchSQL)
	if err != nil {
		p.tx.Rollback()
		p.tx = nil
		return &PostgresReaderError{Op: "fetch_cursor", Err: err}
	}
	return nil
}

// isValidCursorName validates cursor name for SQL injection prevention
func isValidCursorName(name string) bool {
	// Only allow alphanumeric characters and underscores
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return len(name) > 0 && len(name) <= 63 // PostgreSQL identifier limit
}

// prepareScanBuffers prepares the buffers needed for scanning SQL rows
func (p *PostgresReader) prepareScanBuffers() {
	numCols := len(p.columnNames)
	// Try to reuse buffers from pool
	if pooled := p.bufferPool.Get(); pooled != nil {
		if buf, ok := pooled.(*[]interface{}); ok && len(*buf) >= numCols {
			p.scanBuffer = (*buf)[:numCols]
			p.values = make([]interface{}, numCols)
			for i := range p.scanBuffer {
				p.scanBuffer[i] = &p.values[i]
			}
			return
		}
	}
	p.scanBuffer = make([]interface{}, numCols)
	p.values = make([]interface{}, numCols)

	for i := range p.scanBuffer {
		p.scanBuffer[i] = &p.values[i]
	}
}

// convertSQLValue converts SQL driver values to appropriate Go types
func (r *PostgresReader) convertSQLValue(value interface{}, colType *sql.ColumnType) interface{} {

	// Handle byte arrays for text types
	if b, ok := value.([]byte); ok {
		dbType := colType.DatabaseTypeName()
		switch dbType {
		case "TEXT", "VARCHAR", "CHAR", "BPCHAR":
			return string(b)
		default:
			// Keep as byte array for binary types like BYTEA
			return b
		}
	}

	// Handle other types directly
	switch v := value.(type) {
	case time.Time, bool, int64, float64, string:
		return v
	default:
		// Use reflection for type conversion
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			return rv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return int64(rv.Uint())
		case reflect.Float32:
			return float64(rv.Float())
		default:
			// Fallback to string representation
			return fmt.Sprintf("%v", v)
		}
	}
}

// convertRowToRecord converts the scanned SQL row values to a core.Record
func (p *PostgresReader) convertRowToRecord() core.Record {
	record := make(core.Record)

	for i, columnName := range p.columnNames {
		value := p.values[i]

		if value == nil {

			if p.stats.NullValueCounts == nil {
				p.stats.NullValueCounts = make(map[string]int64)
			}
			p.stats.NullValueCounts[columnName]++
			record[columnName] = nil
			continue
		}

		// Convert SQL types to Go types - fix the issue on line 369
		record[columnName] = p.convertSQLValue(value, p.columnTypes[i])
	}

	return record
}
