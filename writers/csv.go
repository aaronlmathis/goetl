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
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/aaronlmathis/goetl"
)

// Package writers provides implementations of goetl.DataSink for writing data to various destinations.
//
// This file implements a high-performance, configurable CSV writer for streaming ETL pipelines.
// It supports batching, header management, delimiter configuration, and statistics for CSV output.

// CSVWriterError wraps CSV-specific write errors with context about the operation.
type CSVWriterError struct {
	Op  string // Operation that failed (e.g., "write", "flush", "write_row")
	Err error  // Underlying error
}

func (e *CSVWriterError) Error() string {
	return fmt.Sprintf("csv writer %s: %v", e.Op, e.Err)
}

func (e *CSVWriterError) Unwrap() error {
	return e.Err
}

// CSVWriterStats holds CSV write performance statistics.
type CSVWriterStats struct {
	RecordsWritten  int64            // Total records written
	FlushCount      int64            // Number of flushes performed
	FlushDuration   time.Duration    // Total time spent flushing
	LastFlushTime   time.Time        // Time of last flush
	NullValueCounts map[string]int64 // Count of null values per field
}

// CSVWriterOptions configures CSV output.
type CSVWriterOptions struct {
	Comma       rune     // Field delimiter (default ',')
	UseCRLF     bool     // Use CRLF line endings
	WriteHeader bool     // Write header row
	Headers     []string // Explicit header order
	BatchSize   int      // Number of records to buffer before writing
}

// WriterOptionCSV is a functional option for CSVWriter configuration.
type WriterOptionCSV func(*CSVWriterOptions)

// WithHeaders sets the header row for the CSV output.
func WithHeaders(headers []string) WriterOptionCSV {
	return func(opts *CSVWriterOptions) {
		opts.Headers = append([]string(nil), headers...) // copy
	}
}

// WithComma sets the field delimiter for the CSV output.
func WithComma(delim rune) WriterOptionCSV {
	return func(opts *CSVWriterOptions) {
		opts.Comma = delim
	}
}

// WithWriteHeader enables or disables writing the header row.
func WithWriteHeader(write bool) WriterOptionCSV {
	return func(opts *CSVWriterOptions) {
		opts.WriteHeader = write
	}
}

// WithCSVBatchSize sets the batch size for the CSVWriter.
func WithCSVBatchSize(size int) WriterOptionCSV {
	return func(opts *CSVWriterOptions) {
		opts.BatchSize = size
	}
}

// WithUseCRLF enables or disables CRLF line endings.
func WithUseCRLF(useCRLF bool) WriterOptionCSV {
	return func(opts *CSVWriterOptions) {
		opts.UseCRLF = useCRLF
	}
}

// Deprecated: Use WithHeaders instead.
func WithCSVHeaders(headers []string) WriterOptionCSV {
	return WithHeaders(headers)
}

// Deprecated: Use WithComma instead.
func WithCSVDelimiter(delim rune) WriterOptionCSV {
	return WithComma(delim)
}

// Deprecated: Use WithWriteHeader instead.
func WithCSVWriteHeader(write bool) WriterOptionCSV {
	return WithWriteHeader(write)
}

// CSVWriter implements goetl.DataSink for CSV output with stats and batching.
// It supports batching, header management, delimiter configuration, and statistics.
type CSVWriter struct {
	writer      *csv.Writer
	closer      io.Closer
	options     CSVWriterOptions
	headers     []string
	recordBuf   []goetl.Record
	stats       CSVWriterStats
	wroteHeader bool
	errorState  bool
	mu          sync.Mutex // Add concurrency safety
}

// NewCSVWriter creates a new CSV writer with extended options.
// Accepts functional options for configuration. Returns a ready-to-use writer or an error.
func NewCSVWriter(w io.WriteCloser, opts ...WriterOptionCSV) (*CSVWriter, error) {
	options := CSVWriterOptions{
		Comma:       ',',
		UseCRLF:     false,
		WriteHeader: true,
		BatchSize:   0,
	}

	for _, opt := range opts {
		opt(&options)
	}

	cw := csv.NewWriter(w)
	cw.Comma = options.Comma
	cw.UseCRLF = options.UseCRLF

	return &CSVWriter{
		writer:    cw,
		closer:    w,
		options:   options,
		headers:   append([]string(nil), options.Headers...),
		recordBuf: make([]goetl.Record, 0, max(options.BatchSize, 1)),
		stats:     CSVWriterStats{NullValueCounts: make(map[string]int64)},
	}, nil
}

// Write implements the goetl.DataSink interface.
// Buffers records and writes in batches or on flush. Thread-safe.
func (c *CSVWriter) Write(ctx context.Context, record goetl.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.errorState {
		return &CSVWriterError{Op: "write", Err: fmt.Errorf("writer is in error state")}
	}

	// Track nulls
	for k, v := range record {
		if v == nil {
			c.stats.NullValueCounts[k]++
		}
	}

	c.recordBuf = append(c.recordBuf, record)
	c.stats.RecordsWritten++

	// Write headers from first record if not specified
	if !c.wroteHeader && c.options.WriteHeader {
		if len(c.headers) == 0 {
			for key := range record {
				c.headers = append(c.headers, key)
			}
			sort.Strings(c.headers)
		}
		if err := c.writer.Write(c.headers); err != nil {
			c.errorState = true
			return &CSVWriterError{Op: "write_header", Err: err}
		}
		c.wroteHeader = true
	}

	if c.options.BatchSize <= 0 {
		if err := c.flushBufferUnsafe(); err != nil {
			c.errorState = true
			return &CSVWriterError{Op: "flush", Err: err}
		}
	} else if len(c.recordBuf) >= c.options.BatchSize {
		if err := c.flushBufferUnsafe(); err != nil {
			c.errorState = true
			return &CSVWriterError{Op: "flush_batch", Err: err}
		}
	}

	return nil
}

// Flush implements the goetl.DataSink interface.
// Forces any buffered records to be written to the CSV output.
func (c *CSVWriter) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.flushBufferUnsafe(); err != nil {
		return &CSVWriterError{Op: "flush", Err: err}
	}
	c.writer.Flush()
	if err := c.writer.Error(); err != nil {
		return &CSVWriterError{Op: "flush_writer", Err: err}
	}
	return nil
}

// Close implements the goetl.DataSink interface.
// Flushes and closes all resources.
func (c *CSVWriter) Close() error {
	if err := c.Flush(); err != nil {
		return err
	}
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

// flushBufferUnsafe writes buffered records to CSV (must hold mutex).
func (c *CSVWriter) flushBufferUnsafe() error {
	if len(c.recordBuf) == 0 {
		return nil
	}

	start := time.Now()

	for _, record := range c.recordBuf {
		row := make([]string, len(c.headers))
		for i, key := range c.headers {
			if val, ok := record[key]; ok && val != nil {
				row[i] = fmt.Sprintf("%v", val)
			} else {
				row[i] = ""
			}
		}
		if err := c.writer.Write(row); err != nil {
			return &CSVWriterError{
				Op:  "write_row",
				Err: fmt.Errorf("failed to write CSV row: %w", err),
			}
		}
	}

	c.writer.Flush()
	if err := c.writer.Error(); err != nil {
		return &CSVWriterError{
			Op:  "csv_flush",
			Err: fmt.Errorf("CSV writer flush error: %w", err),
		}
	}

	// Update statistics
	flushDuration := time.Since(start)
	c.stats.FlushCount++
	c.stats.LastFlushTime = time.Now()
	c.stats.FlushDuration += flushDuration
	c.recordBuf = c.recordBuf[:0]

	return nil
}

// Stats returns write statistics.
func (c *CSVWriter) Stats() CSVWriterStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return a copy to prevent races
	statsCopy := c.stats
	statsCopy.NullValueCounts = make(map[string]int64)
	for k, v := range c.stats.NullValueCounts {
		statsCopy.NullValueCounts[k] = v
	}
	return statsCopy
}

// max returns the maximum of two integers.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
