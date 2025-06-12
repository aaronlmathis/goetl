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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aaronlmathis/goetl"
)

// Package writers provides implementations of goetl.DataSink for writing data to various destinations.
//
// This file implements a high-performance, configurable JSON writer for streaming ETL pipelines.
// It supports batching, buffered output, flush control, and statistics for line-delimited JSON output.

// JSONWriterError wraps detailed error context for JSONWriter operations.
type JSONWriterError struct {
	Op  string // Operation that failed (e.g., "write", "flush", "marshal_record")
	Err error  // Underlying error
}

func (e *JSONWriterError) Error() string {
	return fmt.Sprintf("json writer %s: %v", e.Op, e.Err)
}

func (e *JSONWriterError) Unwrap() error {
	return e.Err
}

// JSONWriterOptions configures the JSONWriter behavior.
type JSONWriterOptions struct {
	BatchSize    int  // Number of records to buffer before writing
	FlushOnWrite bool // Whether to flush after every write
	BufferSize   int  // Size of the underlying buffer in bytes
}

// WithJSONBatchSize sets the batch size for the JSONWriter.
func WithJSONBatchSize(size int) WriterOptionJSON {
	return func(o *JSONWriterOptions) {
		o.BatchSize = size
	}
}

// WithFlushOnWrite enables or disables flushing after every write.
func WithFlushOnWrite(enabled bool) WriterOptionJSON {
	return func(o *JSONWriterOptions) {
		o.FlushOnWrite = enabled
	}
}

// WithBufferSize sets the buffer size for the JSONWriter.
// This controls the size of the underlying bufio.Writer buffer.
// Larger buffers can improve performance for high-throughput scenarios.
func WithBufferSize(size int) WriterOptionJSON {
	return func(o *JSONWriterOptions) {
		o.BufferSize = size
	}
}

// JSONWriterStats holds statistics about the writer's performance.
type JSONWriterStats struct {
	RecordsWritten  int64            // Total records written
	FlushCount      int64            // Number of flushes performed
	FlushDuration   time.Duration    // Total time spent flushing
	LastFlushTime   time.Time        // Time of last flush
	NullValueCounts map[string]int64 // Count of null values per field
}

// JSONWriter implements goetl.DataSink for line-delimited JSON output.
// It supports batching, buffered output, flush control, and statistics.
type JSONWriter struct {
	writer     io.Writer
	closer     io.Closer
	buffered   *bufio.Writer
	options    JSONWriterOptions
	stats      JSONWriterStats
	recordBuf  []goetl.Record
	errorState bool
	mu         sync.Mutex // Add mutex for concurrent safety
}

// WriterOptionJSON is a functional option for JSONWriter configuration.
type WriterOptionJSON func(*JSONWriterOptions)

// withDefaults applies default values to JSONWriterOptions.
func (opts *JSONWriterOptions) withDefaults() *JSONWriterOptions {
	result := &JSONWriterOptions{}

	// Copy existing values if opts is not nil
	if opts != nil {
		*result = *opts
	}

	// Apply defaults for zero values only
	if result.BatchSize <= 0 {
		result.BatchSize = 0 // Default: no batching (immediate write)
	}
	if result.BufferSize <= 0 {
		result.BufferSize = 4096 // Default buffer size following Go's standard
	}
	// FlushOnWrite defaults to true for streaming consistency

	return result
}

// NewJSONWriter creates a JSONWriter with optional buffered output and options.
// Accepts functional options for configuration. Returns a ready-to-use writer.
func NewJSONWriter(w io.WriteCloser, opts ...WriterOptionJSON) *JSONWriter {
	// Start with defaults
	options := (&JSONWriterOptions{}).withDefaults()

	// Apply all functional options
	for _, opt := range opts {
		opt(options)
	}

	jw := &JSONWriter{
		writer:  w,
		closer:  w,
		options: *options,
		stats:   JSONWriterStats{NullValueCounts: make(map[string]int64)},
	}

	// Use custom buffer size from processed options
	if options.BufferSize > 0 {
		jw.buffered = bufio.NewWriterSize(w, options.BufferSize)
	} else {
		jw.buffered = bufio.NewWriter(w)
	}

	return jw
}

// Write implements the goetl.DataSink interface.
// Buffers records and writes in batches or on flush. Thread-safe.
func (j *JSONWriter) Write(ctx context.Context, record goetl.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.errorState {
		return &JSONWriterError{Op: "write", Err: fmt.Errorf("writer is in error state")}
	}

	// Track nulls
	for k, v := range record {
		if v == nil {
			j.stats.NullValueCounts[k]++
		}
	}

	j.recordBuf = append(j.recordBuf, record)
	j.stats.RecordsWritten++

	if j.options.BatchSize > 0 && len(j.recordBuf) >= j.options.BatchSize {
		if err := j.flushBufferUnsafe(); err != nil {
			j.errorState = true
			return &JSONWriterError{Op: "flush_buffer", Err: err}
		}
	} else if j.options.FlushOnWrite {
		if err := j.flushBufferUnsafe(); err != nil {
			j.errorState = true
			return &JSONWriterError{Op: "flush_write", Err: err}
		}
	}

	return nil
}

// Flush implements the goetl.DataSink interface.
// Forces any buffered records to be written to the output.
func (j *JSONWriter) Flush() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if err := j.flushBufferUnsafe(); err != nil {
		return &JSONWriterError{Op: "flush", Err: err}
	}
	if err := j.buffered.Flush(); err != nil {
		return &JSONWriterError{Op: "flush_buffered", Err: err}
	}
	return nil
}

// Close implements the goetl.DataSink interface.
// Flushes and closes all resources.
func (j *JSONWriter) Close() error {
	if err := j.Flush(); err != nil {
		return err
	}
	if j.closer != nil {
		return j.closer.Close()
	}
	return nil
}

// flushBufferUnsafe serializes and writes buffered records (must hold mutex).
func (j *JSONWriter) flushBufferUnsafe() error {
	if len(j.recordBuf) == 0 {
		return nil
	}

	start := time.Now()

	// Serialize and write records in one pass for better performance
	for _, record := range j.recordBuf {
		data, err := json.Marshal(record)
		if err != nil {
			return &JSONWriterError{
				Op:  "marshal_record",
				Err: fmt.Errorf("failed to marshal record: %w", err),
			}
		}
		if _, err := j.buffered.Write(data); err != nil {
			return &JSONWriterError{
				Op:  "write_json",
				Err: fmt.Errorf("failed to write JSON data: %w", err),
			}
		}
		if err := j.buffered.WriteByte('\n'); err != nil {
			return &JSONWriterError{
				Op:  "write_newline",
				Err: fmt.Errorf("failed to write newline: %w", err),
			}
		}
	}

	// Always flush to ensure data is written for consistency
	if err := j.buffered.Flush(); err != nil {
		return &JSONWriterError{
			Op:  "buffer_flush",
			Err: fmt.Errorf("buffer flush failed: %w", err),
		}
	}

	// Update statistics
	flushDuration := time.Since(start)
	j.recordBuf = j.recordBuf[:0]
	j.stats.FlushCount++
	j.stats.LastFlushTime = time.Now()

	// Accumulate timing even if very small
	j.stats.FlushDuration += flushDuration

	return nil
}

// Stats returns the writer's performance stats.
func (j *JSONWriter) Stats() JSONWriterStats {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Return a copy to prevent races
	statsCopy := j.stats
	statsCopy.NullValueCounts = make(map[string]int64)
	for k, v := range j.stats.NullValueCounts {
		statsCopy.NullValueCounts[k] = v
	}
	return statsCopy
}
