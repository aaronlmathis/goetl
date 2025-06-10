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

// JSONWriterError wraps detailed error context for JSONWriter operations.
type JSONWriterError struct {
	Op  string
	Err error
}

func (e *JSONWriterError) Error() string {
	return fmt.Sprintf("json writer %s: %v", e.Op, e.Err)
}

func (e *JSONWriterError) Unwrap() error {
	return e.Err
}

// JSONWriterOptions configures the JSONWriter behavior.
type JSONWriterOptions struct {
	BatchSize    int
	FlushOnWrite bool
}

// JSONWriterStats holds statistics about the writer's performance.
type JSONWriterStats struct {
	RecordsWritten  int64
	FlushCount      int64
	FlushDuration   time.Duration
	LastFlushTime   time.Time
	NullValueCounts map[string]int64
}

// JSONWriter implements DataSink for line-delimited JSON output.
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

// WriterOptionJSON is a functional option for JSONWriter.
type WriterOptionJSON func(*JSONWriterOptions)

func WithJSONBatchSize(size int) WriterOptionJSON {
	return func(o *JSONWriterOptions) {
		o.BatchSize = size
	}
}

func WithFlushOnWrite(enabled bool) WriterOptionJSON {
	return func(o *JSONWriterOptions) {
		o.FlushOnWrite = enabled
	}
}

// NewJSONWriter creates a JSONWriter with optional buffered output and options.
func NewJSONWriter(w io.WriteCloser, opts ...WriterOptionJSON) *JSONWriter {
	options := JSONWriterOptions{
		BatchSize:    0,
		FlushOnWrite: true,
	}
	for _, opt := range opts {
		opt(&options)
	}

	jw := &JSONWriter{
		writer:  w,
		closer:  w,
		options: options,
		stats:   JSONWriterStats{NullValueCounts: make(map[string]int64)},
	}

	// Always use buffered writer for consistent behavior
	jw.buffered = bufio.NewWriter(w)

	return jw
}

// Write implements the DataSink interface.
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

// Flush implements the DataSink interface.
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

// Close implements the DataSink interface.
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
