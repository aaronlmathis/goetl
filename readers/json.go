//
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Copyright (C) 2025 Aaron Mathis aaron.mathis@gmail.com
//
// This file is part of GoETL
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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// Package readers provides implementations of core.DataSource for reading data from various sources.
//
// This file implements a high-performance, configurable JSON reader for streaming ETL pipelines.
// It supports buffered reading, batch configuration, and statistics for line-delimited JSON input.

// JSONReaderError wraps detailed context for JSONReader operations.
type JSONReaderError struct {
	Op  string // Operation that failed (e.g., "read", "scan", "unmarshal")
	Err error  // Underlying error
}

func (e *JSONReaderError) Error() string {
	return fmt.Sprintf("json reader %s: %v", e.Op, e.Err)
}

func (e *JSONReaderError) Unwrap() error {
	return e.Err
}

// JSONReaderStats provides metrics about JSONReader activity.
type JSONReaderStats struct {
	RecordsRead     int64            // Total records read
	BytesRead       int64            // Total bytes read
	ReadDuration    time.Duration    // Total time spent reading
	LastReadTime    time.Time        // Time of last read
	NullValueCounts map[string]int64 // Count of null values per field
}

// JSONReaderOptions configures optional behavior for the reader.
type JSONReaderOptions struct {
	BufferSize int // Optional buffer size in bytes for scanning
}

// ReaderOptionJSON is a functional option for JSONReaderOptions.
type ReaderOptionJSON func(*JSONReaderOptions)

// WithBufferSize sets the buffer size for the JSONReader's scanner.
func WithBufferSize(size int) ReaderOptionJSON {
	return func(opt *JSONReaderOptions) {
		opt.BufferSize = size
	}
}

// JSONReader implements core.DataSource for line-delimited JSON files.
// It supports buffered reading, batch configuration, and statistics.
type JSONReader struct {
	scanner *bufio.Scanner
	closer  io.Closer
	stats   JSONReaderStats
	opts    JSONReaderOptions
}

// NewJSONReader creates a new line-delimited JSON reader with optional config.
// Accepts functional options for configuration. Returns a ready-to-use reader.
func NewJSONReader(r io.ReadCloser, options ...ReaderOptionJSON) *JSONReader {
	opts := JSONReaderOptions{
		BufferSize: 64 * 1024, // 64KB default
	}
	for _, opt := range options {
		opt(&opts)
	}

	scanner := bufio.NewScanner(r)
	if opts.BufferSize > 0 {
		buf := make([]byte, opts.BufferSize)
		scanner.Buffer(buf, opts.BufferSize)
	}

	return &JSONReader{
		scanner: scanner,
		closer:  r,
		stats:   JSONReaderStats{NullValueCounts: make(map[string]int64)},
		opts:    opts,
	}
}

// Read implements the core.DataSource interface, returning one JSON record per line.
// Thread-safe and context-aware.
func (j *JSONReader) Read(ctx context.Context) (core.Record, error) {
	start := time.Now()

	select {
	case <-ctx.Done():
		return nil, &JSONReaderError{Op: "read", Err: ctx.Err()}
	default:
	}

	if !j.scanner.Scan() {
		if err := j.scanner.Err(); err != nil {
			return nil, &JSONReaderError{Op: "scan", Err: err}
		}
		return nil, io.EOF
	}

	line := j.scanner.Bytes()
	j.stats.BytesRead += int64(len(line))

	var record core.Record
	if err := json.Unmarshal(line, &record); err != nil {
		return nil, &JSONReaderError{Op: "unmarshal", Err: err}
	}

	// Track nulls
	for key, val := range record {
		if val == nil {
			j.stats.NullValueCounts[key]++
		}
	}

	j.stats.RecordsRead++
	j.stats.ReadDuration += time.Since(start)
	j.stats.LastReadTime = time.Now()

	return record, nil
}

// Close implements the core.DataSource interface.
// Closes the underlying reader.
func (j *JSONReader) Close() error {
	if j.closer != nil {
		return j.closer.Close()
	}
	return nil
}

// Stats returns reader metrics like bytes read, nulls, and durations.
func (j *JSONReader) Stats() JSONReaderStats {
	return j.stats
}
