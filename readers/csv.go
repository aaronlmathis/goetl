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
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// Package readers provides implementations of core.DataSource for reading data from various sources.
//
// This file implements a high-performance, configurable CSV reader for streaming ETL pipelines.
// It supports header detection, delimiter configuration, type inference, and statistics for CSV input.

// CSVReaderError wraps structured error information for the CSV reader.
type CSVReaderError struct {
	Op  string // Operation that failed (e.g., "read", "read_headers", "read_record")
	Err error  // Underlying error
}

func (e *CSVReaderError) Error() string {
	return fmt.Sprintf("csv reader %s: %v", e.Op, e.Err)
}

func (e *CSVReaderError) Unwrap() error {
	return e.Err
}

// CSVReaderStats holds statistics about the CSV reader's performance.
type CSVReaderStats struct {
	RecordsRead     int64            // Total records read
	ReadDuration    time.Duration    // Total time spent reading
	LastReadTime    time.Time        // Time of last read
	NullValueCounts map[string]int64 // Count of null values per field
}

// CSVReaderOptions configures the CSV reader.
type CSVReaderOptions struct {
	Comma            rune // Field delimiter (default ',')
	Comment          rune // Comment character (optional)
	FieldsPerRecord  int  // Number of expected fields per record (optional)
	LazyQuotes       bool // Allow lazy quotes in CSV
	TrimLeadingSpace bool // Trim leading space in fields
	HasHeaders       bool // Whether the first row is a header
}

// ReaderOptionCSV allows functional customization of CSVReader.
type ReaderOptionCSV func(*CSVReaderOptions)

// WithCSVComma sets the field delimiter for the CSV reader.
func WithCSVComma(r rune) ReaderOptionCSV {
	return func(o *CSVReaderOptions) { o.Comma = r }
}

// WithCSVHasHeaders enables or disables header row detection.
func WithCSVHasHeaders(hasHeaders bool) ReaderOptionCSV {
	return func(o *CSVReaderOptions) { o.HasHeaders = hasHeaders }
}

// WithCSVTrimSpace enables or disables trimming of leading spaces.
func WithCSVTrimSpace(trim bool) ReaderOptionCSV {
	return func(o *CSVReaderOptions) { o.TrimLeadingSpace = trim }
}

// CSVReader implements core.DataSource for CSV files.
// It supports header detection, delimiter configuration, type inference, and statistics.
type CSVReader struct {
	reader  *csv.Reader
	headers []string
	closer  io.Closer
	stats   CSVReaderStats
	opts    CSVReaderOptions
}

// NewCSVReader creates a CSVReader with default or overridden options.
// Accepts functional options for configuration. Returns a ready-to-use reader or an error.
func NewCSVReader(r io.ReadCloser, options ...ReaderOptionCSV) (*CSVReader, error) {
	opts := CSVReaderOptions{
		Comma:            ',',
		HasHeaders:       true,
		TrimLeadingSpace: true,
	}

	for _, opt := range options {
		opt(&opts)
	}

	csvReader := csv.NewReader(r)
	csvReader.Comma = opts.Comma
	csvReader.Comment = opts.Comment
	csvReader.FieldsPerRecord = opts.FieldsPerRecord
	csvReader.LazyQuotes = opts.LazyQuotes
	csvReader.TrimLeadingSpace = opts.TrimLeadingSpace

	reader := &CSVReader{
		reader: csvReader,
		closer: r,
		opts:   opts,
		stats:  CSVReaderStats{NullValueCounts: make(map[string]int64)},
	}

	// Read headers if applicable
	if opts.HasHeaders {
		headers, err := csvReader.Read()
		if err != nil {
			return nil, &CSVReaderError{Op: "read_headers", Err: err}
		}
		reader.headers = headers
	}

	return reader, nil
}

// Read implements the core.DataSource interface.
// Reads the next record from the CSV file. Thread-safe and context-aware.
func (c *CSVReader) Read(ctx context.Context) (core.Record, error) {
	start := time.Now()

	select {
	case <-ctx.Done():
		return nil, &CSVReaderError{Op: "read", Err: ctx.Err()}
	default:
	}

	record, err := c.reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, &CSVReaderError{Op: "read_record", Err: err}
	}

	res := make(core.Record)

	if len(c.headers) > 0 {
		for i, val := range record {
			key := c.headers[i]
			if strings.TrimSpace(val) == "" {
				c.stats.NullValueCounts[key]++
				res[key] = nil
			} else {
				res[key] = c.parseValue(val)
			}
		}
	} else {
		for i, val := range record {
			key := "col_" + strconv.Itoa(i)
			if strings.TrimSpace(val) == "" {
				c.stats.NullValueCounts[key]++
				res[key] = nil
			} else {
				res[key] = c.parseValue(val)
			}
		}
	}

	c.stats.RecordsRead++
	c.stats.LastReadTime = time.Now()
	c.stats.ReadDuration += time.Since(start)

	return res, nil
}

// Close implements the core.DataSource interface.
// Closes the underlying reader.
func (c *CSVReader) Close() error {
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

// Stats returns CSV reader performance stats.
func (c *CSVReader) Stats() CSVReaderStats {
	return c.stats
}

// parseValue attempts to infer int, float, bool, or fallback to string.
func (c *CSVReader) parseValue(value string) interface{} {
	value = strings.TrimSpace(value)

	// Try parsing in common order
	if i, err := strconv.Atoi(value); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(value); err == nil {
		return b
	}
	return value
}
