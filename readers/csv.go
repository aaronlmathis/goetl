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

	"github.com/aaronlmathis/goetl"
)

// CSVReaderError wraps structured error information for the CSV reader.
type CSVReaderError struct {
	Op  string
	Err error
}

func (e *CSVReaderError) Error() string {
	return fmt.Sprintf("csv reader %s: %v", e.Op, e.Err)
}

func (e *CSVReaderError) Unwrap() error {
	return e.Err
}

// CSVReaderStats holds statistics about the CSV reader's performance.
type CSVReaderStats struct {
	RecordsRead     int64
	ReadDuration    time.Duration
	LastReadTime    time.Time
	NullValueCounts map[string]int64
}

// CSVReaderOptions configures the CSV reader.
type CSVReaderOptions struct {
	Comma            rune
	Comment          rune
	FieldsPerRecord  int
	LazyQuotes       bool
	TrimLeadingSpace bool
	HasHeaders       bool
}

// ReaderOptionCSV allows functional customization of CSVReader.
type ReaderOptionCSV func(*CSVReaderOptions)

func WithCSVComma(r rune) ReaderOptionCSV {
	return func(o *CSVReaderOptions) { o.Comma = r }
}

func WithCSVHasHeaders(hasHeaders bool) ReaderOptionCSV {
	return func(o *CSVReaderOptions) { o.HasHeaders = hasHeaders }
}

func WithCSVTrimSpace(trim bool) ReaderOptionCSV {
	return func(o *CSVReaderOptions) { o.TrimLeadingSpace = trim }
}

// CSVReader implements DataSource for CSV files.
type CSVReader struct {
	reader  *csv.Reader
	headers []string
	closer  io.Closer
	stats   CSVReaderStats
	opts    CSVReaderOptions
}

// NewCSVReader creates a CSVReader with default or overridden options.
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

// Read implements the DataSource interface.
func (c *CSVReader) Read(ctx context.Context) (goetl.Record, error) {
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

	res := make(goetl.Record)

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

// Close implements the DataSource interface.
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
