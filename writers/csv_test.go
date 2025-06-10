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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaronlmathis/goetl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock writer for CSV testing
type mockCSVWriteCloser struct {
	*strings.Builder
	closed    bool
	failWrite bool
	failClose bool
	mu        sync.Mutex
}

func (m *mockCSVWriteCloser) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrite {
		return 0, io.ErrUnexpectedEOF
	}
	return m.Builder.Write(p)
}

func (m *mockCSVWriteCloser) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.failClose {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *mockCSVWriteCloser) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Builder.String()
}

func (m *mockCSVWriteCloser) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func newMockCSVWriteCloser() *mockCSVWriteCloser {
	return &mockCSVWriteCloser{
		Builder: &strings.Builder{},
	}
}

// TestCSVWriter_BasicFunctionality tests core write operations
func TestCSVWriter_BasicFunctionality(t *testing.T) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock)
	require.NoError(t, err)

	ctx := context.Background()

	// Test writing a single record
	record := goetl.Record{
		"id":   1,
		"name": "John Doe",
		"age":  30,
	}

	err = writer.Write(ctx, record)
	require.NoError(t, err)

	// Test closing
	err = writer.Close()
	require.NoError(t, err)

	// Verify output format - should have headers and data
	output := mock.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	assert.Len(t, lines, 2) // header + 1 data row

	// Parse and verify CSV
	reader := csv.NewReader(strings.NewReader(output))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, records, 2)

	// Verify writer was closed
	assert.True(t, mock.IsClosed())
}

// TestCSVWriter_WithHeaders tests explicit header specification
func TestCSVWriter_WithHeaders(t *testing.T) {
	mock := newMockCSVWriteCloser()
	headers := []string{"id", "name", "email"}
	writer, err := NewCSVWriter(mock, WithHeaders(headers))
	require.NoError(t, err)

	ctx := context.Background()
	record := goetl.Record{
		"id":    1,
		"name":  "Alice",
		"email": "alice@test.com",
	}

	err = writer.Write(ctx, record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify header order
	output := mock.String()
	reader := csv.NewReader(strings.NewReader(output))
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Equal(t, headers, records[0])
	assert.Equal(t, []string{"1", "Alice", "alice@test.com"}, records[1])
}

// TestCSVWriter_CustomDelimiter tests custom delimiter functionality
func TestCSVWriter_CustomDelimiter(t *testing.T) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock,
		WithComma(';'),
		WithHeaders([]string{"name", "value"}),
	)
	require.NoError(t, err)

	ctx := context.Background()
	record := goetl.Record{
		"name":  "test",
		"value": "data",
	}

	err = writer.Write(ctx, record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := mock.String()
	assert.Contains(t, output, "name;value")
	assert.Contains(t, output, "test;data")
}

// TestCSVWriter_NoHeaders tests writing without headers
func TestCSVWriter_NoHeaders(t *testing.T) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock,
		WithWriteHeader(false),
		WithHeaders([]string{"name", "value"}),
	)
	require.NoError(t, err)

	ctx := context.Background()
	record := goetl.Record{
		"name":  "test",
		"value": "data",
	}

	err = writer.Write(ctx, record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := mock.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	assert.Len(t, lines, 1) // Only data row, no header
	assert.Equal(t, "test,data", lines[0])
}

// TestCSVWriter_BatchedWrites tests batching behavior
func TestCSVWriter_BatchedWrites(t *testing.T) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock,
		WithCSVBatchSize(3),
		WithHeaders([]string{"id", "value"}),
	)
	require.NoError(t, err)

	ctx := context.Background()

	// Write records that should be batched
	for i := 0; i < 5; i++ {
		record := goetl.Record{"id": i, "value": i * 10}
		err = writer.Write(ctx, record)
		require.NoError(t, err)
	}

	// Force flush remaining records
	err = writer.Flush()
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify all records were written
	output := mock.String()
	reader := csv.NewReader(strings.NewReader(output))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, records, 6) // header + 5 data rows

	// Verify stats
	stats := writer.Stats()
	assert.Equal(t, int64(5), stats.RecordsWritten)
	assert.Greater(t, stats.FlushCount, int64(0))
}

// TestCSVWriter_NullValueTracking tests null value statistics
func TestCSVWriter_NullValueTracking(t *testing.T) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock, WithHeaders([]string{"name", "age", "email"}))
	require.NoError(t, err)

	ctx := context.Background()
	records := []goetl.Record{
		{"name": "Alice", "age": 30, "email": nil},
		{"name": "Bob", "age": nil, "email": "bob@test.com"},
		{"name": nil, "age": 25, "email": nil},
	}

	for _, record := range records {
		err = writer.Write(ctx, record)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Verify null value tracking
	stats := writer.Stats()
	assert.Equal(t, int64(2), stats.NullValueCounts["email"])
	assert.Equal(t, int64(1), stats.NullValueCounts["age"])
	assert.Equal(t, int64(1), stats.NullValueCounts["name"])

	// Verify CSV output handles nulls correctly
	output := mock.String()
	reader := csv.NewReader(strings.NewReader(output))
	records_csv, err := reader.ReadAll()
	require.NoError(t, err)

	// Check that null values become empty strings
	assert.Equal(t, []string{"Alice", "30", ""}, records_csv[1])
	assert.Equal(t, []string{"Bob", "", "bob@test.com"}, records_csv[2])
	assert.Equal(t, []string{"", "25", ""}, records_csv[3])
}

// TestCSVWriter_ComplexDataTypes tests various data types
func TestCSVWriter_ComplexDataTypes(t *testing.T) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock, WithHeaders([]string{"string", "int", "float", "bool"}))
	require.NoError(t, err)

	ctx := context.Background()
	record := goetl.Record{
		"string": "hello world",
		"int":    42,
		"float":  3.14159,
		"bool":   true,
	}

	err = writer.Write(ctx, record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify CSV output
	output := mock.String()
	reader := csv.NewReader(strings.NewReader(output))
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Equal(t, []string{"hello world", "42", "3.14159", "true"}, records[1])
}

// TestCSVWriter_ErrorHandling tests error conditions
func TestCSVWriter_ErrorHandling(t *testing.T) {
	t.Run("write_error", func(t *testing.T) {
		mock := newMockCSVWriteCloser()
		writer, err := NewCSVWriter(mock, WithWriteHeader(false)) // Disable headers to test data write directly
		require.NoError(t, err)

		// Set the writer to fail writes before attempting to write
		mock.failWrite = true

		ctx := context.Background()
		record := goetl.Record{"test": "value"}

		err = writer.Write(ctx, record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "csv writer")
	})

	t.Run("header_write_error", func(t *testing.T) {
		mock := newMockCSVWriteCloser()
		writer, err := NewCSVWriter(mock, WithWriteHeader(true)) // Enable headers
		require.NoError(t, err)

		// Set the writer to fail writes before attempting to write headers
		mock.failWrite = true

		ctx := context.Background()
		record := goetl.Record{"test": "value"}

		err = writer.Write(ctx, record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "csv writer")
	})

	t.Run("close_error", func(t *testing.T) {
		mock := newMockCSVWriteCloser()
		mock.failClose = true
		writer, err := NewCSVWriter(mock)
		require.NoError(t, err)

		ctx := context.Background()
		record := goetl.Record{"test": "value"}

		err = writer.Write(ctx, record)
		require.NoError(t, err)

		err = writer.Close()
		assert.Error(t, err)
	})

	t.Run("write_after_error", func(t *testing.T) {
		mock := newMockCSVWriteCloser()
		writer, err := NewCSVWriter(mock, WithWriteHeader(false)) // Disable headers to focus on data write
		require.NoError(t, err)

		// Set writer to fail first
		mock.failWrite = true
		ctx := context.Background()
		record := goetl.Record{"test": "value"}

		// First write should fail and set error state
		err = writer.Write(ctx, record)
		assert.Error(t, err)

		// Reset write failure but writer should still be in error state
		mock.failWrite = false
		err = writer.Write(ctx, record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error state")
	})

	t.Run("flush_error_during_write", func(t *testing.T) {
		// Create a mock that fails after some writes to test flush errors
		mock := &mockCSVWriteCloser{
			Builder: &strings.Builder{},
		}

		writer, err := NewCSVWriter(mock,
			WithWriteHeader(false),
			WithCSVBatchSize(1), // Force flush on each write
		)
		require.NoError(t, err)

		ctx := context.Background()

		// First write should succeed
		record := goetl.Record{"test": "value1"}
		err = writer.Write(ctx, record)
		require.NoError(t, err)

		// Set up failure for subsequent operations
		mock.failWrite = true

		// Second write should fail during flush
		record = goetl.Record{"test": "value2"}
		err = writer.Write(ctx, record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "csv writer")
	})
}

// TestCSVWriter_StatisticsAccuracy tests statistics tracking
func TestCSVWriter_StatisticsAccuracy(t *testing.T) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock,
		WithCSVBatchSize(2),
		WithHeaders([]string{"id", "value", "null_field"}),
	)
	require.NoError(t, err)

	ctx := context.Background()
	start := time.Now()

	// Write multiple records
	for i := 0; i < 5; i++ {
		record := goetl.Record{
			"id":         i,
			"value":      i * 10,
			"null_field": nil,
		}
		err = writer.Write(ctx, record)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	stats := writer.Stats()

	// Verify record count
	assert.Equal(t, int64(5), stats.RecordsWritten)

	// Verify flush count (should be at least 2: batches + final flush)
	assert.GreaterOrEqual(t, stats.FlushCount, int64(2))

	// Lenient timing assertions following performance-focused approach
	assert.GreaterOrEqual(t, stats.FlushDuration, time.Duration(0))

	if !stats.LastFlushTime.IsZero() {
		assert.True(t, stats.LastFlushTime.After(start.Add(-time.Second)))
		assert.True(t, stats.LastFlushTime.Before(time.Now().Add(time.Second)))
	}

	// Verify null tracking
	assert.Equal(t, int64(5), stats.NullValueCounts["null_field"])
}

// TestCSVWriter_ConcurrentSafety tests thread safety
func TestCSVWriter_ConcurrentSafety(t *testing.T) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock,
		WithCSVBatchSize(10),
		WithHeaders([]string{"worker", "record", "data"}),
	)
	require.NoError(t, err)

	ctx := context.Background()
	const numGoroutines = 5
	const recordsPerGoroutine = 3

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	// Start multiple goroutines writing concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				record := goetl.Record{
					"worker": workerID,
					"record": j,
					"data":   "test data",
				}
				if err := writer.Write(ctx, record); err != nil {
					errChan <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		t.Errorf("Concurrent write error: %v", err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Verify all records were written
	stats := writer.Stats()
	assert.Equal(t, int64(numGoroutines*recordsPerGoroutine), stats.RecordsWritten)

	// Verify output is valid CSV
	output := strings.TrimSpace(mock.String())
	if output != "" {
		reader := csv.NewReader(strings.NewReader(output))
		records, err := reader.ReadAll()
		require.NoError(t, err)
		assert.Len(t, records, numGoroutines*recordsPerGoroutine+1) // +1 for header
	}
}

// TestCSVWriter_DeprecatedOptions tests backward compatibility
func TestCSVWriter_DeprecatedOptions(t *testing.T) {
	mock := newMockCSVWriteCloser()

	// Test deprecated option functions still work
	writer, err := NewCSVWriter(mock,
		WithCSVHeaders([]string{"name", "value"}), // Deprecated
		WithCSVDelimiter(';'),                     // Deprecated
		WithCSVWriteHeader(true),                  // Deprecated
	)
	require.NoError(t, err)

	ctx := context.Background()
	record := goetl.Record{
		"name":  "test",
		"value": "data",
	}

	err = writer.Write(ctx, record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := mock.String()
	assert.Contains(t, output, "name;value")
	assert.Contains(t, output, "test;data")
}

// TestCSVWriter_EdgeCases tests edge cases
func TestCSVWriter_EdgeCases(t *testing.T) {
	t.Run("empty_record", func(t *testing.T) {
		mock := newMockCSVWriteCloser()
		writer, err := NewCSVWriter(mock, WithHeaders([]string{"field1", "field2"}))
		require.NoError(t, err)

		ctx := context.Background()
		record := goetl.Record{}

		err = writer.Write(ctx, record)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := mock.String()
		reader := csv.NewReader(strings.NewReader(output))
		records, err := reader.ReadAll()
		require.NoError(t, err)
		assert.Equal(t, []string{"", ""}, records[1]) // Empty values for missing fields
	})

	t.Run("zero_batch_size", func(t *testing.T) {
		mock := newMockCSVWriteCloser()
		writer, err := NewCSVWriter(mock,
			WithCSVBatchSize(0),
			WithHeaders([]string{"test"}),
		)
		require.NoError(t, err)

		ctx := context.Background()
		record := goetl.Record{"test": "value"}

		err = writer.Write(ctx, record)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		// Should flush immediately with zero batch size
		output := mock.String()
		assert.Contains(t, output, "test")
		assert.Contains(t, output, "value")
	})

	t.Run("special_characters", func(t *testing.T) {
		mock := newMockCSVWriteCloser()
		writer, err := NewCSVWriter(mock, WithHeaders([]string{"text"}))
		require.NoError(t, err)

		ctx := context.Background()
		record := goetl.Record{
			"text": "Hello, \"World\"\nNew line",
		}

		err = writer.Write(ctx, record)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		// Verify CSV properly escapes special characters
		output := mock.String()
		reader := csv.NewReader(strings.NewReader(output))
		records, err := reader.ReadAll()
		require.NoError(t, err)
		assert.Equal(t, "Hello, \"World\"\nNew line", records[1][0])
	})
}

// BenchmarkCSVWriter_Write benchmarks write performance
func BenchmarkCSVWriter_Write(b *testing.B) {
	mock := newMockCSVWriteCloser()
	writer, err := NewCSVWriter(mock,
		WithCSVBatchSize(1000),
		WithHeaders([]string{"id", "name", "email", "age", "score"}),
	)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	record := goetl.Record{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
		"score": 85.5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record["id"] = i // Vary the data slightly
		if err := writer.Write(ctx, record); err != nil {
			b.Fatal(err)
		}
	}

	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkCSVWriter_BatchSizes benchmarks different batch sizes
func BenchmarkCSVWriter_BatchSizes(b *testing.B) {
	batchSizes := []int{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			mock := newMockCSVWriteCloser()
			writer, err := NewCSVWriter(mock,
				WithCSVBatchSize(batchSize),
				WithHeaders([]string{"id", "data"}),
			)
			if err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()
			record := goetl.Record{
				"id":   1,
				"data": "benchmark data",
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record["id"] = i
				if err := writer.Write(ctx, record); err != nil {
					b.Fatal(err)
				}
			}

			if err := writer.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
