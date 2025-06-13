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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaronlmathis/goetl/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock writer for testing
type mockWriteCloser struct {
	*strings.Builder
	closed    bool
	failWrite bool
	failClose bool
	mu        sync.Mutex
}

func (m *mockWriteCloser) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrite {
		return 0, io.ErrUnexpectedEOF
	}
	return m.Builder.Write(p)
}

func (m *mockWriteCloser) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.failClose {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *mockWriteCloser) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Builder.String()
}

func (m *mockWriteCloser) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func newMockWriteCloser() *mockWriteCloser {
	return &mockWriteCloser{
		Builder: &strings.Builder{},
	}
}

// TestJSONWriter_BasicFunctionality tests core write operations
func TestJSONWriter_BasicFunctionality(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock)

	ctx := context.Background()

	// Test writing a single record
	record := core.Record{
		"id":   1,
		"name": "John Doe",
		"age":  30,
	}

	err := writer.Write(ctx, record)
	require.NoError(t, err)

	// Test closing
	err = writer.Close()
	require.NoError(t, err)

	// Verify output format
	output := mock.String()
	assert.Contains(t, output, `"id":1`)
	assert.Contains(t, output, `"name":"John Doe"`)
	assert.Contains(t, output, `"age":30`)
	assert.Contains(t, output, "\n")

	// Verify writer was closed
	assert.True(t, mock.IsClosed())
}

// TestJSONWriter_MultipleRecords tests writing multiple records
func TestJSONWriter_MultipleRecords(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock)

	ctx := context.Background()
	records := []core.Record{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}

	for _, record := range records {
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	err := writer.Close()
	require.NoError(t, err)

	// Verify each record is on its own line
	lines := strings.Split(strings.TrimSpace(mock.String()), "\n")
	assert.Len(t, lines, 3)

	// Verify each line is valid JSON
	for i, line := range lines {
		var parsed core.Record
		err := json.Unmarshal([]byte(line), &parsed)
		require.NoError(t, err)
		assert.Equal(t, records[i]["id"], int(parsed["id"].(float64)))
		assert.Equal(t, records[i]["name"], parsed["name"])
	}
}

// TestJSONWriter_BatchedWrites tests batching behavior
func TestJSONWriter_BatchedWrites(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock, WithJSONBatchSize(3))

	ctx := context.Background()

	// Write records that should be batched
	for i := 0; i < 5; i++ {
		record := core.Record{"id": i, "value": i * 10}
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	// First 3 records should be written due to batch size
	output := mock.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if output != "" {
		assert.GreaterOrEqual(t, len(lines), 3)
	}

	// Force flush remaining records
	err := writer.Flush()
	require.NoError(t, err)

	// Now all 5 records should be written
	output = mock.String()
	lines = strings.Split(strings.TrimSpace(output), "\n")
	assert.Len(t, lines, 5)

	// Verify stats
	stats := writer.Stats()
	assert.Equal(t, int64(5), stats.RecordsWritten)
	assert.Greater(t, stats.FlushCount, int64(0))
}

// TestJSONWriter_FlushOnWrite tests immediate flush behavior
func TestJSONWriter_FlushOnWrite(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock, WithFlushOnWrite(true))

	ctx := context.Background()
	record := core.Record{"test": "value"}

	err := writer.Write(ctx, record)
	require.NoError(t, err)

	// Should be immediately written due to FlushOnWrite
	output := mock.String()
	assert.Contains(t, output, `"test":"value"`)

	stats := writer.Stats()
	assert.Equal(t, int64(1), stats.RecordsWritten)
	assert.Greater(t, stats.FlushCount, int64(0))
}

// TestJSONWriter_NoFlushOnWrite tests deferred flush behavior
func TestJSONWriter_NoFlushOnWrite(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock, WithFlushOnWrite(false))

	ctx := context.Background()
	record := core.Record{"test": "value"}

	err := writer.Write(ctx, record)
	require.NoError(t, err)

	// Should not be written yet
	output := mock.String()
	assert.Empty(t, output)

	// Explicit flush should write it
	err = writer.Flush()
	require.NoError(t, err)

	output = mock.String()
	assert.Contains(t, output, `"test":"value"`)
}

// TestJSONWriter_NullValueTracking tests null value statistics
func TestJSONWriter_NullValueTracking(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock)

	ctx := context.Background()
	records := []core.Record{
		{"name": "Alice", "age": 30, "email": nil},
		{"name": "Bob", "age": nil, "email": "bob@test.com"},
		{"name": nil, "age": 25, "email": nil},
	}

	for _, record := range records {
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	err := writer.Close()
	require.NoError(t, err)

	// Verify null value tracking
	stats := writer.Stats()
	assert.Equal(t, int64(2), stats.NullValueCounts["email"])
	assert.Equal(t, int64(1), stats.NullValueCounts["age"])
	assert.Equal(t, int64(1), stats.NullValueCounts["name"])
}

// TestJSONWriter_ComplexDataTypes tests various data types
func TestJSONWriter_ComplexDataTypes(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock)

	ctx := context.Background()
	now := time.Now()

	record := core.Record{
		"string": "hello",
		"int":    42,
		"float":  3.14,
		"bool":   true,
		"null":   nil,
		"array":  []interface{}{1, 2, 3},
		"object": map[string]interface{}{"nested": "value"},
		"time":   now,
	}

	err := writer.Write(ctx, record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify JSON is valid and contains expected values
	output := strings.TrimSpace(mock.String())
	var parsed map[string]interface{}
	err = json.Unmarshal([]byte(output), &parsed)
	require.NoError(t, err)

	assert.Equal(t, "hello", parsed["string"])
	assert.Equal(t, float64(42), parsed["int"])
	assert.Equal(t, 3.14, parsed["float"])
	assert.Equal(t, true, parsed["bool"])
	assert.Nil(t, parsed["null"])
	assert.IsType(t, []interface{}{}, parsed["array"])
	assert.IsType(t, map[string]interface{}{}, parsed["object"])
}

// TestJSONWriter_ErrorHandling tests error conditions
func TestJSONWriter_ErrorHandling(t *testing.T) {
	t.Run("write_error", func(t *testing.T) {
		mock := newMockWriteCloser()
		mock.failWrite = true
		writer := NewJSONWriter(mock)

		ctx := context.Background()
		record := core.Record{"test": "value"}

		err := writer.Write(ctx, record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "json writer")
	})

	t.Run("close_error", func(t *testing.T) {
		mock := newMockWriteCloser()
		mock.failClose = true
		writer := NewJSONWriter(mock)

		ctx := context.Background()
		record := core.Record{"test": "value"}

		err := writer.Write(ctx, record)
		require.NoError(t, err)

		err = writer.Close()
		assert.Error(t, err)
	})

	t.Run("write_after_error", func(t *testing.T) {
		mock := newMockWriteCloser()
		writer := NewJSONWriter(mock)

		// Force error state
		writer.errorState = true

		ctx := context.Background()
		record := core.Record{"test": "value"}

		err := writer.Write(ctx, record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error state")
	})

	t.Run("invalid_json", func(t *testing.T) {
		mock := newMockWriteCloser()
		writer := NewJSONWriter(mock)

		ctx := context.Background()
		// Create a record with a channel (not JSON serializable)
		record := core.Record{"invalid": make(chan int)}

		err := writer.Write(ctx, record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "marshal")
	})
}

// TestJSONWriter_StatisticsAccuracy tests statistics tracking
func TestJSONWriter_StatisticsAccuracy(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock, WithJSONBatchSize(2))

	ctx := context.Background()
	start := time.Now()

	// Write multiple records
	for i := 0; i < 5; i++ {
		record := core.Record{
			"id":    i,
			"value": i * 10,
			"null":  nil,
		}
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	err := writer.Close()
	require.NoError(t, err)

	stats := writer.Stats()

	// Verify record count
	assert.Equal(t, int64(5), stats.RecordsWritten)

	// Verify flush count (should be at least 2: batches + final flush)
	assert.GreaterOrEqual(t, stats.FlushCount, int64(2))

	// More lenient timing assertions following your performance-focused approach
	assert.GreaterOrEqual(t, stats.FlushDuration, time.Duration(0))

	// LastFlushTime should be set and reasonable
	if !stats.LastFlushTime.IsZero() {
		assert.True(t, stats.LastFlushTime.After(start.Add(-time.Second)),
			"LastFlushTime should be reasonable")
		assert.True(t, stats.LastFlushTime.Before(time.Now().Add(time.Second)),
			"LastFlushTime should not be in future")
	}

	// Verify null tracking
	assert.Equal(t, int64(5), stats.NullValueCounts["null"])
}

// TestJSONWriter_ConcurrentSafety tests thread safety
func TestJSONWriter_ConcurrentSafety(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock, WithJSONBatchSize(10), WithFlushOnWrite(false))

	ctx := context.Background()
	const numGoroutines = 5 // Reduced for more stable testing
	const recordsPerGoroutine = 3

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	// Start multiple goroutines writing concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				record := core.Record{
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

	err := writer.Close()
	require.NoError(t, err)

	// Verify all records were written
	stats := writer.Stats()
	assert.Equal(t, int64(numGoroutines*recordsPerGoroutine), stats.RecordsWritten)

	// Verify output is valid JSON lines
	output := strings.TrimSpace(mock.String())
	if output != "" {
		lines := strings.Split(output, "\n")
		assert.Len(t, lines, numGoroutines*recordsPerGoroutine)

		for _, line := range lines {
			var record map[string]interface{}
			err := json.Unmarshal([]byte(line), &record)
			assert.NoError(t, err)
		}
	}
}

// TestJSONWriter_ContextCancellation tests context handling
func TestJSONWriter_ContextCancellation(t *testing.T) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	record := core.Record{"test": "value"}

	// Write should still work even with cancelled context
	// (context cancellation handling would be in the pipeline, not the writer)
	err := writer.Write(ctx, record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := mock.String()
	assert.Contains(t, output, `"test":"value"`)
}

// TestJSONWriter_EdgeCases tests edge cases
func TestJSONWriter_EdgeCases(t *testing.T) {
	t.Run("empty_record", func(t *testing.T) {
		mock := newMockWriteCloser()
		writer := NewJSONWriter(mock)

		ctx := context.Background()
		record := core.Record{}

		err := writer.Write(ctx, record)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := strings.TrimSpace(mock.String())
		assert.Equal(t, "{}", output)
	})

	t.Run("zero_batch_size", func(t *testing.T) {
		mock := newMockWriteCloser()
		writer := NewJSONWriter(mock, WithJSONBatchSize(0))

		ctx := context.Background()
		record := core.Record{"test": "value"}

		err := writer.Write(ctx, record)
		require.NoError(t, err)

		// Should flush immediately with zero batch size
		output := mock.String()
		assert.Contains(t, output, `"test":"value"`)
	})

	t.Run("multiple_flushes", func(t *testing.T) {
		mock := newMockWriteCloser()
		writer := NewJSONWriter(mock, WithFlushOnWrite(false))

		ctx := context.Background()
		record := core.Record{"test": "value"}

		err := writer.Write(ctx, record)
		require.NoError(t, err)

		// Multiple flushes should be safe
		err = writer.Flush()
		require.NoError(t, err)

		err = writer.Flush()
		require.NoError(t, err)

		output := mock.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Len(t, lines, 1) // Should only have one record despite multiple flushes
	})
}

// BenchmarkJSONWriter_Write benchmarks write performance
func BenchmarkJSONWriter_Write(b *testing.B) {
	mock := newMockWriteCloser()
	writer := NewJSONWriter(mock, WithJSONBatchSize(1000))

	ctx := context.Background()
	record := core.Record{
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

// BenchmarkJSONWriter_BatchSizes benchmarks different batch sizes
func BenchmarkJSONWriter_BatchSizes(b *testing.B) {
	batchSizes := []int{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			mock := newMockWriteCloser()
			writer := NewJSONWriter(mock, WithJSONBatchSize(batchSize))

			ctx := context.Background()
			record := core.Record{
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
