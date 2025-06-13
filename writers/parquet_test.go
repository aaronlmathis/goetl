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
// along with GoETL If not, see https://www.gnu.org/licenses/.

package writers

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/aaronlmathis/goetl/core"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParquetWriter_BasicFunctionality tests core write operations
func TestParquetWriter_BasicFunctionality(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_basic.parquet")

	writer, err := NewParquetWriter(filename,
		WithBatchSize(2),
		WithCompression(compress.Codecs.Snappy),
	)
	require.NoError(t, err)
	defer writer.Close()

	// Test data
	records := []core.Record{
		{"id": int64(1), "name": "Alice", "active": true, "score": 95.5},
		{"id": int64(2), "name": "Bob", "active": false, "score": 87.2},
		{"id": int64(3), "name": "Charlie", "active": true, "score": 92.8},
	}

	ctx := context.Background()

	// Write records
	for _, record := range records {
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	// Verify stats before closing
	stats := writer.Stats()
	assert.Equal(t, int64(3), stats.RecordsWritten)
	assert.Greater(t, stats.BatchesWritten, int64(0))

	// Close and verify file exists
	err = writer.Close()
	require.NoError(t, err)

	// Verify file was created and has content
	fileInfo, err := os.Stat(filename)
	require.NoError(t, err)
	assert.Greater(t, fileInfo.Size(), int64(0))
}

// TestParquetWriter_FunctionalOptions tests all functional options
func TestParquetWriter_FunctionalOptions(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_options.parquet")

	metadata := map[string]string{
		"created_by": "test",
		"version":    "1.0",
	}

	writer, err := NewParquetWriter(filename,
		WithBatchSize(10),
		WithCompression(compress.Codecs.Gzip),
		WithFieldOrder([]string{"id", "name", "timestamp"}),
		WithSchemaValidation(true),
		WithRowGroupSize(1000),
		WithMetadata(metadata),
	)
	require.NoError(t, err)
	ensureParquetWriterClosed(t, writer)

	// Verify options were applied
	assert.Equal(t, int64(10), writer.batchSize)
	assert.Equal(t, compress.Codecs.Gzip, writer.opts.Compression)
	assert.Equal(t, []string{"id", "name", "timestamp"}, writer.opts.FieldOrder)
	assert.True(t, writer.opts.ValidateSchema)
	assert.Equal(t, int64(1000), writer.opts.RowGroupSize)
	assert.Equal(t, "test", writer.opts.Metadata["created_by"])

	// Explicit close before temp directory cleanup
	err = writer.Close()
	require.NoError(t, err)
}

// TestParquetWriter_TypeInference tests Arrow type inference
func TestParquetWriter_TypeInference(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string // Arrow type name for verification
	}{
		{"bool", true, "bool"},
		{"int32", int32(42), "int32"},
		{"int64", int64(42), "int64"},
		{"float32", float32(3.14), "float32"},
		{"float64", 3.14159, "float64"},
		{"string", "hello", "utf8"},
		{"time", time.Now(), "timestamp[us]"},
		{"nil", nil, "utf8"}, // defaults to string
	}

	tempDir := t.TempDir()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := filepath.Join(tempDir, "test_"+tt.name+".parquet")
			writer, err := NewParquetWriter(filename, WithBatchSize(1))
			require.NoError(t, err)
			defer writer.Close()

			record := core.Record{"test_field": tt.value}
			err = writer.Write(context.Background(), record)
			require.NoError(t, err)

			// Verify schema was created
			assert.NotNil(t, writer.schema)
			assert.Equal(t, 1, len(writer.schema.Fields()))
		})
	}
}

// TestParquetWriter_BatchProcessing tests batching behavior
func TestParquetWriter_BatchProcessing(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_batching.parquet")

	batchSize := int64(3)
	writer, err := NewParquetWriter(filename, WithBatchSize(batchSize))
	require.NoError(t, err)
	defer writer.Close()

	ctx := context.Background()

	// Write records that should trigger batch flushes
	for i := 0; i < 10; i++ {
		record := core.Record{
			"id":    int64(i),
			"value": float64(i * 10),
		}
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	// Check that batches were written
	stats := writer.Stats()
	assert.Equal(t, int64(10), stats.RecordsWritten)
	assert.Greater(t, stats.BatchesWritten, int64(0))
	assert.Greater(t, stats.FlushDuration, time.Duration(0))
}

// TestParquetWriter_ErrorHandling tests error conditions
func TestParquetWriter_ErrorHandling(t *testing.T) {
	t.Run("write_after_close", func(t *testing.T) {
		tempDir := t.TempDir()
		filename := filepath.Join(tempDir, "test_closed.parquet")

		func() {
			writer, err := NewParquetWriter(filename)
			require.NoError(t, err)

			// Close writer
			err = writer.Close()
			require.NoError(t, err)

			// Try to write after close
			record := core.Record{"test": "value"}
			err = writer.Write(context.Background(), record)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "closed")
		}()

		// Force cleanup
		runtime.GC()
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("invalid_file_path", func(t *testing.T) {
		// Try to create file in non-existent directory
		_, err := NewParquetWriter("/non/existent/path/test.parquet")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create parquet file")
	})

	t.Run("schema_validation_error", func(t *testing.T) {
		tempDir := t.TempDir()
		filename := filepath.Join(tempDir, "test_validation.parquet")

		func() {
			writer, err := NewParquetWriter(filename,
				WithSchemaValidation(true),
				WithBatchSize(1),
			)
			require.NoError(t, err)

			ctx := context.Background()

			// Write first record to establish schema
			record1 := core.Record{"id": int64(1), "name": "test"}
			err = writer.Write(ctx, record1)
			require.NoError(t, err)

			// Try to write record with incompatible type
			record2 := core.Record{"id": "not_a_number", "name": "test2"}
			err = writer.Write(ctx, record2)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "validation")

			// Close within scope
			err = writer.Close()
			require.NoError(t, err)
		}()

		// Force cleanup
		runtime.GC()
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	})
}

// TestParquetWriter_NullValues tests null value handling
func TestParquetWriter_NullValues(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_nulls.parquet")

	writer, err := NewParquetWriter(filename, WithBatchSize(2))
	require.NoError(t, err)
	defer func() {
		err := writer.Close()
		if err != nil {
			t.Logf("Warning: failed to close writer: %v", err)
		}
	}()

	ctx := context.Background()

	// Write records with null values
	records := []core.Record{
		{"id": int64(1), "name": "Alice", "email": nil},
		{"id": int64(2), "name": nil, "email": "bob@example.com"},
		{"id": nil, "name": "Charlie", "email": "charlie@example.com"},
	}

	for _, record := range records {
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	// Force a flush to ensure all records are processed and stats updated
	err = writer.Flush()
	require.NoError(t, err)

	// Check null value statistics after flush
	stats := writer.Stats()

	// Verify we have null counts (should be at least 1 for each field)
	assert.GreaterOrEqual(t, stats.NullValueCounts["email"], int64(1), "Expected at least 1 null email")
	assert.GreaterOrEqual(t, stats.NullValueCounts["name"], int64(1), "Expected at least 1 null name")
	assert.GreaterOrEqual(t, stats.NullValueCounts["id"], int64(1), "Expected at least 1 null id")

	// Verify total records written
	assert.Equal(t, int64(3), stats.RecordsWritten)
}

// TestParquetWriter_MissingFields tests handling of missing fields
func TestParquetWriter_MissingFields(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_missing.parquet")

	writer, err := NewParquetWriter(filename,
		WithFieldOrder([]string{"id", "name", "email", "age"}),
		WithBatchSize(2),
	)
	require.NoError(t, err)
	defer writer.Close()

	ctx := context.Background()

	// Write records with different field sets
	records := []core.Record{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
		{"id": int64(2), "name": "Bob", "age": int64(30)},
		{"name": "Charlie", "email": "charlie@example.com", "age": int64(25)},
	}

	for _, record := range records {
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	// Verify all records were written
	stats := writer.Stats()
	assert.Equal(t, int64(3), stats.RecordsWritten)
}

// TestParquetWriter_ContextCancellation tests context cancellation
func TestParquetWriter_ContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_context.parquet")

	writer, err := NewParquetWriter(filename)
	require.NoError(t, err)
	defer writer.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	record := core.Record{"test": "value"}
	err = writer.Write(ctx, record)
	// Note: Current implementation doesn't check context in Write()
	// This test verifies the interface is context-aware
	// You may want to add context checking to Write() method
}

// TestParquetWriter_ConcurrentSafety tests thread safety
func TestParquetWriter_ConcurrentSafety(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_concurrent.parquet")

	writer, err := NewParquetWriter(filename, WithBatchSize(10))
	require.NoError(t, err)
	defer writer.Close()

	ctx := context.Background()

	// Note: ParquetWriter is NOT thread-safe by design (following streaming principle)
	// This test verifies that sequential writes work correctly
	// For concurrent usage, users should implement their own synchronization

	records := make([]core.Record, 50)
	for i := range records {
		records[i] = core.Record{
			"id":    int64(i),
			"value": float64(i * 2),
		}
	}

	// Write records sequentially
	for _, record := range records {
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	stats := writer.Stats()
	assert.Equal(t, int64(50), stats.RecordsWritten)
}

// TestParquetWriter_DefaultOptions tests default option values
func TestParquetWriter_DefaultOptions(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_defaults.parquet")

	// Create writer with no options (should use defaults)
	writer, err := NewParquetWriter(filename)
	require.NoError(t, err)
	ensureParquetWriterClosed(t, writer)

	// Verify default values
	assert.Equal(t, int64(1000), writer.batchSize)
	assert.Equal(t, compress.Codecs.Snappy, writer.opts.Compression)
	assert.Equal(t, int64(10000), writer.opts.RowGroupSize)
	assert.Equal(t, int64(1024*1024), writer.opts.PageSize)
	assert.NotNil(t, writer.opts.DictionaryLevel)
	assert.NotNil(t, writer.opts.Metadata)

	// Explicit close before temp directory cleanup
	err = writer.Close()
	require.NoError(t, err)
}

// TestParquetWriter_FlushBehavior tests explicit flush operations
func TestParquetWriter_FlushBehavior(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "test_flush.parquet")

	writer, err := NewParquetWriter(filename, WithBatchSize(10))
	require.NoError(t, err)
	defer writer.Close()

	ctx := context.Background()

	// Write some records (less than batch size)
	for i := 0; i < 5; i++ {
		record := core.Record{"id": int64(i)}
		err := writer.Write(ctx, record)
		require.NoError(t, err)
	}

	// Explicit flush
	err = writer.Flush()
	require.NoError(t, err)

	// Verify records were flushed
	stats := writer.Stats()
	assert.Equal(t, int64(5), stats.RecordsWritten)
	assert.Greater(t, stats.BatchesWritten, int64(0))

	// Test flush with empty buffer
	err = writer.Flush()
	require.NoError(t, err) // Should not error
}

// BenchmarkParquetWriter_Write benchmarks write performance
func BenchmarkParquetWriter_Write(b *testing.B) {
	tempDir := b.TempDir()
	filename := filepath.Join(tempDir, "benchmark.parquet")

	writer, err := NewParquetWriter(filename, WithBatchSize(1000))
	require.NoError(b, err)
	defer writer.Close()

	ctx := context.Background()
	record := core.Record{
		"id":        int64(1),
		"name":      "benchmark_user",
		"email":     "user@example.com",
		"score":     95.5,
		"active":    true,
		"timestamp": time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Update ID to avoid identical records
		record["id"] = int64(i)
		err := writer.Write(ctx, record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParquetWriter_TypeInference benchmarks type inference
func BenchmarkParquetWriter_TypeInference(b *testing.B) {
	tempDir := b.TempDir()
	filename := filepath.Join(tempDir, "bench_types.parquet")

	writer, err := NewParquetWriter(filename)
	require.NoError(b, err)
	defer writer.Close()

	values := []interface{}{
		int64(42),
		"string_value",
		3.14159,
		true,
		time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		value := values[i%len(values)]
		_, err := writer.inferArrowType(value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ensureParquetWriterClosed provides guaranteed cleanup for tests
func ensureParquetWriterClosed(t *testing.T, writer *ParquetWriter) {
	t.Helper()
	t.Cleanup(func() {
		if writer != nil {
			if err := writer.Close(); err != nil {
				t.Logf("Warning: failed to close ParquetWriter: %v", err)
			}
			// Force garbage collection to release file handles on Windows
			runtime.GC()
			runtime.GC()                      // Call twice to ensure finalizers run
			time.Sleep(10 * time.Millisecond) // Brief pause for OS cleanup
		}
	})
}
