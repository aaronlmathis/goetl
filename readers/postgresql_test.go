// //
// // SPDX-License-Identifier: GPL-3.0-or-later
// //
// // Copyright (C) 2025 Aaron Mathis aaron.mathis@gmail.com
// //
// // This file is part of GoETL.
// //
// // GoETL is free software: you can redistribute it and/or modify
// // it under the terms of the GNU General Public License as published by
// // the Free Software Foundation, either version 3 of the License, or
// // (at your option) any later version.
// //
// // GoETL is distributed in the hope that it will be useful,
// // but WITHOUT ANY WARRANTY; without even the implied warranty of
// // MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// // GNU General Public License for more details.
// //
// // You should have received a copy of the GNU General Public License
// // along with GoETL. If not, see https://www.gnu.org/licenses/.

package readers

// import (
// 	"context"
// 	"database/sql"
// 	"fmt"
// 	"io"
// 	"os"
// 	"testing"
// 	"time"

// 	_ "github.com/lib/pq"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/require"
// )

// func TestPostgresReaderOptions(t *testing.T) {
// 	tests := []struct {
// 		name     string
// 		options  []PostgresReaderOption
// 		expected PostgresReaderOptions
// 	}{
// 		{
// 			name:    "default options",
// 			options: []PostgresReaderOption{},
// 			expected: PostgresReaderOptions{
// 				BatchSize:       1000,
// 				QueryTimeout:    30 * time.Second,
// 				ConnMaxLifetime: 5 * time.Minute,
// 				ConnMaxIdleTime: 1 * time.Minute,
// 				MaxOpenConns:    10,
// 				MaxIdleConns:    5,
// 				Metadata:        make(map[string]string),
// 			},
// 		},
// 		{
// 			name: "custom DSN and query",
// 			options: []PostgresReaderOption{
// 				WithPostgresDSN("postgres://test:test@localhost:5432/testdb"),
// 				WithPostgresQuery("SELECT * FROM users WHERE active = $1", true),
// 			},
// 			expected: PostgresReaderOptions{
// 				DSN:             "postgres://test:test@localhost:5432/testdb",
// 				Query:           "SELECT * FROM users WHERE active = $1",
// 				Params:          []interface{}{true},
// 				BatchSize:       1000,
// 				QueryTimeout:    30 * time.Second,
// 				ConnMaxLifetime: 5 * time.Minute,
// 				ConnMaxIdleTime: 1 * time.Minute,
// 				MaxOpenConns:    10,
// 				MaxIdleConns:    5,
// 				Metadata:        make(map[string]string),
// 			},
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			opts := &PostgresReaderOptions{}
// 			opts = opts.withDefaults()
// 			for _, option := range tt.options {
// 				option(opts)
// 			}

// 			assert.Equal(t, tt.expected.DSN, opts.DSN)
// 			assert.Equal(t, tt.expected.Query, opts.Query)
// 			assert.Equal(t, tt.expected.BatchSize, opts.BatchSize)
// 		})
// 	}
// }

// func TestPostgresReaderError(t *testing.T) {
// 	baseErr := fmt.Errorf("connection failed")
// 	pgErr := &PostgresReaderError{
// 		Op:  "connect",
// 		Err: baseErr,
// 	}

// 	assert.Equal(t, "postgres reader connect: connection failed", pgErr.Error())
// 	assert.Equal(t, baseErr, pgErr.Unwrap())
// }

// func TestPostgresReaderValidation(t *testing.T) {
// 	tests := []struct {
// 		name        string
// 		options     []PostgresReaderOption
// 		expectedErr string
// 	}{
// 		{
// 			name:        "missing DSN",
// 			options:     []PostgresReaderOption{WithPostgresQuery("SELECT 1")},
// 			expectedErr: "dsn is required",
// 		},
// 		{
// 			name:        "missing query",
// 			options:     []PostgresReaderOption{WithPostgresDSN("postgres://localhost/test")},
// 			expectedErr: "query is required",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			_, err := NewPostgresReader(tt.options...)
// 			require.Error(t, err)
// 			assert.Contains(t, err.Error(), tt.expectedErr)
// 		})
// 	}
// }

// // func TestPostgresReaderConvertSQLValue(t *testing.T) {
// // 	reader := &PostgresReader{
// // 		stats: PostgresReaderStats{
// // 			NullValueCounts: make(map[string]int64),
// // 		},
// // 	}

// // 	tests := []struct {
// // 		name     string
// // 		value    interface{}
// // 		colType  string
// // 		expected interface{}
// // 	}{
// // 		{
// // 			name:     "nil value",
// // 			value:    nil,
// // 			expected: nil,
// // 		},
// // 		{
// // 			name:     "byte array to string for text type",
// // 			value:    []byte("hello world"),
// // 			colType:  "TEXT",
// // 			expected: "hello world",
// // 		},
// // 		{
// // 			name:     "byte array to string for varchar type",
// // 			value:    []byte("test"),
// // 			colType:  "VARCHAR",
// // 			expected: "test",
// // 		},
// // 		{
// // 			name:     "byte array unchanged for binary type",
// // 			value:    []byte{1, 2, 3},
// // 			colType:  "BYTEA",
// // 			expected: []byte{1, 2, 3},
// // 		},
// // 		{
// // 			name:     "time value",
// // 			value:    time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
// // 			expected: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
// // 		},
// // 		{
// // 			name:     "bool value",
// // 			value:    true,
// // 			expected: true,
// // 		},
// // 		{
// // 			name:     "int64 value",
// // 			value:    int64(42),
// // 			expected: int64(42),
// // 		},
// // 		{
// // 			name:     "float64 value",
// // 			value:    float64(3.14),
// // 			expected: float64(3.14),
// // 		},
// // 		{
// // 			name:     "string value",
// // 			value:    "test string",
// // 			expected: "test string",
// // 		},
// // 		{
// // 			name:     "int value via reflection",
// // 			value:    int(123),
// // 			expected: int64(123),
// // 		},
// // 		{
// // 			name:     "float32 value via reflection",
// // 			value:    float32(2.5),
// // 			expected: float64(2.5),
// // 		},
// // 		{
// // 			name:     "unknown type fallback",
// // 			value:    complex(1, 2),
// // 			expected: "(1+2i)",
// // 		},
// // 	}

// // 	for _, tt := range tests {
// // 		t.Run(tt.name, func(t *testing.T) {
// // 			// Create a mock column type that implements sql.ColumnType interface
// // 			mockColType := &mockColumnType{typeName: tt.colType}
// // 			result := reader.convertSQLValue(tt.value, sql.ColumnType(mockColType))
// // 			assert.Equal(t, tt.expected, result)
// // 		})
// // 	}
// // }

// func TestPostgresReaderIntegration(t *testing.T) {
// 	// Skip this test unless POSTGRES_TEST_DSN environment variable is set
// 	// This allows for optional integration testing
// 	dsn := os.Getenv("POSTGRES_TEST_DSN")
// 	if dsn == "" {
// 		t.Skip("POSTGRES_TEST_DSN not set")
// 		return
// 	}

// 	reader, err := NewPostgresReader(
// 		WithPostgresDSN(dsn),
// 		WithPostgresQuery("SELECT 1 as id, 'test' as name, true as active"),
// 	)
// 	require.NoError(t, err)
// 	defer reader.Close()

// 	ctx := context.Background()
// 	record, err := reader.Read(ctx)
// 	require.NoError(t, err)

// 	assert.Equal(t, int64(1), record["id"])
// 	assert.Equal(t, "test", record["name"])
// 	assert.Equal(t, true, record["active"])

// 	// Verify EOF on next read
// 	_, err = reader.Read(ctx)
// 	assert.Equal(t, io.EOF, err)

// 	// Check stats
// 	stats := reader.Stats()
// 	assert.Equal(t, int64(1), stats.RecordsRead)
// 	assert.True(t, stats.QueryDuration > 0)
// }

// func TestPostgresReaderSchema(t *testing.T) {
// 	// Test the schema method with mock data
// 	reader := &PostgresReader{
// 		columnNames: []string{"id", "name", "active"},
// 		// Since we can't mock sql.ColumnType easily, test the method behavior
// 		columnTypes: make([]*sql.ColumnType, 3),
// 	}

// 	// For this test, we'll verify the schema method exists and returns a map
// 	schema := reader.Schema()
// 	assert.NotNil(t, schema)
// 	assert.IsType(t, map[string]string{}, schema)

// 	// Verify it has the expected number of columns
// 	assert.Len(t, schema, len(reader.columnNames))

// 	// Verify all column names are present as keys
// 	for _, colName := range reader.columnNames {
// 		_, exists := schema[colName]
// 		assert.True(t, exists, "Column %s should exist in schema", colName)
// 	}
// }

// func TestPostgresReaderStats(t *testing.T) {
// 	stats := PostgresReaderStats{
// 		RecordsRead:     100,
// 		QueryDuration:   time.Millisecond * 500,
// 		ReadDuration:    time.Millisecond * 200,
// 		ConnectionTime:  time.Millisecond * 100,
// 		NullValueCounts: map[string]int64{"optional_field": 5},
// 	}

// 	reader := &PostgresReader{stats: stats}
// 	retrievedStats := reader.Stats()

// 	assert.Equal(t, stats.RecordsRead, retrievedStats.RecordsRead)
// 	assert.Equal(t, stats.QueryDuration, retrievedStats.QueryDuration)
// 	assert.Equal(t, stats.ReadDuration, retrievedStats.ReadDuration)
// 	assert.Equal(t, stats.ConnectionTime, retrievedStats.ConnectionTime)
// 	assert.Equal(t, stats.NullValueCounts, retrievedStats.NullValueCounts)
// }

// // Example showing how to use the PostgreSQL reader
// func ExamplePostgresReader() {
// 	// This example shows basic usage - would require a real database
// 	fmt.Println("PostgreSQL Reader Example")

// 	// Example configuration
// 	options := []PostgresReaderOption{
// 		WithPostgresDSN("postgres://user:password@localhost:5432/database"),
// 		WithPostgresQuery("SELECT id, name, email FROM users WHERE active = $1", true),
// 		WithPostgresBatchSize(1000),
// 	}

// 	fmt.Printf("Options configured: %d\n", len(options))

// 	// Output:
// 	// PostgreSQL Reader Example
// 	// Options configured: 3
// }
