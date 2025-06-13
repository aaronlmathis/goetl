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

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"database/sql"

	"github.com/aaronlmathis/goetl/core"
	"github.com/aaronlmathis/goetl/pipeline"
	"github.com/aaronlmathis/goetl/readers"
	"github.com/aaronlmathis/goetl/transform"
	"github.com/aaronlmathis/goetl/writers"
	_ "github.com/lib/pq"
)

func main() {
	// fmt.Printf("=== GoETL Connection Test ===\n")
	// if err := simpleConnectionTest(); err != nil {
	// 	fmt.Printf("error executing connection test: %v\n", err)
	// 	return
	// }
	// fmt.Printf("=== GoETL PostgreSQL Example #1 ===\n")
	// if err := csvToPostgresExample(); err != nil {
	// 	fmt.Printf("Error executing pipeline: %v\n", err)
	// 	return
	// }
	// fmt.Printf("=== GoETL Minimal Pipeline Test ===\n")
	// if err := minimalPipelineTest(); err != nil {
	// 	fmt.Printf("Error executing minimal pipeline test: %v\n", err)
	// 	return
	// }
	// fmt.Printf("=== GoETL Enhanced Pipeline Test ===\n")
	// if err := enhancedTransformationExample(); err != nil {
	// 	fmt.Printf("Error executing enhanced transformation example: %v\n", err)
	// 	return
	// }
	if err := simplePostgresToJSON(); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func minimalPipelineTest() error {
	fmt.Println("=== Minimal Pipeline Test ===")

	// Create reader
	reader, err := readers.NewPostgresReader(
		readers.WithPostgresDSN("postgres://test:test@localhost/pagila?sslmode=disable"),
		readers.WithPostgresQuery("SELECT actor_id, first_name FROM actor LIMIT 20"),
	)
	if err != nil {
		return fmt.Errorf("reader creation failed: %w", err)
	}
	defer reader.Close()

	// Create writer to stdout for immediate feedback
	writer := writers.NewJSONWriter(os.Stdout, writers.WithFlushOnWrite(true))

	// Build minimal pipeline
	pipeline, err := pipeline.NewPipeline().
		From(reader).
		To(writer).
		Build()
	if err != nil {
		return fmt.Errorf("pipeline build failed: %w", err)
	}

	// Execute synchronously
	fmt.Println("Executing pipeline synchronously...")
	if err := pipeline.Execute(context.Background()); err != nil {
		return fmt.Errorf("pipeline execution failed: %w", err)
	}

	fmt.Println("\n✓ Pipeline completed")
	return nil
}

func simplePostgresToJSON() error {
	fmt.Println("=== Simple PostgreSQL to JSON ===")

	// Create fresh reader for pipeline test with same generous timeout configuration
	fmt.Println("\nCreating reader for pipeline...")
	freshReader, err := readers.NewPostgresReader(
		readers.WithPostgresDSN("postgres://test:test@localhost/pagila?sslmode=disable&connect_timeout=30&statement_timeout=0"),
		readers.WithPostgresQuery("SELECT actor_id, first_name, last_name FROM actor"),
		readers.WithPostgresQueryTimeout(0),      // Disable query timeout, rely on context
		readers.WithPostgresBatchSize(100),       // Larger batches for efficiency
		readers.WithPostgresConnectionPool(5, 2), // More connections
	)
	if err != nil {
		return fmt.Errorf("fresh reader creation failed: %w", err)
	}
	defer freshReader.Close()

	outputFile, err := os.Create("../output/postgres-to-json-output.json")
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	// Create JSON writer with performance optimizations
	writer := writers.NewJSONWriter(outputFile,
		writers.WithFlushOnWrite(true), // Immediate flush for debugging
		writers.WithBufferSize(8192),
	)

	// Build pipeline with proper error strategy
	pipeline, err := pipeline.NewPipeline().
		From(freshReader).
		To(writer).
		Transform(transform.AddField("full_name", func(r core.Record) interface{} {
			firstName, _ := r["first_name"].(string)
			lastName, _ := r["last_name"].(string)
			return fmt.Sprintf("%s %s", firstName, lastName)
		})).
		Transform(transform.ToLower("first_name")).
		// Step 3: Add processing metadata
		Transform(transform.AddField("processed_at", func(r core.Record) interface{} {
			return time.Now().Format(time.RFC3339)
		})).
		// Step 4: Add record counter
		Transform(func() core.Transformer {
			counter := int64(0)
			return core.TransformFunc(func(ctx context.Context, record core.Record) (core.Record, error) {
				counter++
				result := make(core.Record, len(record)+1)
				for k, v := range record {
					result[k] = v
				}
				result["record_number"] = counter
				return result, nil
			})
		}()).
		WithErrorStrategy(core.FailFast).
		Build()
	if err != nil {
		return fmt.Errorf("pipeline build failed: %w", err)
	}

	fmt.Println("Executing pipeline with generous timeout...")
	pipelineCtx, pipelineCancel := context.WithTimeout(context.Background(), 300*time.Second) // 5 minutes
	defer pipelineCancel()

	if err := pipeline.Execute(pipelineCtx); err != nil {
		stats := writer.Stats()
		return fmt.Errorf("pipeline execution failed after %d records: %w", stats.RecordsWritten, err)
	}

	// Force final flush and get stats
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("final flush failed: %w", err)
	}

	stats := writer.Stats()
	fmt.Printf("\n✓ Pipeline completed with %d records\n", stats.RecordsWritten)
	fmt.Printf("JSON output written to %s\n", outputFile.Name())
	fmt.Printf("Null value counts: %+v\n", stats.NullValueCounts)
	// Print final stats
	fmt.Printf("Total records processed: %d\n", stats.RecordsWritten)
	fmt.Printf("Total null values encountered: %+v\n", stats.NullValueCounts)

	fmt.Printf("Total flush duration: %v\n", stats.FlushDuration)

	fmt.Printf("Last flush time: %v\n", stats.LastFlushTime)

	return nil
}

func simpleConnectionTest() error {
	fmt.Println("=== Simple Connection Test ===")

	// Test raw database connection

	db, err := sql.Open("postgres", "postgres://test:test@localhost/pagila?sslmode=disable&connect_timeout=5")
	if err != nil {
		return fmt.Errorf("sql.Open failed: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test ping
	fmt.Println("Testing ping...")
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	fmt.Println("✓ Ping successful")

	// Test query
	fmt.Println("Testing query...")
	rows, err := db.QueryContext(ctx, "SELECT actor_id, first_name FROM actor LIMIT 1")
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		fmt.Printf("✓ Query result: id=%d, name=%s\n", id, name)
	}

	return nil
}
