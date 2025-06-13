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
	"log"
	"os"
	"time"

	"github.com/aaronlmathis/goetl/core"
	"github.com/aaronlmathis/goetl/filter"
	"github.com/aaronlmathis/goetl/pipeline"
	"github.com/aaronlmathis/goetl/readers"
	"github.com/aaronlmathis/goetl/transform"
	"github.com/aaronlmathis/goetl/writers"
)

// Example 1: Basic PostgreSQL to JSON conversion
func postgresqlToJSONExample() {
	fmt.Println("=== PostgreSQL to JSON Example ===")

	// Note: This example requires a PostgreSQL database connection
	// Set environment variable: export POSTGRES_DSN="postgres://user:pass@host:port/dbname"
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		fmt.Println("Skipping PostgreSQL example - POSTGRES_DSN not set")
		return
	}

	// Create PostgreSQL reader
	postgresReader, err := readers.NewPostgresReader(
		readers.WithPostgresDSN(dsn),
		readers.WithPostgresQuery(`
			SELECT 
				id,
				name,
				email,
				created_at,
				is_active,
				age
			FROM users 
			WHERE created_at >= $1 
			ORDER BY created_at DESC
			LIMIT 1000
		`, time.Now().AddDate(0, -1, 0)), // Last month
		readers.WithPostgresBatchSize(100),
		readers.WithPostgresQueryTimeout(60*time.Second),
	)
	if err != nil {
		log.Fatal("Failed to create PostgreSQL reader:", err)
	}
	defer postgresReader.Close()

	// Create JSON output file
	outputFile, err := os.Create("users_export.json")
	if err != nil {
		log.Fatal("Failed to create output file:", err)
	}
	defer outputFile.Close()

	// Create JSON writer
	jsonWriter := writers.NewJSONWriter(outputFile,
		writers.WithJSONBatchSize(50),
		writers.WithFlushOnWrite(false),
	)

	// Build and execute pipeline
	pipeline, err := pipeline.NewPipeline().
		From(postgresReader).
		To(jsonWriter).
		WithErrorStrategy(core.SkipErrors).
		Build()

	if err != nil {
		log.Fatal("Failed to build pipeline:", err)
	}

	ctx := context.Background()
	if err := pipeline.Execute(ctx); err != nil {
		log.Fatal("Pipeline execution failed:", err)
	}

	// Print statistics
	pgStats := postgresReader.Stats()
	writerStats := jsonWriter.Stats()

	fmt.Printf("Successfully processed %d records\n", pgStats.RecordsRead)
	fmt.Printf("Query took: %v\n", pgStats.QueryDuration)
	fmt.Printf("Read duration: %v\n", pgStats.ReadDuration)
	fmt.Printf("Connection time: %v\n", pgStats.ConnectionTime)
	fmt.Printf("Records written: %d\n", writerStats.RecordsWritten)

	// Print null value counts for data quality assessment
	if len(pgStats.NullValueCounts) > 0 {
		fmt.Println("Null value counts:")
		for field, count := range pgStats.NullValueCounts {
			fmt.Printf("  %s: %d\n", field, count)
		}
	}
}

// Example 2: Using cursor for large datasets
func postgresqlLargeDatasetExample() {
	fmt.Println("\n=== PostgreSQL Large Dataset with Cursor ===")

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		fmt.Println("Skipping PostgreSQL example - POSTGRES_DSN not set")
		return
	}

	// Create PostgreSQL reader with cursor for memory-efficient processing
	postgresReader, err := readers.NewPostgresReader(
		readers.WithPostgresDSN(dsn),
		readers.WithPostgresQuery(`
			SELECT 
				id,
				transaction_date,
				amount,
				description,
				customer_id
			FROM transactions 
			WHERE transaction_date >= $1
		`, time.Now().AddDate(0, -6, 0)), // Last 6 months
		readers.WithPostgresBatchSize(5000), // Larger batch for better performance
		readers.WithPostgresCursor(true, "large_transactions_cursor"),
		readers.WithPostgresConnectionPool(5, 2), // Optimized connection pool
	)
	if err != nil {
		log.Fatal("Failed to create PostgreSQL reader:", err)
	}
	defer postgresReader.Close()

	// Create Parquet writer for efficient storage
	parquetWriter, err := writers.NewParquetWriter(
		"transactions_export.parquet",
		writers.WithBatchSize(1000),
		writers.WithFieldOrder([]string{"id", "transaction_date", "amount", "description", "customer_id"}),
		writers.WithMetadata(map[string]string{
			"source":     "postgresql",
			"created_by": "goetl",
			"created_at": time.Now().Format(time.RFC3339),
		}),
	)
	if err != nil {
		log.Fatal("Failed to create Parquet writer:", err)
	}

	// Build pipeline
	pipeline, err := pipeline.NewPipeline().
		From(postgresReader).
		To(parquetWriter).
		WithErrorStrategy(core.SkipErrors).
		Build()

	if err != nil {
		log.Fatal("Failed to build pipeline:", err)
	}

	ctx := context.Background()
	if err := pipeline.Execute(ctx); err != nil {
		log.Fatal("Pipeline execution failed:", err)
	}

	// Print performance statistics
	pgStats := postgresReader.Stats()
	parquetStats := parquetWriter.Stats()

	fmt.Printf("Processed %d records using cursor\n", pgStats.RecordsRead)
	fmt.Printf("Wrote %d records to Parquet in %d batches\n",
		parquetStats.RecordsWritten, parquetStats.BatchesWritten)
}

// Example 3: PostgreSQL with data transformations and filtering
func postgresqlTransformExample() {
	fmt.Println("\n=== PostgreSQL with Transformations ===")

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		fmt.Println("Skipping PostgreSQL example - POSTGRES_DSN not set")
		return
	}

	// Create PostgreSQL reader
	postgresReader, err := readers.NewPostgresReader(
		readers.WithPostgresDSN(dsn),
		readers.WithPostgresQuery(`
			SELECT 
				id,
				first_name,
				last_name,
				email,
				phone,
				address,
				city,
				state,
				zip_code,
				registration_date,
				status
			FROM customers
		`),
		readers.WithPostgresBatchSize(500),
	)
	if err != nil {
		log.Fatal("Failed to create PostgreSQL reader:", err)
	}
	defer postgresReader.Close()

	// Create CSV output
	outputFile, err := os.Create("customers_cleaned.csv")
	if err != nil {
		log.Fatal("Failed to create output file:", err)
	}
	defer outputFile.Close()

	csvWriter, err := writers.NewCSVWriter(outputFile,
		writers.WithHeaders([]string{
			"id", "full_name", "email", "phone", "full_address",
			"registration_date", "status", "processed_at",
		}),
		writers.WithWriteHeader(true),
		writers.WithCSVBatchSize(100),
	)
	if err != nil {
		log.Fatal("Failed to create CSV writer:", err)
	}

	// Build pipeline with transformations and filters
	pipeline, err := pipeline.NewPipeline().
		From(postgresReader).
		// Filter out inactive customers
		Filter(filter.Equals("status", "active")).
		// Filter out records with missing email
		Filter(filter.NotNull("email")).
		// Clean and standardize data
		Transform(transform.TrimSpace("first_name", "last_name", "email", "city", "state")).
		Transform(transform.ToLower("email")).
		// Create computed fields
		Transform(transform.AddField("full_name", func(r core.Record) interface{} {
			firstName, _ := r["first_name"].(string)
			lastName, _ := r["last_name"].(string)
			return fmt.Sprintf("%s %s", firstName, lastName)
		})).
		Transform(transform.AddField("full_address", func(r core.Record) interface{} {
			address, _ := r["address"].(string)
			city, _ := r["city"].(string)
			state, _ := r["state"].(string)
			zip, _ := r["zip_code"].(string)
			return fmt.Sprintf("%s, %s, %s %s", address, city, state, zip)
		})).
		Transform(transform.AddField("processed_at", func(r core.Record) interface{} {
			return time.Now().Format(time.RFC3339)
		})).
		// Select only the fields we want in output
		Transform(transform.Select(
			"id", "full_name", "email", "phone", "full_address",
			"registration_date", "status", "processed_at",
		)).
		To(csvWriter).
		WithErrorStrategy(core.SkipErrors).
		Build()

	if err != nil {
		log.Fatal("Failed to build pipeline:", err)
	}

	ctx := context.Background()
	if err := pipeline.Execute(ctx); err != nil {
		log.Fatal("Pipeline execution failed:", err)
	}

	// Print comprehensive statistics
	pgStats := postgresReader.Stats()
	csvStats := csvWriter.Stats()

	fmt.Printf("Input: %d records from PostgreSQL\n", pgStats.RecordsRead)
	fmt.Printf("Output: %d records to CSV\n", csvStats.RecordsWritten)
	fmt.Printf("Data quality - Null values detected:\n")
	for field, count := range pgStats.NullValueCounts {
		if count > 0 {
			fmt.Printf("  %s: %d null values\n", field, count)
		}
	}

	fmt.Printf("Performance:\n")
	fmt.Printf("  Connection time: %v\n", pgStats.ConnectionTime)
	fmt.Printf("  Query time: %v\n", pgStats.QueryDuration)
	fmt.Printf("  Read time: %v\n", pgStats.ReadDuration)

}

// Helper function to demonstrate error handling patterns
func handlePostgresError(err error) {
	if pgErr, ok := err.(*readers.PostgresReaderError); ok {
		switch pgErr.Op {
		case "connect":
			log.Printf("Database connection failed: %v", pgErr.Err)
			log.Println("Check your DSN and ensure PostgreSQL is running")
		case "query":
			log.Printf("Query execution failed: %v", pgErr.Err)
			log.Println("Check your SQL syntax and parameters")
		case "scan":
			log.Printf("Row scanning failed: %v", pgErr.Err)
			log.Println("Check data types and null handling")
		default:
			log.Printf("PostgreSQL reader error [%s]: %v", pgErr.Op, pgErr.Err)
		}
	} else {
		log.Printf("General error: %v", err)
	}
}
