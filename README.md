# Go ETL Library

A comprehensive, high-performance Go library for building complete ETL (Extract, Transform, Load) data processing pipelines. GoETL provides streaming data readers, configurable transformations, and efficient writers with a fluent API for complex data workflows.

## Features

### Core Capabilities
- **Streaming Processing**: Efficient record-by-record processing for large datasets
- **Fluent API**: Chainable operations for building intuitive pipelines
- **Type Safety**: Robust type conversion with configurable error handling
- **Extensible Design**: Interface-driven architecture for custom implementations
- **Concurrent Processing**: Thread-safe operations with proper synchronization
- **Comprehensive Error Handling**: Multiple error strategies (fail-fast, skip, collect)

### Transformation Operations
- **Mapping/Projection**: Select, rename, and reorder fields
- **Filtering**: Conditional record filtering with multiple criteria
- **Type Conversion**: String, numeric, boolean, and date/time conversions
- **Data Cleaning**: Trimming, case conversion, normalization
- **Field Enhancement**: Add computed fields and derived values
- **Aggregation**: Group by operations with sum, count, avg, min, max

### Supported Formats
- **CSV**: Configurable delimiters, headers, quoting, batching
- **JSON**: Line-delimited JSON (JSONL) format with batching
- **Parquet**: High-performance columnar format with compression
- **PostgreSQL**: Direct database reading/writing with connection pooling, batching, and conflict resolution
- **Extensible**: Easy to add custom readers and writers

## Quick Start

```go
package main

import (
    "context"
    "log"
    "os"
    "time"
    
    "github.com/aaronlmathis/goetl"
    "github.com/aaronlmathis/goetl/readers"
    "github.com/aaronlmathis/goetl/writers"
    "github.com/aaronlmathis/goetl/transform"
    "github.com/aaronlmathis/goetl/filter"
)

func main() {
    // Open input CSV file
    inputFile, err := os.Open("input.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer inputFile.Close()

    // Create CSV reader
    csvReader, err := readers.NewCSVReader(inputFile)
    if err != nil {
        log.Fatal(err)
    }

    // Open output JSON file
    outputFile, err := os.Create("output.json")
    if err != nil {
        log.Fatal(err)
    }
    defer outputFile.Close()

    // Create JSON writer with batching
    jsonWriter := writers.NewJSONWriter(outputFile, 
        writers.WithJSONBatchSize(100),
        writers.WithFlushOnWrite(false),
    )

    // Build and execute pipeline
    pipeline, err := goetl.NewPipeline().
        From(csvReader).
        Filter(filter.NotNull("email")).
        Transform(transform.TrimSpace("name", "email")).
        Transform(transform.ToLower("email")).
        Transform(transform.AddField("processed_at", func(r goetl.Record) interface{} {
            return time.Now().Format(time.RFC3339)
        })).
        To(jsonWriter).
        WithErrorStrategy(goetl.SkipErrors).
        Build()

    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    if err := pipeline.Execute(ctx); err != nil {
        log.Fatal(err)
    }

    log.Println("Pipeline executed successfully!")
}
```

## Transformations

```go
import "github.com/aaronlmathis/goetl/transform"

// Field selection and renaming
transform.Select("field1", "field2", "field3")
transform.Rename(map[string]string{"old_name": "new_name"})

// Type conversions
transform.ToString("field")
transform.ToInt("field")
transform.ToFloat("field")

// String operations
transform.TrimSpace("field1", "field2")
transform.ToUpper("field1", "field2")
transform.ToLower("field1", "field2")

// Add computed fields
transform.AddField("new_field", func(record goetl.Record) interface{} {
    // Compute value based on other fields
    return record["field1"].(string) + "_processed"
})

// Date/time parsing
transform.ParseTime("date_field", "2006-01-02")
```

## Filters

```go
import "github.com/aaronlmathis/goetl/filter"

// Null/empty checks
filter.NotNull("field")

// Value comparisons
filter.Equals("status", "active")
filter.GreaterThan("age", 18)
filter.LessThan("score", 100)
filter.Between("salary", 50000, 100000)

// String operations
filter.Contains("email", "@example.com")
filter.StartsWith("name", "John")
filter.EndsWith("file", ".pdf")
filter.MatchesRegex("phone", `^\d{3}-\d{3}-\d{4}$`)

// Set operations
filter.In("category", "books", "music", "movies")

// Logical operations
filter.And(filter1, filter2, filter3)
filter.Or(filter1, filter2)
filter.Not(filter1)
```

## Readers and Writers

### CSV Reader/Writer

```go
import (
    "github.com/aaronlmathis/goetl/readers"
    "github.com/aaronlmathis/goetl/writers"
)

// CSV Reader
csvReader, err := readers.NewCSVReader(file)

// CSV Writer with functional options
csvWriter, err := writers.NewCSVWriter(file,
    writers.WithHeaders([]string{"id", "name", "email"}),
    writers.WithComma(','),
    writers.WithWriteHeader(true),
    writers.WithCSVBatchSize(100),
    writers.WithUseCRLF(false),
)
```

### JSON Reader/Writer

```go
// JSON Reader (line-delimited)
jsonReader := readers.NewJSONReader(file)

// JSON Writer with batching
jsonWriter := writers.NewJSONWriter(file,
    writers.WithJSONBatchSize(100),
    writers.WithFlushOnWrite(false),
)
```

### Parquet Reader/Writer

```go
import "github.com/apache/arrow/go/v12/parquet/compress"

// Parquet Reader with column projection and performance tuning
parquetReader, err := readers.NewParquetReader(
    "input.parquet",
    readers.WithBatchSize(5000),
    readers.WithColumns([]string{"id", "name", "email", "timestamp"}),
    readers.WithMemoryLimit(128*1024*1024), // 128MB
    readers.WithParallelRead(true),
    readers.WithPreloadBatch(true),
)

// Alternative column projection syntax
parquetReader, err := readers.NewParquetReader(
    "input.parquet",
    readers.WithColumnProjection("id", "name", "email"),
    readers.WithBatchSize(1000),
)

// Parquet Writer with compression and metadata
parquetWriter, err := writers.NewParquetWriter(
    "output.parquet",
    writers.WithBatchSize(1000),
    writers.WithCompression(compress.Codecs.Snappy),
    writers.WithFieldOrder([]string{"id", "name", "email", "age"}),
    writers.WithMetadata(map[string]string{
        "dataset": "users",
        "version": "1.0",
    }),
    writers.WithRowGroupSize(10000),
    writers.WithSchemaValidation(true),
)
```

### PostgreSQL Reader/Writer

```go
import (
    "github.com/aaronlmathis/goetl/readers"
    "github.com/aaronlmathis/goetl/writers"
)

// PostgreSQL Reader with query parameterization and connection pooling
postgresReader, err := readers.NewPostgresReader(
    readers.WithPostgresDSN("postgres://user:password@localhost/dbname?sslmode=disable"),
    readers.WithPostgresQuery("SELECT id, name, email, age FROM users WHERE status = $1", "active"),
    readers.WithPostgresBatchSize(1000),
    readers.WithPostgresConnectionPool(10, 5),
    readers.WithPostgresQueryTimeout(30*time.Second),
)

// PostgreSQL Writer with conflict resolution and transaction support
postgresWriter, err := writers.NewPostgresWriter(
    writers.WithPostgresDSN("postgres://user:password@localhost/dbname?sslmode=disable"),
    writers.WithTableName("processed_users"),
    writers.WithColumns([]string{"id", "name", "email", "age"}),
    writers.WithPostgresBatchSize(1000),
    writers.WithCreateTable(true),
    writers.WithConflictResolution(writers.ConflictUpdate, 
        []string{"id"}, // conflict columns
        []string{"name", "email", "age"}, // columns to update
    ),
    writers.WithTransactionMode(true),
    writers.WithPostgresConnectionPool(10, 5, 5*time.Minute, 1*time.Minute),
)

// Using cursor for large result sets
largePGReader, err := readers.NewPostgresReader(
    readers.WithPostgresDSN("postgres://user:password@localhost/dbname"),
    readers.WithPostgresQuery("SELECT * FROM large_table"),
    readers.WithPostgresCursor(true, "my_cursor"),
    readers.WithPostgresBatchSize(5000),
)
```

## Error Handling

The library provides comprehensive error handling with granular error types:

### Error Strategies

1. **FailFast** (default): Stop processing on the first error
2. **SkipErrors**: Continue processing, skip problematic records
3. **CollectErrors**: Continue processing, collect all errors for review

```go
pipeline.WithErrorStrategy(goetl.SkipErrors)

// Custom error handler
pipeline.WithErrorHandler(goetl.ErrorHandlerFunc(func(ctx context.Context, record goetl.Record, err error) error {
    log.Printf("Error processing record %v: %v", record, err)
    return nil // Continue processing
}))
```

### Error Types

Each component provides specific error types for better error handling:

```go
// CSV Writer errors
&writers.CSVWriterError{Op: "write_row", Err: ...}
&writers.CSVWriterError{Op: "csv_flush", Err: ...}

// JSON Writer errors  
&writers.JSONWriterError{Op: "marshal_record", Err: ...}
&writers.JSONWriterError{Op: "write_json", Err: ...}

// Parquet Reader errors
&readers.ParquetReaderError{Op: "open_file", Err: ...}
&readers.ParquetReaderError{Op: "load_batch", Err: ...}
&readers.ParquetReaderError{Op: "column_projection", Err: ...}
&readers.ParquetReaderError{Op: "schema", Err: ...}

// Parquet Writer errors
&writers.ParquetWriterError{Op: "schema_inference", Err: ...}
&writers.ParquetWriterError{Op: "write_batch", Err: ...}

// PostgreSQL Reader errors
&readers.PostgresReaderError{Op: "connect", Err: ...}
&readers.PostgresReaderError{Op: "query", Err: ...}
&readers.PostgresReaderError{Op: "scan", Err: ...}

// PostgreSQL Writer errors
&writers.PostgresWriterError{Op: "write", Err: ...}
&writers.PostgresWriterError{Op: "flush_batch", Err: ...}
&writers.PostgresWriterError{Op: "connect", Err: ...}
```

## Performance and Statistics

All readers and writers provide detailed performance statistics:

```go
// Writer statistics
stats := writer.Stats()
fmt.Printf("Records written: %d\n", stats.RecordsWritten)
fmt.Printf("Batches written: %d\n", stats.BatchesWritten)
fmt.Printf("Flush count: %d\n", stats.FlushCount)
fmt.Printf("Processing time: %v\n", stats.FlushDuration)

// Reader statistics (Parquet example)
readerStats := parquetReader.Stats()
fmt.Printf("Records read: %d\n", readerStats.RecordsRead)
fmt.Printf("Batches read: %d\n", readerStats.BatchesRead)
fmt.Printf("Bytes read: %d\n", readerStats.BytesRead)
fmt.Printf("Read duration: %v\n", readerStats.ReadDuration)

// Null value tracking for data quality
for field, count := range stats.NullValueCounts {
    fmt.Printf("Field %s has %d null values\n", field, count)
}
```

## Examples

Check the `examples/` directory for complete working examples:

- **Basic CSV Processing**: Data cleaning and validation
- **JSON Transformation**: Format conversion and field manipulation  
- **Complex Pipeline**: Multi-step transformation with filtering and aggregation
- **Parquet Processing**: High-performance columnar data processing
- **JSON to Parquet**: High-performance format conversion with compression

### Example: Parquet Data Processing

```go
func parquetProcessingExample() error {
    // Read from Parquet with column projection for better performance
    parquetReader, err := readers.NewParquetReader(
        "large_dataset.parquet",
        readers.WithColumnProjection("user_id", "email", "created_at", "status"),
        readers.WithBatchSize(5000),
        readers.WithMemoryLimit(256*1024*1024), // 256MB
        readers.WithParallelRead(true),
    )
    if err != nil {
        return err
    }
    defer parquetReader.Close()

    // Create output JSON writer
    outputFile, err := os.Create("processed_users.json")
    if err != nil {
        return err
    }
    defer outputFile.Close()

    jsonWriter := writers.NewJSONWriter(outputFile,
        writers.WithJSONBatchSize(1000),
        writers.WithFlushOnWrite(false),
    )

    // Build processing pipeline
    pipeline, err := goetl.NewPipeline().
        From(parquetReader).
        Filter(filter.Equals("status", "active")).
        Transform(transform.ToLower("email")).
        Transform(transform.AddField("processed_date", func(r goetl.Record) interface{} {
            return time.Now().Format("2006-01-02")
        })).
        Transform(transform.AddField("domain", func(r goetl.Record) interface{} {
            if email, ok := r["email"].(string); ok {
                if idx := strings.Index(email, "@"); idx != -1 {
                    return email[idx+1:]
                }
            }
            return "unknown"
        })).
        To(jsonWriter).
        WithErrorStrategy(goetl.SkipErrors).
        Build()

    if err != nil {
        return err
    }

    ctx := context.Background()
    if err := pipeline.Execute(ctx); err != nil {
        return err
    }

    // Print performance statistics
    readerStats := parquetReader.Stats()
    writerStats := jsonWriter.Stats()
    
    fmt.Printf("Read %d records from %d batches (%d bytes) in %v\n",
        readerStats.RecordsRead, readerStats.BatchesRead, 
        readerStats.BytesRead, readerStats.ReadDuration)
    
    fmt.Printf("Wrote %d records with %d flushes in %v\n",
        writerStats.RecordsWritten, writerStats.FlushCount, 
        writerStats.FlushDuration)

    return nil
}
```

### Example: JSON to Parquet with Data Quality

```go
func jsonToParquetExample() error {
    file, err := os.Open("input.json")
    if err != nil {
        return err
    }
    defer file.Close()

    jsonReader := readers.NewJSONReader(file)

    parquetWriter, err := writers.NewParquetWriter(
        "output.parquet",
        writers.WithBatchSize(1000),
        writers.WithCompression(compress.Codecs.Snappy),
        writers.WithFieldOrder([]string{"id", "name", "email", "age"}),
        writers.WithMetadata(map[string]string{
            "dataset": "users",
            "version": "1.0",
            "created_by": "goetl",
        }),
    )
    if err != nil {
        return err
    }

    pipeline, err := goetl.NewPipeline().
        From(jsonReader).
        Transform(transform.TrimSpace("name", "email")).
        Transform(transform.ToInt("id")).
        Transform(transform.ToFloat("age")).
        Transform(transform.ToLower("email")).
        Transform(transform.AddField("has_email", func(r goetl.Record) interface{} {
            email := r["email"]
            return email != nil && email != ""
        })).
        Filter(filter.NotNull("id")).
        To(parquetWriter).
        WithErrorStrategy(goetl.SkipErrors).
        Build()

    if err != nil {
        return err
    }

    ctx := context.Background()
    if err := pipeline.Execute(ctx); err != nil {
        return err
    }

    // Print statistics
    stats := parquetWriter.Stats()
    fmt.Printf("Processed %d records in %d batches\n", 
        stats.RecordsWritten, stats.BatchesWritten)
    
    return nil
}
```

## Performance Considerations

- **Streaming**: Records are processed one at a time to minimize memory usage
- **Batching**: Configure optimal batch sizes for your workload
- **Type Assertions**: Use type assertions carefully in custom transformations
- **Error Handling**: Choose appropriate error strategy based on your use case
- **Concurrent Safety**: All writers are thread-safe with proper synchronization
- **Memory Management**: Parquet reader includes memory limits and efficient batch processing

### Optimal Batch Sizes

- **CSV**: 100-1000 records per batch
- **JSON**: 100-500 records per batch  
- **Parquet Reading**: 1000-10000 records per batch (depends on schema complexity and available memory)
- **Parquet Writing**: 1000-5000 records per batch (optimize for compression and query performance)

### Parquet Performance Tips

```go
// For large files, use column projection to read only needed columns
parquetReader, err := readers.NewParquetReader("large.parquet",
    readers.WithColumnProjection("id", "name", "email"), // Only read these columns
    readers.WithBatchSize(5000),                         // Larger batches for better throughput
    readers.WithMemoryLimit(512*1024*1024),             // 512MB memory limit
    readers.WithParallelRead(true),                      // Enable parallel column reading
    readers.WithPreloadBatch(true),                      // Preload next batch
)

// Monitor memory usage
stats := parquetReader.Stats()
if stats.BytesRead > 1024*1024*1024 { // 1GB
    log.Printf("High memory usage: %d bytes read", stats.BytesRead)
}
```

## Error Recovery and Data Quality

The library provides tools for monitoring data quality:

```go
// Track null values across fields
stats := writer.Stats()
for field, count := range stats.NullValueCounts {
    if count > 0 {
        log.Printf("Warning: Field '%s' has %d null values", field, count)
    }
}

// For Parquet readers, monitor read performance
readerStats := parquetReader.Stats()
avgBytesPerRecord := float64(readerStats.BytesRead) / float64(readerStats.RecordsRead)
log.Printf("Average bytes per record: %.2f", avgBytesPerRecord)

// Custom validation
pipeline.Transform(transform.AddField("validation_errors", func(r goetl.Record) interface{} {
    var errors []string
    if r["email"] == nil || r["email"] == "" {
        errors = append(errors, "missing_email")
    }
    if age, ok := r["age"].(int); !ok || age < 0 || age > 150 {
        errors = append(errors, "invalid_age")
    }
    return errors
}))
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality  
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the GPL-3.0-or-later License - see the LICENSE file for details.

## Roadmap

- [ ] Advanced aggregation operations with grouping
- [ ] Database readers/writers (SQL, NoSQL)
- [ ] Message queue integration (Kafka, RabbitMQ)
- [ ] Schema validation and inference improvements
- [ ] Metrics and observability hooks
- [ ] Additional compression formats
- [ ] Performance optimizations for large datasets
- [ ] Parquet writer implementation with Arrow integration
- [ ] Advanced Parquet features (nested types, complex schemas)
- [ ] Documentation improvements and tutorials
