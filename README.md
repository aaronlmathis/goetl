# Go ETL Library

A comprehensive, high-performance Go library for building complete ETL (Extract, Transform, Load) data processing pipelines using Directed Acyclic Graphs (DAGs). GoETL provides streaming data readers, configurable transformations, and efficient writers with a fluent API for complex data workflows.

## Features

### Core Capabilities
- **DAG-Based Processing**: Define complex ETL workflows as Directed Acyclic Graphs
- **Streaming Processing**: Efficient record-by-record processing for large datasets
- **Fluent API**: Chainable operations for building intuitive DAG pipelines
- **Type Safety**: Robust type conversion with configurable error handling
- **Extensible Design**: Interface-driven architecture for custom implementations
- **Concurrent Processing**: Task-level parallelism with dependency management
- **Comprehensive Error Handling**: Multiple error strategies with retry and backoff
- **Task Management**: Retry policies, timeouts, triggers, and conditional execution

### DAG Features
- **Dependency Management**: Automatic task ordering based on dependencies
- **Parallel Execution**: Execute independent tasks concurrently
- **Task Types**: Source, Transform, Filter, Sink, Join, Aggregate, CDC, SCD, Conditional
- **Retry Logic**: Configurable retry policies with exponential backoff
- **Timeouts**: Per-task and global timeout configuration
- **Trigger Rules**: Control task execution based on dependency outcomes
- **Debugging**: Comprehensive DAG structure analysis and validation

### Transformation Operations
- **Mapping/Projection**: Select, rename, and reorder fields
- **Filtering**: Conditional record filtering with multiple criteria
- **Type Conversion**: String, numeric, boolean, and date/time conversions
- **Data Cleaning**: Trimming, case conversion, normalization
- **Field Enhancement**: Add computed fields and derived values
- **Aggregation**: Group by operations with sum, count, avg, min, max
- **Joins**: Inner, left, right joins with hash and merge strategies

### Supported Formats & Sources
- **CSV**: Configurable delimiters, headers, quoting, batching
- **JSON**: Line-delimited JSON (JSONL) format with batching
- **Parquet**: High-performance columnar format with compression
- **PostgreSQL**: Direct database reading/writing with connection pooling, batching, and conflict resolution
- **MongoDB**: Full-featured MongoDB reader with aggregation pipelines, change streams, and pagination
- **HTTP APIs**: RESTful API integration with authentication, pagination, and retry logic
- **Extensible**: Easy to add custom readers and writers

## Quick Start

### Simple DAG Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    
    "github.com/aaronlmathis/goetl/dag"
    "github.com/aaronlmathis/goetl/dag/tasks"
    "github.com/aaronlmathis/goetl/readers"
    "github.com/aaronlmathis/goetl/writers"
    "github.com/aaronlmathis/goetl/transform"
)

func main() {
    // Open input JSON file
    jsonFile, err := os.Open("iris.json")
    if err != nil {
        panic(err)
    }
    defer jsonFile.Close()

    // Create readers and writers
    jsonReader := readers.NewJSONReader(jsonFile)
    
    csvFile, err := os.Create("iris_output.csv")
    if err != nil {
        panic(err)
    }
    defer csvFile.Close()
    
    csvWriter, err := writers.NewCSVWriter(csvFile)
    if err != nil {
        panic(err)
    }

    // Create transformation
    addSepalArea := transform.AddField("sepal_area", func(r core.Record) interface{} {
        length, _ := r["sepal.length"].(float64)
        width, _ := r["sepal.width"].(float64)
        return length * width
    })

    // Build DAG
    dagInstance, err := dag.NewDAG("iris_processing", "Iris Data Processing").
        AddSourceTask("extract_iris", jsonReader).
        AddTransformTask("add_area", addSepalArea, []string{"extract_iris"}).
        AddSinkTask("write_csv", csvWriter, []string{"add_area"}).
        WithMaxParallelism(4).
        Build()

    if err != nil {
        panic(err)
    }

    // Execute DAG
    executor := dag.NewDAGExecutor(dag.WithMaxWorkers(4))
    result, err := executor.Execute(context.Background(), dagInstance)
    if err != nil {
        panic(err)
    }

    fmt.Printf("DAG completed in %v\n", result.EndTime.Sub(result.StartTime))
}
```

### Complex Multi-Source DAG

```go
func complexDAGExample() error {
    // Multiple data sources
    pgReader, _ := readers.NewPostgresReader(
        readers.WithPostgresDSN("postgres://user:pass@localhost/db"),
        readers.WithPostgresQuery("SELECT * FROM customers"),
    )
    
    mongoReader, _ := readers.NewMongoReader(
        readers.WithMongoURI("mongodb://localhost:27017"),
        readers.WithMongoDB("orders"),
        readers.WithMongoCollection("orders"),
    )
    
    apiReader, _ := readers.NewHTTPReader("https://api.example.com/metadata",
        readers.WithHTTPBearerToken("your-token"),
        readers.WithHTTPPagination(&readers.PaginationConfig{
            Type:     "offset",
            PageSize: 100,
        }),
    )

    // Output writers
    warehouseWriter, _ := writers.NewParquetWriter("warehouse.parquet")
    analyticsWriter := writers.NewJSONWriter(os.Stdout)

    // Build complex DAG
    dagInstance, err := dag.NewDAG("complex_etl", "Multi-Source ETL Pipeline").
        // Parallel data extraction
        AddSourceTask("extract_customers", pgReader).
        AddSourceTask("extract_orders", mongoReader).
        AddSourceTask("extract_metadata", apiReader).
        
        // Data transformations
        AddTransformTask("clean_customers", 
            transform.TrimSpace("name", "email"), 
            []string{"extract_customers"}).
        AddTransformTask("enrich_orders", 
            transform.AddField("order_date_str", func(r core.Record) interface{} {
                if date, ok := r["order_date"].(time.Time); ok {
                    return date.Format("2006-01-02")
                }
                return ""
            }), 
            []string{"extract_orders"}).
        
        // Join operations
        AddJoinTask("join_customer_orders", tasks.JoinConfig{
            JoinType:  "inner",
            LeftKeys:  []string{"customer_id"},
            RightKeys: []string{"customer_id"},
            Strategy:  "hash",
        }, []string{"clean_customers", "enrich_orders"}).
        
        // Add metadata
        AddJoinTask("add_metadata", tasks.JoinConfig{
            JoinType:  "left",
            LeftKeys:  []string{"product_id"},
            RightKeys: []string{"id"},
            Strategy:  "hash",
        }, []string{"join_customer_orders", "extract_metadata"}).
        
        // Multiple outputs
        AddSinkTask("write_warehouse", warehouseWriter, []string{"add_metadata"}).
        AddSinkTask("write_analytics", analyticsWriter, []string{"add_metadata"}).
        
        // Configuration
        WithMaxParallelism(8).
        WithDefaultTimeout(10 * time.Minute).
        Build()

    if err != nil {
        return err
    }

    // Execute with debugging
    fmt.Println("=== DAG Structure ===")
    dagInstance.PrintDAGStructure()
    
    executor := dag.NewDAGExecutor(dag.WithMaxWorkers(8))
    result, err := executor.Execute(context.Background(), dagInstance)
    if err != nil {
        return err
    }

    // Print results
    fmt.Printf("Pipeline completed in %v\n", result.EndTime.Sub(result.StartTime))
    for taskID, taskResult := range result.TaskResults {
        fmt.Printf("Task %s: %d→%d records in %v\n",
            taskID, taskResult.RecordsIn, taskResult.RecordsOut,
            taskResult.EndTime.Sub(taskResult.StartTime))
    }
    
    return nil
}
```

## DAG Components

### Task Types

```go
// Source Tasks - Extract data from external systems
AddSourceTask("extract_data", dataSource, 
    tasks.WithTimeout(5*time.Minute),
    tasks.WithRetries(3, time.Second),
    tasks.WithDescription("Extract customer data"),
    tasks.WithTags("source", "postgresql"))

// Transform Tasks - Data transformation operations  
AddTransformTask("clean_data", transformer, []string{"extract_data"},
    tasks.WithDescription("Clean and normalize data"))

// Filter Tasks - Conditional data filtering
AddFilterTask("filter_active", filter, []string{"clean_data"})

// Join Tasks - Combine data from multiple sources
AddJoinTask("join_datasets", joinConfig, []string{"source1", "source2"})

// Aggregate Tasks - Group and aggregate operations
AddAggregateTask("summarize", aggregator, []string{"join_datasets"})

// Sink Tasks - Load data to destination systems
AddSinkTask("write_output", dataSink, []string{"summarize"},
    tasks.WithTimeout(2*time.Minute))
```

### Retry and Error Handling

```go
// Configure retry policies
retryConfig := &tasks.RetryConfig{
    MaxRetries: 3,
    Strategy:   &tasks.ExponentialBackoff{
        BaseDelay: time.Second,
        MaxDelay:  time.Minute,
    },
}

// Apply to tasks
AddSourceTask("extract", reader, 
    tasks.WithRetryConfig(retryConfig),
    tasks.WithTimeout(5*time.Minute))

// Different backoff strategies
&tasks.LinearBackoff{BaseDelay: time.Second, MaxDelay: 30*time.Second}
&tasks.FixedBackoff{FixedDelay: 5*time.Second}
&tasks.JitteredBackoff{BaseDelay: time.Second, MaxDelay: time.Minute, Jitter: 0.1}
```

### Trigger Rules

```go
// Control task execution based on dependency outcomes
AddTransformTask("critical_path", transformer, []string{"task1", "task2"},
    tasks.WithTriggerRule(tasks.TriggerRuleAllSuccess))    // All must succeed

AddSinkTask("error_handler", errorSink, []string{"task1", "task2"},
    tasks.WithTriggerRule(tasks.TriggerRuleOneFailed))     // Execute if any fails

// Available trigger rules:
// - TriggerRuleAllSuccess (default)
// - TriggerRuleAllFailed  
// - TriggerRuleAllDone
// - TriggerRuleOneSuccess
// - TriggerRuleOneFailed
// - TriggerRuleNoneFailedMin
```

## Data Sources

### MongoDB Reader

```go
// Basic MongoDB reader
mongoReader, err := readers.NewMongoReader(
    readers.WithMongoURI("mongodb://localhost:27017"),
    readers.WithMongoDB("mydb"),
    readers.WithMongoCollection("users"),
    readers.WithMongoFilter(bson.M{"status": "active"}),
    readers.WithMongoBatchSize(1000),
)

// Aggregation pipeline
pipeline := []bson.M{
    {"$match": bson.M{"status": "active"}},
    {"$group": bson.M{
        "_id": "$department",
        "count": bson.M{"$sum": 1},
        "avg_salary": bson.M{"$avg": "$salary"},
    }},
}

mongoAggReader, err := readers.NewMongoAggregationReader(
    "mongodb://localhost:27017", "hr", "employees", pipeline)

// Change stream reader for real-time processing
mongoStreamReader, err := readers.NewMongoChangeStreamReader(
    "mongodb://localhost:27017", "orders", "transactions")
```

### HTTP API Reader

```go
// Basic API reader
apiReader, err := readers.NewHTTPReader("https://api.example.com/data")

// With authentication and pagination
apiReader, err := readers.NewHTTPReader("https://api.example.com/users",
    readers.WithHTTPBearerToken("your-api-token"),
    readers.WithHTTPPagination(&readers.PaginationConfig{
        Type:        "offset",
        PageSize:    100,
        LimitParam:  "limit",
        OffsetParam: "offset",
    }),
    readers.WithHTTPRetries(3, 2*time.Second),
    readers.WithHTTPRateLimit(100*time.Millisecond),
)

// Custom headers and query parameters
apiReader, err := readers.NewHTTPReader("https://api.example.com/data",
    readers.WithHTTPHeaders(map[string]string{
        "X-API-Version": "v2",
        "Accept":        "application/json",
    }),
    readers.WithHTTPQueryParams(map[string]string{
        "include": "metadata",
        "format":  "json",
    }),
)

// Different authentication methods
readers.WithHTTPBasicAuth("username", "password")
readers.WithHTTPAPIKey("X-API-Key", "your-key")
readers.WithHTTPAuth(&readers.AuthConfig{
    Type:          "custom",
    CustomHeaders: map[string]string{"Authorization": "Custom token"},
})
```

### PostgreSQL Reader/Writer

```go
// PostgreSQL reader with connection pooling
pgReader, err := readers.NewPostgresReader(
    readers.WithPostgresDSN("postgres://user:password@localhost/dbname"),
    readers.WithPostgresQuery("SELECT id, name, email FROM users WHERE active = $1", true),
    readers.WithPostgresBatchSize(1000),
    readers.WithPostgresConnectionPool(10, 5),
    readers.WithPostgresQueryTimeout(30*time.Second),
)

// PostgreSQL writer with conflict resolution
pgWriter, err := writers.NewPostgresWriter(
    writers.WithPostgresDSN("postgres://user:password@localhost/dbname"),
    writers.WithTableName("processed_users"),
    writers.WithColumns([]string{"id", "name", "email", "processed_at"}),
    writers.WithConflictResolution(writers.ConflictUpdate, 
        []string{"id"}, // conflict columns
        []string{"name", "email", "processed_at"}), // update columns
    writers.WithPostgresBatchSize(1000),
    writers.WithTransactionMode(true),
)
```

## Advanced Features

### Change Data Capture (CDC)

```go
// CDC pipeline for tracking data changes
dagInstance, err := dag.NewDAG("cdc_pipeline", "Change Data Capture").
    AddSourceTask("current_data", currentSnapshot).
    AddSourceTask("previous_data", previousSnapshot).
    AddCDCTask("detect_changes", tasks.CDCConfig{
        KeyFields:     []string{"id"},
        CompareFields: []string{"name", "email", "updated_at"},
        ChangeTypes:   []string{"INSERT", "UPDATE", "DELETE"},
    }, []string{"current_data", "previous_data"}).
    AddSinkTask("write_changes", changeLogWriter, []string{"detect_changes"}).
    Build()
```

### Slowly Changing Dimensions (SCD)

```go
// SCD Type 2 processing
dagInstance, err := dag.NewDAG("scd_pipeline", "SCD Processing").
    AddSourceTask("source_data", sourceReader).
    AddSourceTask("dimension_data", dimensionReader).
    AddSCDTask("process_scd2", tasks.SCDConfig{
        Type:               "SCD2",
        KeyFields:          []string{"customer_id"},
        TrackingFields:     []string{"name", "address", "phone"},
        EffectiveFromField: "effective_from",
        EffectiveToField:   "effective_to", 
        CurrentFlag:        "is_current",
    }, []string{"source_data", "dimension_data"}).
    AddSinkTask("write_dimension", dimensionWriter, []string{"process_scd2"}).
    Build()
```

### Conditional Execution

```go
// Data quality validation with conditional processing
dataQualityValidator := &validators.DataQualityValidator{
    MinRecords:     1000,
    RequiredFields: []string{"id", "email"},
    MaxNullPercent: 0.05,
}

dagInstance, err := dag.NewDAG("quality_pipeline", "Data Quality Pipeline").
    AddSourceTask("extract_data", dataReader).
    AddConditionalTask("validate_quality", dataQualityValidator, []string{"extract_data"}).
    // Only proceed if validation passes
    AddTransformTask("process_data", transformer, []string{"validate_quality"},
        tasks.WithTriggerRule(tasks.TriggerRuleAllSuccess)).
    AddSinkTask("write_output", outputWriter, []string{"process_data"}).
    Build()
```

## DAG Debugging and Monitoring

### Structure Analysis

```go
// Print comprehensive DAG information
dagInstance.PrintDAGStructure()

// Get detailed metrics
metrics := dagInstance.GetDAGMetrics()
fmt.Printf("Total tasks: %v\n", metrics["total_tasks"])
fmt.Printf("Max depth: %v\n", metrics["max_depth"])
fmt.Printf("Has cycles: %v\n", metrics["has_cycles"])

// Validate DAG structure
if errors := dagInstance.ValidateDAGStructure(); len(errors) > 0 {
    for _, err := range errors {
        fmt.Printf("Validation error: %v\n", err)
    }
}

// Get execution order
if order, err := dagInstance.GetExecutionOrder(); err == nil {
    fmt.Printf("Execution order: %v\n", order)
}
```

### Task Monitoring

```go
// Execute with detailed monitoring
executor := dag.NewDAGExecutor(dag.WithMaxWorkers(4))
result, err := executor.Execute(context.Background(), dagInstance)

if err != nil {
    fmt.Printf("DAG failed: %v\n", err)
} else {
    fmt.Printf("DAG completed in %v\n", result.EndTime.Sub(result.StartTime))
    
    // Detailed task results
    for taskID, taskResult := range result.TaskResults {
        status := "[success]"
        if !taskResult.Success {
            status = "[failed]]"
        }
        fmt.Printf("%s %s: %d→%d records, %v\n",
            status, taskID, taskResult.RecordsIn, taskResult.RecordsOut,
            taskResult.EndTime.Sub(taskResult.StartTime))
    }
}
```

## Transformations

```go
import "github.com/aaronlmathis/goetl/transform"

// Field operations
transform.Select("field1", "field2", "field3")
transform.Rename(map[string]string{"old_name": "new_name"})
transform.RemoveField("unwanted_field")
transform.RemoveFields("field1", "field2", "field3")

// Type conversions
transform.ToString("field")
transform.ToInt("field") 
transform.ToFloat("field")

// String operations
transform.TrimSpace("field1", "field2")
transform.ToUpper("field1", "field2")
transform.ToLower("field1", "field2")

// Custom computed fields
transform.AddField("full_name", func(record core.Record) interface{} {
    firstName, _ := record["first_name"].(string)
    lastName, _ := record["last_name"].(string)
    return fmt.Sprintf("%s %s", firstName, lastName)
})

// Date/time parsing
transform.ParseTime("date_field", "2006-01-02")
```

## Filters

```go
import "github.com/aaronlmathis/goetl/filter"

// Value comparisons
filter.Equals("status", "active")
filter.GreaterThan("age", 18)
filter.LessThan("score", 100)
filter.Between("salary", 50000, 100000)

// String operations
filter.Contains("email", "@company.com")
filter.StartsWith("name", "John")
filter.EndsWith("file", ".pdf")
filter.MatchesRegex("phone", `^\d{3}-\d{3}-\d{4}$`)

// Set operations
filter.In("category", "books", "music", "movies")
filter.NotNull("required_field")

// Logical operations
filter.And(filter1, filter2, filter3)
filter.Or(filter1, filter2)
filter.Not(filter1)

// Custom filters
filter.Custom(func(record core.Record) bool {
    age, _ := record["age"].(int)
    income, _ := record["income"].(float64)
    return age >= 18 && income > 50000
})
```

## Performance Considerations

- **Streaming**: Records are processed one at a time to minimize memory usage
- **Parallel Execution**: Independent tasks run concurrently with configurable worker pools
- **Batching**: Configure optimal batch sizes for your workload
- **Connection Pooling**: Database readers/writers include connection pooling
- **Memory Management**: Efficient record handling and garbage collection
- **Error Recovery**: Comprehensive retry logic with backoff strategies

### Optimal Configuration

```go
// DAG-level configuration
dagInstance, err := dag.NewDAG("pipeline", "My Pipeline").
    // ... tasks ...
    WithMaxParallelism(8).                    // Parallel task execution
    WithDefaultTimeout(10 * time.Minute).     // Default task timeout
    Build()

// Executor configuration  
executor := dag.NewDAGExecutor(
    dag.WithMaxWorkers(4),                    // Worker pool size
    dag.WithBackoffStrategy(&tasks.ExponentialBackoff{
        BaseDelay: time.Second,
        MaxDelay:  time.Minute,
    }),
)

// Task-level optimization
AddSourceTask("extract", reader,
    tasks.WithTimeout(5*time.Minute),         // Task-specific timeout
    tasks.WithRetries(3, time.Second),        // Retry configuration
    tasks.WithDescription("Extract data"),    // Documentation
    tasks.WithTags("source", "mongodb"))     // Categorization
```

## Examples

Check the `examples/dag_example/` directory for complete working examples:

- **Simple DAG**: Basic ETL pipeline with JSON to CSV conversion
- **Multi-Source DAG**: Complex pipeline with PostgreSQL, MongoDB, and HTTP sources
- **Fan-Out Processing**: Single source feeding multiple parallel transformations
- **Fan-In Processing**: Multiple sources joining into unified output
- **CDC Pipeline**: Change data capture with comparison logic
- **SCD Pipeline**: Slowly changing dimension processing
- **Conditional Execution**: Data quality validation with conditional processing

## Error Handling

The DAG framework provides comprehensive error handling:

### Task-Level Errors

```go
// Retry configuration with different strategies
retryConfig := &tasks.RetryConfig{
    MaxRetries: 3,
    Strategy:   &tasks.ExponentialBackoff{BaseDelay: time.Second, MaxDelay: time.Minute},
    RetryOn:    []error{}, // Empty means retry all errors
}

AddSourceTask("extract", reader, tasks.WithRetryConfig(retryConfig))
```

### DAG-Level Error Handling

```go
// Execute with error context
result, err := executor.Execute(context.Background(), dagInstance)
if err != nil {
    fmt.Printf("DAG execution failed: %v\n", err)
    
    // Check individual task results
    for taskID, taskResult := range result.TaskResults {
        if !taskResult.Success {
            fmt.Printf("Task %s failed: %v\n", taskID, taskResult.Error)
        }
    }
}
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

- [ ] Additional data sources (Redis, Elasticsearch, Kafka)
- [ ] Advanced aggregation operations with windowing
- [ ] Schema validation and inference improvements  
- [ ] Metrics and observability hooks
- [ ] Streaming execution mode for real-time processing
- [ ] Plugin system for custom tasks
- [ ] Performance optimizations for large-scale deployments
