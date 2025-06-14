<!-- Code generated by gomarkdoc. DO NOT EDIT -->

# writers

```go
import "github.com/aaronlmathis/goetl/writers"
```

## Index

- [type CSVWriter](<#CSVWriter>)
  - [func NewCSVWriter\(w io.WriteCloser, opts ...WriterOptionCSV\) \(\*CSVWriter, error\)](<#NewCSVWriter>)
  - [func \(c \*CSVWriter\) Close\(\) error](<#CSVWriter.Close>)
  - [func \(c \*CSVWriter\) Flush\(\) error](<#CSVWriter.Flush>)
  - [func \(c \*CSVWriter\) Stats\(\) CSVWriterStats](<#CSVWriter.Stats>)
  - [func \(c \*CSVWriter\) Write\(ctx context.Context, record goetl.Record\) error](<#CSVWriter.Write>)
- [type CSVWriterError](<#CSVWriterError>)
  - [func \(e \*CSVWriterError\) Error\(\) string](<#CSVWriterError.Error>)
  - [func \(e \*CSVWriterError\) Unwrap\(\) error](<#CSVWriterError.Unwrap>)
- [type CSVWriterOptions](<#CSVWriterOptions>)
- [type CSVWriterStats](<#CSVWriterStats>)
- [type ConflictResolution](<#ConflictResolution>)
- [type JSONWriter](<#JSONWriter>)
  - [func NewJSONWriter\(w io.WriteCloser, opts ...WriterOptionJSON\) \*JSONWriter](<#NewJSONWriter>)
  - [func \(j \*JSONWriter\) Close\(\) error](<#JSONWriter.Close>)
  - [func \(j \*JSONWriter\) Flush\(\) error](<#JSONWriter.Flush>)
  - [func \(j \*JSONWriter\) Stats\(\) JSONWriterStats](<#JSONWriter.Stats>)
  - [func \(j \*JSONWriter\) Write\(ctx context.Context, record goetl.Record\) error](<#JSONWriter.Write>)
- [type JSONWriterError](<#JSONWriterError>)
  - [func \(e \*JSONWriterError\) Error\(\) string](<#JSONWriterError.Error>)
  - [func \(e \*JSONWriterError\) Unwrap\(\) error](<#JSONWriterError.Unwrap>)
- [type JSONWriterOptions](<#JSONWriterOptions>)
- [type JSONWriterStats](<#JSONWriterStats>)
- [type ParquetWriter](<#ParquetWriter>)
  - [func NewParquetWriter\(filename string, options ...WriterOption\) \(\*ParquetWriter, error\)](<#NewParquetWriter>)
  - [func \(p \*ParquetWriter\) Close\(\) error](<#ParquetWriter.Close>)
  - [func \(p \*ParquetWriter\) Flush\(\) error](<#ParquetWriter.Flush>)
  - [func \(p \*ParquetWriter\) Stats\(\) WriterStats](<#ParquetWriter.Stats>)
  - [func \(p \*ParquetWriter\) Write\(ctx context.Context, record goetl.Record\) error](<#ParquetWriter.Write>)
- [type ParquetWriterError](<#ParquetWriterError>)
  - [func \(e \*ParquetWriterError\) Error\(\) string](<#ParquetWriterError.Error>)
  - [func \(e \*ParquetWriterError\) Unwrap\(\) error](<#ParquetWriterError.Unwrap>)
- [type ParquetWriterOptions](<#ParquetWriterOptions>)
- [type PostgresWriter](<#PostgresWriter>)
  - [func NewPostgresWriter\(opts ...PostgresWriterOption\) \(\*PostgresWriter, error\)](<#NewPostgresWriter>)
  - [func \(w \*PostgresWriter\) Close\(\) error](<#PostgresWriter.Close>)
  - [func \(w \*PostgresWriter\) Flush\(\) error](<#PostgresWriter.Flush>)
  - [func \(w \*PostgresWriter\) Stats\(\) PostgresWriterStats](<#PostgresWriter.Stats>)
  - [func \(w \*PostgresWriter\) Write\(ctx context.Context, record goetl.Record\) error](<#PostgresWriter.Write>)
- [type PostgresWriterError](<#PostgresWriterError>)
  - [func \(e \*PostgresWriterError\) Error\(\) string](<#PostgresWriterError.Error>)
  - [func \(e \*PostgresWriterError\) Unwrap\(\) error](<#PostgresWriterError.Unwrap>)
- [type PostgresWriterOption](<#PostgresWriterOption>)
  - [func WithColumns\(columns \[\]string\) PostgresWriterOption](<#WithColumns>)
  - [func WithConflictResolution\(resolution ConflictResolution, conflictCols, updateCols \[\]string\) PostgresWriterOption](<#WithConflictResolution>)
  - [func WithCreateTable\(create bool\) PostgresWriterOption](<#WithCreateTable>)
  - [func WithPostgresBatchSize\(size int\) PostgresWriterOption](<#WithPostgresBatchSize>)
  - [func WithPostgresConnectionPool\(maxOpen, maxIdle int, maxLifetime, maxIdleTime time.Duration\) PostgresWriterOption](<#WithPostgresConnectionPool>)
  - [func WithPostgresDSN\(dsn string\) PostgresWriterOption](<#WithPostgresDSN>)
  - [func WithPostgresMetadata\(metadata map\[string\]string\) PostgresWriterOption](<#WithPostgresMetadata>)
  - [func WithPostgresQueryTimeout\(timeout time.Duration\) PostgresWriterOption](<#WithPostgresQueryTimeout>)
  - [func WithTableName\(tableName string\) PostgresWriterOption](<#WithTableName>)
  - [func WithTransactionMode\(enabled bool\) PostgresWriterOption](<#WithTransactionMode>)
  - [func WithTruncateTable\(truncate bool\) PostgresWriterOption](<#WithTruncateTable>)
- [type PostgresWriterOptions](<#PostgresWriterOptions>)
- [type PostgresWriterStats](<#PostgresWriterStats>)
- [type WriterOption](<#WriterOption>)
  - [func WithBatchSize\(size int64\) WriterOption](<#WithBatchSize>)
  - [func WithCompression\(compression compress.Compression\) WriterOption](<#WithCompression>)
  - [func WithFieldOrder\(fields \[\]string\) WriterOption](<#WithFieldOrder>)
  - [func WithMetadata\(metadata map\[string\]string\) WriterOption](<#WithMetadata>)
  - [func WithRowGroupSize\(size int64\) WriterOption](<#WithRowGroupSize>)
  - [func WithSchemaValidation\(validate bool\) WriterOption](<#WithSchemaValidation>)
- [type WriterOptionCSV](<#WriterOptionCSV>)
  - [func WithCSVBatchSize\(size int\) WriterOptionCSV](<#WithCSVBatchSize>)
  - [func WithCSVDelimiter\(delim rune\) WriterOptionCSV](<#WithCSVDelimiter>)
  - [func WithCSVHeaders\(headers \[\]string\) WriterOptionCSV](<#WithCSVHeaders>)
  - [func WithCSVWriteHeader\(write bool\) WriterOptionCSV](<#WithCSVWriteHeader>)
  - [func WithComma\(delim rune\) WriterOptionCSV](<#WithComma>)
  - [func WithHeaders\(headers \[\]string\) WriterOptionCSV](<#WithHeaders>)
  - [func WithUseCRLF\(useCRLF bool\) WriterOptionCSV](<#WithUseCRLF>)
  - [func WithWriteHeader\(write bool\) WriterOptionCSV](<#WithWriteHeader>)
- [type WriterOptionJSON](<#WriterOptionJSON>)
  - [func WithFlushOnWrite\(enabled bool\) WriterOptionJSON](<#WithFlushOnWrite>)
  - [func WithJSONBatchSize\(size int\) WriterOptionJSON](<#WithJSONBatchSize>)
- [type WriterStats](<#WriterStats>)


<a name="CSVWriter"></a>
## type [CSVWriter](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L127-L137>)

CSVWriter implements goetl.DataSink for CSV output with stats and batching. It supports batching, header management, delimiter configuration, and statistics.

```go
type CSVWriter struct {
    // contains filtered or unexported fields
}
```

<a name="NewCSVWriter"></a>
### func [NewCSVWriter](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L141>)

```go
func NewCSVWriter(w io.WriteCloser, opts ...WriterOptionCSV) (*CSVWriter, error)
```

NewCSVWriter creates a new CSV writer with extended options. Accepts functional options for configuration. Returns a ready\-to\-use writer or an error.

<a name="CSVWriter.Close"></a>
### func \(\*CSVWriter\) [Close](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L230>)

```go
func (c *CSVWriter) Close() error
```

Close implements the goetl.DataSink interface. Flushes and closes all resources.

<a name="CSVWriter.Flush"></a>
### func \(\*CSVWriter\) [Flush](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L214>)

```go
func (c *CSVWriter) Flush() error
```

Flush implements the goetl.DataSink interface. Forces any buffered records to be written to the CSV output.

<a name="CSVWriter.Stats"></a>
### func \(\*CSVWriter\) [Stats](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L284>)

```go
func (c *CSVWriter) Stats() CSVWriterStats
```

Stats returns write statistics.

<a name="CSVWriter.Write"></a>
### func \(\*CSVWriter\) [Write](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L169>)

```go
func (c *CSVWriter) Write(ctx context.Context, record goetl.Record) error
```

Write implements the goetl.DataSink interface. Buffers records and writes in batches or on flush. Thread\-safe.

<a name="CSVWriterError"></a>
## type [CSVWriterError](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L41-L44>)

CSVWriterError wraps CSV\-specific write errors with context about the operation.

```go
type CSVWriterError struct {
    Op  string // Operation that failed (e.g., "write", "flush", "write_row")
    Err error  // Underlying error
}
```

<a name="CSVWriterError.Error"></a>
### func \(\*CSVWriterError\) [Error](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L46>)

```go
func (e *CSVWriterError) Error() string
```



<a name="CSVWriterError.Unwrap"></a>
### func \(\*CSVWriterError\) [Unwrap](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L50>)

```go
func (e *CSVWriterError) Unwrap() error
```



<a name="CSVWriterOptions"></a>
## type [CSVWriterOptions](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L64-L70>)

CSVWriterOptions configures CSV output.

```go
type CSVWriterOptions struct {
    Comma       rune     // Field delimiter (default ',')
    UseCRLF     bool     // Use CRLF line endings
    WriteHeader bool     // Write header row
    Headers     []string // Explicit header order
    BatchSize   int      // Number of records to buffer before writing
}
```

<a name="CSVWriterStats"></a>
## type [CSVWriterStats](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L55-L61>)

CSVWriterStats holds CSV write performance statistics.

```go
type CSVWriterStats struct {
    RecordsWritten  int64            // Total records written
    FlushCount      int64            // Number of flushes performed
    FlushDuration   time.Duration    // Total time spent flushing
    LastFlushTime   time.Time        // Time of last flush
    NullValueCounts map[string]int64 // Count of null values per field
}
```

<a name="ConflictResolution"></a>
## type [ConflictResolution](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L71>)

ConflictResolution defines how to handle INSERT conflicts in PostgreSQL.

```go
type ConflictResolution int
```

<a name="ConflictError"></a>

```go
const (
    // ConflictError returns an error on conflict (default PostgreSQL behavior).
    ConflictError ConflictResolution = iota
    // ConflictIgnore ignores conflicting rows (ON CONFLICT DO NOTHING).
    ConflictIgnore
    // ConflictUpdate updates conflicting rows (ON CONFLICT DO UPDATE).
    ConflictUpdate
)
```

<a name="JSONWriter"></a>
## type [JSONWriter](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L71-L80>)

JSONWriter implements goetl.DataSink for line\-delimited JSON output. It supports batching, buffered output, flush control, and statistics.

```go
type JSONWriter struct {
    // contains filtered or unexported fields
}
```

<a name="NewJSONWriter"></a>
### func [NewJSONWriter](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L101>)

```go
func NewJSONWriter(w io.WriteCloser, opts ...WriterOptionJSON) *JSONWriter
```

NewJSONWriter creates a JSONWriter with optional buffered output and options. Accepts functional options for configuration. Returns a ready\-to\-use writer.

<a name="JSONWriter.Close"></a>
### func \(\*JSONWriter\) [Close](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L175>)

```go
func (j *JSONWriter) Close() error
```

Close implements the goetl.DataSink interface. Flushes and closes all resources.

<a name="JSONWriter.Flush"></a>
### func \(\*JSONWriter\) [Flush](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L160>)

```go
func (j *JSONWriter) Flush() error
```

Flush implements the goetl.DataSink interface. Forces any buffered records to be written to the output.

<a name="JSONWriter.Stats"></a>
### func \(\*JSONWriter\) [Stats](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L237>)

```go
func (j *JSONWriter) Stats() JSONWriterStats
```

Stats returns the writer's performance stats.

<a name="JSONWriter.Write"></a>
### func \(\*JSONWriter\) [Write](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L125>)

```go
func (j *JSONWriter) Write(ctx context.Context, record goetl.Record) error
```

Write implements the goetl.DataSink interface. Buffers records and writes in batches or on flush. Thread\-safe.

<a name="JSONWriterError"></a>
## type [JSONWriterError](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L41-L44>)

JSONWriterError wraps detailed error context for JSONWriter operations.

```go
type JSONWriterError struct {
    Op  string // Operation that failed (e.g., "write", "flush", "marshal_record")
    Err error  // Underlying error
}
```

<a name="JSONWriterError.Error"></a>
### func \(\*JSONWriterError\) [Error](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L46>)

```go
func (e *JSONWriterError) Error() string
```



<a name="JSONWriterError.Unwrap"></a>
### func \(\*JSONWriterError\) [Unwrap](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L50>)

```go
func (e *JSONWriterError) Unwrap() error
```



<a name="JSONWriterOptions"></a>
## type [JSONWriterOptions](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L55-L58>)

JSONWriterOptions configures the JSONWriter behavior.

```go
type JSONWriterOptions struct {
    BatchSize    int  // Number of records to buffer before writing
    FlushOnWrite bool // Whether to flush after every write
}
```

<a name="JSONWriterStats"></a>
## type [JSONWriterStats](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L61-L67>)

JSONWriterStats holds statistics about the writer's performance.

```go
type JSONWriterStats struct {
    RecordsWritten  int64            // Total records written
    FlushCount      int64            // Number of flushes performed
    FlushDuration   time.Duration    // Total time spent flushing
    LastFlushTime   time.Time        // Time of last flush
    NullValueCounts map[string]int64 // Count of null values per field
}
```

<a name="ParquetWriter"></a>
## type [ParquetWriter](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L67-L84>)

ParquetWriter implements goetl.DataSink for Parquet files. It supports batching, Arrow schema inference, compression, field ordering, schema validation, and statistics.

```go
type ParquetWriter struct {
    // contains filtered or unexported fields
}
```

<a name="NewParquetWriter"></a>
### func [NewParquetWriter](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L166>)

```go
func NewParquetWriter(filename string, options ...WriterOption) (*ParquetWriter, error)
```

NewParquetWriter creates a new Parquet writer for a file. Accepts functional options for configuration. Returns a ready\-to\-use writer or an error.

<a name="ParquetWriter.Close"></a>
### func \(\*ParquetWriter\) [Close](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L292>)

```go
func (p *ParquetWriter) Close() error
```

Close implements the goetl.DataSink interface. Flushes and closes all resources.

<a name="ParquetWriter.Flush"></a>
### func \(\*ParquetWriter\) [Flush](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L283>)

```go
func (p *ParquetWriter) Flush() error
```

Flush implements the goetl.DataSink interface. Forces any buffered records to be written to the Parquet file.

<a name="ParquetWriter.Stats"></a>
### func \(\*ParquetWriter\) [Stats](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L219>)

```go
func (p *ParquetWriter) Stats() WriterStats
```

Stats returns the current statistics of the Parquet writer.

<a name="ParquetWriter.Write"></a>
### func \(\*ParquetWriter\) [Write](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L225>)

```go
func (p *ParquetWriter) Write(ctx context.Context, record goetl.Record) error
```

Write implements the goetl.DataSink interface. Buffers records and writes in batches. Thread\-safe.

<a name="ParquetWriterError"></a>
## type [ParquetWriterError](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L50-L53>)

ParquetWriterError wraps Parquet\-specific write errors with context about the operation.

```go
type ParquetWriterError struct {
    Op  string // Operation that failed (e.g., "read", "load_batch", "open_file", "schema")
    Err error  // Underlying error
}
```

<a name="ParquetWriterError.Error"></a>
### func \(\*ParquetWriterError\) [Error](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L56>)

```go
func (e *ParquetWriterError) Error() string
```

Error returns the error string for ParquetWriterError.

<a name="ParquetWriterError.Unwrap"></a>
### func \(\*ParquetWriterError\) [Unwrap](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L61>)

```go
func (e *ParquetWriterError) Unwrap() error
```

Unwrap returns the underlying error for ParquetWriterError.

<a name="ParquetWriterOptions"></a>
## type [ParquetWriterOptions](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L87-L98>)

ParquetWriterOptions configures the Parquet writer.

```go
type ParquetWriterOptions struct {
    BatchSize       int64                // Number of records to buffer before writing
    Schema          *arrow.Schema        // Pre-defined schema (optional)
    Compression     compress.Compression // Compression algorithm
    FieldOrder      []string             // Explicit field ordering
    RowGroupSize    int64                // New: Control row group size
    PageSize        int64                // New: Control page size
    DictionaryLevel map[string]bool      // New: Per-field dictionary encoding
    Metadata        map[string]string    // New: File metadata
    ValidateSchema  bool                 // New: Enable strict schema validation

}
```

<a name="PostgresWriter"></a>
## type [PostgresWriter](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L194-L204>)

PostgresWriter implements goetl.DataSink for PostgreSQL output. It supports batching, transactions, conflict resolution, and statistics.

```go
type PostgresWriter struct {
    // contains filtered or unexported fields
}
```

<a name="NewPostgresWriter"></a>
### func [NewPostgresWriter](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L208>)

```go
func NewPostgresWriter(opts ...PostgresWriterOption) (*PostgresWriter, error)
```

NewPostgresWriter creates a new PostgreSQL writer with the given options. Accepts functional options for configuration. Returns a ready\-to\-use writer or an error.

<a name="PostgresWriter.Close"></a>
### func \(\*PostgresWriter\) [Close](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L299>)

```go
func (w *PostgresWriter) Close() error
```

Close implements the goetl.DataSink interface. Flushes and closes all resources.

<a name="PostgresWriter.Flush"></a>
### func \(\*PostgresWriter\) [Flush](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L287>)

```go
func (w *PostgresWriter) Flush() error
```

Flush implements the goetl.DataSink interface. Forces any buffered records to be written to PostgreSQL.

<a name="PostgresWriter.Stats"></a>
### func \(\*PostgresWriter\) [Stats](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L235>)

```go
func (w *PostgresWriter) Stats() PostgresWriterStats
```

Stats returns a copy of the current write statistics.

<a name="PostgresWriter.Write"></a>
### func \(\*PostgresWriter\) [Write](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L250>)

```go
func (w *PostgresWriter) Write(ctx context.Context, record goetl.Record) error
```

Write implements the goetl.DataSink interface. Buffers records and writes in batches. Thread\-safe.

<a name="PostgresWriterError"></a>
## type [PostgresWriterError](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L43-L46>)

PostgresWriterError wraps PostgreSQL\-specific write errors with context about the operation.

```go
type PostgresWriterError struct {
    Op  string // The operation being performed (e.g., "write", "connect")
    Err error  // The underlying error
}
```

<a name="PostgresWriterError.Error"></a>
### func \(\*PostgresWriterError\) [Error](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L49>)

```go
func (e *PostgresWriterError) Error() string
```

Error returns the error string for PostgresWriterError.

<a name="PostgresWriterError.Unwrap"></a>
### func \(\*PostgresWriterError\) [Unwrap](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L54>)

```go
func (e *PostgresWriterError) Unwrap() error
```

Unwrap returns the underlying error for PostgresWriterError.

<a name="PostgresWriterOption"></a>
## type [PostgresWriterOption](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L103>)

PostgresWriterOption represents a configuration function for PostgresWriterOptions.

```go
type PostgresWriterOption func(*PostgresWriterOptions)
```

<a name="WithColumns"></a>
### func [WithColumns](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L120>)

```go
func WithColumns(columns []string) PostgresWriterOption
```

WithColumns sets the columns to write.

<a name="WithConflictResolution"></a>
### func [WithConflictResolution](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L148>)

```go
func WithConflictResolution(resolution ConflictResolution, conflictCols, updateCols []string) PostgresWriterOption
```

WithConflictResolution sets the conflict resolution strategy and columns.

<a name="WithCreateTable"></a>
### func [WithCreateTable](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L134>)

```go
func WithCreateTable(create bool) PostgresWriterOption
```

WithCreateTable enables or disables table creation.

<a name="WithPostgresBatchSize"></a>
### func [WithPostgresBatchSize](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L127>)

```go
func WithPostgresBatchSize(size int) PostgresWriterOption
```

WithPostgresBatchSize sets the batch size for writes.

<a name="WithPostgresConnectionPool"></a>
### func [WithPostgresConnectionPool](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L164>)

```go
func WithPostgresConnectionPool(maxOpen, maxIdle int, maxLifetime, maxIdleTime time.Duration) PostgresWriterOption
```

WithPostgresConnectionPool configures the connection pool.

<a name="WithPostgresDSN"></a>
### func [WithPostgresDSN](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L106>)

```go
func WithPostgresDSN(dsn string) PostgresWriterOption
```

WithPostgresDSN sets the PostgreSQL connection string.

<a name="WithPostgresMetadata"></a>
### func [WithPostgresMetadata](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L181>)

```go
func WithPostgresMetadata(metadata map[string]string) PostgresWriterOption
```

WithPostgresMetadata sets user metadata for the writer.

<a name="WithPostgresQueryTimeout"></a>
### func [WithPostgresQueryTimeout](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L174>)

```go
func WithPostgresQueryTimeout(timeout time.Duration) PostgresWriterOption
```

WithPostgresQueryTimeout sets the query timeout.

<a name="WithTableName"></a>
### func [WithTableName](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L113>)

```go
func WithTableName(tableName string) PostgresWriterOption
```

WithTableName sets the target table name.

<a name="WithTransactionMode"></a>
### func [WithTransactionMode](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L157>)

```go
func WithTransactionMode(enabled bool) PostgresWriterOption
```

WithTransactionMode enables or disables transaction wrapping for batches.

<a name="WithTruncateTable"></a>
### func [WithTruncateTable](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L141>)

```go
func WithTruncateTable(truncate bool) PostgresWriterOption
```

WithTruncateTable enables or disables table truncation before writing.

<a name="PostgresWriterOptions"></a>
## type [PostgresWriterOptions](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L83-L100>)

PostgresWriterOptions configures the PostgreSQL writer.

```go
type PostgresWriterOptions struct {
    DSN                string             // PostgreSQL connection string
    TableName          string             // Target table name
    Columns            []string           // Columns to write (order matters)
    BatchSize          int                // Number of records per batch
    CreateTable        bool               // Create table if not exists
    TruncateTable      bool               // Truncate table before writing
    ConflictResolution ConflictResolution // Conflict handling strategy
    ConflictColumns    []string           // Columns that define uniqueness for conflict resolution
    UpdateColumns      []string           // Columns to update on conflict (for ConflictUpdate)
    TransactionMode    bool               // Wrap batches in transactions
    ConnMaxLifetime    time.Duration      // Max connection lifetime
    ConnMaxIdleTime    time.Duration      // Max idle connection time
    MaxOpenConns       int                // Max open connections
    MaxIdleConns       int                // Max idle connections
    QueryTimeout       time.Duration      // Timeout for queries
    Metadata           map[string]string  // Arbitrary metadata for user tracking
}
```

<a name="PostgresWriterStats"></a>
## type [PostgresWriterStats](<https://github.com/aaronlmathis/goetl/blob/main/writers/postgresql.go#L59-L68>)

PostgresWriterStats holds PostgreSQL write performance statistics.

```go
type PostgresWriterStats struct {
    RecordsWritten   int64            // Total records written
    BatchesWritten   int64            // Number of batches written
    TransactionCount int64            // Number of transactions committed
    LastWriteTime    time.Time        // Time of last write
    WriteDuration    time.Duration    // Total time spent writing
    ConnectionTime   time.Duration    // Time spent establishing connection
    NullValueCounts  map[string]int64 // Count of null values per column
    ConflictCount    int64            // Number of conflicts encountered
}
```

<a name="WriterOption"></a>
## type [WriterOption](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L112>)

WriterOption represents a configuration function for ParquetWriterOptions.

```go
type WriterOption func(*ParquetWriterOptions)
```

<a name="WithBatchSize"></a>
### func [WithBatchSize](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L115>)

```go
func WithBatchSize(size int64) WriterOption
```

WithBatchSize sets the number of records to buffer before writing a batch.

<a name="WithCompression"></a>
### func [WithCompression](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L122>)

```go
func WithCompression(compression compress.Compression) WriterOption
```

WithCompression sets the Parquet compression algorithm.

<a name="WithFieldOrder"></a>
### func [WithFieldOrder](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L129>)

```go
func WithFieldOrder(fields []string) WriterOption
```

WithFieldOrder sets the explicit field ordering for the Parquet schema.

<a name="WithMetadata"></a>
### func [WithMetadata](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L152>)

```go
func WithMetadata(metadata map[string]string) WriterOption
```

WithMetadata sets user metadata for the Parquet file.

<a name="WithRowGroupSize"></a>
### func [WithRowGroupSize](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L145>)

```go
func WithRowGroupSize(size int64) WriterOption
```

WithRowGroupSize sets the row group size for the Parquet file.

<a name="WithSchemaValidation"></a>
### func [WithSchemaValidation](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L138>)

```go
func WithSchemaValidation(validate bool) WriterOption
```

WithSchemaValidation enables or disables strict schema validation.

<a name="WriterOptionCSV"></a>
## type [WriterOptionCSV](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L73>)

WriterOptionCSV is a functional option for CSVWriter configuration.

```go
type WriterOptionCSV func(*CSVWriterOptions)
```

<a name="WithCSVBatchSize"></a>
### func [WithCSVBatchSize](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L97>)

```go
func WithCSVBatchSize(size int) WriterOptionCSV
```

WithCSVBatchSize sets the batch size for the CSVWriter.

<a name="WithCSVDelimiter"></a>
### func [WithCSVDelimiter](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L116>)

```go
func WithCSVDelimiter(delim rune) WriterOptionCSV
```

Deprecated: Use WithComma instead.

<a name="WithCSVHeaders"></a>
### func [WithCSVHeaders](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L111>)

```go
func WithCSVHeaders(headers []string) WriterOptionCSV
```

Deprecated: Use WithHeaders instead.

<a name="WithCSVWriteHeader"></a>
### func [WithCSVWriteHeader](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L121>)

```go
func WithCSVWriteHeader(write bool) WriterOptionCSV
```

Deprecated: Use WithWriteHeader instead.

<a name="WithComma"></a>
### func [WithComma](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L83>)

```go
func WithComma(delim rune) WriterOptionCSV
```

WithComma sets the field delimiter for the CSV output.

<a name="WithHeaders"></a>
### func [WithHeaders](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L76>)

```go
func WithHeaders(headers []string) WriterOptionCSV
```

WithHeaders sets the header row for the CSV output.

<a name="WithUseCRLF"></a>
### func [WithUseCRLF](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L104>)

```go
func WithUseCRLF(useCRLF bool) WriterOptionCSV
```

WithUseCRLF enables or disables CRLF line endings.

<a name="WithWriteHeader"></a>
### func [WithWriteHeader](<https://github.com/aaronlmathis/goetl/blob/main/writers/csv.go#L90>)

```go
func WithWriteHeader(write bool) WriterOptionCSV
```

WithWriteHeader enables or disables writing the header row.

<a name="WriterOptionJSON"></a>
## type [WriterOptionJSON](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L83>)

WriterOptionJSON is a functional option for JSONWriter configuration.

```go
type WriterOptionJSON func(*JSONWriterOptions)
```

<a name="WithFlushOnWrite"></a>
### func [WithFlushOnWrite](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L93>)

```go
func WithFlushOnWrite(enabled bool) WriterOptionJSON
```

WithFlushOnWrite enables or disables flushing after every write.

<a name="WithJSONBatchSize"></a>
### func [WithJSONBatchSize](<https://github.com/aaronlmathis/goetl/blob/main/writers/json.go#L86>)

```go
func WithJSONBatchSize(size int) WriterOptionJSON
```

WithJSONBatchSize sets the batch size for the JSONWriter.

<a name="WriterStats"></a>
## type [WriterStats](<https://github.com/aaronlmathis/goetl/blob/main/writers/parquet.go#L101-L109>)

WriterStats holds statistics about the Parquet writer's performance.

```go
type WriterStats struct {
    RecordsWritten  int64
    BatchesWritten  int64
    BytesWritten    int64
    FlushDuration   time.Duration
    LastFlushTime   time.Time
    ErrorCount      int64
    NullValueCounts map[string]int64
}
```

Generated by [gomarkdoc](<https://github.com/princjef/gomarkdoc>)
