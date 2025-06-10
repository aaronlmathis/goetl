package readers

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"

	"github.com/aaronlmathis/goetl"
)

// ParquetReaderError provides structured error information for parquet reader operations
type ParquetReaderError struct {
	Op  string // Operation that failed (e.g., "read", "load_batch", "open_file", "schema")
	Err error  // Underlying error
}

func (e *ParquetReaderError) Error() string {
	return fmt.Sprintf("parquet reader %s: %v", e.Op, e.Err)
}

func (e *ParquetReaderError) Unwrap() error {
	return e.Err
}

// ParquetReader implements DataSource for Parquet files
// Supports optional column projection and safe resource management
type ParquetReader struct {
	fileHandle      *os.File
	reader          *file.Reader
	arrowReader     *pqarrow.FileReader
	recordReader    pqarrow.RecordReader
	currentBatch    arrow.Record
	currentBatchIdx int
	currentRow      int64
	totalRows       int64
	batchSize       int64
	schema          *arrow.Schema
	columnIndexMap  map[string]int // Cache column name to index mapping
	stats           ReaderStats
	opts            *ParquetReaderOptions
}

// ReaderStats holds statistics about the Parquet reader's performance
type ReaderStats struct {
	RecordsRead     int64
	BatchesRead     int64
	BytesRead       int64
	ReadDuration    time.Duration
	LastReadTime    time.Time
	NullValueCounts map[string]int64
}

// ParquetReaderOptions configures the Parquet reader
// BatchSize: rows per batch
// Columns: optional list of column names to project
type ParquetReaderOptions struct {
	BatchSize     int64
	Columns       []string
	UseArrowTypes bool              // Return native Arrow types vs Go primitives
	PreloadBatch  bool              // Preload next batch for better performance
	MemoryLimit   int64             // Memory usage limit for batches
	ParallelRead  bool              // Enable parallel column reading
	Metadata      map[string]string // Custom metadata filters
}

// ReaderOption represents a configuration function
type ReaderOption func(*ParquetReaderOptions)

// Functional option functions
func WithBatchSize(size int64) ReaderOption {
	return func(opts *ParquetReaderOptions) {
		opts.BatchSize = size
	}
}

func WithColumns(columns []string) ReaderOption {
	return func(opts *ParquetReaderOptions) {
		// Defensive copy to avoid shared slices
		opts.Columns = make([]string, len(columns))
		copy(opts.Columns, columns)
	}
}

func WithColumnProjection(columns ...string) ReaderOption {
	return func(opts *ParquetReaderOptions) {
		opts.Columns = make([]string, len(columns))
		copy(opts.Columns, columns)
	}
}
func WithUseArrowTypes(useArrow bool) ReaderOption {
	return func(opts *ParquetReaderOptions) {
		opts.UseArrowTypes = useArrow
	}
}

func WithPreloadBatch(preload bool) ReaderOption {
	return func(opts *ParquetReaderOptions) {
		opts.PreloadBatch = preload
	}
}

func WithMemoryLimit(limit int64) ReaderOption {
	return func(opts *ParquetReaderOptions) {
		opts.MemoryLimit = limit
	}
}

func WithParallelRead(parallel bool) ReaderOption {
	return func(opts *ParquetReaderOptions) {
		opts.ParallelRead = parallel
	}
}

func WithMetadata(metadata map[string]string) ReaderOption {
	return func(opts *ParquetReaderOptions) {
		if opts.Metadata == nil {
			opts.Metadata = make(map[string]string)
		}
		// Defensive copy
		for k, v := range metadata {
			opts.Metadata[k] = v
		}
	}
}

// NewParquetReader opens a Parquet file and prepares an Arrow RecordReader
func NewParquetReader(filename string, options ...ReaderOption) (*ParquetReader, error) {
	// Start with defaults
	opts := (&ParquetReaderOptions{}).withDefaults()

	// Apply functional options
	for _, option := range options {
		option(opts)
	}

	return createParquetReader(filename, opts)
}

// createParquetReader initializes the Parquet reader with the given options
// This function is separated to allow for better testing and error handling
// It handles file opening, Arrow reader creation, schema retrieval,
// and optional column projection
func createParquetReader(filename string, opts *ParquetReaderOptions) (*ParquetReader, error) {
	// Open underlying file
	f, err := os.Open(filename)
	if err != nil {
		return nil, &ParquetReaderError{Op: "open_file", Err: err}
	}

	// Create Parquet reader
	parquetReader, err := file.NewParquetReader(f)
	if err != nil {
		f.Close()
		return nil, &ParquetReaderError{Op: "create_reader", Err: err}
	}

	// Create Arrow FileReader with memory allocator
	allocator := memory.NewGoAllocator()
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, allocator)
	if err != nil {
		f.Close()
		return nil, &ParquetReaderError{Op: "create_arrow_reader", Err: err}
	}

	// Retrieve schema
	schema, err := arrowReader.Schema()
	if err != nil {
		f.Close()
		return nil, &ParquetReaderError{Op: "get_schema", Err: err}
	}

	// Prepare column index projection if requested
	var colIndices []int
	if len(opts.Columns) > 0 {
		for _, name := range opts.Columns {
			idx := -1
			for i, field := range schema.Fields() {
				if field.Name == name {
					idx = i
					break
				}
			}
			if idx < 0 {
				f.Close()
				return nil, &ParquetReaderError{Op: "column_projection", Err: fmt.Errorf("column %q not found in schema", name)}
			}
			colIndices = append(colIndices, idx)
		}
	}

	// Create RecordReader with optional projection
	recordReader, err := arrowReader.GetRecordReader(context.Background(), colIndices, nil)
	if err != nil {
		f.Close()
		return nil, &ParquetReaderError{Op: "create_record_reader", Err: err}
	}

	reader := &ParquetReader{
		fileHandle:      f,
		reader:          parquetReader,
		arrowReader:     arrowReader,
		recordReader:    recordReader,
		currentBatch:    nil,
		currentBatchIdx: 0,
		totalRows:       parquetReader.NumRows(),
		currentRow:      0,
		batchSize:       opts.BatchSize,
		schema:          schema,
		columnIndexMap:  make(map[string]int, len(schema.Fields())),
		stats:           ReaderStats{NullValueCounts: make(map[string]int64)},
		opts:            opts,
	}

	// Build column index map
	reader.buildColumnIndexMap()

	return reader, nil
}

// Read reads the next record from the Parquet file, returning goetl.Record or io.EOF
func (p *ParquetReader) Read(ctx context.Context) (goetl.Record, error) {
	startTime := time.Now()
	defer func() {
		p.stats.ReadDuration += time.Since(startTime)
		p.stats.LastReadTime = time.Now()
	}()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, &ParquetReaderError{Op: "read", Err: ctx.Err()}
	default:
	}

	// Load next batch if needed
	if p.currentBatch == nil || p.currentBatchIdx >= int(p.currentBatch.NumRows()) {
		if err := p.loadNextBatch(); err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, &ParquetReaderError{Op: "load_batch", Err: err}
		}
	}

	// Extract one row
	if p.currentBatch.NumRows() == 0 {
		return nil, io.EOF
	}

	result := p.extractRecordFromBatch(p.currentBatch, p.currentBatchIdx)
	p.currentBatchIdx++
	p.currentRow++
	p.stats.RecordsRead++

	return result, nil
}

// Close releases resources and closes the underlying file
func (p *ParquetReader) Close() error {
	if p.currentBatch != nil {
		p.currentBatch.Release()
		p.currentBatch = nil
	}
	if p.recordReader != nil {
		p.recordReader.Release()
		p.recordReader = nil
	}
	if p.fileHandle != nil {
		return p.fileHandle.Close()
	}
	return nil
}

// Schema returns the Arrow schema of the Parquet file
func (p *ParquetReader) Schema() *arrow.Schema {
	return p.schema
}

// Stats returns statistics about the Parquet reader's performance
// such as number of records read, batches processed, and null value counts
// This can be useful for monitoring and debugging
// the reading process
func (p *ParquetReader) Stats() ReaderStats {
	return p.stats
}

func (opts *ParquetReaderOptions) withDefaults() *ParquetReaderOptions {
	result := &ParquetReaderOptions{}

	// Copy existing values if opts is not nil
	if opts != nil {
		*result = *opts
	}

	// Apply defaults for zero values
	if result.BatchSize <= 0 {
		result.BatchSize = 1000
	}
	if result.MemoryLimit <= 0 {
		result.MemoryLimit = 64 * 1024 * 1024 // 64MB default
	}

	// Initialize maps if nil
	if result.Metadata == nil {
		result.Metadata = make(map[string]string)
	}

	return result
}

// Extract batch loading logic for better error handling and stats
func (p *ParquetReader) loadNextBatch() error {
	// Check memory limit before loading new batch
	if p.stats.BytesRead > 0 && p.stats.BytesRead >= p.opts.MemoryLimit {
		return &ParquetReaderError{
			Op:  "load_batch",
			Err: fmt.Errorf("memory limit exceeded: %d bytes >= %d limit", p.stats.BytesRead, p.opts.MemoryLimit),
		}
	}

	if p.currentBatch != nil {
		p.currentBatch.Release()
		p.currentBatch = nil
	}

	rec, err := p.recordReader.Read()
	if err != nil {
		return err
	}
	if rec == nil || rec.NumRows() == 0 {
		return io.EOF
	}

	p.currentBatch = rec
	p.currentBatchIdx = 0
	p.stats.BatchesRead++

	// More accurate byte tracking based on Arrow data types
	if rec.NumRows() > 0 {
		var estimatedBytes int64
		for i := 0; i < int(rec.NumCols()); i++ {
			col := rec.Column(i)
			switch col.DataType().ID() {
			case arrow.BOOL:
				estimatedBytes += rec.NumRows() // 1 byte per bool (simplified)
			case arrow.INT8, arrow.UINT8:
				estimatedBytes += rec.NumRows() * 1
			case arrow.INT16, arrow.UINT16:
				estimatedBytes += rec.NumRows() * 2
			case arrow.INT32, arrow.UINT32, arrow.FLOAT32:
				estimatedBytes += rec.NumRows() * 4
			case arrow.INT64, arrow.UINT64, arrow.FLOAT64, arrow.TIMESTAMP:
				estimatedBytes += rec.NumRows() * 8
			case arrow.STRING, arrow.BINARY:
				// For variable-length types, use a rough estimate
				estimatedBytes += rec.NumRows() * 32 // Average string/binary size
			default:
				// Fallback for other types
				estimatedBytes += rec.NumRows() * 8
			}
		}
		p.stats.BytesRead += estimatedBytes
	}
	return nil
}

// extractRecordFromBatch builds a goetl.Record from a row in an Arrow Record batch
func (p *ParquetReader) extractRecordFromBatch(record arrow.Record, pos int) goetl.Record {
	res := make(goetl.Record)
	sch := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := sch.Field(i)
		col := record.Column(i)
		res[field.Name] = p.extractValueFromColumn(col, pos, field.Name) // Add field name
	}
	return res
}

// Build column index map for efficient lookups
func (p *ParquetReader) buildColumnIndexMap() {
	p.columnIndexMap = make(map[string]int, len(p.schema.Fields()))
	for i, field := range p.schema.Fields() {
		p.columnIndexMap[field.Name] = i
	}
}

// Enhanced extractValueFromColumn with null counting
func (p *ParquetReader) extractValueFromColumn(col arrow.Array, rowIdx int, fieldName string) interface{} {
	if col.IsNull(rowIdx) {
		p.stats.NullValueCounts[fieldName]++
		return nil
	}

	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(rowIdx)
	case *array.Int8:
		return int8(arr.Value(rowIdx))
	case *array.Int16:
		return int16(arr.Value(rowIdx))
	case *array.Int32:
		return int32(arr.Value(rowIdx))
	case *array.Int64:
		return int64(arr.Value(rowIdx))
	case *array.Uint8:
		return uint8(arr.Value(rowIdx))
	case *array.Uint16:
		return uint16(arr.Value(rowIdx))
	case *array.Uint32:
		return uint32(arr.Value(rowIdx))
	case *array.Uint64:
		return uint64(arr.Value(rowIdx))
	case *array.Float32:
		return float32(arr.Value(rowIdx))
	case *array.Float64:
		return arr.Value(rowIdx)
	case *array.String:
		return arr.Value(rowIdx)
	case *array.Binary:
		return arr.Value(rowIdx) // Return []byte instead of string
	case *array.Timestamp:
		return arr.Value(rowIdx).ToTime(arrow.Microsecond)
	case *array.Date32:
		return arr.Value(rowIdx).ToTime()
	case *array.Date64:
		return arr.Value(rowIdx).ToTime()
	default:
		// Fallback to string representation
		return fmt.Sprintf("%v", col.GetOneForMarshal(rowIdx))
	}
}
