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

package readers

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aaronlmathis/goetl/core"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Package readers provides implementations of core.DataSource for reading data from various sources.
//
// This file implements a high-performance, configurable MongoDB reader for streaming ETL pipelines.
// It supports authentication, connection pooling, aggregation pipelines, change streams, and pagination.

// MongoReaderError provides structured error information for MongoDB reader operations
type MongoReaderError struct {
	Op         string // Operation that failed (e.g., "connect", "query", "decode", "aggregate")
	Collection string // Collection being accessed when error occurred
	Err        error  // Underlying error
}

func (e *MongoReaderError) Error() string {
	if e.Collection != "" {
		return fmt.Sprintf("mongo reader %s [%s]: %v", e.Op, e.Collection, e.Err)
	}
	return fmt.Sprintf("mongo reader %s: %v", e.Op, e.Err)
}

func (e *MongoReaderError) Unwrap() error {
	return e.Err
}

// MongoReaderStats holds statistics about the MongoDB reader's performance
type MongoReaderStats struct {
	RecordsRead     int64            // Total records read
	QueriesExecuted int64            // Total queries executed
	ReadDuration    time.Duration    // Total time spent reading
	LastReadTime    time.Time        // Time of last read
	BatchesRead     int64            // Number of batches processed
	BytesRead       int64            // Estimated bytes read
	NullValueCounts map[string]int64 // Count of null values per field
	ErrorCount      int64            // Number of errors encountered
}

// MongoReadMode defines how data should be read from MongoDB
type MongoReadMode string

const (
	ModeFind      MongoReadMode = "find"      // Standard find query
	ModeAggregate MongoReadMode = "aggregate" // Aggregation pipeline
	ModeWatch     MongoReadMode = "watch"     // Change stream
	ModeBulk      MongoReadMode = "bulk"      // Bulk read with pagination
)

// MongoReaderOptions configures the MongoDB reader
type MongoReaderOptions struct {
	URI              string                   // MongoDB connection URI
	Database         string                   // Database name
	Collection       string                   // Collection name
	Mode             MongoReadMode            // Read mode
	Filter           bson.M                   // Query filter for find operations
	Projection       bson.M                   // Field projection
	Sort             bson.M                   // Sort specification
	Pipeline         []bson.M                 // Aggregation pipeline stages
	BatchSize        int32                    // Batch size for cursor
	Limit            int64                    // Maximum number of documents to read
	Skip             int64                    // Number of documents to skip
	Timeout          time.Duration            // Operation timeout
	MaxPoolSize      uint64                   // Connection pool size
	MinPoolSize      uint64                   // Minimum connections in pool
	MaxConnIdleTime  time.Duration            // Max idle time for connections
	ReadPreference   string                   // Read preference: primary, secondary, etc.
	ReadConcern      string                   // Read concern level
	AuthDatabase     string                   // Authentication database
	Username         string                   // Authentication username
	Password         string                   // Authentication password
	TLS              bool                     // Enable TLS
	TLSInsecure      bool                     // Skip TLS verification
	ReplicaSetName   string                   // Replica set name
	RetryReads       bool                     // Enable read retries
	RetryWrites      bool                     // Enable write retries
	Compressors      []string                 // Compression algorithms
	ZlibLevel        int                      // Zlib compression level
	CustomClientOpts []*options.ClientOptions // Custom client options
	AllowDiskUse     bool                     // Allow aggregation to use disk
	Hint             interface{}              // Index hint for queries
	MaxTimeMS        int64                    // Maximum execution time
	Comment          string                   // Query comment for profiling
	Collation        *options.Collation       // Collation options
}

// ReaderOptionMongo is a functional option for MongoReaderOptions
type ReaderOptionMongo func(*MongoReaderOptions)

// Connection options
func WithMongoURI(uri string) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.URI = uri
	}
}

func WithMongoDB(database string) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Database = database
	}
}

func WithMongoCollection(collection string) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Collection = collection
	}
}

// Query options
func WithMongoFilter(filter bson.M) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Filter = filter
	}
}

func WithMongoProjection(projection bson.M) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Projection = projection
	}
}

func WithMongoSort(sort bson.M) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Sort = sort
	}
}

func WithMongoPipeline(pipeline []bson.M) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Pipeline = pipeline
		opts.Mode = ModeAggregate
	}
}

func WithMongoLimit(limit int64) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Limit = limit
	}
}

func WithMongoSkip(skip int64) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Skip = skip
	}
}

func WithMongoBatchSize(batchSize int32) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.BatchSize = batchSize
	}
}

// Performance options
func WithMongoTimeout(timeout time.Duration) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Timeout = timeout
	}
}

func WithMongoPoolSize(min, max uint64) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.MinPoolSize = min
		opts.MaxPoolSize = max
	}
}

func WithMongoReadPreference(preference string) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.ReadPreference = preference
	}
}

func WithMongoReadConcern(concern string) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.ReadConcern = concern
	}
}

// Authentication options
func WithMongoAuth(username, password, authDB string) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Username = username
		opts.Password = password
		opts.AuthDatabase = authDB
	}
}

// TLS options
func WithMongoTLS(enabled, insecure bool) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.TLS = enabled
		opts.TLSInsecure = insecure
	}
}

// Advanced options
func WithMongoHint(hint interface{}) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Hint = hint
	}
}

func WithMongoComment(comment string) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Comment = comment
	}
}

func WithMongoAllowDiskUse(allow bool) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.AllowDiskUse = allow
	}
}

func WithMongoMaxTime(maxTimeMS int64) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.MaxTimeMS = maxTimeMS
	}
}

func WithMongoCollation(collation *options.Collation) ReaderOptionMongo {
	return func(opts *MongoReaderOptions) {
		opts.Collation = collation
	}
}

// MongoReader implements core.DataSource for MongoDB collections
type MongoReader struct {
	client       *mongo.Client
	collection   *mongo.Collection
	cursor       *mongo.Cursor
	changeStream *mongo.ChangeStream // Separate field for change streams
	opts         *MongoReaderOptions
	stats        MongoReaderStats
	ctx          context.Context
	cancel       context.CancelFunc
	connected    bool
}

// NewMongoReader creates a new MongoDB reader with configurable options
func NewMongoReader(options ...ReaderOptionMongo) (*MongoReader, error) {
	opts := &MongoReaderOptions{
		URI:             "mongodb://localhost:27017",
		Mode:            ModeFind,
		BatchSize:       1000,
		Timeout:         30 * time.Second,
		MaxPoolSize:     100,
		MinPoolSize:     5,
		MaxConnIdleTime: 10 * time.Minute,
		ReadPreference:  "primary",
		ReadConcern:     "local",
		RetryReads:      true,
		RetryWrites:     true,
		Compressors:     []string{"zstd", "zlib", "snappy"},
		ZlibLevel:       6,
	}

	// Apply functional options
	for _, option := range options {
		option(opts)
	}

	// Validate required options
	if opts.Database == "" {
		return nil, &MongoReaderError{Op: "validate", Err: fmt.Errorf("database name is required")}
	}
	if opts.Collection == "" {
		return nil, &MongoReaderError{Op: "validate", Err: fmt.Errorf("collection name is required")}
	}

	reader := &MongoReader{
		opts:  opts,
		stats: MongoReaderStats{NullValueCounts: make(map[string]int64)},
	}

	// Create context with timeout
	reader.ctx, reader.cancel = context.WithCancel(context.Background())

	return reader, nil
}

// Connect establishes connection to MongoDB
func (mr *MongoReader) Connect(ctx context.Context) error {
	if mr.connected {
		return nil
	}

	start := time.Now()
	defer func() {
		mr.stats.ReadDuration += time.Since(start)
	}()

	// Build client options
	clientOpts, err := mr.buildClientOptions()
	if err != nil {
		return &MongoReaderError{Op: "build_options", Err: err}
	}

	// Create client
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return &MongoReaderError{Op: "connect", Err: err}
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return &MongoReaderError{Op: "ping", Err: err}
	}

	mr.client = client
	mr.collection = client.Database(mr.opts.Database).Collection(mr.opts.Collection)
	mr.connected = true

	return nil
}

// buildClientOptions constructs MongoDB client options from reader configuration
func (mr *MongoReader) buildClientOptions() (*options.ClientOptions, error) {
	clientOpts := options.Client().ApplyURI(mr.opts.URI)

	// Connection pool settings
	if mr.opts.MaxPoolSize > 0 {
		clientOpts.SetMaxPoolSize(mr.opts.MaxPoolSize)
	}
	if mr.opts.MinPoolSize > 0 {
		clientOpts.SetMinPoolSize(mr.opts.MinPoolSize)
	}
	if mr.opts.MaxConnIdleTime > 0 {
		clientOpts.SetMaxConnIdleTime(mr.opts.MaxConnIdleTime)
	}

	// Timeout settings
	if mr.opts.Timeout > 0 {
		clientOpts.SetConnectTimeout(mr.opts.Timeout)
	}

	// Authentication
	if mr.opts.Username != "" && mr.opts.Password != "" {
		auth := options.Credential{
			Username:   mr.opts.Username,
			Password:   mr.opts.Password,
			AuthSource: mr.opts.AuthDatabase,
		}
		if auth.AuthSource == "" {
			auth.AuthSource = mr.opts.Database
		}
		clientOpts.SetAuth(auth)
	}

	// TLS configuration
	if mr.opts.TLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: mr.opts.TLSInsecure,
		}
		clientOpts.SetTLSConfig(tlsConfig)
	}

	// Read preference
	if mr.opts.ReadPreference != "" {
		var readPref *readpref.ReadPref

		switch mr.opts.ReadPreference {
		case "primary":
			readPref = readpref.Primary()
		case "primaryPreferred":
			readPref = readpref.PrimaryPreferred()
		case "secondary":
			readPref = readpref.Secondary()
		case "secondaryPreferred":
			readPref = readpref.SecondaryPreferred()
		case "nearest":
			readPref = readpref.Nearest()
		default:
			return nil, fmt.Errorf("invalid read preference: %s", mr.opts.ReadPreference)
		}

		clientOpts.SetReadPreference(readPref)
	}

	// Read concern
	if mr.opts.ReadConcern != "" {
		var rc *readconcern.ReadConcern

		switch mr.opts.ReadConcern {
		case "local":
			rc = readconcern.Local()
		case "available":
			rc = readconcern.Available()
		case "majority":
			rc = readconcern.Majority()
		case "linearizable":
			rc = readconcern.Linearizable()
		case "snapshot":
			rc = readconcern.Snapshot()
		default:
			return nil, fmt.Errorf("invalid read concern: %s", mr.opts.ReadConcern)
		}

		clientOpts.SetReadConcern(rc)
	}

	// Retry configuration
	if mr.opts.RetryReads {
		clientOpts.SetRetryReads(mr.opts.RetryReads)
	}
	if mr.opts.RetryWrites {
		clientOpts.SetRetryWrites(mr.opts.RetryWrites)
	}

	// Compression
	if len(mr.opts.Compressors) > 0 {
		clientOpts.SetCompressors(mr.opts.Compressors)
	}
	if mr.opts.ZlibLevel > 0 {
		clientOpts.SetZlibLevel(mr.opts.ZlibLevel)
	}

	// Replica set
	if mr.opts.ReplicaSetName != "" {
		clientOpts.SetReplicaSet(mr.opts.ReplicaSetName)
	}

	// Apply custom client options - Fixed: Simple merging approach
	for _, customOpt := range mr.opts.CustomClientOpts {
		if customOpt != nil {
			// Apply URI from custom options if set
			if customOpt.Hosts != nil {
				clientOpts.SetHosts(customOpt.Hosts)
			}
		}
	}

	return clientOpts, nil
}

// Read implements the core.DataSource interface
func (mr *MongoReader) Read(ctx context.Context) (core.Record, error) {
	start := time.Now()
	defer func() {
		mr.stats.ReadDuration += time.Since(start)
		mr.stats.LastReadTime = time.Now()
	}()

	// Ensure connection
	if !mr.connected {
		if err := mr.Connect(ctx); err != nil {
			return nil, err
		}
	}

	// Initialize cursor if needed
	if mr.cursor == nil && mr.changeStream == nil {
		if err := mr.initializeCursor(ctx); err != nil {
			return nil, &MongoReaderError{Op: "init_cursor", Collection: mr.opts.Collection, Err: err}
		}
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, &MongoReaderError{Op: "read", Collection: mr.opts.Collection, Err: ctx.Err()}
	default:
	}

	var doc bson.M

	// Handle different cursor types
	if mr.opts.Mode == ModeWatch && mr.changeStream != nil {
		// For change streams
		if !mr.changeStream.Next(ctx) {
			if err := mr.changeStream.Err(); err != nil {
				mr.stats.ErrorCount++
				return nil, &MongoReaderError{Op: "changestream_next", Collection: mr.opts.Collection, Err: err}
			}
			return nil, io.EOF
		}

		if err := mr.changeStream.Decode(&doc); err != nil {
			mr.stats.ErrorCount++
			return nil, &MongoReaderError{Op: "changestream_decode", Collection: mr.opts.Collection, Err: err}
		}
	} else {
		// For regular cursors
		if !mr.cursor.Next(ctx) {
			if err := mr.cursor.Err(); err != nil {
				mr.stats.ErrorCount++
				return nil, &MongoReaderError{Op: "cursor_next", Collection: mr.opts.Collection, Err: err}
			}
			return nil, io.EOF
		}

		if err := mr.cursor.Decode(&doc); err != nil {
			mr.stats.ErrorCount++
			return nil, &MongoReaderError{Op: "decode", Collection: mr.opts.Collection, Err: err}
		}
	}

	// Convert to core.Record
	record := mr.convertBSONToRecord(doc)

	// Update statistics
	mr.stats.RecordsRead++
	mr.stats.BytesRead += mr.estimateDocumentSize(doc)

	// Track null values
	for key, val := range record {
		if val == nil {
			mr.stats.NullValueCounts[key]++
		}
	}

	return record, nil
}

// Close implements the core.DataSource interface
func (mr *MongoReader) Close() error {
	var errs []string

	// Close cursor
	if mr.cursor != nil {
		if err := mr.cursor.Close(mr.ctx); err != nil {
			errs = append(errs, fmt.Sprintf("cursor close: %v", err))
		}
		mr.cursor = nil
	}

	// Close change stream
	if mr.changeStream != nil {
		if err := mr.changeStream.Close(mr.ctx); err != nil {
			errs = append(errs, fmt.Sprintf("changestream close: %v", err))
		}
		mr.changeStream = nil
	}

	// Disconnect client
	if mr.client != nil {
		if err := mr.client.Disconnect(mr.ctx); err != nil {
			errs = append(errs, fmt.Sprintf("client disconnect: %v", err))
		}
		mr.client = nil
	}

	// Cancel context
	if mr.cancel != nil {
		mr.cancel()
	}

	mr.connected = false

	if len(errs) > 0 {
		return &MongoReaderError{Op: "close", Err: fmt.Errorf("multiple errors: %s", strings.Join(errs, "; "))}
	}

	return nil
}

// Stats returns MongoDB reader performance statistics
func (mr *MongoReader) Stats() MongoReaderStats {
	return mr.stats
}

// initializeCursor creates and configures the MongoDB cursor based on read mode
func (mr *MongoReader) initializeCursor(ctx context.Context) error {
	mr.stats.QueriesExecuted++

	switch mr.opts.Mode {
	case ModeFind:
		return mr.initializeFindCursor(ctx)
	case ModeAggregate:
		return mr.initializeAggregateCursor(ctx)
	case ModeWatch:
		return mr.initializeWatchCursor(ctx)
	case ModeBulk:
		return mr.initializeBulkCursor(ctx)
	default:
		return fmt.Errorf("unsupported read mode: %s", mr.opts.Mode)
	}
}

// initializeFindCursor creates a find cursor
func (mr *MongoReader) initializeFindCursor(ctx context.Context) error {
	findOpts := options.Find()

	// Configure find options
	if mr.opts.BatchSize > 0 {
		findOpts.SetBatchSize(mr.opts.BatchSize)
	}
	if mr.opts.Limit > 0 {
		findOpts.SetLimit(mr.opts.Limit)
	}
	if mr.opts.Skip > 0 {
		findOpts.SetSkip(mr.opts.Skip)
	}
	if mr.opts.Projection != nil {
		findOpts.SetProjection(mr.opts.Projection)
	}
	if mr.opts.Sort != nil {
		findOpts.SetSort(mr.opts.Sort)
	}
	if mr.opts.Hint != nil {
		findOpts.SetHint(mr.opts.Hint)
	}
	if mr.opts.MaxTimeMS > 0 {
		findOpts.SetMaxTime(time.Duration(mr.opts.MaxTimeMS) * time.Millisecond)
	}
	if mr.opts.Comment != "" {
		findOpts.SetComment(mr.opts.Comment)
	}
	if mr.opts.Collation != nil {
		findOpts.SetCollation(mr.opts.Collation)
	}

	// Execute find
	filter := mr.opts.Filter
	if filter == nil {
		filter = bson.M{}
	}

	cursor, err := mr.collection.Find(ctx, filter, findOpts)
	if err != nil {
		return err
	}

	mr.cursor = cursor
	return nil
}

// initializeAggregateCursor creates an aggregation cursor
func (mr *MongoReader) initializeAggregateCursor(ctx context.Context) error {
	if len(mr.opts.Pipeline) == 0 {
		return fmt.Errorf("pipeline is required for aggregate mode")
	}

	aggOpts := options.Aggregate()

	// Configure aggregation options
	if mr.opts.BatchSize > 0 {
		aggOpts.SetBatchSize(mr.opts.BatchSize)
	}
	if mr.opts.AllowDiskUse {
		aggOpts.SetAllowDiskUse(mr.opts.AllowDiskUse)
	}
	if mr.opts.MaxTimeMS > 0 {
		aggOpts.SetMaxTime(time.Duration(mr.opts.MaxTimeMS) * time.Millisecond)
	}
	if mr.opts.Comment != "" {
		aggOpts.SetComment(mr.opts.Comment)
	}
	if mr.opts.Hint != nil {
		aggOpts.SetHint(mr.opts.Hint)
	}
	if mr.opts.Collation != nil {
		aggOpts.SetCollation(mr.opts.Collation)
	}

	// Execute aggregation
	cursor, err := mr.collection.Aggregate(ctx, mr.opts.Pipeline, aggOpts)
	if err != nil {
		return err
	}

	mr.cursor = cursor
	return nil
}

// initializeWatchCursor creates a change stream cursor
func (mr *MongoReader) initializeWatchCursor(ctx context.Context) error {
	opts := options.ChangeStream()

	if mr.opts.BatchSize > 0 {
		opts.SetBatchSize(mr.opts.BatchSize)
	}

	if mr.opts.MaxTimeMS > 0 {
		maxTime := time.Duration(mr.opts.MaxTimeMS) * time.Millisecond
		opts.SetMaxAwaitTime(maxTime)
	}

	var pipeline interface{}
	if len(mr.opts.Pipeline) > 0 {
		pipeline = mr.opts.Pipeline
	} else {
		pipeline = mongo.Pipeline{} // Watch all changes
	}

	// Execute watch
	changeStream, err := mr.collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("failed to create change stream: %w", err)
	}

	mr.changeStream = changeStream
	return nil
}

// initializeBulkCursor creates a paginated bulk read cursor
func (mr *MongoReader) initializeBulkCursor(ctx context.Context) error {
	// For bulk mode, use find with automatic pagination
	return mr.initializeFindCursor(ctx)
}

// convertBSONToRecord converts BSON document to core.Record
func (mr *MongoReader) convertBSONToRecord(doc bson.M) core.Record {
	record := make(core.Record, len(doc))

	for key, value := range doc {
		record[key] = mr.convertBSONValue(value)
	}

	return record
}

// convertBSONValue converts BSON values to appropriate Go types
func (mr *MongoReader) convertBSONValue(value interface{}) interface{} {
	switch v := value.(type) {
	case primitive.ObjectID:
		return v.Hex()
	case primitive.DateTime:
		return v.Time()
	case primitive.Decimal128:
		// Proper Decimal128 conversion
		bigInt, _, err := v.BigInt()
		if err == nil {
			return bigInt.String()
		}
		// Fallback to string if BigInt conversion fails
		return v.String()
	case primitive.Binary:
		return v.Data
	case primitive.Regex:
		return v.Pattern
	case primitive.JavaScript:
		return string(v)
	case primitive.Symbol:
		return string(v)
	case primitive.CodeWithScope:
		return string(v.Code)
	case primitive.Timestamp:
		return time.Unix(int64(v.T), 0)
	case primitive.MinKey:
		return "MinKey"
	case primitive.MaxKey:
		return "MaxKey"
	case primitive.Undefined:
		return nil
	case primitive.Null:
		return nil
	case bson.M:
		// Recursively convert nested documents
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = mr.convertBSONValue(val)
		}
		return result
	case bson.A:
		// Convert arrays
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = mr.convertBSONValue(val)
		}
		return result
	default:
		return v
	}
}

// estimateDocumentSize provides a rough estimate of document size for statistics
func (mr *MongoReader) estimateDocumentSize(doc bson.M) int64 {
	// Simple estimation based on number of fields and average field size
	baseSize := int64(len(doc) * 20) // Rough estimate: 20 bytes per field on average

	for key, value := range doc {
		baseSize += int64(len(key))

		switch v := value.(type) {
		case string:
			baseSize += int64(len(v))
		case primitive.ObjectID:
			baseSize += 12
		case primitive.DateTime:
			baseSize += 8
		case int, int32:
			baseSize += 4
		case int64:
			baseSize += 8
		case float64:
			baseSize += 8
		case bool:
			baseSize += 1
		case bson.M:
			baseSize += int64(len(v) * 15) // Rough estimate for nested docs
		case bson.A:
			baseSize += int64(len(v) * 10) // Rough estimate for arrays
		default:
			baseSize += 8 // Default estimate
		}
	}

	return baseSize
}

// Convenience constructors for common MongoDB reader patterns

// NewMongoReaderFromURI creates a basic MongoDB reader from a URI
func NewMongoReaderFromURI(uri, database, collection string) (*MongoReader, error) {
	return NewMongoReader(
		WithMongoURI(uri),
		WithMongoDB(database),
		WithMongoCollection(collection),
	)
}

// NewMongoAggregationReader creates a MongoDB reader with aggregation pipeline
func NewMongoAggregationReader(uri, database, collection string, pipeline []bson.M) (*MongoReader, error) {
	return NewMongoReader(
		WithMongoURI(uri),
		WithMongoDB(database),
		WithMongoCollection(collection),
		WithMongoPipeline(pipeline),
	)
}

// NewMongoChangeStreamReader creates a MongoDB change stream reader
func NewMongoChangeStreamReader(uri, database, collection string) (*MongoReader, error) {
	return NewMongoReader(
		WithMongoURI(uri),
		WithMongoDB(database),
		WithMongoCollection(collection),
		func(opts *MongoReaderOptions) {
			opts.Mode = ModeWatch
		},
	)
}

// NewMongoFilteredReader creates a MongoDB reader with filter and projection
func NewMongoFilteredReader(uri, database, collection string, filter, projection bson.M) (*MongoReader, error) {
	return NewMongoReader(
		WithMongoURI(uri),
		WithMongoDB(database),
		WithMongoCollection(collection),
		WithMongoFilter(filter),
		WithMongoProjection(projection),
	)
}
