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
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aaronlmathis/goetl/core"
)

// S3ReaderError provides structured error information for S3 reader operations
type S3ReaderError struct {
	Op  string // Operation that failed (e.g., "list_objects", "get_object", "read")
	Err error  // Underlying error
}

func (e *S3ReaderError) Error() string {
	return fmt.Sprintf("s3 reader %s: %v", e.Op, e.Err)
}

func (e *S3ReaderError) Unwrap() error {
	return e.Err
}

// S3ReaderStats holds statistics about the S3 reader's performance
type S3ReaderStats struct {
	ObjectsListed  int64         // Total objects discovered
	ObjectsRead    int64         // Total objects successfully read
	RecordsRead    int64         // Total records read across all objects
	BytesRead      int64         // Total bytes read
	ReadDuration   time.Duration // Total time spent reading
	LastReadTime   time.Time     // Time of last read operation
	ObjectErrors   int64         // Number of objects that failed to read
	CurrentObject  string        // Currently processing object
	ProcessedFiles []string      // List of successfully processed files
}

// S3ReaderOptions configures the S3 reader behavior
type S3ReaderOptions struct {
	Bucket          string            // S3 bucket name
	Prefix          string            // Key prefix filter
	Suffix          string            // Key suffix filter (e.g., ".csv", ".json")
	MaxKeys         int32             // Maximum number of objects to list
	Region          string            // AWS region
	Profile         string            // AWS profile to use
	Credentials     aws.Credentials   // Explicit credentials
	EndpointURL     string            // Custom S3 endpoint (for S3-compatible services)
	ForcePathStyle  bool              // Use path-style addressing
	FilePattern     string            // Regex pattern for file matching
	Recursive       bool              // Process subdirectories recursively
	SortOrder       SortOrder         // Order to process files
	BatchSize       int               // Number of files to process in parallel
	BufferSize      int64             // Buffer size for reading objects
	Metadata        map[string]string // Custom metadata filters
	IncludeMetadata bool              // Include S3 object metadata in records
}

// SortOrder defines how files should be ordered for processing
type SortOrder string

const (
	SortByName         SortOrder = "name"          // Sort by object key
	SortByLastModified SortOrder = "last_modified" // Sort by modification time
	SortBySize         SortOrder = "size"          // Sort by object size
	SortNone           SortOrder = "none"          // No sorting (S3 order)
)

// ReaderOptionS3 represents a configuration function for S3Reader
type ReaderOptionS3 func(*S3ReaderOptions)

// Functional option functions
func WithS3Bucket(bucket string) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.Bucket = bucket
	}
}

func WithS3Prefix(prefix string) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.Prefix = prefix
	}
}

func WithS3Suffix(suffix string) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.Suffix = suffix
	}
}

func WithS3Region(region string) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.Region = region
	}
}

func WithS3Profile(profile string) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.Profile = profile
	}
}

func WithS3Credentials(creds aws.Credentials) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.Credentials = creds
	}
}

func WithS3Endpoint(endpoint string) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.EndpointURL = endpoint
	}
}

func WithS3PathStyle(pathStyle bool) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.ForcePathStyle = pathStyle
	}
}

func WithS3MaxKeys(maxKeys int32) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.MaxKeys = maxKeys
	}
}

func WithS3FilePattern(pattern string) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.FilePattern = pattern
	}
}

func WithS3Recursive(recursive bool) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.Recursive = recursive
	}
}

func WithS3SortOrder(order SortOrder) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.SortOrder = order
	}
}

func WithS3BatchSize(batchSize int) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.BatchSize = batchSize
	}
}

func WithS3BufferSize(bufferSize int64) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.BufferSize = bufferSize
	}
}

func WithS3IncludeMetadata(include bool) ReaderOptionS3 {
	return func(opts *S3ReaderOptions) {
		opts.IncludeMetadata = include
	}
}

// S3Object represents an S3 object with metadata
type S3Object struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	Metadata     map[string]string
}

// S3Reader implements core.DataSource for reading from Amazon S3
type S3Reader struct {
	client        *s3.Client
	objects       []S3Object
	currentIndex  int
	currentReader core.DataSource // Current file reader (CSV, JSON, etc.)
	stats         S3ReaderStats
	opts          S3ReaderOptions
	mu            sync.RWMutex
}

// NewS3Reader creates a new S3 reader with the specified options
func NewS3Reader(options ...ReaderOptionS3) (*S3Reader, error) {
	opts := S3ReaderOptions{
		MaxKeys:    1000,
		SortOrder:  SortByName,
		BatchSize:  1,
		BufferSize: 64 * 1024, // 64KB
		Recursive:  true,
		Metadata:   make(map[string]string),
	}

	// Apply functional options
	for _, option := range options {
		option(&opts)
	}

	// Validate required options
	if opts.Bucket == "" {
		return nil, &S3ReaderError{Op: "validate_options", Err: fmt.Errorf("bucket is required")}
	}

	// Create AWS config
	cfg, err := createAWSConfig(opts)
	if err != nil {
		return nil, &S3ReaderError{Op: "create_aws_config", Err: err}
	}

	// Create S3 client
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opts.EndpointURL != "" {
			o.BaseEndpoint = aws.String(opts.EndpointURL)
		}
		o.UsePathStyle = opts.ForcePathStyle
	})

	reader := &S3Reader{
		client: client,
		opts:   opts,
		stats:  S3ReaderStats{ProcessedFiles: make([]string, 0)},
	}

	// List objects from S3
	if err := reader.listObjects(context.Background()); err != nil {
		return nil, &S3ReaderError{Op: "list_objects", Err: err}
	}

	return reader, nil
}

// Read implements the core.DataSource interface
func (s *S3Reader) Read(ctx context.Context) (core.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	start := time.Now()
	defer func() {
		s.stats.ReadDuration += time.Since(start)
		s.stats.LastReadTime = time.Now()
	}()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, &S3ReaderError{Op: "read", Err: ctx.Err()}
	default:
	}

	// If no current reader, try to open the next file
	for s.currentReader == nil {
		if s.currentIndex >= len(s.objects) {
			return nil, io.EOF // All objects processed
		}

		if err := s.openNextObject(ctx); err != nil {
			s.stats.ObjectErrors++
			s.currentIndex++
			continue // Try next object
		}
	}

	// Read from current reader
	record, err := s.currentReader.Read(ctx)
	if err == io.EOF {
		// Current file is done, close it and try next
		s.closeCurrentReader()
		return s.Read(ctx) // Recursive call to get next record
	}
	if err != nil {
		return nil, &S3ReaderError{Op: "read_record", Err: err}
	}

	// Add S3 metadata if requested
	if s.opts.IncludeMetadata {
		currentObj := s.objects[s.currentIndex]
		record["_s3_key"] = currentObj.Key
		record["_s3_size"] = currentObj.Size
		record["_s3_last_modified"] = currentObj.LastModified
		record["_s3_etag"] = currentObj.ETag
		for k, v := range currentObj.Metadata {
			record["_s3_meta_"+k] = v
		}
	}

	s.stats.RecordsRead++
	return record, nil
}

// Close implements the core.DataSource interface
func (s *S3Reader) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeCurrentReader()
}

// Stats returns S3 reader performance statistics
func (s *S3Reader) Stats() S3ReaderStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

// Objects returns the list of S3 objects that will be/have been processed
func (s *S3Reader) Objects() []S3Object {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.objects
}

// createAWSConfig creates AWS configuration from options
func createAWSConfig(opts S3ReaderOptions) (aws.Config, error) {
	configOpts := []func(*config.LoadOptions) error{}

	if opts.Region != "" {
		configOpts = append(configOpts, config.WithRegion(opts.Region))
	}

	if opts.Profile != "" {
		configOpts = append(configOpts, config.WithSharedConfigProfile(opts.Profile))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), configOpts...)
	if err != nil {
		return aws.Config{}, err
	}

	// Override with explicit credentials if provided
	if opts.Credentials.AccessKeyID != "" {
		// Use StaticCredentialsProvider instead of NewCredentialsProvider
		cfg.Credentials = aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(
				opts.Credentials.AccessKeyID,
				opts.Credentials.SecretAccessKey,
				opts.Credentials.SessionToken,
			),
		)
	}

	return cfg, nil
}

// listObjects retrieves and filters objects from S3
func (s *S3Reader) listObjects(ctx context.Context) error {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.opts.Bucket),
		MaxKeys: &s.opts.MaxKeys,
	}

	if s.opts.Prefix != "" {
		input.Prefix = aws.String(s.opts.Prefix)
	}

	var allObjects []S3Object

	// List objects with pagination
	paginator := s3.NewListObjectsV2Paginator(s.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			if s.shouldIncludeObject(*obj.Key) {
				s3Obj := S3Object{
					Key:          *obj.Key,
					Size:         *obj.Size,
					LastModified: *obj.LastModified,
					ETag:         strings.Trim(*obj.ETag, "\""),
				}

				// Get object metadata if requested
				if s.opts.IncludeMetadata {
					metadata, err := s.getObjectMetadata(ctx, *obj.Key)
					if err == nil {
						s3Obj.Metadata = metadata
					}
				}

				allObjects = append(allObjects, s3Obj)
			}
		}
	}

	// Sort objects if requested
	s.sortObjects(allObjects)

	s.objects = allObjects
	s.stats.ObjectsListed = int64(len(allObjects))

	return nil
}

// shouldIncludeObject determines if an object should be processed
func (s *S3Reader) shouldIncludeObject(key string) bool {
	// Check suffix filter
	if s.opts.Suffix != "" && !strings.HasSuffix(key, s.opts.Suffix) {
		return false
	}

	// Check recursive setting
	if !s.opts.Recursive && strings.Contains(strings.TrimPrefix(key, s.opts.Prefix), "/") {
		return false
	}

	// Add file pattern matching here if needed
	// if s.opts.FilePattern != "" { ... }

	return true
}

// getObjectMetadata retrieves metadata for a specific object
func (s *S3Reader) getObjectMetadata(ctx context.Context, key string) (map[string]string, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(key),
	}

	result, err := s.client.HeadObject(ctx, input)
	if err != nil {
		return nil, err
	}

	return result.Metadata, nil
}

// sortObjects sorts the object list based on the specified sort order
func (s *S3Reader) sortObjects(objects []S3Object) {
	// Implementation depends on sort order
	// For now, keeping simple - could add proper sorting logic
}

// openNextObject opens the next S3 object for reading
func (s *S3Reader) openNextObject(ctx context.Context) error {
	if s.currentIndex >= len(s.objects) {
		return io.EOF
	}

	obj := s.objects[s.currentIndex]
	s.stats.CurrentObject = obj.Key

	// Get object from S3
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(obj.Key),
	}

	result, err := s.client.GetObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to get object %s: %w", obj.Key, err)
	}

	// Determine file type and create appropriate reader
	reader, err := s.createReaderForObject(result.Body, obj.Key)
	if err != nil {
		result.Body.Close()
		return fmt.Errorf("failed to create reader for %s: %w", obj.Key, err)
	}

	s.currentReader = reader
	s.stats.ObjectsRead++
	s.stats.ProcessedFiles = append(s.stats.ProcessedFiles, obj.Key)

	return nil
}

// createReaderForObject creates the appropriate reader based on file extension
func (s *S3Reader) createReaderForObject(body io.ReadCloser, key string) (core.DataSource, error) {
	ext := strings.ToLower(filepath.Ext(key))

	switch ext {
	case ".csv":
		return NewCSVReader(body, WithCSVHasHeaders(true))
	case ".json", ".jsonl":
		return NewJSONReader(body), nil
	// Add support for other formats as needed
	// case ".parquet":
	//     return NewParquetReader(body)
	default:
		// Default to treating as line-delimited JSON
		return NewJSONReader(body), nil
	}
}

// closeCurrentReader closes the current file reader
func (s *S3Reader) closeCurrentReader() error {
	if s.currentReader != nil {
		err := s.currentReader.Close()
		s.currentReader = nil
		s.currentIndex++
		return err
	}
	return nil
}
