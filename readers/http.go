//
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Copyright (C) 2025 Aaron Mathis aaron.mathis@gmail.com
//
// This file is part of GoETL.
//
// GoETL is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License, either version 3 of the License, or
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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// Package readers provides implementations of core.DataSource for reading data from various sources.
//
// This file implements a high-performance, configurable HTTP reader for streaming ETL pipelines.
// It supports authentication, pagination, retries, rate limiting, and various response formats.

// HTTPReaderError provides structured error information for HTTP reader operations
type HTTPReaderError struct {
	Op         string // Operation that failed (e.g., "request", "auth", "parse", "pagination")
	StatusCode int    // HTTP status code if applicable
	URL        string // URL being accessed when error occurred
	Err        error  // Underlying error
}

func (e *HTTPReaderError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("http reader %s [%d] %s: %v", e.Op, e.StatusCode, e.URL, e.Err)
	}
	return fmt.Sprintf("http reader %s %s: %v", e.Op, e.URL, e.Err)
}

func (e *HTTPReaderError) Unwrap() error {
	return e.Err
}

// HTTPReaderStats holds statistics about the HTTP reader's performance
type HTTPReaderStats struct {
	RequestCount    int64            // Total HTTP requests made
	RecordsRead     int64            // Total records read
	BytesRead       int64            // Total bytes read
	ReadDuration    time.Duration    // Total time spent reading
	LastReadTime    time.Time        // Time of last read
	RetryCount      int64            // Number of retries performed
	RateLimitHits   int64            // Number of rate limit hits
	NullValueCounts map[string]int64 // Count of null values per field
	ResponseTimes   []time.Duration  // Response times for monitoring
}

// AuthConfig defines authentication configuration
type AuthConfig struct {
	Type          string            // "bearer", "basic", "apikey", "oauth2", "custom"
	Token         string            // Bearer token or API key
	Username      string            // For basic auth
	Password      string            // For basic auth
	HeaderName    string            // Custom header name for API key
	HeaderValue   string            // Custom header value
	QueryParam    string            // Query parameter name for API key
	CustomHeaders map[string]string // Additional custom headers
}

// PaginationConfig defines pagination behavior
type PaginationConfig struct {
	Type         string // "offset", "cursor", "page", "link_header", "none"
	LimitParam   string // Parameter name for limit/page size
	OffsetParam  string // Parameter name for offset
	PageParam    string // Parameter name for page number
	CursorParam  string // Parameter name for cursor
	PageSize     int    // Number of records per page
	MaxPages     int    // Maximum pages to fetch (0 = unlimited)
	NextURLField string // JSON field containing next page URL
	CursorField  string // JSON field containing next cursor
	TotalField   string // JSON field containing total count
	HasMoreField string // JSON field indicating more data available
}

// HTTPReaderOptions configures the HTTP reader
type HTTPReaderOptions struct {
	Method           string            // HTTP method (default: GET)
	Headers          map[string]string // Additional headers
	QueryParams      map[string]string // Query parameters
	Body             io.Reader         // Request body for POST/PUT
	Auth             *AuthConfig       // Authentication configuration
	Pagination       *PaginationConfig // Pagination configuration
	Timeout          time.Duration     // Request timeout
	RetryAttempts    int               // Number of retry attempts
	RetryDelay       time.Duration     // Base delay between retries
	RateLimit        time.Duration     // Minimum time between requests
	ResponseFormat   string            // "json", "jsonl", "csv", "xml"
	DataPath         string            // JSON path to extract data array
	BufferSize       int               // Buffer size for response reading
	MaxResponseSize  int64             // Maximum response size in bytes
	FollowRedirects  bool              // Follow HTTP redirects
	ValidStatusCodes []int             // Valid HTTP status codes
	UserAgent        string            // User agent string
	AcceptEncoding   string            // Accept encoding header
	CustomClient     *http.Client      // Custom HTTP client
}

// ReaderOptionHTTP is a functional option for HTTPReaderOptions
type ReaderOptionHTTP func(*HTTPReaderOptions)

// Functional option functions
func WithHTTPMethod(method string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.Method = method
	}
}

func WithHTTPHeaders(headers map[string]string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		if opts.Headers == nil {
			opts.Headers = make(map[string]string)
		}
		for k, v := range headers {
			opts.Headers[k] = v
		}
	}
}

func WithHTTPQueryParams(params map[string]string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		if opts.QueryParams == nil {
			opts.QueryParams = make(map[string]string)
		}
		for k, v := range params {
			opts.QueryParams[k] = v
		}
	}
}

func WithHTTPAuth(auth *AuthConfig) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.Auth = auth
	}
}

func WithHTTPBearerToken(token string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.Auth = &AuthConfig{
			Type:  "bearer",
			Token: token,
		}
	}
}

func WithHTTPBasicAuth(username, password string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.Auth = &AuthConfig{
			Type:     "basic",
			Username: username,
			Password: password,
		}
	}
}

func WithHTTPAPIKey(headerName, apiKey string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.Auth = &AuthConfig{
			Type:        "apikey",
			HeaderName:  headerName,
			HeaderValue: apiKey,
		}
	}
}

func WithHTTPPagination(pagination *PaginationConfig) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.Pagination = pagination
	}
}

func WithHTTPTimeout(timeout time.Duration) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.Timeout = timeout
	}
}

func WithHTTPRetries(attempts int, delay time.Duration) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.RetryAttempts = attempts
		opts.RetryDelay = delay
	}
}

func WithHTTPRateLimit(delay time.Duration) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.RateLimit = delay
	}
}

func WithHTTPResponseFormat(format string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.ResponseFormat = format
	}
}

func WithHTTPDataPath(path string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.DataPath = path
	}
}

func WithHTTPUserAgent(userAgent string) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.UserAgent = userAgent
	}
}

func WithHTTPClient(client *http.Client) ReaderOptionHTTP {
	return func(opts *HTTPReaderOptions) {
		opts.CustomClient = client
	}
}

// HTTPReader implements core.DataSource for HTTP APIs
type HTTPReader struct {
	baseURL         string
	client          *http.Client
	opts            *HTTPReaderOptions
	stats           HTTPReaderStats
	scanner         *bufio.Scanner
	currentData     []core.Record
	currentIndex    int
	hasMoreData     bool
	nextURL         string
	nextCursor      string
	currentPage     int
	lastRequestTime time.Time
}

// NewHTTPReader creates a new HTTP API reader with configurable options
func NewHTTPReader(url string, options ...ReaderOptionHTTP) (*HTTPReader, error) {
	opts := &HTTPReaderOptions{
		Method:           "GET",
		Headers:          make(map[string]string),
		QueryParams:      make(map[string]string),
		Timeout:          30 * time.Second,
		RetryAttempts:    3,
		RetryDelay:       time.Second,
		ResponseFormat:   "json",
		BufferSize:       64 * 1024,
		MaxResponseSize:  100 * 1024 * 1024, // 100MB
		FollowRedirects:  true,
		ValidStatusCodes: []int{200, 201, 202},
		UserAgent:        "GoETL-HTTPReader/1.0",
		AcceptEncoding:   "gzip, deflate",
	}

	// Apply functional options
	for _, option := range options {
		option(opts)
	}

	// Create HTTP client
	client := opts.CustomClient
	if client == nil {
		client = &http.Client{
			Timeout: opts.Timeout,
		}

		if !opts.FollowRedirects {
			client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			}
		}
	}

	reader := &HTTPReader{
		baseURL:      url,
		client:       client,
		opts:         opts,
		stats:        HTTPReaderStats{NullValueCounts: make(map[string]int64)},
		hasMoreData:  true,
		currentPage:  1,
		currentIndex: 0,
	}

	return reader, nil
}

// Read implements the core.DataSource interface
func (hr *HTTPReader) Read(ctx context.Context) (core.Record, error) {
	start := time.Now()
	defer func() {
		hr.stats.ReadDuration += time.Since(start)
		hr.stats.LastReadTime = time.Now()
	}()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, &HTTPReaderError{Op: "read", URL: hr.baseURL, Err: ctx.Err()}
	default:
	}

	// Load data if needed
	if hr.currentIndex >= len(hr.currentData) {
		if !hr.hasMoreData {
			return nil, io.EOF
		}

		if err := hr.loadNextBatch(ctx); err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, &HTTPReaderError{Op: "load_batch", URL: hr.baseURL, Err: err}
		}

		hr.currentIndex = 0
	}

	// Return current record
	if hr.currentIndex < len(hr.currentData) {
		record := hr.currentData[hr.currentIndex]
		hr.currentIndex++
		hr.stats.RecordsRead++

		// Track null values
		for key, val := range record {
			if val == nil {
				hr.stats.NullValueCounts[key]++
			}
		}

		return record, nil
	}

	return nil, io.EOF
}

// Close implements the core.DataSource interface
func (hr *HTTPReader) Close() error {
	// HTTP reader doesn't need explicit cleanup
	return nil
}

// Stats returns HTTP reader performance statistics
func (hr *HTTPReader) Stats() HTTPReaderStats {
	return hr.stats
}

// loadNextBatch fetches the next batch of data from the API
func (hr *HTTPReader) loadNextBatch(ctx context.Context) error {
	// Rate limiting
	if hr.opts.RateLimit > 0 {
		timeSinceLastRequest := time.Since(hr.lastRequestTime)
		if timeSinceLastRequest < hr.opts.RateLimit {
			sleepTime := hr.opts.RateLimit - timeSinceLastRequest
			select {
			case <-time.After(sleepTime):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Determine URL for this request
	requestURL := hr.getRequestURL()

	// Create and execute request with retries
	data, err := hr.executeRequestWithRetry(ctx, requestURL)
	if err != nil {
		return err
	}

	hr.lastRequestTime = time.Now()
	hr.stats.RequestCount++

	// Parse response based on format
	records, err := hr.parseResponse(data)
	if err != nil {
		return &HTTPReaderError{Op: "parse", URL: requestURL, Err: err}
	}

	hr.currentData = records

	// Update pagination state
	hr.updatePaginationState(data)

	return nil
}

// getRequestURL builds the URL for the current request
func (hr *HTTPReader) getRequestURL() string {
	if hr.nextURL != "" {
		return hr.nextURL
	}

	url := hr.baseURL
	params := make(map[string]string)

	// Copy base query params
	for k, v := range hr.opts.QueryParams {
		params[k] = v
	}

	// Add pagination parameters
	if hr.opts.Pagination != nil {
		pg := hr.opts.Pagination

		switch pg.Type {
		case "offset":
			if pg.LimitParam != "" {
				params[pg.LimitParam] = strconv.Itoa(pg.PageSize)
			}
			if pg.OffsetParam != "" {
				offset := (hr.currentPage - 1) * pg.PageSize
				params[pg.OffsetParam] = strconv.Itoa(offset)
			}
		case "page":
			if pg.LimitParam != "" {
				params[pg.LimitParam] = strconv.Itoa(pg.PageSize)
			}
			if pg.PageParam != "" {
				params[pg.PageParam] = strconv.Itoa(hr.currentPage)
			}
		case "cursor":
			if pg.LimitParam != "" {
				params[pg.LimitParam] = strconv.Itoa(pg.PageSize)
			}
			if pg.CursorParam != "" && hr.nextCursor != "" {
				params[pg.CursorParam] = hr.nextCursor
			}
		}
	}

	// Build query string
	if len(params) > 0 {
		var queryParts []string
		for k, v := range params {
			queryParts = append(queryParts, fmt.Sprintf("%s=%s", k, v))
		}
		url += "?" + strings.Join(queryParts, "&")
	}

	return url
}

// executeRequestWithRetry executes HTTP request with retry logic
func (hr *HTTPReader) executeRequestWithRetry(ctx context.Context, url string) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= hr.opts.RetryAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := hr.opts.RetryDelay * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			hr.stats.RetryCount++
		}

		data, err := hr.executeRequest(ctx, url)
		if err == nil {
			return data, nil
		}

		lastErr = err

		// Check if we should retry
		if httpErr, ok := err.(*HTTPReaderError); ok {
			if httpErr.StatusCode == 429 { // Rate limited
				hr.stats.RateLimitHits++
				continue // Always retry rate limits
			}
			if httpErr.StatusCode >= 500 { // Server errors
				continue // Retry server errors
			}
			// Don't retry client errors (4xx except 429)
			break
		}
	}

	return nil, lastErr
}

// executeRequest executes a single HTTP request
func (hr *HTTPReader) executeRequest(ctx context.Context, url string) ([]byte, error) {
	// Create request
	req, err := http.NewRequestWithContext(ctx, hr.opts.Method, url, hr.opts.Body)
	if err != nil {
		return nil, &HTTPReaderError{Op: "create_request", URL: url, Err: err}
	}

	// Add headers
	req.Header.Set("User-Agent", hr.opts.UserAgent)
	req.Header.Set("Accept-Encoding", hr.opts.AcceptEncoding)

	for k, v := range hr.opts.Headers {
		req.Header.Set(k, v)
	}

	// Add authentication
	if hr.opts.Auth != nil {
		if err := hr.addAuthentication(req); err != nil {
			return nil, &HTTPReaderError{Op: "auth", URL: url, Err: err}
		}
	}

	// Execute request
	requestStart := time.Now()
	resp, err := hr.client.Do(req)
	if err != nil {
		return nil, &HTTPReaderError{Op: "request", URL: url, Err: err}
	}
	defer resp.Body.Close()

	hr.stats.ResponseTimes = append(hr.stats.ResponseTimes, time.Since(requestStart))

	// Check status code
	if !hr.isValidStatusCode(resp.StatusCode) {
		return nil, &HTTPReaderError{
			Op:         "status_check",
			URL:        url,
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("unexpected status code: %d", resp.StatusCode),
		}
	}

	// Read response body with size limit
	reader := io.LimitReader(resp.Body, hr.opts.MaxResponseSize)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, &HTTPReaderError{Op: "read_response", URL: url, Err: err}
	}

	hr.stats.BytesRead += int64(len(data))
	return data, nil
}

// addAuthentication adds authentication to the request
func (hr *HTTPReader) addAuthentication(req *http.Request) error {
	if hr.opts.Auth == nil {
		return nil
	}

	auth := hr.opts.Auth
	switch auth.Type {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+auth.Token)
	case "basic":
		req.SetBasicAuth(auth.Username, auth.Password)
	case "apikey":
		if auth.HeaderName != "" {
			req.Header.Set(auth.HeaderName, auth.HeaderValue)
		}
		if auth.QueryParam != "" {
			q := req.URL.Query()
			q.Set(auth.QueryParam, auth.HeaderValue)
			req.URL.RawQuery = q.Encode()
		}
	case "custom":
		for k, v := range auth.CustomHeaders {
			req.Header.Set(k, v)
		}
	default:
		return fmt.Errorf("unsupported auth type: %s", auth.Type)
	}

	return nil
}

// parseResponse parses the HTTP response based on the configured format
func (hr *HTTPReader) parseResponse(data []byte) ([]core.Record, error) {
	switch hr.opts.ResponseFormat {
	case "json":
		return hr.parseJSONResponse(data)
	case "jsonl":
		return hr.parseJSONLResponse(data)
	case "csv":
		return hr.parseCSVResponse(data)
	default:
		return nil, fmt.Errorf("unsupported response format: %s", hr.opts.ResponseFormat)
	}
}

// parseJSONResponse parses JSON response
func (hr *HTTPReader) parseJSONResponse(data []byte) ([]core.Record, error) {
	var response interface{}
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("json unmarshal failed: %w", err)
	}

	// Extract data using path if specified
	if hr.opts.DataPath != "" {
		extracted, err := hr.extractDataFromPath(response, hr.opts.DataPath)
		if err != nil {
			return nil, fmt.Errorf("data path extraction failed: %w", err)
		}
		response = extracted
	}

	// Convert to records
	return hr.convertToRecords(response)
}

// parseJSONLResponse parses JSON Lines response
func (hr *HTTPReader) parseJSONLResponse(data []byte) ([]core.Record, error) {
	var records []core.Record
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var record core.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("jsonl parse error: %w", err)
		}

		records = append(records, record)
	}

	return records, nil
}

// parseCSVResponse parses CSV response
func (hr *HTTPReader) parseCSVResponse(data []byte) ([]core.Record, error) {
	// For CSV, we'd need to create a temporary CSV reader
	// This is a simplified implementation
	lines := strings.Split(string(data), "\n")
	if len(lines) < 2 {
		return nil, fmt.Errorf("insufficient CSV data")
	}

	headers := strings.Split(lines[0], ",")
	var records []core.Record

	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		values := strings.Split(line, ",")
		if len(values) != len(headers) {
			continue // Skip malformed lines
		}

		record := make(core.Record)
		for j, header := range headers {
			record[strings.TrimSpace(header)] = strings.TrimSpace(values[j])
		}

		records = append(records, record)
	}

	return records, nil
}

// extractDataFromPath extracts data using a simple JSON path
func (hr *HTTPReader) extractDataFromPath(data interface{}, path string) (interface{}, error) {
	parts := strings.Split(path, ".")
	current := data

	for _, part := range parts {
		if part == "" {
			continue
		}

		switch v := current.(type) {
		case map[string]interface{}:
			var exists bool
			current, exists = v[part]
			if !exists {
				return nil, fmt.Errorf("path element %s not found", part)
			}
		default:
			return nil, fmt.Errorf("cannot traverse path %s: expected object", part)
		}
	}

	return current, nil
}

// convertToRecords converts response data to core.Record slice
func (hr *HTTPReader) convertToRecords(data interface{}) ([]core.Record, error) {
	switch v := data.(type) {
	case []interface{}:
		var records []core.Record
		for _, item := range v {
			if record, ok := item.(map[string]interface{}); ok {
				records = append(records, core.Record(record))
			}
		}
		return records, nil
	case map[string]interface{}:
		// Single object response
		return []core.Record{core.Record(v)}, nil
	default:
		return nil, fmt.Errorf("unexpected response format: %T", data)
	}
}

// updatePaginationState updates pagination state for next request
func (hr *HTTPReader) updatePaginationState(data []byte) {
	if hr.opts.Pagination == nil {
		hr.hasMoreData = false
		return
	}

	pg := hr.opts.Pagination

	// Check max pages limit
	if pg.MaxPages > 0 && hr.currentPage >= pg.MaxPages {
		hr.hasMoreData = false
		return
	}

	// Parse response for pagination info
	var response map[string]interface{}
	if err := json.Unmarshal(data, &response); err != nil {
		hr.hasMoreData = false
		return
	}

	switch pg.Type {
	case "link_header":
		// Would need to parse Link header from HTTP response
		hr.hasMoreData = false
	case "cursor":
		if pg.CursorField != "" {
			if cursor, ok := response[pg.CursorField].(string); ok {
				hr.nextCursor = cursor
				hr.hasMoreData = cursor != ""
			} else {
				hr.hasMoreData = false
			}
		}
	case "offset", "page":
		hr.currentPage++
		if pg.HasMoreField != "" {
			if hasMore, ok := response[pg.HasMoreField].(bool); ok {
				hr.hasMoreData = hasMore
			} else {
				hr.hasMoreData = false
			}
		} else if pg.TotalField != "" {
			if total, ok := response[pg.TotalField].(float64); ok {
				totalRecords := int(total)
				processedRecords := (hr.currentPage - 1) * pg.PageSize
				hr.hasMoreData = processedRecords < totalRecords
			} else {
				hr.hasMoreData = false
			}
		} else {
			// Check if we got a full page
			hr.hasMoreData = len(hr.currentData) >= pg.PageSize
		}
	case "none":
		hr.hasMoreData = false
	default:
		hr.hasMoreData = false
	}

	if pg.NextURLField != "" {
		if nextURL, ok := response[pg.NextURLField].(string); ok {
			hr.nextURL = nextURL
			hr.hasMoreData = nextURL != ""
		}
	}
}

// isValidStatusCode checks if the status code is considered valid
func (hr *HTTPReader) isValidStatusCode(statusCode int) bool {
	for _, validCode := range hr.opts.ValidStatusCodes {
		if statusCode == validCode {
			return true
		}
	}
	return false
}

// Convenience constructors for common HTTP reader patterns

// NewHTTPReaderFromURL creates a basic HTTP reader for a single URL
func NewHTTPReaderFromURL(url string) (*HTTPReader, error) {
	return NewHTTPReader(url)
}

// NewPaginatedHTTPReader creates an HTTP reader with pagination support
func NewPaginatedHTTPReader(url string, pageSize int, paginationType string) (*HTTPReader, error) {
	pagination := &PaginationConfig{
		Type:        paginationType,
		PageSize:    pageSize,
		LimitParam:  "limit",
		OffsetParam: "offset",
		PageParam:   "page",
	}

	return NewHTTPReader(url, WithHTTPPagination(pagination))
}

// NewAuthenticatedHTTPReader creates an HTTP reader with bearer token authentication
func NewAuthenticatedHTTPReader(url, token string) (*HTTPReader, error) {
	return NewHTTPReader(url, WithHTTPBearerToken(token))
}
