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
//

package types

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/aaronlmathis/goetl"
	"github.com/aaronlmathis/goetl/writers"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// OutputFormat represents a supported sink format.
type OutputFormat int

const (
	FormatCSV OutputFormat = iota
	FormatJSON
	FormatParquet
	FormatPostgres
)

// OutputLocation creates a DataSink for a given format.
type OutputLocation interface {
	NewSink(format OutputFormat) (goetl.DataSink, error)
}

// FileLocation writes output to a local filesystem path.
type FileLocation struct {
	Path string
}

// NewSink instantiates a writer for the file location.
func (f FileLocation) NewSink(format OutputFormat) (goetl.DataSink, error) {
	switch format {
	case FormatCSV:
		file, err := os.Create(f.Path)
		if err != nil {
			return nil, err
		}
		return writers.NewCSVWriter(file)
	case FormatJSON:
		file, err := os.Create(f.Path)
		if err != nil {
			return nil, err
		}
		return writers.NewJSONWriter(file), nil
	case FormatParquet:
		return writers.NewParquetWriter(f.Path)
	default:
		return nil, fmt.Errorf("unsupported format for FileLocation")
	}
}

// S3Location writes objects to an S3 bucket.
type S3Location struct {
	Bucket   string
	Key      string
	Uploader *s3manager.Uploader
}

type s3WriteCloser struct {
	buf      *bytes.Buffer
	uploader *s3manager.Uploader
	bucket   string
	key      string
}

func newS3WriteCloser(u *s3manager.Uploader, bucket, key string) *s3WriteCloser {
	return &s3WriteCloser{
		buf:      &bytes.Buffer{},
		uploader: u,
		bucket:   bucket,
		key:      key,
	}
}

func (s *s3WriteCloser) Write(p []byte) (int, error) { return s.buf.Write(p) }

func (s *s3WriteCloser) Close() error {
	_, err := s.uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
		Body:   bytes.NewReader(s.buf.Bytes()),
	})
	return err
}

type parquetS3Sink struct {
	*writers.ParquetWriter
	uploader *s3manager.Uploader
	bucket   string
	key      string
	filename string
}

func (p *parquetS3Sink) Close() error {
	if err := p.ParquetWriter.Close(); err != nil {
		return err
	}
	file, err := os.Open(p.filename)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = p.uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: &p.bucket,
		Key:    &p.key,
		Body:   file,
	})
	if err != nil {
		return err
	}
	os.Remove(p.filename)
	return nil
}

// NewSink creates a writer uploading to S3.
func (s S3Location) NewSink(format OutputFormat) (goetl.DataSink, error) {
	if s.Uploader == nil {
		cfg, err := awsconfig.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}
		s.Uploader = s3manager.NewUploader(s3.NewFromConfig(cfg))
	}

	switch format {
	case FormatCSV:
		return writers.NewCSVWriter(newS3WriteCloser(s.Uploader, s.Bucket, s.Key))
	case FormatJSON:
		return writers.NewJSONWriter(newS3WriteCloser(s.Uploader, s.Bucket, s.Key)), nil
	case FormatParquet:
		tmp, err := os.CreateTemp("", "parquet-*.parquet")
		if err != nil {
			return nil, err
		}
		filename := tmp.Name()
		tmp.Close()
		pw, err := writers.NewParquetWriter(filename)
		if err != nil {
			os.Remove(filename)
			return nil, err
		}
		return &parquetS3Sink{ParquetWriter: pw, uploader: s.Uploader, bucket: s.Bucket, key: s.Key, filename: filename}, nil
	default:
		return nil, fmt.Errorf("unsupported format for S3Location")
	}
}

// PostgresLocation directs output to a PostgreSQL database.
type PostgresLocation struct {
	DSN string
}

// NewSink instantiates a PostgreSQL writer.
func (p PostgresLocation) NewSink(format OutputFormat) (goetl.DataSink, error) {
	if format != FormatPostgres {
		return nil, fmt.Errorf("unsupported format for PostgresLocation")
	}
	return writers.NewPostgresWriter(writers.WithPostgresDSN(p.DSN))
}
