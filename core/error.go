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

package core

import "context"

// Package core defines the error handling types for the GoETL library.
//
// This file contains error handling interfaces, strategies, and function adapters.

// ErrorHandler defines how errors are handled during processing.
// Custom error handlers can be used to log, collect, or transform errors.
type ErrorHandler interface {
	// HandleError processes an error that occurred during transformation.
	// Returning a non-nil error will stop the pipeline; returning nil will continue.
	HandleError(ctx context.Context, record Record, err error) error
}

// ErrorStrategy defines how to handle transformation errors in the pipeline.
type ErrorStrategy int

const (
	// FailFast stops processing on the first error encountered.
	FailFast ErrorStrategy = iota
	// SkipErrors continues processing, skipping failed records.
	SkipErrors
	// CollectErrors continues processing, collecting all errors for later inspection.
	CollectErrors
)

// ErrorHandlerFunc is a function adapter for the ErrorHandler interface.
// Allows ordinary functions to be used as error handlers.
type ErrorHandlerFunc func(ctx context.Context, record Record, err error) error

// HandleError implements the ErrorHandler interface for ErrorHandlerFunc.
func (f ErrorHandlerFunc) HandleError(ctx context.Context, record Record, err error) error {
	return f(ctx, record, err)
}
