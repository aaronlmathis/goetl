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

// Package core defines the core types for the GoETL library.
//
// GoETL is a high-performance, interface-driven ETL (Extract, Transform, Load) library for Go,
// designed for streaming large datasets efficiently with extensibility, type safety, and minimal dependencies.
//
// This file contains the primary types and function adapters.

// Record represents a single data record in the pipeline.
// Each record is a map from field names to values, supporting heterogeneous data.
type Record map[string]interface{}

// TransformFunc is a function adapter for the Transformer interface.
// Allows ordinary functions to be used as Transformers.
type TransformFunc func(ctx context.Context, record Record) (Record, error)

// Transform implements the Transformer interface for TransformFunc.
func (f TransformFunc) Transform(ctx context.Context, record Record) (Record, error) {
	return f(ctx, record)
}

// FilterFunc is a function adapter for the Filter interface.
// Allows ordinary functions to be used as Filters.
type FilterFunc func(ctx context.Context, record Record) (bool, error)

// ShouldInclude implements the Filter interface for FilterFunc.
func (f FilterFunc) ShouldInclude(ctx context.Context, record Record) (bool, error) {
	return f(ctx, record)
}
