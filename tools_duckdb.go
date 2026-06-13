//go:build tools

// Pins DuckDB + parquet writer deps for packages under construction.
package main

import (
	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/parquet-go/parquet-go"
)
