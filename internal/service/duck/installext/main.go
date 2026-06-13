// Command installext pre-installs DuckDB extensions into a directory so they
// can be baked into the container image (the distroless runtime has no
// network access or writable home for on-demand installs).
//
// Usage: go run ./internal/service/duck/installext -dir /duckdb/extensions
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	dir := flag.String("dir", "", "directory to install extensions into (extension_directory)")
	extensions := flag.String("extensions", "httpfs,aws,spatial,ducklake", "comma-separated list of extensions to install")
	flag.Parse()

	if *dir == "" {
		log.Fatal("-dir is required")
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}
	defer db.Close() //nolint:errcheck

	if _, err := db.Exec(fmt.Sprintf("SET extension_directory = '%s'", strings.ReplaceAll(*dir, "'", "''"))); err != nil {
		log.Fatalf("failed to set extension_directory: %v", err)
	}

	for ext := range strings.SplitSeq(*extensions, ",") {
		ext = strings.TrimSpace(ext)
		if ext == "" {
			continue
		}
		if _, err := db.Exec("INSTALL " + ext); err != nil {
			log.Fatalf("failed to install extension %s: %v", ext, err)
		}
		// LOAD verifies the downloaded extension is usable on this platform.
		if _, err := db.Exec("LOAD " + ext); err != nil {
			log.Fatalf("failed to load extension %s: %v", ext, err)
		}
		log.Printf("installed %s into %s", ext, *dir)
	}
}
