package duck

import (
	"strings"
	"testing"
)

// The reader role (query fleet) must attach the DuckLake catalog
// READ_ONLY so it can never write the shared catalog and can sit on a Postgres
// read replica. The writer role (materializer) must attach read-write against
// the primary. These are pure string builders, so assert the exact SQL.

func TestAttachDuckLakeSQL_Writer(t *testing.T) {
	cfg := Config{
		CatalogDSN: "postgresql://host=primary dbname=dq",
		DataPath:   "s3://dimo-storage-prod/lake/",
	}
	got := attachDuckLakeSQL(cfg)
	want := "ATTACH IF NOT EXISTS 'ducklake:postgres:postgresql://host=primary dbname=dq?connect_timeout=10' AS lake (DATA_PATH 's3://dimo-storage-prod/lake/')"
	if got != want {
		t.Fatalf("writer attach:\n got: %s\nwant: %s", got, want)
	}
	if strings.Contains(got, "READ_ONLY") {
		t.Fatalf("writer attach must not be READ_ONLY: %s", got)
	}
}

func TestAttachDuckLakeSQL_ReaderReadOnly(t *testing.T) {
	cfg := Config{
		CatalogDSN: "postgresql://host=primary dbname=dq",
		DataPath:   "s3://dimo-storage-prod/lake/",
		ReadOnly:   true,
	}
	got := attachDuckLakeSQL(cfg)
	want := "ATTACH IF NOT EXISTS 'ducklake:postgres:postgresql://host=primary dbname=dq?connect_timeout=10' AS lake (DATA_PATH 's3://dimo-storage-prod/lake/', READ_ONLY)"
	if got != want {
		t.Fatalf("reader attach:\n got: %s\nwant: %s", got, want)
	}
}

func TestAttachDuckLakeSQL_ReaderUsesReplicaDSN(t *testing.T) {
	cfg := Config{
		CatalogDSN:     "postgresql://host=primary dbname=dq",
		CatalogReadDSN: "postgresql://host=replica dbname=dq",
		DataPath:       "s3://dimo-storage-prod/lake/",
		ReadOnly:       true,
	}
	got := attachDuckLakeSQL(cfg)
	if !strings.Contains(got, "host=replica") {
		t.Fatalf("read-only reader with a replica DSN must attach the replica: %s", got)
	}
	if strings.Contains(got, "host=primary") {
		t.Fatalf("read-only reader must not attach the primary when a replica is set: %s", got)
	}
	if !strings.Contains(got, "READ_ONLY") {
		t.Fatalf("reader attach must be READ_ONLY: %s", got)
	}
}

func TestEffectiveCatalogDSN(t *testing.T) {
	primary := "postgresql://host=primary dbname=dq"
	replica := "postgresql://host=replica dbname=dq"

	cases := []struct {
		name     string
		cfg      Config
		wantHost string
	}{
		{"writer ignores replica", Config{CatalogDSN: primary, CatalogReadDSN: replica}, "host=primary"},
		{"reader without replica reads primary", Config{CatalogDSN: primary, ReadOnly: true}, "host=primary"},
		{"reader with replica reads replica", Config{CatalogDSN: primary, CatalogReadDSN: replica, ReadOnly: true}, "host=replica"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.effectiveCatalogDSN(); !strings.Contains(got, tc.wantHost) {
				t.Fatalf("effectiveCatalogDSN()=%q, want it to contain %q", got, tc.wantHost)
			}
		})
	}
}

func TestMetaAttachOpts_ReadOnly(t *testing.T) {
	pg := Config{CatalogDSN: "postgresql://host=primary dbname=dq"}
	if got := pg.MetaAttachOpts(); got != " (TYPE postgres)" {
		t.Fatalf("writer meta opts = %q", got)
	}
	pgRO := Config{CatalogDSN: "postgresql://host=primary dbname=dq", ReadOnly: true}
	if got := pgRO.MetaAttachOpts(); got != " (TYPE postgres, READ_ONLY)" {
		t.Fatalf("reader meta opts = %q", got)
	}
	// A read-only reader on a replica reports the catalog/meta target as the
	// replica, and the meta side database is the Postgres catalog itself.
	pgReplica := Config{
		CatalogDSN:     "postgresql://host=primary dbname=dq",
		CatalogReadDSN: "postgresql://host=replica dbname=dq",
		ReadOnly:       true,
	}
	if got := pgReplica.MetaTarget(); !strings.Contains(got, "host=replica") {
		t.Fatalf("reader MetaTarget must follow the replica DSN: %q", got)
	}
}
