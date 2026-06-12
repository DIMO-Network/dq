// s3_minio_test.go runs the parse-on-read pipeline against a real S3 API by
// launching a throwaway MinIO server (single binary, no Docker):
//
//	raw cloudevent bundles → S3-backed materializer (decode, watermark,
//	latest/summary bucket read-merge-write) → DuckDB httpfs reads
//	(CREATE SECRET with endpoint/path-style, s3:// globs, hive partitions)
//	→ decoded compaction with identical query results.
//
// It mirrors a slice of pipeline_e2e_test.go's stage assertions so the
// hand-computed values are pinned on both backends. The store adapter copies
// internal/app's s3ObjectStore semantics (including the NoSuchKey →
// materializer.ErrNotFound translation) because that type is unexported and
// production code must not change for tests.
//
// The test skips when the minio binary is not on PATH or in -short mode, so
// CI without MinIO stays green.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/DIMO-Network/dq/internal/service/duck"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	minioCreds  = "minioadmin"
	minioRegion = "us-east-1"
)

// startMinIO launches a MinIO server on a free localhost port with a
// t.TempDir() data directory and returns its http:// endpoint. The test is
// skipped when minio is not installed (or in -short mode) so suites stay
// green on machines without it.
func startMinIO(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping MinIO integration test in -short mode")
	}
	bin, err := exec.LookPath("minio")
	if err != nil {
		t.Skip("minio binary not on PATH; install with `brew install minio/stable/minio`")
	}

	// Pick a free port: bind :0, read it back, release it for minio.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	cmd := exec.Command(bin, "server", t.TempDir(), "--address", addr)
	cmd.Env = append(os.Environ(),
		"MINIO_ROOT_USER="+minioCreds,
		"MINIO_ROOT_PASSWORD="+minioCreds,
		"MINIO_BROWSER=off",
	)
	require.NoError(t, cmd.Start(), "starting minio server")
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	endpoint := "http://" + addr
	healthURL := endpoint + "/minio/health/live"
	require.Eventually(t, func() bool {
		resp, err := http.Get(healthURL) //nolint:gosec // local test server
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond, "minio did not report ready at %s", healthURL)

	return endpoint
}

// newMinIOS3Client builds an AWS S3 client against the MinIO endpoint with
// the same options production wiring uses (path-style + custom endpoint).
func newMinIOS3Client(endpoint string) *s3.Client {
	return s3.New(s3.Options{
		Region:       minioRegion,
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(minioCreds, minioCreds, ""),
	})
}

// minioS3Store is a test-local copy of internal/app's unexported
// s3ObjectStore: the materializer.ObjectStore/CompactStore contract over one
// bucket, including the NoSuchKey → ErrNotFound translation and
// quiet-delete-on-missing semantics.
type minioS3Store struct {
	client *s3.Client
	bucket string
}

var (
	_ materializer.ObjectStore  = (*minioS3Store)(nil)
	_ materializer.CompactStore = (*minioS3Store)(nil)
)

// List returns every object under prefix, in lexicographic key order.
func (s *minioS3Store) List(ctx context.Context, prefix string) ([]materializer.ObjectInfo, error) {
	var infos []materializer.ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing s3://%s/%s: %w", s.bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			infos = append(infos, materializer.ObjectInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			})
		}
	}
	return infos, nil
}

// GetObject downloads one object, translating S3 not-found errors to
// materializer.ErrNotFound as the store contract requires.
func (s *minioS3Store) GetObject(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noKey *s3types.NoSuchKey
		if errors.As(err, &noKey) {
			return nil, fmt.Errorf("s3://%s/%s: %w", s.bucket, key, materializer.ErrNotFound)
		}
		return nil, fmt.Errorf("getting s3://%s/%s: %w", s.bucket, key, err)
	}
	defer out.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("reading s3://%s/%s: %w", s.bucket, key, err)
	}
	return body, nil
}

// PutObject uploads one object.
func (s *minioS3Store) PutObject(ctx context.Context, key string, body []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("putting s3://%s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// DeleteObject removes one object, treating not-found as done.
func (s *minioS3Store) DeleteObject(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noKey *s3types.NoSuchKey
		if errors.As(err, &noKey) {
			return nil
		}
		return fmt.Errorf("deleting s3://%s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// TestMinIO_PipelineEndToEnd is the pipeline e2e on real S3: it must produce
// the same hand-computed answers TestPipelineEndToEnd pins on the filesystem
// store, and decoded compaction must be invisible to queries.
func TestMinIO_PipelineEndToEnd(t *testing.T) {
	endpoint := startMinIO(t)
	ctx := context.Background()
	const bucket = "dq-pipeline"

	s3c := newMinIOS3Client(endpoint)
	_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "creating bucket %s", bucket)
	store := &minioS3Store{client: s3c, bucket: bucket}

	// The adapter must translate MinIO's not-found into the store contract's
	// sentinel — the materializer's watermark bootstrap depends on it.
	_, err = store.GetObject(ctx, "decoded/v1/_state/watermark.json")
	require.ErrorIs(t, err, materializer.ErrNotFound, "NoSuchKey must map to materializer.ErrNotFound")

	// --- Stage 1: raw bundles land on S3 as din writes them. ---------------
	day1 := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)

	a1 := day1.Add(10 * time.Hour)
	dupEvent := deviceStatus("a-dup", vehicleA, a1.Add(30*time.Minute), speedAt(a1.Add(30*time.Minute), 60))

	writeRawBundle(t, store, day1, 1,
		deviceStatus("a-1", vehicleA, a1, speedAt(a1, 50), speedAt(a1.Add(10*time.Minute), 70)),
		dupEvent,
	)
	// The same event again in a second bundle: at-least-once redelivery.
	writeRawBundle(t, store, day1, 2,
		dupEvent,
		deviceStatus("b-1", vehicleB, a1, speedAt(a1, 100)),
	)
	b2 := day2.Add(9 * time.Hour)
	writeRawBundle(t, store, day2, 3,
		deviceStatus("a-2", vehicleA, b2, speedAt(b2, 90)),
	)

	// --- Stage 2: materializer decodes post fact, against real S3. ---------
	runner := materializer.New(materializer.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
		CompactMinFiles:   2,
	}, store, zerolog.Nop())

	processed, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Positive(t, processed)
	for processed != 0 {
		processed, err = runner.RunOnce(ctx)
		require.NoError(t, err)
	}

	// Watermark published to S3 — the contract the din compactor gates on.
	wm, err := store.GetObject(ctx, "decoded/v1/_state/watermark.json")
	require.NoError(t, err)
	var cursor map[string]string
	require.NoError(t, json.Unmarshal(wm, &cursor))
	assert.Contains(t, cursor, "type=dimo.status/date=2026-06-08")
	assert.Contains(t, cursor, "type=dimo.status/date=2026-06-09")

	// --- Stage 3: DuckDB reads the decoded layout over httpfs s3:// globs. --
	// S3Enabled exercises INSTALL/LOAD httpfs+aws and CREATE SECRET with the
	// http:// endpoint (URL_STYLE path, USE_SSL false).
	svc, err := duck.NewService(duck.Config{
		S3Enabled:            true,
		S3AWSRegion:          minioRegion,
		S3AWSAccessKeyID:     minioCreds,
		S3AWSSecretAccessKey: minioCreds,
		S3Endpoint:           endpoint,
		Bucket:               bucket,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	queries := duck.NewQueries(svc, bucket)

	// Aggregation: vehicle A speed over both days, single full-range bucket.
	// Values: 50, 70, 60 (dup collapsed to one), 90 → avg 67.5, max 90.
	from := day1
	to := day2.Add(24 * time.Hour)
	aggArgs := &model.AggregatedSignalArgs{
		SignalArgs: model.SignalArgs{Subject: vehicleA},
		FromTS:     from,
		ToTS:       to,
		Interval:   to.Sub(from).Microseconds(),
		FloatArgs: []model.FloatSignalArgs{
			{Name: "speed", Agg: model.FloatAggregationAvg},
			{Name: "speed", Agg: model.FloatAggregationMax},
		},
	}
	aggs, err := queries.GetAggregatedSignals(ctx, vehicleA, aggArgs)
	require.NoError(t, err)
	require.Len(t, aggs, 2)
	byIndex := map[uint16]float64{}
	for _, agg := range aggs {
		byIndex[agg.SignalIndex] = agg.ValueNumber
	}
	assert.InDelta(t, 67.5, byIndex[0], 1e-9, "avg speed over s3:// globs: duplicate event must count once")
	assert.InDelta(t, 90.0, byIndex[1], 1e-9, "max speed over s3:// globs")

	// Latest: vehicle A's newest speed and full-history lastSeen, read from
	// the hash-bucketed latest.parquet on S3.
	latestArgs := &model.LatestSignalsArgs{
		SignalArgs:      model.SignalArgs{Subject: vehicleA},
		SignalNames:     map[string]struct{}{"speed": {}},
		IncludeLastSeen: true,
	}
	latest, err := queries.GetLatestSignals(ctx, vehicleA, latestArgs)
	require.NoError(t, err)
	got := map[string]float64{}
	var lastSeen time.Time
	for _, sig := range latest {
		if sig.Data.Name == model.LastSeenField {
			lastSeen = sig.Data.Timestamp
			continue
		}
		got[sig.Data.Name] = sig.Data.ValueNumber
	}
	assert.InDelta(t, 90.0, got["speed"], 1e-9, "latest speed is day-2 value")
	assert.True(t, lastSeen.Equal(b2), "lastSeen tracks the newest signal")

	// Available signals come from the summary bucket on S3.
	available, err := queries.GetAvailableSignals(ctx, vehicleA, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"speed"}, available)

	// --- Stage 4: freshness — a late bundle for day 1 lands as a second
	// decoded file in the partition (the small-file pattern compaction fixes).
	a4 := day1.Add(12 * time.Hour)
	writeRawBundle(t, store, day1, 4,
		deviceStatus("a-4", vehicleA, a4, speedAt(a4, 80)),
	)
	processed, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Positive(t, processed, "materializer picks up new raw files incrementally")

	// New value visible immediately through fresh s3:// globs:
	// values 50, 70, 60, 80, 90 → avg 70, max still 90.
	aggs, err = queries.GetAggregatedSignals(ctx, vehicleA, aggArgs)
	require.NoError(t, err)
	require.Len(t, aggs, 2)
	for _, agg := range aggs {
		byIndex[agg.SignalIndex] = agg.ValueNumber
	}
	assert.InDelta(t, 70.0, byIndex[0], 1e-9, "avg includes the late day-1 value")
	assert.InDelta(t, 90.0, byIndex[1], 1e-9, "max unchanged")

	// --- Stage 5: decoded compaction is invisible to queries on real S3. ---
	partition := "decoded/v1/signals/date=" + day1.Format("2006-01-02") + "/"
	before, err := store.List(ctx, partition)
	require.NoError(t, err)
	require.Greater(t, len(before), 1, "fixture must produce the small-file pattern")

	wantAggs, err := queries.GetAggregatedSignals(ctx, vehicleA, aggArgs)
	require.NoError(t, err)
	wantLatest, err := queries.GetLatestSignals(ctx, vehicleA, latestArgs)
	require.NoError(t, err)

	n, err := runner.CompactOnce(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	after, err := store.List(ctx, partition)
	require.NoError(t, err)
	require.Len(t, after, 1, "day-1 signals partition merged to one object")

	gotAggs, err := queries.GetAggregatedSignals(ctx, vehicleA, aggArgs)
	require.NoError(t, err)
	assert.Equal(t, wantAggs, gotAggs, "aggregations identical across compaction on S3")
	gotLatest, err := queries.GetLatestSignals(ctx, vehicleA, latestArgs)
	require.NoError(t, err)
	assert.Equal(t, wantLatest, gotLatest, "latest signals identical across compaction on S3")
}
