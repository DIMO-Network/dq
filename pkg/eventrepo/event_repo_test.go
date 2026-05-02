//go:generate go tool mockgen -source=./eventrepo.go -destination=eventrepo_mock_test.go -package=eventrepo_test
package eventrepo_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	chindexer "github.com/DIMO-Network/cloudevent/clickhouse"
	"github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var dataType = "small"

// TestGetLatestIndexKey tests the GetLatestIndexKey function.
func TestGetLatestIndexKey(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	contractAddr := randAddress()
	device1TokenID := big.NewInt(1234567890)
	device2TokenID := big.NewInt(976543210)
	ctx := context.Background()
	now := time.Now()

	// Create test indices
	eventIdx1 := &cloudevent.CloudEventHeader{
		Subject: cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		}.String(),
		DataVersion: dataType,
		Time:        now.Add(-1 * time.Hour),
	}

	eventIdx2 := &cloudevent.CloudEventHeader{
		Subject: cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		}.String(),
		DataVersion: dataType,
		Time:        now,
	}

	// Insert test data
	_ = insertTestData(t, ctx, conn, eventIdx1)
	indexKey2 := insertTestData(t, ctx, conn, eventIdx2)

	tests := []struct {
		name          string
		subject       cloudevent.ERC721DID
		expectedKey   string
		expectedError bool
	}{
		{
			name: "valid latest object",
			subject: cloudevent.ERC721DID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device1TokenID,
			},
			expectedKey: indexKey2,
		},
		{
			name: "no records",
			subject: cloudevent.ERC721DID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device2TokenID,
			},
			expectedError: true,
		},
	}

	indexService := eventrepo.New(conn, nil, nil, "", "")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &grpc.SearchOptions{
				DataVersion: &wrapperspb.StringValue{Value: dataType},
				Subject:     &wrapperspb.StringValue{Value: tt.subject.String()},
			}
			metadata, err := indexService.GetLatestIndex(context.Background(), opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedKey, metadata.Data.Key)
			}
		})
	}
}

// TestGetDataFromIndex tests the GetDataFromIndex function.
func TestGetDataFromIndex(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)
	contractAddr := randAddress()
	device1TokenID := big.NewInt(1234567890)
	device2TokenID := big.NewInt(976543210)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()

	eventIdx := &cloudevent.CloudEventHeader{
		Subject: cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		}.String(),
		DataVersion: dataType,
		Time:        time.Now().Add(-1 * time.Hour),
	}

	_ = insertTestData(t, ctx, conn, eventIdx)

	tests := []struct {
		name            string
		subject         cloudevent.ERC721DID
		expectedContent []byte
		expectedError   bool
	}{
		{
			name: "valid object content",
			subject: cloudevent.ERC721DID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device1TokenID,
			},
			expectedContent: []byte(`{"vin": "1HGCM82633A123456"}`),
		},
		{
			name: "no records",
			subject: cloudevent.ERC721DID{
				ChainID:         153,
				ContractAddress: contractAddr,
				TokenID:         device2TokenID,
			},
			expectedError: true,
		},
	}

	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	// Stored object is full CloudEvent JSON (cloudevent unmarshaler expects envelope with "data").
	content := []byte(`{"data":{"vin": "1HGCM82633A123456"}}`)
	mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(content)),
		ContentLength: ref(int64(len(content))),
	}, nil).AnyTimes()

	indexService := eventrepo.New(conn, mockS3Client, nil, "", "")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &grpc.SearchOptions{
				DataVersion: &wrapperspb.StringValue{Value: dataType},
				Subject:     &wrapperspb.StringValue{Value: tt.subject.String()},
			}
			content, err := indexService.GetLatestCloudEvent(context.Background(), "test-bucket", opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, tt.expectedContent, []byte(content.Data))
			}
		})
	}
}

func TestStoreObject(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	mockS3Client.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.PutObjectOutput{}, nil).AnyTimes()

	indexService := eventrepo.New(conn, mockS3Client, nil, "", "")

	content := []byte(`{"vin": "1HGCM82633A123456"}`)
	did := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: randAddress(),
		TokenID:         big.NewInt(123456),
	}

	event := cloudevent.CloudEvent[json.RawMessage]{
		CloudEventHeader: cloudevent.CloudEventHeader{
			Subject:     did.String(),
			Time:        time.Now(),
			DataVersion: dataType,
		},
		Data: content,
	}
	err = indexService.StoreObject(ctx, "test-bucket", &event.CloudEventHeader, event.Data)
	require.NoError(t, err)

	// Verify the data is stored in ClickHouse
	opts := &grpc.SearchOptions{
		DataVersion: &wrapperspb.StringValue{Value: dataType},
		Subject:     &wrapperspb.StringValue{Value: did.String()},
	}
	metadata, err := indexService.GetLatestIndex(ctx, opts)
	require.NoError(t, err)
	expectedIndexKey := chindexer.CloudEventToObjectKey(&event.CloudEventHeader)
	require.NoError(t, err)
	require.Equal(t, expectedIndexKey, metadata.Data.Key)
}

// TestGetData tests the GetData function with different SearchOptions combinations.
func TestGetData(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	source1 := randAddress()
	contractAddr := randAddress()
	device1TokenID := big.NewInt(123456)
	device2TokenID := big.NewInt(654321)
	ctx := context.Background()
	now := time.Now()
	eventDID := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: contractAddr,
		TokenID:         device1TokenID,
	}
	eventIdx := cloudevent.CloudEventHeader{
		Subject: eventDID.String(),
		Time:    now.Add(-4 * time.Hour),
		Producer: cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         device1TokenID,
		}.String(),
		Type:        cloudevent.TypeStatus,
		Source:      source1.Hex(),
		DataVersion: dataType,
	}
	indexKey1 := insertTestData(t, ctx, conn, &eventIdx)
	eventIdx2 := eventIdx
	eventIdx2.Time = now.Add(-3 * time.Hour)
	indexKey2 := insertTestData(t, ctx, conn, &eventIdx2)
	eventIdx3 := eventIdx
	eventIdx3.Time = now.Add(-2 * time.Hour)
	eventIdx3.Type = cloudevent.TypeFingerprint
	indexKey3 := insertTestData(t, ctx, conn, &eventIdx3)
	eventIdx4 := eventIdx
	eventIdx4.Time = now.Add(-1 * time.Hour)
	eventIdx4.DataContentType = "utf-8"
	indexKey4 := insertTestData(t, ctx, conn, &eventIdx4)

	tests := []struct {
		name              string
		opts              *grpc.SearchOptions
		expectedIndexKeys []string
		expectedError     bool
	}{
		{
			name: "valid data with address",
			opts: &grpc.SearchOptions{
				DataVersion: &wrapperspb.StringValue{Value: dataType},
				Subject:     &wrapperspb.StringValue{Value: eventDID.String()},
			},
			expectedIndexKeys: []string{indexKey4, indexKey3, indexKey2, indexKey1},
		},
		{
			name: "no records with address",
			opts: &grpc.SearchOptions{
				DataVersion: &wrapperspb.StringValue{Value: dataType},
				Subject: &wrapperspb.StringValue{Value: cloudevent.ERC721DID{
					ChainID:         153,
					ContractAddress: contractAddr,
					TokenID:         device2TokenID,
				}.String()},
			},
			expectedIndexKeys: nil,
			expectedError:     true,
		},
		{
			name: "data within time range",
			opts: &grpc.SearchOptions{
				Subject:     &wrapperspb.StringValue{Value: eventDID.String()},
				DataVersion: &wrapperspb.StringValue{Value: dataType},
				After:       &timestamppb.Timestamp{Seconds: now.Add(-3 * time.Hour).Unix()},
				Before:      &timestamppb.Timestamp{Seconds: now.Add(-1 * time.Minute).Unix()},
			},
			expectedIndexKeys: []string{indexKey4, indexKey3},
		},
		{
			name: "data with type filter",
			opts: &grpc.SearchOptions{
				Subject:     &wrapperspb.StringValue{Value: eventDID.String()},
				DataVersion: &wrapperspb.StringValue{Value: dataType},
				Type:        &wrapperspb.StringValue{Value: cloudevent.TypeStatus},
			},
			expectedIndexKeys: []string{indexKey4, indexKey2, indexKey1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockS3Client := NewMockObjectGetter(ctrl)

			indexService := eventrepo.New(conn, mockS3Client, nil, "", "")
			// Allow GetObject calls in any order since fetches are concurrent.
			if len(tt.expectedIndexKeys) > 0 {
				mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					quotedKey := `"` + *params.Key + `"`
					body := []byte(`{"data":` + quotedKey + `}`)
					return &s3.GetObjectOutput{
						Body:          io.NopCloser(bytes.NewReader(body)),
						ContentLength: ref(int64(len(body))),
					}, nil
				}).Times(len(tt.expectedIndexKeys))
			}
			events, err := indexService.ListCloudEvents(context.Background(), "test-bucket", 10, tt.opts)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, events, len(tt.expectedIndexKeys))
				// Verify each event's data matches its index key (order matches ClickHouse result order).
				for i, expectedKey := range tt.expectedIndexKeys {
					expectedData := `"` + expectedKey + `"`
					require.Equal(t, expectedData, string(events[i].Data), "data mismatch at position %d", i)
				}
			}
		})
	}
}

// TestGetEventWithAllHeaderFields tests retrieving events with all header fields properly populated
func TestGetEventWithAllHeaderFields(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	// Create a DID for the test
	contractAddr := randAddress()
	deviceTokenID := big.NewInt(1234567890)
	eventDID1 := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: contractAddr,
		TokenID:         deviceTokenID,
	}
	eventDID2 := cloudevent.ERC721DID{
		ChainID:         151,
		ContractAddress: contractAddr,
		TokenID:         deviceTokenID,
	}

	// Create event with all header fields populated
	fullHeaderEvent := cloudevent.CloudEventHeader{
		ID:              "test-id-123456",
		Source:          "test-source",
		Producer:        "test-producer",
		Subject:         eventDID1.String(),
		Time:            now,
		Type:            cloudevent.TypeStatus,
		DataContentType: "application/json",
		DataSchema:      "https://example.com/schemas/status.json",
		DataVersion:     dataType,
		SpecVersion:     cloudevent.SpecVersion,
		Signature:       "0x1234567890",
		RawEventID:      "raw-event-id-123",
		Tags:            []string{"tests.tag1", "tests.tag2"},
		Extras: map[string]any{
			"extraField": "extra-value",
		},
	}

	fullHeaderEvent2 := fullHeaderEvent
	fullHeaderEvent2.Subject = eventDID2.String()
	fullHeaderEvent2.Signature = ""
	fullHeaderEvent2.Extras = map[string]any{
		"signature": "0x09876543210",
	}

	// Insert the event
	indexKey := insertTestData(t, ctx, conn, &fullHeaderEvent)
	indexKey2 := insertTestData(t, ctx, conn, &fullHeaderEvent2)

	// Setup mock S3 client for retrieving the event data
	ctrl := gomock.NewController(t)
	mockS3Client := NewMockObjectGetter(ctrl)
	eventData := []byte(`{"status": "online", "lastSeen": "2023-01-01T12:00:00Z"}`)
	// Stored object is full CloudEvent JSON (cloudevent unmarshaler expects envelope with "data").
	eventDataEnvelope := []byte(`{"data":` + string(eventData) + `}`)

	// Create service
	indexService := eventrepo.New(conn, mockS3Client, nil, "", "")

	// Test retrieving the event
	t.Run("retrieve event with full headers", func(t *testing.T) {
		mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				// Verify the correct key was requested
				require.Equal(t, indexKey, *params.Key)
				return &s3.GetObjectOutput{
					Body:          io.NopCloser(bytes.NewReader(eventDataEnvelope)),
					ContentLength: ref(int64(len(eventDataEnvelope))),
				}, nil
			},
		)
		opts := &grpc.SearchOptions{
			DataVersion: &wrapperspb.StringValue{Value: dataType},
			Subject:     &wrapperspb.StringValue{Value: eventDID1.String()},
		}

		retrievedEvent, err := indexService.GetLatestCloudEvent(ctx, "test-bucket", opts)
		require.NoError(t, err)

		// Verify all header fields
		assert.Equal(t, fullHeaderEvent.ID, retrievedEvent.ID, "ID mismatch")
		assert.Equal(t, fullHeaderEvent.Source, retrievedEvent.Source, "Source mismatch")
		assert.Equal(t, fullHeaderEvent.Producer, retrievedEvent.Producer, "Producer mismatch")
		assert.Equal(t, fullHeaderEvent.Subject, retrievedEvent.Subject, "Subject mismatch")
		assert.Equal(t, fullHeaderEvent.Time.UTC().Truncate(time.Second), retrievedEvent.Time.UTC().Truncate(time.Second), "Time mismatch")
		assert.Equal(t, fullHeaderEvent.Type, retrievedEvent.Type, "Type mismatch")
		assert.Equal(t, fullHeaderEvent.DataContentType, retrievedEvent.DataContentType, "DataContentType mismatch")
		assert.Equal(t, fullHeaderEvent.DataSchema, retrievedEvent.DataSchema, "DataSchema mismatch")
		assert.Equal(t, fullHeaderEvent.DataVersion, retrievedEvent.DataVersion, "DataVersion mismatch")
		assert.Equal(t, cloudevent.SpecVersion, retrievedEvent.SpecVersion, "SpecVersion mismatch")
		assert.Equal(t, fullHeaderEvent.Signature, retrievedEvent.Signature, "Signature mismatch")
		assert.Equal(t, fullHeaderEvent.RawEventID, retrievedEvent.RawEventID, "RawEventID mismatch")
		assert.Equal(t, fullHeaderEvent.Tags, retrievedEvent.Tags, "Tags mismatch")

		// Verify extras
		require.NotNil(t, retrievedEvent.Extras)
		require.Equal(t, 1, len(retrievedEvent.Extras))
		require.Equal(t, "extra-value", retrievedEvent.Extras["extraField"])

		// Verify data content
		require.Equal(t, string(eventData), string(retrievedEvent.Data))
	})

	t.Run("retrieve event with signature that is originally in extras", func(t *testing.T) {
		mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				// Verify the correct key was requested
				require.Equal(t, indexKey2, *params.Key)
				return &s3.GetObjectOutput{
					Body:          io.NopCloser(bytes.NewReader(eventDataEnvelope)),
					ContentLength: ref(int64(len(eventDataEnvelope))),
				}, nil
			},
		)
		opts := &grpc.SearchOptions{
			DataVersion: &wrapperspb.StringValue{Value: dataType},
			Subject:     &wrapperspb.StringValue{Value: eventDID2.String()},
		}
		retrievedEvent, err := indexService.GetLatestCloudEvent(ctx, "test-bucket", opts)
		require.NoError(t, err)

		// Verify all header fields
		assert.Equal(t, fullHeaderEvent2.ID, retrievedEvent.ID, "ID mismatch")
		assert.Equal(t, "0x09876543210", retrievedEvent.Signature, "Signature field not set correctly")
		assert.Nil(t, retrievedEvent.Extras["signature"], "Signature should not be in extras")
	})

	t.Run("retrieve event with raw event id that is originally in extras", func(t *testing.T) {
		fullHeaderEvent3 := fullHeaderEvent
		fullHeaderEvent3.Subject = eventDID2.String()
		fullHeaderEvent3.RawEventID = ""
		fullHeaderEvent3.Extras = map[string]any{
			"raweventid": "raw-event-id-from-extras",
		}
		indexKey3 := insertTestData(t, ctx, conn, &fullHeaderEvent3)

		mockS3Client.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				require.Equal(t, indexKey3, *params.Key)
				return &s3.GetObjectOutput{
					Body:          io.NopCloser(bytes.NewReader(eventDataEnvelope)),
					ContentLength: ref(int64(len(eventDataEnvelope))),
				}, nil
			},
		)
		opts := &grpc.SearchOptions{
			DataVersion: &wrapperspb.StringValue{Value: dataType},
			Subject:     &wrapperspb.StringValue{Value: eventDID2.String()},
		}

		retrievedEvent, err := indexService.GetLatestCloudEvent(ctx, "test-bucket", opts)
		require.NoError(t, err)

		assert.Equal(t, fullHeaderEvent3.ID, retrievedEvent.ID, "ID mismatch")
		assert.Equal(t, "raw-event-id-from-extras", retrievedEvent.RawEventID, "RawEventID field not set correctly")
		assert.Nil(t, retrievedEvent.Extras["raweventid"], "RawEventID should not be in extras")
	})
}

func ref[T any](x T) *T {
	return &x
}

// encodeTestParquet encodes the given events into a parquet byte buffer and
// returns the raw bytes and the map of event index to index key.
func encodeTestParquet(t *testing.T, events []cloudevent.RawEvent, objectKey string) ([]byte, map[int]string) {
	t.Helper()
	stored := make([]cloudevent.StoredEvent, len(events))
	for i, ev := range events {
		stored[i] = cloudevent.StoredEvent{RawEvent: ev}
	}
	var buf bytes.Buffer
	indexKeys, err := parquet.Encode(&buf, stored, objectKey)
	require.NoError(t, err)
	return buf.Bytes(), indexKeys
}

// mockS3ParquetReader sets up GetObject expectations to serve the full parquet
// file bytes (downloaded in one GET by S3ReaderAt). Returns the mock.
func mockS3ParquetReader(t *testing.T, ctrl *gomock.Controller, parquetBytes []byte) *MockObjectGetter {
	t.Helper()
	mock := NewMockObjectGetter(ctrl)
	mock.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader(parquetBytes)),
				ContentLength: ref(int64(len(parquetBytes))),
			}, nil
		},
	).AnyTimes()
	return mock
}

// TestGetCloudEventFromIndex_ParquetRef tests the parquet retrieval path.
func TestGetCloudEventFromIndex_ParquetRef(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	contractAddr := randAddress()
	tokenID := big.NewInt(999)
	subject := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: contractAddr,
		TokenID:         tokenID,
	}.String()

	// Create a test event and encode it to parquet
	testEvent := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:          "pq-event-1",
			Source:      "test-source",
			Producer:    "test-producer",
			Subject:     subject,
			Time:        now,
			Type:        cloudevent.TypeStatus,
			DataVersion: "v1.0",
		},
		Data: json.RawMessage(`{"vin":"1HGCM82633A999999"}`),
	}

	objectKey := "test-prefix/batch.parquet"
	parquetBytes, indexKeys := encodeTestParquet(t, []cloudevent.RawEvent{testEvent}, objectKey)
	require.Len(t, indexKeys, 1)
	parquetIndexKey := indexKeys[0] // e.g. "test-prefix/batch.parquet#0"

	// Insert an index row in ClickHouse pointing to the parquet ref
	indexHdr := &cloudevent.CloudEventHeader{
		ID:          testEvent.ID,
		Source:      testEvent.Source,
		Producer:    testEvent.Producer,
		Subject:     subject,
		Time:        now,
		Type:        cloudevent.TypeStatus,
		DataVersion: "v1.0",
	}
	insertTestData(t, ctx, conn, indexHdr)

	// Set up mock S3 to serve parquet range requests
	ctrl := gomock.NewController(t)
	mockS3 := mockS3ParquetReader(t, ctrl, parquetBytes)

	indexService := eventrepo.New(conn, mockS3, nil, "test-parquet-bucket", "")

	// Build the index object as GetCloudEventFromIndex expects
	index := cloudevent.CloudEvent[eventrepo.ObjectInfo]{
		CloudEventHeader: *indexHdr,
		Data:             eventrepo.ObjectInfo{Key: parquetIndexKey},
	}

	result, err := indexService.GetCloudEventFromIndex(ctx, &index, "legacy-bucket")
	require.NoError(t, err)

	// Verify the returned event
	assert.Equal(t, testEvent.ID, result.ID, "ID mismatch")
	assert.Equal(t, testEvent.Source, result.Source, "Source mismatch")
	assert.Equal(t, testEvent.Producer, result.Producer, "Producer mismatch")
	assert.Equal(t, testEvent.Subject, result.Subject, "Subject mismatch")
	assert.Equal(t, testEvent.Type, result.Type, "Type mismatch")
	assert.JSONEq(t, string(testEvent.Data), string(result.Data), "Data mismatch")
	// Tags should be normalized (empty -> [])
	assert.NotNil(t, result.Tags, "Tags should be normalized, not nil")
}

// TestListCloudEventsFromIndexes_ParquetCaching tests that multiple rows from
// the same parquet file share one S3ReaderAt (HeadObject called once).
func TestListCloudEventsFromIndexes_ParquetCaching(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	contractAddr := randAddress()
	tokenID := big.NewInt(888)
	subject := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: contractAddr,
		TokenID:         tokenID,
	}.String()

	// Create two events and encode them into one parquet file
	event0 := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:          "pq-cache-0",
			Source:      "src",
			Producer:    "prod",
			Subject:     subject,
			Time:        now.Add(-1 * time.Hour),
			Type:        cloudevent.TypeStatus,
			DataVersion: "v1.0",
		},
		Data: json.RawMessage(`{"row":0}`),
	}
	event1 := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:          "pq-cache-1",
			Source:      "src",
			Producer:    "prod",
			Subject:     subject,
			Time:        now,
			Type:        cloudevent.TypeStatus,
			DataVersion: "v1.0",
		},
		Data: json.RawMessage(`{"row":1}`),
	}

	objectKey := "cache-test/batch.parquet"
	parquetBytes, indexKeys := encodeTestParquet(t, []cloudevent.RawEvent{event0, event1}, objectKey)
	require.Len(t, indexKeys, 2)

	// Insert index rows in ClickHouse
	hdr0 := event0.CloudEventHeader
	hdr1 := event1.CloudEventHeader
	insertTestData(t, ctx, conn, &hdr0)
	insertTestData(t, ctx, conn, &hdr1)

	ctrl := gomock.NewController(t)
	mockS3 := mockS3ParquetReader(t, ctrl, parquetBytes)

	indexService := eventrepo.New(conn, mockS3, nil, "test-parquet-bucket", "")

	indexes := []cloudevent.CloudEvent[eventrepo.ObjectInfo]{
		{CloudEventHeader: hdr0, Data: eventrepo.ObjectInfo{Key: indexKeys[0]}},
		{CloudEventHeader: hdr1, Data: eventrepo.ObjectInfo{Key: indexKeys[1]}},
	}

	results, err := indexService.ListCloudEventsFromIndexes(ctx, indexes, "legacy-bucket")
	require.NoError(t, err)
	require.Len(t, results, 2)

	// Verify both events returned correctly
	assert.Equal(t, "pq-cache-0", results[0].ID)
	assert.JSONEq(t, `{"row":0}`, string(results[0].Data))
	assert.Equal(t, "pq-cache-1", results[1].ID)
	assert.JSONEq(t, `{"row":1}`, string(results[1].Data))

	// Tags normalized
	for _, r := range results {
		assert.NotNil(t, r.Tags, "Tags should be normalized")
	}
}

// TestListIndexesAdvanced tests the ListIndexesAdvanced function with various advanced filter options.
func TestListIndexesAdvanced(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)

	// Insert test data
	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	contractAddr := randAddress()
	device1TokenID := big.NewInt(1234567890)
	ctx := context.Background()
	now := time.Now()
	subject1 := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: contractAddr,
		TokenID:         device1TokenID,
	}.String()

	// Create test events with different characteristics for filtering
	eventIdx1 := &cloudevent.CloudEventHeader{
		ID:          "event-source1-producer1",
		Subject:     subject1,
		Type:        cloudevent.TypeStatus,
		Source:      "source-1",
		Producer:    "producer-1",
		DataVersion: "v1.0",
		Time:        now.Add(-3 * time.Hour),
		Tags:        []string{"vehicle", "status"},
	}

	eventIdx2 := &cloudevent.CloudEventHeader{
		ID:          "event-source2-producer2",
		Subject:     subject1,
		Type:        cloudevent.TypeFingerprint,
		Source:      "source-2",
		Producer:    "producer-2",
		DataVersion: "v2.0",
		Time:        now.Add(-2 * time.Hour),
		Tags:        []string{"security", "fingerprint"},
	}

	eventIdx3 := &cloudevent.CloudEventHeader{
		ID:          "event-source1-producer3",
		Subject:     subject1,
		Type:        cloudevent.TypeStatus,
		Source:      "source-1",
		Producer:    "producer-3",
		DataVersion: "v1.0",
		Time:        now.Add(-1 * time.Hour),
		Tags:        []string{"vehicle", "telemetry", "status"},
	}

	eventIdx4 := &cloudevent.CloudEventHeader{
		ID:          "event-source3-producer4",
		Subject:     subject1,
		Type:        cloudevent.TypeStatus,
		Source:      "source-3",
		Producer:    "producer-4",
		DataVersion: "v1.0",
		Time:        now.Add(-30 * time.Minute),
		Tags:        []string{"telemetry", "realtime"},
	}

	// Insert test data
	keyTypeStatusSource1Producer1 := insertTestData(t, ctx, conn, eventIdx1)
	keyTypeFingerprintSource2Producer2 := insertTestData(t, ctx, conn, eventIdx2)
	keyTypeStatusSource1Producer3 := insertTestData(t, ctx, conn, eventIdx3)
	keyTypeStatusSource3Producer4 := insertTestData(t, ctx, conn, eventIdx4)

	indexService := eventrepo.New(conn, nil, nil, "", "")

	tests := []struct {
		name              string
		advancedOpts      *grpc.AdvancedSearchOptions
		expectedIndexKeys []string
		expectedError     bool
	}{
		{
			name: "filter by single type status events",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Type: &grpc.StringFilterOption{
					In: []string{cloudevent.TypeStatus},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource3Producer4, keyTypeStatusSource1Producer3, keyTypeStatusSource1Producer1},
		},
		{
			name: "filter by multiple types with OR logic",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Type: &grpc.StringFilterOption{
					In: []string{cloudevent.TypeStatus, cloudevent.TypeFingerprint},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource3Producer4, keyTypeStatusSource1Producer3, keyTypeFingerprintSource2Producer2, keyTypeStatusSource1Producer1},
		},
		{
			name: "filter by type with negation",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Type: &grpc.StringFilterOption{
					NotIn: []string{cloudevent.TypeFingerprint},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource3Producer4, keyTypeStatusSource1Producer3, keyTypeStatusSource1Producer1},
		},
		{
			name: "filter by source: multiple results",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Source: &grpc.StringFilterOption{
					In: []string{"source-1"},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource1Producer3, keyTypeStatusSource1Producer1},
		},
		{
			name: "combine multiple filters (AND logic)",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Type: &grpc.StringFilterOption{
					In: []string{cloudevent.TypeStatus},
				},
				Source: &grpc.StringFilterOption{
					In: []string{"source-1"},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource1Producer3, keyTypeStatusSource1Producer1},
		},
		{
			name: "OR logic within StringFilterOption",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Type: &grpc.StringFilterOption{
					In: []string{cloudevent.TypeAttestation},
					Or: []*grpc.StringFilterOption{
						{NotIn: []string{cloudevent.TypeStatus}},
					},
				},
			},
			expectedIndexKeys: []string{keyTypeFingerprintSource2Producer2},
		},
		{
			name: "no matching records",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Type: &grpc.StringFilterOption{
					In: []string{"non-existent-type"},
				},
			},
			expectedIndexKeys: []string{},
			expectedError:     true,
		},
		{
			name: "filter by tags: has any (telemetry)",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Tags: &grpc.ArrayFilterOption{
					ContainsAny: []string{"telemetry"},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource3Producer4, keyTypeStatusSource1Producer3},
		},
		{
			name: "filter by tags: has all (vehicle AND status)",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Tags: &grpc.ArrayFilterOption{
					ContainsAll: []string{"vehicle", "status"},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource1Producer3, keyTypeStatusSource1Producer1},
		},
		{
			name: "filter by tags: has any with multiple values",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Tags: &grpc.ArrayFilterOption{
					ContainsAny: []string{"security", "realtime"},
					Or: []*grpc.ArrayFilterOption{
						{ContainsAll: []string{"vehicle", "status"}},
					},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource3Producer4, keyTypeStatusSource1Producer3, keyTypeFingerprintSource2Producer2, keyTypeStatusSource1Producer1},
		},
		{
			name: "filter by tags: negated has_any",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Tags: &grpc.ArrayFilterOption{
					NotContainsAny: []string{"vehicle"},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource3Producer4, keyTypeFingerprintSource2Producer2},
		},
		{
			name: "complex tags filter: has_any OR has_all combination",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Tags: &grpc.ArrayFilterOption{
					ContainsAny: []string{"fingerprint"},
					Or: []*grpc.ArrayFilterOption{
						{ContainsAll: []string{"telemetry", "realtime"}},
					},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource3Producer4, keyTypeFingerprintSource2Producer2},
		},
		{
			name: "complex tags filter: has_all with OR chain",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Tags: &grpc.ArrayFilterOption{
					ContainsAll: []string{"vehicle", "telemetry"},
					Or: []*grpc.ArrayFilterOption{
						{ContainsAny: []string{"security"}},
					},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource1Producer3, keyTypeFingerprintSource2Producer2},
		},
		{
			name: "complex negated tags with multiple conditions",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Tags: &grpc.ArrayFilterOption{
					NotContainsAny: []string{"fingerprint", "telemetry"},
					Or: []*grpc.ArrayFilterOption{
						{ContainsAll: []string{"telemetry", "status"}},
					},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource1Producer3, keyTypeStatusSource1Producer1},
		},
		{
			name: "complex negated tags with or override",
			advancedOpts: &grpc.AdvancedSearchOptions{
				Subject: &grpc.StringFilterOption{
					In: []string{subject1},
				},
				Tags: &grpc.ArrayFilterOption{
					ContainsAll:    []string{"vehicle", "telemetry"},
					NotContainsAny: []string{"fingerprint", "telemetry", "security"},
					Or: []*grpc.ArrayFilterOption{
						{ContainsAll: []string{"telemetry", "status"}},
					},
				},
			},
			expectedIndexKeys: []string{keyTypeStatusSource1Producer3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := indexService.ListIndexesAdvanced(t.Context(), 10, tt.advancedOpts, true)
			if tt.expectedError {
				require.Error(t, err, "Expected error but got none")
			} else {
				require.NoError(t, err, "Unexpected error: %v", err)
				require.Len(t, results, len(tt.expectedIndexKeys), "Number of results mismatch")

				// Verify each result matches expected index keys in order
				actualKeys := make([]string, len(results))
				for i, result := range results {
					actualKeys[i] = result.Data.Key
				}

				require.Equal(t, tt.expectedIndexKeys, actualKeys, "Index keys mismatch")
			}
		})
	}
}

// TestGetCloudEventTypeSummaries tests the GetCloudEventTypeSummaries function.
func TestGetCloudEventTypeSummaries(t *testing.T) {
	t.Parallel()
	chContainer := setupClickHouseContainer(t)

	conn, err := chContainer.GetClickHouseAsConn()
	require.NoError(t, err)
	ctx := context.Background()

	contractAddr := randAddress()
	deviceTokenID := big.NewInt(555555)
	subject := cloudevent.ERC721DID{
		ChainID:         153,
		ContractAddress: contractAddr,
		TokenID:         deviceTokenID,
	}
	subjectStr := subject.String()

	now := time.Now().UTC().Truncate(time.Second)

	// Insert 3 status events and 1 fingerprint event
	status1 := &cloudevent.CloudEventHeader{
		Subject: subjectStr,
		Type:    cloudevent.TypeStatus,
		Source:  "source-A",
		Time:    now.Add(-3 * time.Hour),
	}
	status2 := &cloudevent.CloudEventHeader{
		Subject: subjectStr,
		Type:    cloudevent.TypeStatus,
		Source:  "source-A",
		Time:    now.Add(-2 * time.Hour),
	}
	status3 := &cloudevent.CloudEventHeader{
		Subject: subjectStr,
		Type:    cloudevent.TypeStatus,
		Source:  "source-B",
		Time:    now.Add(-1 * time.Hour),
	}
	fp1 := &cloudevent.CloudEventHeader{
		Subject: subjectStr,
		Type:    cloudevent.TypeFingerprint,
		Source:  "source-A",
		Time:    now.Add(-30 * time.Minute),
	}

	insertTestData(t, ctx, conn, status1)
	insertTestData(t, ctx, conn, status2)
	insertTestData(t, ctx, conn, status3)
	insertTestData(t, ctx, conn, fp1)

	indexService := eventrepo.New(conn, nil, nil, "", "")

	t.Run("no filter returns all types", func(t *testing.T) {
		opts := &grpc.SearchOptions{
			Subject: &wrapperspb.StringValue{Value: subjectStr},
		}
		summaries, err := indexService.GetCloudEventTypeSummaries(ctx, opts)
		require.NoError(t, err)
		require.Len(t, summaries, 2)

		// Results ordered by event_type; fingerprint < status alphabetically
		fpSummary := summaries[0]
		assert.Equal(t, cloudevent.TypeFingerprint, fpSummary.Type)
		assert.Equal(t, uint64(1), fpSummary.Count)
		assert.Equal(t, fp1.Time.UTC().Truncate(time.Second), fpSummary.FirstSeen.UTC().Truncate(time.Second))
		assert.Equal(t, fp1.Time.UTC().Truncate(time.Second), fpSummary.LastSeen.UTC().Truncate(time.Second))

		statusSummary := summaries[1]
		assert.Equal(t, cloudevent.TypeStatus, statusSummary.Type)
		assert.Equal(t, uint64(3), statusSummary.Count)
		assert.Equal(t, status1.Time.UTC().Truncate(time.Second), statusSummary.FirstSeen.UTC().Truncate(time.Second))
		assert.Equal(t, status3.Time.UTC().Truncate(time.Second), statusSummary.LastSeen.UTC().Truncate(time.Second))
	})

	t.Run("type filter", func(t *testing.T) {
		opts := &grpc.SearchOptions{
			Subject: &wrapperspb.StringValue{Value: subjectStr},
			Type:    &wrapperspb.StringValue{Value: cloudevent.TypeStatus},
		}
		summaries, err := indexService.GetCloudEventTypeSummaries(ctx, opts)
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		assert.Equal(t, cloudevent.TypeStatus, summaries[0].Type)
		assert.Equal(t, uint64(3), summaries[0].Count)
	})

	t.Run("time range filter", func(t *testing.T) {
		opts := &grpc.SearchOptions{
			Subject: &wrapperspb.StringValue{Value: subjectStr},
			After:   &timestamppb.Timestamp{Seconds: now.Add(-2*time.Hour - 30*time.Second).Unix()},
			Before:  &timestamppb.Timestamp{Seconds: now.Add(-30*time.Minute + 30*time.Second).Unix()},
		}
		summaries, err := indexService.GetCloudEventTypeSummaries(ctx, opts)
		require.NoError(t, err)
		// Should include status2 (-2h), status3 (-1h), fp1 (-30m)
		require.Len(t, summaries, 2)

		fpSummary := summaries[0]
		assert.Equal(t, cloudevent.TypeFingerprint, fpSummary.Type)
		assert.Equal(t, uint64(1), fpSummary.Count)

		statusSummary := summaries[1]
		assert.Equal(t, cloudevent.TypeStatus, statusSummary.Type)
		assert.Equal(t, uint64(2), statusSummary.Count)
	})

	t.Run("source filter", func(t *testing.T) {
		opts := &grpc.SearchOptions{
			Subject: &wrapperspb.StringValue{Value: subjectStr},
			Source:  &wrapperspb.StringValue{Value: "source-B"},
		}
		summaries, err := indexService.GetCloudEventTypeSummaries(ctx, opts)
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		assert.Equal(t, cloudevent.TypeStatus, summaries[0].Type)
		assert.Equal(t, uint64(1), summaries[0].Count)
	})

	t.Run("no matching events returns empty slice", func(t *testing.T) {
		otherSubject := cloudevent.ERC721DID{
			ChainID:         153,
			ContractAddress: contractAddr,
			TokenID:         big.NewInt(999999),
		}
		opts := &grpc.SearchOptions{
			Subject: &wrapperspb.StringValue{Value: otherSubject.String()},
		}
		summaries, err := indexService.GetCloudEventTypeSummaries(ctx, opts)
		require.NoError(t, err)
		require.Empty(t, summaries)
	})
}

func randAddress() common.Address {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		log.Fatalf("Failed to generate private key: %v", err)
	}
	return crypto.PubkeyToAddress(privateKey.PublicKey)
}
