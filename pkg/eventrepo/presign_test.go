package eventrepo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/DIMO-Network/dq/pkg/eventrepo"
)

func TestPresignBlobURL(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mockPresigner := NewMockPresigner(ctrl)

	const (
		bucket      = "test-bucket"
		key         = "cloudevent/blobs/some-scan.bin"
		expectedURL = "https://s3.amazonaws.com/test-bucket/cloudevent/blobs/some-scan.bin?X-Amz-Signature=abc123"
	)

	svc := eventrepo.New(nil, nil, mockPresigner, "", bucket)

	mockPresigner.EXPECT().
		PresignGetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
			assert.Equal(t, bucket, *params.Bucket)
			assert.Equal(t, key, *params.Key)

			// Verify the TTL is applied.
			opts := s3.PresignOptions{}
			for _, fn := range optFns {
				fn(&opts)
			}
			assert.Equal(t, 15*time.Minute, opts.Expires)

			return &v4.PresignedHTTPRequest{URL: expectedURL}, nil
		})

	url, err := svc.PresignBlobURL(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, expectedURL, url)
}

func TestPresignBlobURL_PresignerError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mockPresigner := NewMockPresigner(ctrl)

	svc := eventrepo.New(nil, nil, mockPresigner, "", "test-bucket")

	mockPresigner.EXPECT().
		PresignGetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("signing failure"))

	_, err := svc.PresignBlobURL(context.Background(), "cloudevent/blobs/test.bin")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signing failure")
}

func TestPresignBlobURL_NilPresigner(t *testing.T) {
	t.Parallel()
	svc := eventrepo.New(nil, nil, nil, "", "test-bucket")

	_, err := svc.PresignBlobURL(context.Background(), "cloudevent/blobs/test.bin")
	require.Error(t, err)
}

func TestPresignBlobURL_NoBucket(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	mockPresigner := NewMockPresigner(ctrl)

	svc := eventrepo.New(nil, nil, mockPresigner, "", "")

	_, err := svc.PresignBlobURL(context.Background(), "cloudevent/blobs/test.bin")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "blob bucket not configured")
}
