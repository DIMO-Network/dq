package app

import (
	"time"

	"github.com/DIMO-Network/dq/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3RequestTimeout bounds every S3 request (the fetch presign/download and the
// materializer blob GET). Without it a black-holed connection (TCP established, body
// never arrives) hangs the caller forever — fatal for the single-writer materializer,
// whose blob resolution runs on a loop context that only cancels at shutdown, so a
// stalled GET would silently wedge decode fleet-wide.
const s3RequestTimeout = 30 * time.Second

func s3ClientFromSettings(settings *config.Settings) *s3.Client {
	conf := aws.Config{
		Region: settings.S3AWSRegion,
		Credentials: credentials.NewStaticCredentialsProvider(
			settings.S3AWSAccessKeyID,
			settings.S3AWSSecretAccessKey,
			"",
		),
		HTTPClient:       awshttp.NewBuildableClient().WithTimeout(s3RequestTimeout),
		RetryMaxAttempts: 3,
	}
	return s3.NewFromConfig(conf)
}
