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
		Region:           settings.S3AWSRegion,
		HTTPClient:       awshttp.NewBuildableClient().WithTimeout(s3RequestTimeout),
		RetryMaxAttempts: 3,
	}
	// Static creds when provided; otherwise fall through to the AWS default chain (IRSA /
	// instance profile), matching the DuckDB-secret credential_chain fallback — so the
	// aws-sdk blob/presign path doesn't 403 while the lake path works on a keyless deploy.
	if settings.S3AWSAccessKeyID != "" && settings.S3AWSSecretAccessKey != "" {
		conf.Credentials = credentials.NewStaticCredentialsProvider(
			settings.S3AWSAccessKeyID, settings.S3AWSSecretAccessKey, "")
	}
	// A custom endpoint (MinIO and friends) needs path-style addressing — the
	// virtual-hosted bucket-as-subdomain form AWS defaults to doesn't resolve
	// against a single-host store.
	if settings.S3Endpoint != "" {
		return s3.NewFromConfig(conf, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(settings.S3Endpoint)
			o.UsePathStyle = true
		})
	}
	return s3.NewFromConfig(conf)
}
