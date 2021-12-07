package s3

import (
	"context"
)

type S3 interface {
	ListAllBuckets(ctx context.Context) ([]string, error)
}
