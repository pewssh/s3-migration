package s3

import (
	"context"
)

type Bucket struct {
	Name     string
	Location string
}

type S3 interface {
	ListAllBuckets(ctx context.Context) ([]string, error)
	GetBucketRegion(ctx context.Context, bucketList []string) ([]Bucket, error)
}
