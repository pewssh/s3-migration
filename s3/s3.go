package s3

import (
	"context"
	"github.com/0chain/s3migration/model"
)

type S3 interface {
	ListAllBuckets(ctx context.Context) ([]string, error)
	ListFilesInBucket(ctx context.Context, opts model.ListFileOptions) (map[string]int64, error)
}
