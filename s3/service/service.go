package service

import (
	"context"
	"fmt"
	"github.com/0chain/s3migration/model"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
)

var existingFiles []model.FileRef

func SetExistingFileList(dStorageFiles []model.FileRef) {
	existingFiles = dStorageFiles
}

type Service struct {
	sess *session.Session
}

func NewService(sess *session.Session) *Service {
	return &Service{
		sess: sess,
	}
}

func (s *Service) ListAllBuckets(ctx context.Context) ([]string, error) {
	svc := s3.New(s.sess)
	result, err := svc.ListBuckets(nil)
	if err != nil {
		log.Println("Unable to list buckets, %v" + err.Error())
		return nil, err
	}

	buckets := make([]string, 0)
	for _, b := range result.Buckets {
		buckets = append(buckets, aws.StringValue(b.Name))
	}

	return buckets, nil
}

func (s *Service) ListFilesInBucket(ctx context.Context, opts model.ListFileOptions) (map[string]int64, error) {
	svc := s3.New(s.sess)

	bucketFiles := map[string]int64{}

	log.Println("contents of bucket : ", opts.Bucket)

	err := svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{Bucket: aws.String(opts.Bucket), Prefix: aws.String(opts.Prefix)}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range page.Contents {
			if aws.Int64Value(item.Size) == 0 {
				continue
			}
			//count++
			//log.Println(count,":", aws.StringValue(item.Key))
			//remoteFilePath := fmt.Sprintf("/%s/%s", bucket, aws.StringValue(item.Key))
			//bucketFiles[remoteFilePath] = aws.Int64Value(item.Size)

			//todo: compare with existing files and manage conflicts (skip, replace, rename)

			opts.WaitGroup.Add(1)
			opts.FileQueue <- model.FileRef{
				Name:       fmt.Sprintf("/%s/%s", opts.Bucket, aws.StringValue(item.Key)),
				Size:       aws.Int64Value(item.Size),
				UploadType: "later", //regular, replace, rename
			}
		}

		return true
	})
	if err != nil {
		return nil, err
	}

	return bucketFiles, nil
}
