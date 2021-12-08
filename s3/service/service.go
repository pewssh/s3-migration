package service

import (
	"context"
	"errors"
	"github.com/0chain/s3migration/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
	"log"
	"sync"

	"github.com/0chain/s3migration/model"
)

var existingFiles []model.FileRef

func SetExistingFileList(dStorageFiles []model.FileRef) {
	existingFiles = dStorageFiles
}

type Service struct {
	clientMap map[string]*awsS3.Client
}

func NewService(region string) *Service {
	clientMap := make(map[string]*awsS3.Client)
	if len(region) == 0 {
		region = "us-east-1"
	}

	var cfg aws.Config
	cfg, err := awsConfig.LoadDefaultConfig(context.Background())
	if err != nil {
		panic("configuration error " + err.Error() + "region: " + region)
	}

	cfg.Region = region
	client := awsS3.NewFromConfig(cfg)
	clientMap[region] = client

	return &Service{
		clientMap: clientMap,
	}
}

func (s *Service) InitClientWithRegion(region string) {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background())
	if err != nil {
		panic("configuration error " + err.Error() + "region: " + region)
	}
	cfg.Region = region
	client := awsS3.NewFromConfig(cfg)
	s.clientMap[region] = client
}

func (s *Service) ListAllBuckets(ctx context.Context) ([]string, error) {
	var err error
	var bucketList []string
	g, groupCtx := errgroup.WithContext(ctx)
	mutex := &sync.Mutex{}
	for _, c := range s.clientMap {
		client := c
		g.Go(func() error {
			buckets, err := client.ListBuckets(groupCtx, &awsS3.ListBucketsInput{})
			if err != nil {
				return err
			}

			mutex.Lock()
			for _, b := range buckets.Buckets {
				if b.Name != nil {
					bucketList = append(bucketList, *b.Name)
				}
			}
			mutex.Unlock()
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return bucketList, err
	}

	return bucketList, nil
}

func (s *Service) getBucketRegion(ctx context.Context, bucketName string, client *awsS3.Client) (string, error) {
	locationInfo, err := client.GetBucketLocation(ctx, &awsS3.GetBucketLocationInput{
		Bucket: &bucketName,
	})
	if err != nil {
		return "", err
	}

	return string(locationInfo.LocationConstraint), nil
}

func (s *Service) getClient(location string) *awsS3.Client {
	if location == "" {
		for _, c := range s.clientMap {
			if c != nil {
				return c
			}
		}
	} else {
		if c, ok := s.clientMap[location]; ok {
			return c
		}
	}

	panic("client not initialized")
}

func (s *Service) GetBucketRegion(ctx context.Context, bucketList []string) ([]s3.Bucket, error) {
	client := s.getClient("")

	var bucketLocationData []s3.Bucket

	var locErr []struct{
		err error
		bucketName string
	}
	g, groupCtx := errgroup.WithContext(ctx)
	mutex := &sync.Mutex{}
	for _, b := range bucketList {
		bucket := b
		g.Go(func() error {
			locDetail, err := client.GetBucketLocation(groupCtx, &awsS3.GetBucketLocationInput{Bucket: &bucket})
			if err != nil {
				locErr = append(locErr, struct {
					err        error
					bucketName string
				}{err: err, bucketName: bucket})
				return nil
			}

			location := string(locDetail.LocationConstraint)
			mutex.Lock()
			bucketLocationData = append(bucketLocationData, s3.Bucket{
				Name:     bucket,
				Location: location,
			})

			if _, ok := s.clientMap[location]; !ok {
				s.InitClientWithRegion(location)
			}
			mutex.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return bucketLocationData, err
	}

	if len(locErr) > 0 {
		for _, loc := range locErr {
			log.Printf("errors fetching loc data bucketName: %s, err : %+v", loc.bucketName, loc.err)
		}
	}

	if len(locErr) == len(bucketList) {
		return bucketLocationData, errors.New("error fetching location Data")
	}

	return bucketLocationData, nil
}

func (s *Service) ListFilesInBucket(ctx context.Context, opts model.ListFileOptions) (map[string]int64, error) {
	client := s.getClient(opts.Region)
	bucketFiles := map[string]int64{}

	log.Println("contents of bucket : ", opts.Bucket)

	listObjectsInput := &awsS3.ListObjectsV2Input{
		Bucket: &opts.Bucket,
	}
	if len(opts.Prefix) != 0 {
		listObjectsInput.Prefix = &opts.Prefix
	}

	maxKeys := int32(1000)
	pageNumber := 0

	listObjectsPaginator := awsS3.NewListObjectsV2Paginator(client, listObjectsInput, func(o *awsS3.ListObjectsV2PaginatorOptions) {
		if v := maxKeys; v != 0 {
			o.Limit = v
		}
	})

	for listObjectsPaginator.HasMorePages() {
		pageNumber++
		page, err := listObjectsPaginator.NextPage(ctx)
		if err != nil {
			log.Fatalf("failed to get page %v, %v", pageNumber, err)
		}

		for _, obj := range page.Contents {
			opts.FileQueue <- model.FileRef{
				Bucket: opts.Bucket,
				Region: opts.Region,
				Name:   aws.ToString(obj.Key),
				Size: 	obj.Size,
				ModifiedTime: aws.ToTime(obj.LastModified).Unix(),
				UploadType: "later", //regular, replace, rename
			}
		}

	}

	return bucketFiles, nil
}
