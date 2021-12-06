package service

import (
	"context"
	"github.com/0chain/s3migration/s3"
	"github.com/0chain/s3migration/util"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
	"sync"
)

type Service struct {
	clientMap map[string]*awsS3.Client
}

func NewService(awsAccessKey, awsSecretKey string, regions ...string) *Service {
	err := util.SetAwsEnvCredentials(awsAccessKey, awsSecretKey)
	if err != nil {
		panic("setting aws cred, " + err.Error())
	}

	clientMap := make(map[string]*awsS3.Client)
	if len(regions) == 0 {
		regions = []string{"us-east-1"}
	}
	for _, region := range regions {
		var cfg aws.Config
		cfg, err = awsConfig.LoadDefaultConfig(context.TODO())
		if err != nil {
			panic("configuration error " + err.Error() + "region: " + region)
		}
		cfg.Region = region
		client := awsS3.NewFromConfig(cfg)
		clientMap[region] = client
	}

	return &Service{
		clientMap: clientMap,
	}
}

func (s *Service) InitClientWithRegion(region string) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO())
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

func (s *Service) GetBucketRegion(ctx context.Context, bucketList []string) ([]s3.Bucket, error) {
	var client *awsS3.Client
	for _, c := range s.clientMap {
		if c != nil {
			client = c
			break
		}
	}

	if client == nil {
		panic("client not initialized")
	}

	var bucketLocationData []s3.Bucket

	g, groupCtx := errgroup.WithContext(ctx)
	mutex := &sync.Mutex{}
	for _, b := range bucketList {
		bucket := b
		g.Go(func() error {
			locDetail, err := client.GetBucketLocation(groupCtx, &awsS3.GetBucketLocationInput{Bucket: &bucket})
			if err != nil {
				return err
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

	return bucketLocationData, nil
}
