package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/0chain/s3migration/s3"
	"github.com/0chain/s3migration/util"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
	"log"
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

func (s *Service) getObjects(ctx context.Context, bucketName, objectPrefix, location string, maxKeys int32) {
	client := s.getClient(location)

	params := &awsS3.ListObjectsV2Input{
		Bucket: &bucketName,
	}
	if len(objectPrefix) != 0 {
		params.Prefix = &objectPrefix
	}
	p := awsS3.NewListObjectsV2Paginator(client, params, func(o *awsS3.ListObjectsV2PaginatorOptions) {
		if v := maxKeys; v != 0 {
			o.Limit = v
		}
	})

	var i int
	log.Println("Objects:")

	for p.HasMorePages() {
		i++

		// Next Page takes a new context for each page retrieval. This is where
		// you could add timeouts or deadlines.
		page, err := p.NextPage(ctx)
		if err != nil {
			log.Fatalf("failed to get page %v, %v", i, err)
		}

		// Log the objects found
		for _, obj := range page.Contents {
			fmt.Println("Object:", *obj.Key, *obj.LastModified, obj.Size)
		}
	}
}
