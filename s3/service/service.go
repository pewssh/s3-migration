package service

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
)

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

	log.Println("listAllBuckets list buckets OK")

	buckets := make([]string, 0)
	for _, b := range result.Buckets {
		log.Println(b.Name)
		buckets = append(buckets, aws.StringValue(b.Name))
	}

	return buckets, nil
}
