package service

import (
	"context"
	"github.com/aws/aws-sdk-go/aws/session"
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
	panic("implement me")
}

func (s *Service) GetBucketRegion(ctx context.Context, bucketName string) string {
	panic("implement me")
}


