package model

import (
	"sync"
	"time"
)

type AppConfig struct {
	Skip          int
	Resume        bool
	Concurrency   int
	Buckets       []string
	Region        string
	MigrateToPath string
	Encrypt       bool
}

type FileRef struct {
	Name       string
	Size       int64
	UploadType string
	Bucket  string
	Region string
	ModifiedAt time.Time
}

type ListFileOptions struct {
	Bucket    string
	Prefix    string
	Region        string
	FileQueue chan FileRef
	WaitGroup *sync.WaitGroup
}
