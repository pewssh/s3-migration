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
	ModifiedAt time.Time
}

type ListFileOptions struct {
	Bucket    string
	Prefix    string
	FileQueue chan FileRef
	WaitGroup *sync.WaitGroup
}
