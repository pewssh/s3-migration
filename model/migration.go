package model

import "sync"

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
	ModifiedTime int64
}

type ListFileOptions struct {
	Bucket    string
	Prefix    string
	Region        string
	FileQueue chan FileRef
	WaitGroup *sync.WaitGroup
}
