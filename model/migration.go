package model

type AppConfig struct {
	Skip          int
	Resume        bool
	Concurrency   int
	Buckets       []string
	Region        string
	MigrateToPath string
	Encrypt       bool
}
