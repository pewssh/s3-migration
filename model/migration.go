package model

type AppConfig struct {
	Skip          int
	Resume        bool
	Concurrency   int
	Buckets       []string
	MigrateToPath string
	Encrypt       bool
}
