package migration

import "time"

type MigrationConfig struct {
	AllocationID  string
	Skip          int
	Resume        bool
	Concurrency   int
	Buckets       [][2]string
	Region        string
	MigrateToPath string
	WhoPays       int
	Encrypt       bool
	RetryCount    int
	NewerThan     time.Time
	OlderThan     time.Time
	DeleteSource  bool
}
