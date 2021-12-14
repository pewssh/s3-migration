package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/0chain/s3migration/dstorage"
	"github.com/0chain/s3migration/s3"
)

const Batch = 10
const (
	Replace   = iota //Will replace existing file
	Skip             // Will skip migration if file already exists
	Duplicate        // Will add _copy prefix and uploads the file
)

var migration Migration
var isMigrationInitialized bool

//Use context for all requests.
var rootContext context.Context
var rootContextCancel context.CancelFunc

var StateFilePath = func(homeDir, bucketName string) string {
	return fmt.Sprintf("%v/.aws/%v.state", homeDir, bucketName)
}

func abandonAllOperations() {
	rootContextCancel()
}

type bucket struct {
	name   string
	region string
	prefix string
}

type Migration struct {
	allocation      *sdk.Allocation
	s3Service       s3.S3
	dStorageService dstorage.DStorage

	//Slice of map of bucket name and prefix. If prefix is empty string then every object will be uploaded.
	//If buckets is empty; migrate all buckets
	buckets      []bucket //{"bucket1": "prefix1"}
	bucketStates []map[string]*MigrationState

	resume     bool
	skip       int
	retryCount int

	//Number of goroutines to run. So at most concurrency * Batch goroutines will run. i.e. for bucket level and object level
	concurrency          int
	whoPays              int
	encrypt              bool
	newerThan, olderThan time.Time
	ctx                  context.Context
}

func InitMigration(appConfig *MigrationConfig) error {

	//init sdk
	//get allocation
	//get s3 client
	migration = Migration{
		whoPays:     appConfig.WhoPays,
		encrypt:     appConfig.Encrypt,
		resume:      appConfig.Resume,
		skip:        appConfig.Skip,
		concurrency: appConfig.Concurrency,
		retryCount:  appConfig.RetryCount,
		newerThan:   appConfig.NewerThan,
		olderThan:   appConfig.OlderThan,
	}

	rootContext, rootContextCancel = context.WithCancel(context.Background())

	isMigrationInitialized = true

	return nil
}

func Migrate() error {
	defer rootContextCancel()

	if !isMigrationInitialized {
		return fmt.Errorf("migration is not initialized")
	}
	return nil
}

type objectUploadStatus struct {
	name       string
	isUploaded <-chan struct{}
	errCh      <-chan error
}

//MigrationState is state for each bucket.
type MigrationState struct {
	bucketName string

	uploadsInProgress [Batch]objectUploadStatus //Array that holds each objects status on successful upload or error

	//Migration is done in sorted order.
	//This key provides information that upto this key all the files are migrated.
	uptoKey string
}

func (ms *MigrationState) cleanBatch() {
	var errorNoticed bool
	for i := 0; i < Batch; i++ {
		oups := ms.uploadsInProgress[i]
		select {
		case <-oups.isUploaded:
			//Successfully uploaded
		case <-oups.errCh:
			errorNoticed = true
			//error occurred.
		}
	}

	if errorNoticed {
		//Stop migration from this bucket.
		//Save state in some file
	}
}

func (ms *MigrationState) saveState() {
	//Write its state to the file
	//Check objectUploadStatus
}

type MigrationConfig struct {
	AllocationID  string
	Skip          int
	Resume        bool
	Concurrency   int
	Buckets       []string
	Region        string
	MigrateToPath string
	WhoPays       int
	Encrypt       bool
	RetryCount    int
	NewerThan     time.Time
	OlderThan     time.Time
}
