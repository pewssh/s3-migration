package migration

import (
	"context"
	"fmt"

	dStorage "github.com/0chain/s3migration/dstorage"
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

type Migration struct {
	zStore   dStorage.DStoreI
	awsStore s3.AwsI

	//Slice of map of bucket name and prefix. If prefix is empty string then every object will be uploaded.
	//If buckets is empty; migrate all buckets
	bucketStates []map[string]*MigrationState

	skip       int
	retryCount int

	//Number of goroutines to run. So at most concurrency * Batch goroutines will run. i.e. for bucket level and object level
	concurrency int
}

func InitMigration(mConfig *MigrationConfig) error {
	dStorageService, err := dStorage.GetDStorageService(mConfig.AllocationID, mConfig.MigrateToPath, mConfig.Encrypt, mConfig.WhoPays)
	if err != nil {
		return err
	}

	awsStorageService, err := s3.GetAwsClient(
		mConfig.Bucket,
		mConfig.Prefix,
		mConfig.Region,
		mConfig.DeleteSource,
		mConfig.NewerThan,
		mConfig.OlderThan,
		mConfig.StartAfter,
	)
	if err != nil {
		return err
	}

	migration = Migration{
		zStore:      dStorageService,
		awsStore:    awsStorageService,
		skip:        mConfig.Skip,
		concurrency: mConfig.Concurrency,
		retryCount:  mConfig.RetryCount,
	}

	rootContext, rootContextCancel = context.WithCancel(context.Background())

	isMigrationInitialized = true

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

func Migrate() error {
	defer rootContextCancel()

	if !isMigrationInitialized {
		return fmt.Errorf("migration is not initialized")
	}

	return nil
}
