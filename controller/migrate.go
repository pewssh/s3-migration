package controller

import (
	"context"
	"errors"
	"fmt"
	"github.com/0chain/s3migration/model"
	"github.com/0chain/s3migration/s3"
	s3svc "github.com/0chain/s3migration/s3/service"
	"strings"
	"sync"

	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/aws/aws-sdk-go/aws/session"
)

const Batch = 10
const (
	Replace   = iota //Will replace existing file
	Skip             // Will skip migration if file already exists
	Duplicate        // Will add _copy prefix and uploads the file
)

var migration Migration = Migration{}
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
	allocation *sdk.Allocation
	s3Service s3.S3

	//Slice of map of bucket name and prefix. If prefix is empty string then every object will be uploaded.
	buckets      []bucket //{"bucket1": "prefix1"}
	bucketStates []map[string]*MigrationState

	resume bool
	skip   int

	//Number of goroutines to run. So at most concurrency * Batch goroutines will run. i.e. for bucket level and object level
	concurrency int
	encrypt     bool
}

func InitMigration(allocation *sdk.Allocation, sess *session.Session, appConfig *model.AppConfig) error {
	s3Service := s3svc.NewService(sess)
	migration.s3Service = s3Service
	migration.allocation = allocation
	migration.resume = appConfig.Resume
	migration.encrypt = appConfig.Encrypt
	migration.skip = appConfig.Skip
	migration.concurrency = appConfig.Concurrency


	if appConfig.Buckets == nil{
		// list all buckets form s3 and append them to migration.buckets

	} else {
		for _, bkt := range appConfig.Buckets {
			res := strings.Split(bkt, ":")
			l := len(res)
			if l < 1 || l > 2 {
				return fmt.Errorf("bucket flag has fields less than 1 or greater than 2. Arg \"%v\"", bkt)
			}

			bucketName := res[0]
			var prefix string

			if l == 2 {
				prefix = res[1]
			}

			region := migration.s3Service.GetBucketRegion(context.Background(), bucketName)

			migration.buckets = append(migration.buckets, bucket{
				name:   bucketName,
				prefix: prefix,
				region: region,
			})
		}
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
		return errors.New("migration is not initialized")
	}

	limitCh := make(chan struct{}, migration.concurrency)
	wg := sync.WaitGroup{}
	//Now you have all you need; Start to migrate
	for _, bkt := range migration.buckets {

		limitCh <- struct{}{}

		wg.Add(1)

		go func(bkt bucket) {
			defer func() {
				<-limitCh
				wg.Done()
			}()

			//Now here you can run migration in batch for each bucket. Limit this for loop by concurrency number. Make batch to some number, 10 may be good.
			//

		}(bkt)

	}
	fmt.Println("Waiting for all goroutine to complete")
	wg.Wait()

	return nil
}
