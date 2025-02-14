package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/0chain/s3migration/migration"
	"github.com/spf13/cobra"

	zlogger "github.com/0chain/s3migration/logger"
	"github.com/0chain/s3migration/util"
)

var (
	allocationId               string
	accessKey, secretKey       string
	bucket                     string
	prefix                     string
	region                     string
	migrateToPath              string
	duplicateSuffix            string
	concurrency                int
	encrypt                    bool
	resume                     bool
	skip                       int // 0 --> Replace; 1 --> Skip; 2 --> Duplicate
	allocationTextPath         string
	newerThanStr, olderThanStr string
	awsCredPath                string
	retryCount                 int
	deleteSource               bool
	workDir                    string
	chunkSize                  int64
	chunkNumber                int
	batchSize                  int
	source                     string
	accessToken                string // if source is google drive or dropbox
)

// migrateCmd is the migrateFromS3 sub command to migrate whole objects from some buckets.
func init() {
	rootCmd.AddCommand(migrateCmd)

	//flags related to dStorage
	migrateCmd.PersistentFlags().StringVar(&allocationId, "allocation", "", "allocation ID for dStorage")
	migrateCmd.Flags().StringVar(&allocationTextPath, "alloc-path", "", "File Path to allocation text")
	migrateCmd.Flags().BoolVar(&encrypt, "encrypt", false, "pass this option to encrypt and upload the file")
	//flags related to s3
	migrateCmd.PersistentFlags().StringVar(&accessKey, "access-key", "", "access-key of aws")
	migrateCmd.PersistentFlags().StringVar(&secretKey, "secret-key", "", "secret-key of aws")
	migrateCmd.Flags().StringVar(&awsCredPath, "aws-cred-path", "", "File Path to aws credentials")
	migrateCmd.PersistentFlags().StringVar(&bucket, "bucket", "", "Bucket to migrate")
	migrateCmd.MarkFlagRequired("bucket")
	migrateCmd.PersistentFlags().StringVar(&prefix, "prefix", "", "Migrate objects starting with this prefix")
	migrateCmd.PersistentFlags().StringVar(&region, "region", "us-east-2", "Bucket location")
	migrateCmd.Flags().StringVar(&migrateToPath, "migrate-to", "/", "Remote path where bucket's objects will be migrated to")
	migrateCmd.Flags().StringVar(&duplicateSuffix, "dup-suffix", "_copy", "Duplicate suffix to use for migrated file")
	migrateCmd.Flags().BoolVar(&deleteSource, "delete-source", false, "Delete object in s3 that is migrated to dStorage")
	migrateCmd.Flags().StringVar(&workDir, "wd", filepath.Join(util.GetHomeDir(), ".s3migration"), "Working directory")
	migrateCmd.Flags().IntVar(&concurrency, "concurrency", 10, "number of concurrent files to process concurrently during migration")
	migrateCmd.Flags().BoolVar(&resume, "resume", false, "pass this option to resume migration from previous state")
	migrateCmd.Flags().IntVar(&skip, "skip", 1, "0 --> Replace existing files; 1 --> Skip migration; 2 --> Duplicate with timestamp attached at the end of file name")
	migrateCmd.Flags().IntVar(&retryCount, "retry", 3, "retry count for upload to dstorage")
	migrateCmd.Flags().StringVar(&newerThanStr, "newer-than", "", "eg; 7d10h --> migrate objects that is newer than 7 days 10 hours")
	migrateCmd.Flags().StringVar(&olderThanStr, "older-than", "", "eg; 7d10h --> migrate objects that is older than 7 days 10 hours")
	migrateCmd.Flags().Int64Var(&chunkSize, "chunk-size", 50*1024*1024, "chunk size in bytes")
	migrateCmd.Flags().IntVar(&chunkNumber, "chunk-number", 250, "number of chunks to upload")
	migrateCmd.Flags().IntVar(&batchSize, "batch-size", 20, "number of files to upload in a batch")
	migrateCmd.Flags().StringVar(&source, "source", "s3", "s3 or google_drive or dropbox")
	migrateCmd.Flags().StringVar(&accessToken, "access-token", "", "access token for google drive or dropbox")
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate user data from S3 bucket to dStorage",
	Long: `Migrate files from s3 buckets to some remote directory(default is /) by using aws-s3-sdk and 0chain gosdk.
	All the objects from bucket will be migrated. However user can specify some prefix to migrate only the files with
	those prefix. Also if there is name conflict within dStorage file and bucket file use can specify whether to skip,
	replace or duplicate them. Migration state is maintained is some file so user can also resume migration operation
	if some error had occurred in previous migration session. User can also specify whether to delete migrated file.
	Note the defaults.

	Note: Parameters passed with flag are first priority, with environment variable are second priority and with file path
	are third priority.

	Note: Addition of new object or modification of existing file while migrating is not recommended, as it cannot track
	such changes and you might loose your data.
	`,
	RunE: func(cmd *cobra.Command, args []string) error {
		_ = cmd.Flags().Parse(args)
		zlogger.Logger.Info("S3 migration started")
		var err error
		if allocationId == "" {
			if allocationId = util.GetAllocationIDFromEnv(); allocationId == "" {
				if allocationTextPath == "" {
					return errors.New("allocation id is missing")
				}

				allocationId, err = util.GetAllocationIDFromFile(allocationTextPath)
				if err != nil {
					return err
				}

				if allocationId == "" {
					return errors.New("allocation id is missing")
				}
			}
		}

		if source == "" {
			source = "s3"
		}

		if (accessKey == "" || secretKey == "") && source == "s3" {
			if accessKey, secretKey = util.GetAwsCredentialsFromEnv(); accessKey == "" || secretKey == "" {
				if awsCredPath == "" {
					return errors.New("aws credentials missing")
				}
				if accessKey, secretKey = util.GetAwsCredentialsFromFile(awsCredPath); accessKey == "" || secretKey == "" {
					return fmt.Errorf("empty access or secret key. Access Key:%v\tSecret Key: %v", accessKey, secretKey)
				}
			}
		}

		if accessToken == "" {
			if accessToken = util.GetAccessToken(); accessToken == "" {
				return errors.New("Missing Access Token")
			}
		}
		if bucket == "" && source == "s3" {
			bucket, region, prefix, err = util.GetBucketRegionPrefixFromFile(awsCredPath)
			if err != nil {
				return err
			}
		}

		if skip < 0 || skip > 2 {
			return fmt.Errorf("skip value not in range 0-2. Provided value is %v", skip)
		}

		if duplicateSuffix != "" {
			if strings.Contains(duplicateSuffix, "/") {
				return fmt.Errorf("duplicate suffix cannot have path delimiter")
			}
		} else {
			duplicateSuffix = "_copy"
		}

		var newerThanPtr *time.Time
		if newerThanStr != "" {
			timestampInt64, err := strconv.ParseInt(newerThanStr, 10, 64)
			if err != nil {
				return err
			}
			newerThan := time.Unix(timestampInt64, 0)
			newerThanPtr = &newerThan
		}

		var olderThanPtr *time.Time
		if olderThanStr != "" {
			timestampInt64, err := strconv.ParseInt(olderThanStr, 10, 64)
			if err != nil {
				return err
			}
			olderThan := time.Unix(timestampInt64, 0)
			olderThanPtr = &olderThan
		}

		if workDir == "" {
			workDir = filepath.Join(util.GetHomeDir(), ".s3migration")
		}

		dir, err := os.ReadDir(workDir)
		if err != nil {
			if err := os.MkdirAll(workDir, 0755); err != nil {
				return err
			}
		} else {
			if workDir == filepath.Join(util.GetHomeDir(), ".s3migration") {
				for _, d := range dir {
					os.RemoveAll(path.Join([]string{workDir, d.Name()}...))
				}
			} else if len(dir) > 0 {
				return fmt.Errorf("working directory not empty")
			}
		}

		var startAfter string
		stateFilePath := migration.StateFilePath(workDir, bucket)
		if resume {
			f, err := os.Open(stateFilePath)
			if err != nil && errors.Is(err, os.ErrNotExist) {
			} else if err != nil {
				return err
			} else {
				b, err := io.ReadAll(f)
				if err != nil {
					return err
				}

				startAfter = string(b)
				startAfter = strings.ReplaceAll(strings.ReplaceAll(startAfter, " ", ""), "\n", "")
			}
		}

		if err := util.SetAwsEnvCredentials(accessKey, secretKey); err != nil {
			return err
		}

		if chunkNumber == 0 {
			chunkNumber = 500
		}
		if chunkSize == 0 {
			chunkSize = 50 * 1024 * 1024
		}
		if batchSize == 0 {
			batchSize = 30
		}

		mConfig := migration.MigrationConfig{
			AllocationID:    allocationId,
			Region:          region,
			Skip:            skip,
			Concurrency:     concurrency,
			Bucket:          bucket,
			Prefix:          prefix,
			MigrateToPath:   migrateToPath,
			DuplicateSuffix: duplicateSuffix,
			Encrypt:         encrypt,
			RetryCount:      retryCount,
			NewerThan:       newerThanPtr,
			OlderThan:       olderThanPtr,
			DeleteSource:    deleteSource,
			StartAfter:      startAfter,
			StateFilePath:   stateFilePath,
			WorkDir:         workDir,
			ChunkSize:       chunkSize,
			ChunkNumber:     chunkNumber,
			BatchSize:       batchSize,

			Source:      source,
			AccessToken: accessToken,
		}

		if err := migration.InitMigration(&mConfig); err != nil {
			return err
		}
		err = migration.StartMigration()
		if err != nil {
			return err
		} else {
			fmt.Println("Migration completed successfully")
		}
		return nil
	},
}

// getTimeFromDHString get timestamp before days and hours mentioned in string; eg 7d10h.
func getTimeFromDHString(s string) (t time.Time, err error) {
	dhReg := `^(([0-9]*)d)?(([0-9]*)h)?$` //day hour regex; matches strings like: 7d10h, etc.
	re := regexp.MustCompile(dhReg)

	if !re.Match([]byte(s)) {
		err = fmt.Errorf("input string doesn't match regex %v", dhReg)
		return
	}

	res := re.FindSubmatch([]byte(s))
	days, _ := strconv.Atoi(string(res[2]))
	hours, _ := strconv.Atoi(string(res[4]))

	duration := time.Hour*24*time.Duration(days) + time.Hour*time.Duration(hours)
	t = time.Now().Add(-duration)

	return
}

// func splitIntoBucketNameAndPrefix(buckets []string) (bucketArr [][2]string, err error) {
// 	for _, bkt := range buckets {
// 		res := strings.Split(bkt, ":")
// 		l := len(res)
// 		if l < 1 || l > 2 {
// 			err = fmt.Errorf("bucket flag has fields less than 1 or greater than 2. Arg \"%v\"", bkt)
// 			return
// 		}

// 		var bucket [2]string
// 		bucket[0] = res[0]

// 		if l == 2 {
// 			bucket[1] = res[1]
// 		}

// 		bucketArr = append(bucketArr, bucket)
// 	}
// 	return
// }
