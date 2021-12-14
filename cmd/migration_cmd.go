package cmd

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/0chain/s3migration/migration"
	"github.com/spf13/cobra"

	"github.com/0chain/s3migration/util"
)

var (
	allocationId               string
	accessKey, secretKey       string
	buckets                    []string
	migrateToPath              string
	concurrency                int
	encrypt                    bool
	resume                     bool
	skip                       int // 0 --> Replace; 1 --> Skip; 2 --> Duplicate
	region                     string
	allocationTextPath         string
	ownerPays                  bool
	newerThanStr, olderThanStr string
	awsCredPath                string
	retryCount                 int
)

// migrateCmd is the migrateFromS3 sub command to migrate whole objects from some buckets.
func init() {
	rootCmd.AddCommand(migrateCmd)

	//flags related to dStorage
	migrateCmd.PersistentFlags().StringVar(&allocationId, "allocation", "", "allocation ID for dStorage")
	migrateCmd.Flags().StringVar(&allocationTextPath, "alloc-path", "alloc-path", "File Path to allocation text")
	migrateCmd.Flags().BoolVar(&ownerPays, "owner-pays", false, "Read payment source(Default: owner pays)")
	migrateCmd.Flags().BoolVar(&encrypt, "encrypt", false, "pass this option to encrypt and upload the file")
	//flags related to s3
	migrateCmd.PersistentFlags().StringVar(&accessKey, "access-key", "", "access-key of aws")
	migrateCmd.PersistentFlags().StringVar(&secretKey, "secret-key", "", "secret-key of aws")
	migrateCmd.PersistentFlags().StringVar(&region, "region", "", "region of s3 buckets")
	migrateCmd.PersistentFlags().StringSliceVar(&buckets, "buckets", []string{}, "specific s3 buckets to use. Use bucketName:prefix "+
		"format if prefix filter is required or only bucketName for migrating all objects. If no value is provided all buckets will be migrated")
	migrateCmd.Flags().StringVar(&migrateToPath, "migrate-to", "/", "Remote path where buckets will be migrated to")
	migrateCmd.Flags().StringVar(&awsCredPath, "aws-cred-path", "", "File Path to aws credentials")

	migrateCmd.Flags().IntVar(&concurrency, "concurrency", 10, "number of concurrent files to process concurrently during migration")
	migrateCmd.Flags().BoolVar(&resume, "resume", false, "pass this option to resume migration from previous state")
	migrateCmd.Flags().IntVar(&skip, "skip", 1, "0 --> Replace existing files; 1 --> Skip migration; 2 --> Duplicate")
	migrateCmd.Flags().IntVar(&retryCount, "retry", 3, "retry count for upload to dstorage")
	migrateCmd.Flags().StringVar(&newerThanStr, "newer-than", "", "eg; 7d10h --> migrate objects that is newer than 7 days 10 hours")
	migrateCmd.Flags().StringVar(&olderThanStr, "older-than", "", "eg; 7d10h --> migrate objects that is older than 7 days 10 hours")

}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate user data from S3 bucket to dStorage",
	Long: `Migrate files from s3 buckets to some remote directory(default is /) by using aws-s3-sdk and 0chain gosdk. All the objects from bucket will be migrated.
	However user can specify some prefix to migrate only the files with those prefix. Also if there is name conflict within dStorage file and bucket file use can 
	specify whether to skip, replace or duplicate them. Migration state is maintained is some file so user can also resume migration operation if some error had 
	occurred in previous migration session. User can also specify whether to delete migrated file. Note the defaults.

	Note: Addition of new object or modification of existing file while migrating is not recommended, as it cannot track such changes and you might loose your data.
	`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.Flags().Parse(args)
		var err error
		if allocationId == "" {
			if allocationId = util.GetAllocationIDFromEnv(); allocationId == "" {
				if allocationTextPath == "" {
					return errors.New("allocation text file not passed in argument")
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

		if accessKey == "" || secretKey == "" {
			if accessKey, secretKey = util.GetAwsCredentialsFromEnv(); accessKey == "" || secretKey == "" {
				if awsCredPath == "" {
					return errors.New("aws credentials path missing.")
				}
				if accessKey, secretKey = util.GetAwsCredentialsFromFile(awsCredPath); accessKey == "" || secretKey == "" {
					return fmt.Errorf("empty access or secret key. Access Key:%v\tSecret Key: %v", accessKey, secretKey)
				}
			}
		}

		if err := util.SetAwsEnvCredentials(accessKey, secretKey); err != nil {
			return err
		}

		if buckets == nil { //nil means all bucket from the account
			buckets = util.GetBucketsFromFile(awsCredPath) // if buckets is nil, migrate all buckets in root dir
		}

		if skip < 0 || skip > 2 {
			return fmt.Errorf("skip value not in range 0-2. Provided value is %v", skip)
		}

		newerThan, err := getTimeFromDHString(newerThanStr)
		if err != nil {
			return err
		}

		olderThan, err := getTimeFromDHString(olderThanStr)
		if err != nil {
			return err
		}

		var whoPays int
		if !ownerPays {
			whoPays = 1
		}

		mConfig := migration.MigrationConfig{
			AllocationID:  allocationId,
			Region:        region,
			Skip:          skip,
			Resume:        resume,
			Concurrency:   concurrency,
			Buckets:       buckets,
			MigrateToPath: migrateToPath,
			WhoPays:       whoPays,
			Encrypt:       encrypt,
			RetryCount:    retryCount,
			NewerThan:     newerThan,
			OlderThan:     olderThan,
		}

		if err := migration.InitMigration(&mConfig); err != nil {
			return err
		}

		return migration.Migrate()
	},
}

//getTimeFromDHString get timestamp before days and hours mentioned in string; eg 7d10h.
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
