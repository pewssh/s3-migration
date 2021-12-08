package controller

const (
	DefaultBucketRegion = "us-east-2"
)

func GetDefaultRegion(region string) string {
	if region == "" {
		return DefaultBucketRegion
	}

	return region
}
