package dStorage

import (
	"context"
	"errors"
	"fmt"
	"github.com/0chain/gosdk/zboxcore/fileref"
	"github.com/0chain/gosdk/zboxcore/zboxutil"
	"github.com/0chain/s3migration/util"
	"github.com/mitchellh/go-homedir"
	"io"
	"path/filepath"
	"strings"

	"github.com/0chain/gosdk/core/common"
	"github.com/0chain/gosdk/zboxcore/sdk"
)

//use rate limiter here.
//All upload should go through this file so you can limit rate of upload request so you don't get blocked by blobber.
//Its better to put rate limit value in some variable; check rate limit of all blobbers and put rate limit value of the blobber that has minimum capacity.
//While this file helps to rate limit; there might be goroutine leak in migrate.go so that we need to process uploads in batch.
//
//Batch is simpler to use than the continuous upload.
//Concept is you take a bunch of s3 objects in batch and wait until all the objects from this batch is uploaded. If any upload fails then terminate migration of this bucket.
//let other bucket operate.
//This way you can update state for each bucket;

//We also need to be careful about committing upload. There might be race between committing request resulting in commit failure.
//So lets put commit request in a queue(use channel) and try three times. If it fails to commit then save state of all bucket and abort the program.

type DStoreI interface {
	GetFileMetaData(ctx context.Context, remotepath string) (*sdk.ORef, error)
	Replace(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string) error
	Duplicate(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string) error
	Upload(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string, isUpdate bool) error
	IsFileExist(ctx context.Context, remotePath string) bool
	GetAvailableSpace() int64
	GetTotalSpace() int64
}

type DStorageService struct {
	allocation *sdk.Allocation
	encrypt    bool // Should encrypt before uploading/updating
	//After file is available in dStorage owner can decide who is going to pay for read
	whoPays common.WhoPays
	//Where to migrate all buckets to. Default is /
	migrateTo string
	//Duplicate suffix to use if file already exists in dStorage. So if remotepath if /path/to/remote/file.txt
	//then duplicate path should be /path/to/remote/file{duplicateSuffix}.txt
	duplicateSuffix string
	availableSpace  int64
	totalSpace      int64
}

const (
	DefaultChunkSize = 64 * 1024
	FiveHundredKB    = 500 * 1024
	OneMB            = 1024 * 1024
	TenMB            = 10 * OneMB
	HundredMB        = 10 * TenMB
)

func (d *DStorageService) GetFileMetaData(ctx context.Context, remotepath string) (*sdk.ORef, error) {
	//if error is nil and ref too is nil then it means remoepath does not exist.
	//in this case return error with code from error.go
	return nil, nil
}

func getChunkSize(size int64) int64 {
	var chunkSize int64
	switch {
	case size > HundredMB:
		chunkSize = 2 * TenMB
	case size > TenMB:
		chunkSize = TenMB
	case size > OneMB:
		chunkSize = OneMB
	case size > FiveHundredKB:
		chunkSize = FiveHundredKB
	default:
		chunkSize = DefaultChunkSize
	}
	return chunkSize
}

func (d *DStorageService) Upload(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string, isUpdate bool) error {
	cb := &StatusCB{
		DoneCh: make(chan struct{}, 1),
		ErrCh:  make(chan error, 1),
	}

	attrs := fileref.Attributes{
		WhoPaysForReads: d.whoPays,
	}

	remotePath = zboxutil.RemoteClean(remotePath)
	isAbs := zboxutil.IsRemoteAbs(remotePath)
	if !isAbs {
		err := errors.New("invalid_path Path should be valid and absolute")
		return err
	}

	_, fileName := filepath.Split(remotePath)
	fileMeta := sdk.FileMeta{
		RemotePath: remotePath,
		ActualSize: size,
		MimeType:   contentType,
		RemoteName: fileName,
		Attributes: attrs,
	}

	workDir, err := homedir.Dir()
	if err != nil {
		return err
	}

	chunkSize := getChunkSize(size)
	chunkUpload, err := sdk.CreateChunkedUpload(workDir, d.allocation, fileMeta, util.NewStreamReader(r), isUpdate, false,
		sdk.WithStatusCallback(cb),
		sdk.WithChunkSize(chunkSize),
		sdk.WithEncrypt(d.encrypt),
	)

	if err != nil {
		return err
	}

	err = chunkUpload.Start()
	if err != nil {
		return err
	}

	return nil
}

func (d *DStorageService) Replace(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string) error {
	return d.Upload(ctx, remotePath, r, size, contentType, true)
}

func (d *DStorageService) Duplicate(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string) error {
	li := strings.LastIndex(remotePath, ".")
	if li == -1 {
		remotePath = fmt.Sprintf("%s%s", remotePath, d.duplicateSuffix)
	} else if li == 0 {
		remotePath = fmt.Sprintf("%s%s", d.duplicateSuffix, remotePath)
	} else {
		remotePath = fmt.Sprintf("%s%s.%s", remotePath[:li], d.duplicateSuffix, remotePath[li+1:])
	}

	return d.Upload(ctx, remotePath, r, size, contentType, false)
}

func (d *DStorageService) IsFileExist(ctx context.Context, remotePath string) bool {
	fileMeta, _ := d.allocation.GetFileMeta(remotePath)
	if fileMeta != nil {
		return true
	}
	return false
}

func (d *DStorageService) GetAvailableSpace() int64 {
	// allocationdetails := getallocationdetailsfrom0chain()
	return d.availableSpace
}

func (d *DStorageService) GetTotalSpace() int64 {
	return d.totalSpace
}

func GetDStorageService(allocationID, migrateTo, duplicateSuffix string, encrypt bool, whoPays int) (*DStorageService, error) {
	allocation, err := sdk.GetAllocation(allocationID)

	if err != nil {
		return nil, err
	}

	var availableSpace = allocation.Size
	if allocation.Stats != nil {
		availableSpace -= (*allocation.Stats).UsedSize
	}

	return &DStorageService{
		allocation:     allocation,
		encrypt:        encrypt,
		whoPays:        common.WhoPays(whoPays),
		migrateTo:      migrateTo,
		totalSpace:     allocation.Size,
		availableSpace: availableSpace,
	}, nil
}
