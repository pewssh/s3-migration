package dStorage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	zlogger "github.com/0chain/s3migration/logger"
	"github.com/0chain/s3migration/util"
	zerror "github.com/0chain/s3migration/zErrors"

	"github.com/0chain/gosdk/constants"
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

//go:generate mockgen -destination mocks/mock_dstorage.go -package mock_dstorage github.com/0chain/s3migration/dstorage DStoreI
type DStoreI interface {
	GetFileMetaData(ctx context.Context, remotePath string) (*sdk.ORef, error)
	Replace(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string) sdk.OperationRequest
	Duplicate(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string) sdk.OperationRequest
	Upload(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string, isUpdate bool) sdk.OperationRequest
	MultiUpload(ctx context.Context, ops []sdk.OperationRequest) error
	IsFileExist(ctx context.Context, remotePath string) (bool, error)
	GetAvailableSpace() int64
	GetTotalSpace() int64
	UpdateAllocationDetails() error
	GetChunkWriteSize() int64
}

type DStorageService struct {
	allocation *sdk.Allocation
	encrypt    bool // Should encrypt before uploading/updating
	//Where to migrate all buckets to. Default is /
	migrateTo string
	//Duplicate suffix to use if file already exists in dStorage. So if remotepath if /path/to/remote/file.txt
	//then duplicate path should be /path/to/remote/file{duplicateSuffix}.txt
	duplicateSuffix string
	workDir         string
	chunkNumer      int
}

const (
	GetRefRetryWaitTime = 500 * time.Millisecond
	GetRefRetryCount    = 2
)

func (d *DStorageService) GetFileMetaData(ctx context.Context, remotePath string) (*sdk.ORef, error) {
	//if error is nil and ref too is nil then it means remoepath does not exist.
	//in this case return error with code from error.go
	level := len(strings.Split(strings.TrimSuffix(remotePath, "/"), "/"))
	var oResult *sdk.ObjectTreeResult
	var err error
	for retryCount := 1; retryCount <= GetRefRetryCount; retryCount++ {
		oResult, err = d.allocation.GetRefs(remotePath, "", "", "", "", "regular", level, 1)
		if err == nil {
			break
		}
		if zerror.IsConsensusFailedError(err) {
			time.Sleep(GetRefRetryWaitTime)
		} else {
			return nil, err
		}
	}

	if oResult == nil || len(oResult.Refs) == 0 {
		return nil, zerror.ErrFileNoExist
	}

	return &oResult.Refs[0], nil
}

func (d *DStorageService) MultiUpload(ctx context.Context, ops []sdk.OperationRequest) (err error) {
	err = d.allocation.DoMultiOperation(ops)
	return err
}

func (d *DStorageService) Replace(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string) sdk.OperationRequest {
	return d.Upload(ctx, remotePath, r, size, contentType, true)
}

func (d *DStorageService) Upload(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string, isUpdate bool) sdk.OperationRequest {
	defer func(start time.Time) {
		zlogger.Logger.Error("<><>upload object size key:  ", size, "context", ctx, "content type", contentType, "time taken for the object :: ", time.Since(start))
	}(time.Now())

	fileMeta := sdk.FileMeta{
		RemotePath: filepath.Clean(remotePath),
		ActualSize: size,
		MimeType:   contentType,
		RemoteName: filepath.Base(remotePath),
	}

	opType := constants.FileOperationInsert
	if isUpdate {
		opType = constants.FileOperationUpdate
	}

	options := []sdk.ChunkedUploadOption{
		sdk.WithEncrypt(d.encrypt),
		sdk.WithChunkNumber(d.chunkNumer),
	}

	op := sdk.OperationRequest{
		OperationType: opType,
		FileMeta:      fileMeta,
		Workdir:       d.workDir,
		FileReader:    util.NewFileReader(r),
		RemotePath:    remotePath,
		Opts:          options,
	}
	return op
}

func (d *DStorageService) Duplicate(ctx context.Context, remotePath string, r io.Reader, size int64, contentType string) sdk.OperationRequest {
	li := strings.LastIndex(remotePath, ".")

	var duplicateSuffix string

	if d.duplicateSuffix == "_copy" {
		duplicateSuffix = d.duplicateSuffix + "_" + strconv.FormatInt(time.Now().Unix(), 10)
	} else {
		duplicateSuffix = d.duplicateSuffix
	}

	if li == -1 || li == 0 {
		remotePath = fmt.Sprintf("%s%s", remotePath, duplicateSuffix)
	} else {
		remotePath = fmt.Sprintf("%s%s.%s", remotePath[:li], duplicateSuffix, remotePath[li+1:])
	}

	return d.Upload(ctx, remotePath, r, size, contentType, false)
}

func (d *DStorageService) IsFileExist(ctx context.Context, remotePath string) (bool, error) {
	_, err := d.GetFileMetaData(ctx, remotePath)
	if err != nil {
		if zerror.IsFileNotExistError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (d *DStorageService) UpdateAllocationDetails() error {
	return sdk.GetAllocationUpdates(d.allocation)
}

func (d *DStorageService) GetAvailableSpace() int64 {
	var availableSpace = d.allocation.Size
	if d.allocation.Stats != nil {
		availableSpace -= (*d.allocation.Stats).UsedSize
	}
	return availableSpace
}

func (d *DStorageService) GetTotalSpace() int64 {
	return d.allocation.Size
}

func GetDStorageService(allocationID, migrateTo, duplicateSuffix, workDir string, encrypt bool, chunkNumber, batchSize int) (*DStorageService, error) {
	allocation, err := sdk.GetAllocation(allocationID)
	sdk.MultiOpBatchSize = batchSize
	if err != nil {
		return nil, err
	}

	workDir = filepath.Join(workDir, "zstore")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, err
	}

	zlogger.Logger.Info(fmt.Sprintf("Dstorage service initialized with "+
		"allocation: %v,"+
		"encrypt: %v,"+
		"migrateTo: %v,"+
		"duplicateSuffix: %v, "+
		"workDir: %v", allocationID, encrypt, migrateTo, duplicateSuffix, workDir))

	return &DStorageService{
		allocation:      allocation,
		encrypt:         encrypt,
		migrateTo:       migrateTo,
		duplicateSuffix: duplicateSuffix,
		workDir:         workDir,
		chunkNumer:      chunkNumber,
	}, nil
}

func (d *DStorageService) GetChunkWriteSize() int64 {
	return d.allocation.GetChunkReadSize(d.encrypt)
}
