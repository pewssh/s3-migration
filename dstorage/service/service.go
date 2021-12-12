package service

import (
	"context"
	thrown "github.com/0chain/errors"
	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/0chain/gosdk/zboxcore/zboxutil"
	"github.com/0chain/s3migration/model"
	"github.com/0chain/s3migration/util"
	"io"
	"path/filepath"
)

type Service struct {
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) UploadToDStorage(ctx context.Context, allocationObj *sdk.Allocation, fileReader io.Reader, options model.DStorageUploadOptions) error {
	remotePath := zboxutil.RemoteClean(options.RemotePath)
	isAbs := zboxutil.IsRemoteAbs(remotePath)
	if !isAbs {
		err := thrown.New("invalid_path", "Path should be valid and absolute")
		return err
	}
	remotePath = zboxutil.GetFullRemotePath(remotePath, remotePath)

	_, fileName := filepath.Split(remotePath)

	fileMeta := sdk.FileMeta{
		Path:       remotePath,
		ActualSize: options.Size,
		MimeType:   options.MimeType,
		RemoteName: fileName,
		RemotePath: remotePath,
		Attributes: options.Attrs,
	}

	ChunkedUpload, err := sdk.CreateChunkedUpload(util.GetHomeDir(), allocationObj, fileMeta, util.NewStreamReader(fileReader), options.IsUpdate, false,
		sdk.WithChunkSize(sdk.DefaultChunkSize),
		sdk.WithEncrypt(options.Encrypt),
		sdk.WithStatusCallback(options.StatusBar))
	if err != nil {
		return err
	}

	return ChunkedUpload.Start()
}
