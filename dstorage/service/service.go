package service

import (
	"context"
	thrown "github.com/0chain/errors"
	"github.com/0chain/gosdk/zboxcore/fileref"
	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/0chain/gosdk/zboxcore/zboxutil"
	"github.com/0chain/s3migration/util"
	"io"
	"path/filepath"
)

type Service struct {
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) DStorageUpload(ctx context.Context, allocationObj *sdk.Allocation, fileReader io.Reader, remotePath, mimeType string, size int64, encrypt bool, attrs fileref.Attributes, statusBar sdk.StatusCallback, isUpdate bool) error {
	remotePath = zboxutil.RemoteClean(remotePath)
	isAbs := zboxutil.IsRemoteAbs(remotePath)
	if !isAbs {
		err := thrown.New("invalid_path", "Path should be valid and absolute")
		return err
	}
	remotePath = zboxutil.GetFullRemotePath(remotePath, remotePath)

	_, fileName := filepath.Split(remotePath)

	fileMeta := sdk.FileMeta{
		Path:       remotePath,
		ActualSize: size,
		MimeType:   mimeType,
		RemoteName: fileName,
		RemotePath: remotePath,
		Attributes: attrs,
	}

	ChunkedUpload, err := sdk.CreateChunkedUpload(util.GetHomeDir(), allocationObj, fileMeta, util.NewStreamReader(fileReader), isUpdate, false,
		sdk.WithChunkSize(sdk.DefaultChunkSize),
		sdk.WithEncrypt(encrypt),
		sdk.WithStatusCallback(statusBar))
	if err != nil {
		return err
	}

	return ChunkedUpload.Start()
}
