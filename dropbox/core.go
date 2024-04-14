package dropbox

import (
	"context"
	"io"
	"mime"
	"os"
	"path/filepath"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
)

type DropboxI interface {
	ListFiles(ctx context.Context) ([]*ObjectMeta, error)
	ListFilesInFolder(ctx context.Context) ([]*ObjectMeta, error)
	GetFileContent(ctx context.Context, filePath string) (*Object, error)
	DeleteFile(ctx context.Context, filePath string) error
	DownloadToFile(ctx context.Context, filePath string) (string, error)
	DownloadToMemory(ctx context.Context, objectKey string, offset int64, chunkSize, objectSize int64) ([]byte, error)
}

type Object struct {
	Body          io.ReadCloser
	ContentType   string
	ContentLength uint64
}

type ObjectMeta struct {
	Name        string
	Path        string
	Size        uint64
	ContentType string
}

type DropboxClient struct {
	token        string
	dropboxConf  *dropbox.Config
	dropboxFiles files.Client
}

func GetDropboxClient(token string) (*DropboxClient, error) {
	config := dropbox.Config{
		Token: token,
	}

	client := files.New(config)

	return &DropboxClient{
		token:        token,
		dropboxConf:  &config,
		dropboxFiles: client,
	}, nil
}

func (d *DropboxClient) ListFiles(ctx context.Context) ([]*ObjectMeta, error) {
	var objects []*ObjectMeta

	arg := files.NewListFolderArg("")
	arg.Recursive = true

	res, err := d.dropboxFiles.ListFolder(arg)

	if err != nil {
		return nil, err
	}

	for _, entry := range res.Entries {

		if meta, ok := entry.(*files.FileMetadata); ok {

			objects = append(objects, &ObjectMeta{
				Name:        meta.Name,
				Path:        meta.PathDisplay,
				Size:        meta.Size,
				ContentType: mime.TypeByExtension(filepath.Ext(meta.PathDisplay)),
			})

		}
	}

	return objects, nil
}

func (d *DropboxClient) ListFilesInFolder(ctx context.Context, folderName string) ([]*ObjectMeta, error) {
	var objects []*ObjectMeta

	arg := files.NewListFolderArg(folderName)
	arg.Recursive = true

	res, err := d.dropboxFiles.ListFolder(arg)

	if err != nil {
		return nil, err
	}

	for _, entry := range res.Entries {

		if meta, ok := entry.(*files.FileMetadata); ok {

			objects = append(objects, &ObjectMeta{
				Name:        meta.Name,
				Path:        meta.PathDisplay,
				Size:        meta.Size,
				ContentType: mime.TypeByExtension(filepath.Ext(meta.PathDisplay)),
			})

		}
	}

	return objects, nil
}

func (d *DropboxClient) GetFileContent(ctx context.Context, filePath string) (*Object, error) {
	arg := files.NewDownloadArg(filePath)
	res, content, err := d.dropboxFiles.Download(arg)
	if err != nil {
		return nil, err
	}

	return &Object{
		Body:          content,
		ContentType:   mime.TypeByExtension(filepath.Ext(filePath)),
		ContentLength: res.Size,
	}, nil
}

func (d *DropboxClient) DeleteFile(ctx context.Context, filePath string) error {
	arg := files.NewDeleteArg(filePath)
	_, err := d.dropboxFiles.DeleteV2(arg)
	return err
}

func (d *DropboxClient) DownloadToFile(ctx context.Context, filePath string) (string, error) {
	arg := files.NewDownloadArg(filePath)
	_, content, err := d.dropboxFiles.Download(arg)
	if err != nil {
		return "", err
	}

	fileName := filepath.Base(filePath)
	downloadPath := filepath.Join(".", fileName)
	file, err := os.Create(downloadPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = io.Copy(file, content)
	if err != nil {
		return "", err
	}

	return downloadPath, nil
}

func (d *DropboxClient) DownloadToMemory(ctx context.Context, objectKey string, offset int64, chunkSize, objectSize int64) ([]byte, error) {
	arg := files.NewDownloadArg(objectKey)
	_, content, err := d.dropboxFiles.Download(arg)
	if err != nil {
		return nil, err
	}
	defer content.Close()

	if _, err := io.CopyN(io.Discard, content, offset); err != nil {
		return nil, err
	}

	data := make([]byte, chunkSize)
	n, err := io.ReadFull(content, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	if int64(n) < chunkSize && objectSize != chunkSize {
		data = data[:n]
	}

	return data, nil
}
