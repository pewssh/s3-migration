package gdrive

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	zlogger "github.com/0chain/s3migration/logger"
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

type GoogleDriveI interface {
	ListFiles(ctx context.Context) ([]*ObjectMeta, error)
	GetFileContent(ctx context.Context, fileID string, keepOpen bool) (*Object, error)
	DeleteFile(ctx context.Context, fileID string) error
	DownloadToFile(ctx context.Context, fileID, destinationPath string) error
	DownloadToMemory(ctx context.Context, fileID string, offset int64, chunkSize, fileSize int64) ([]byte, error)
}

type Object struct {
	Body          io.ReadCloser
	ContentType   string
	ContentLength int64
}

type ObjectMeta struct {
	ID           string
	Name         string
	Size         int64
	ContentType  string
	LastModified string
}

type GoogleDriveClient struct {
	service *drive.Service
}

func NewGoogleDriveClient(accessToken string) (*GoogleDriveClient, error) {
	ctx := context.Background()

	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})

	httpClient := oauth2.NewClient(ctx, tokenSource)

	service, err := drive.NewService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}

	return &GoogleDriveClient{
		service: service,
	}, nil
}

func (g *GoogleDriveClient) ListFiles(ctx context.Context) ([]*ObjectMeta, error) {
	files, err := g.service.Files.List().Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	var objects []*ObjectMeta
	for _, file := range files.Files {
		if strings.HasSuffix(file.Name, "/") { // Skip dirs
			continue
		}

		objects = append(objects, &ObjectMeta{
			ID:           file.Id,
			Name:         file.Name,
			Size:         file.Size,
			ContentType:  file.MimeType,
			LastModified: file.ModifiedTime,
		})
	}

	return objects, nil
}

func (g *GoogleDriveClient) GetFileContent(ctx context.Context, fileID string, keepOpen bool) (*Object, error) {
	resp, err := g.service.Files.Get(fileID).Download()
	if err != nil {
		return nil, err
	}

	if !keepOpen {
		defer resp.Body.Close()
	}

	obj := &Object{
		Body:          resp.Body,
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: resp.ContentLength,
	}

	return obj, nil
}

func (g *GoogleDriveClient) DeleteFile(ctx context.Context, fileID string) error {
	err := g.service.Files.Delete(fileID).Do()
	if err != nil {
		return err
	}
	return nil
}

func (g *GoogleDriveClient) DownloadToFile(ctx context.Context, fileID, destinationPath string) error {
	resp, err := g.service.Files.Get(fileID).Download()
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(destinationPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	zlogger.Logger.Info(fmt.Sprintf("Downloaded file ID: %s to %s\n", fileID, destinationPath))
	return nil
}

func (g *GoogleDriveClient) DownloadToMemory(ctx context.Context, fileID string, offset int64, chunkSize, fileSize int64) ([]byte, error) {
    limit := offset + chunkSize - 1
    if limit > fileSize {
        limit = fileSize
    }

    rng := fmt.Sprintf("bytes=%d-%d", offset, limit)

    req := g.service.Files.Get(fileID)

    req.Header().Set("Range", rng)

    resp, err := req.Download()
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    data := make([]byte, chunkSize)
    n, err := io.ReadFull(resp.Body, data)

    if err != nil && err != io.ErrUnexpectedEOF {
        return nil, err
    }

    if int64(n) < chunkSize && fileSize != chunkSize {
        data = data[:n]
    }

    return data, nil
}
