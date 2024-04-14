package gdrive

import (
	"context"
	"log"
	"testing"
)

var (
	driveAccessToken=""
	testFileID=""
)

// using: https://developers.google.com/oauthplayground

// For reference (626 bytes text file)
const TestFileContent = ` by Manuel Gutiérrez Nájera

I want to die as the day declines, 
at high sea and facing the sky, 
while agony seems like a dream 
and my soul like a bird that can fly. 

To hear not, at this last moment, 
once alone with sky and sea, 
any more voices nor weeping prayers 
than the majestic beating of the waves. 

To die when the sad light retires 
its golden network from the green waves 
to be like the sun that slowly expires; 
something very luminous that fades. 

To die, and die young, before 
fleeting time removes the gentle crown, 
while life still says: "I'm yours" 
though we know with our hearts that she lies. 
`

func TestGoogleDriveClient_ListFiles(t *testing.T) {
	client, err := NewGoogleDriveClient(driveAccessToken)
	if err != nil {
		log.Printf("err while creating Google Drive client: %v", err)
		return
	}

	ctx := context.Background()
	files, err := client.ListFiles(ctx)
	if err != nil {
		log.Printf("err while list files: %v", err)
		return
	}

	for _, file := range files {
		log.Printf("file: %s, name: %s, size: %d bytes", file.ID, file.Name, file.Size)
	}
}

func TestGoogleDriveClient_GetFileContent(t *testing.T) {
	client, err := NewGoogleDriveClient(driveAccessToken)
	if err != nil {
		log.Printf("Failed to creating Google Drive client: %v", err)
		return
	}

	ctx := context.Background()
	fileID := testFileID
	obj, err := client.GetFileContent(ctx, fileID, true)

	if err != nil {
		log.Printf("err while getting file content: %v", err)
		return
	}

	defer obj.Body.Close()

	log.Printf("file content type: %s, length: %d", obj.ContentType, obj.ContentLength)

	if (obj.Body == nil) || (obj.ContentLength == 0) {
		log.Printf("empty file content")
		return
	}

	buf := make([]byte, obj.ContentLength)
	n, err := obj.Body.Read(buf)
	if err != nil {
		log.Printf("err while read file content: %v", err)
		return
	}
	log.Printf("read data: %s", buf[:n])
}

func TestGoogleDriveClient_DeleteFile(t *testing.T) {
	client, err := NewGoogleDriveClient(driveAccessToken)
	if err != nil {
		log.Printf("err while creating Google Drive client: %v", err)
		return
	}

	ctx := context.Background()
	fileID := testFileID
	err = client.DeleteFile(ctx, fileID)
	if err != nil {
		log.Printf("err while delete file: %v", err)
		return
	}
	log.Printf("file: %s deleted successfully", fileID)
}

func TestGoogleDriveClient_DownloadToFile(t *testing.T) {
	client, err := NewGoogleDriveClient(driveAccessToken)
	if err != nil {
		log.Printf("err while creating Google Drive client: %v", err)
	}

	ctx := context.Background()
	fileID := testFileID
	destinationPath := "./downloaded_file.txt"
	err = client.DownloadToFile(ctx, fileID, destinationPath)
	if err != nil {
		log.Printf("err while downloading file: %v", err)
		return
	}
	log.Printf("downloaded to: %s", destinationPath)
}

func TestGoogleDriveClient_DownloadToMemory(t *testing.T) {
	client, err := NewGoogleDriveClient(driveAccessToken)
	if err != nil {
		log.Printf("err while creating Google Drive client: %v", err)
	}

	ctx := context.Background()

	fileID := testFileID

	offset := int64(0)

	// download only half chunk for testing
	chunkSize := int64(313)

	fileSize := int64(626)

	if err != nil {
		log.Printf("err while getting file size: %v", err)
		return
	}

	data, err := client.DownloadToMemory(ctx, fileID, offset, chunkSize, fileSize)

	if err != nil {
		log.Printf("err while downloading file: %v", err)
		return
	}

	log.Printf("downloaded data: %s", data)
}
