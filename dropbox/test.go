package dropbox

import (
	"context"
	"fmt"
	"log"
	"testing"
)

var (
	dropboxAccessToken = ""
	testFilePath = ""
)

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

func TestDropboxClient_ListFiles(t *testing.T) {
	client, err := GetDropboxClient(dropboxAccessToken)
	if err != nil {
		log.Printf("Failed to create Dropbox client: %v", err)
		return
	}

	ctx := context.Background()
	files, err := client.ListFiles(ctx)
	if err != nil {
		log.Printf("Error while listing files: %v", err)
		return
	}

	for _, file := range files {
		log.Printf("File: %s, Name: %s, Size: %d bytes", file.Path, file.ContentType, file.Size)
	}
}

func TestDropboxClient_GetFileContent(t *testing.T) {
	client, err := GetDropboxClient(dropboxAccessToken)
	if err != nil {
		log.Printf("Failed to create Dropbox client: %v", err)
	}

	ctx := context.Background()
	filePath := testFilePath
	obj, err := client.GetFileContent(ctx, filePath)
	if err != nil {
		log.Printf("Error while getting file content: %v", err)
		return
	}
	defer obj.Body.Close()

	log.Printf("File content type: %s, Length: %d", obj.ContentType, obj.ContentLength)

	if (obj.Body == nil) || (obj.ContentLength == 0) {
		fmt.Println("Empty file content")
		return
	}

	buf := make([]byte, obj.ContentLength)
	n, err := obj.Body.Read(buf)

	if err != nil && err.Error() != "EOF" {
		log.Printf("Error while reading file content: %v", err)
		return
	}

	log.Printf("File content: %s", string(buf[:n]))
}

func TestDropboxClient_DeleteFile(t *testing.T) {
	client, err := GetDropboxClient(dropboxAccessToken)
	if err != nil {
		log.Printf("Failed to create Dropbox client: %v", err)
		return
	}

	ctx := context.Background()
	filePath := testFilePath
	err = client.DeleteFile(ctx, filePath)
	if err != nil {
		log.Printf("Error while deleting file: %v", err)
		return
	}
	log.Printf("File %s deleted successfully", filePath)
}

func TestDropboxClient_DownloadToFile(t *testing.T) {
	client, err := GetDropboxClient(dropboxAccessToken)
	if err != nil {
		log.Printf("Failed to create Dropbox client: %v", err)
		return
	}

	ctx := context.Background()
	filePath := testFilePath
	downloadedPath, err := client.DownloadToFile(ctx, filePath)
	if err != nil {
		log.Printf("Error while downloading file: %v", err)
		return
	}
	log.Printf("Downloaded to: %s", downloadedPath)
}

func TestDropboxClient_DownloadToMemory(t *testing.T) {
	client, err := GetDropboxClient(dropboxAccessToken)
	if err != nil {
		log.Printf("Failed to create Dropbox client: %v", err)
		return
	}

	ctx := context.Background()

	filePath := testFilePath
	offset := int64(0)

	// half chunk
	chunkSize := int64(313)
	objectSize := int64(626)

	data, err := client.DownloadToMemory(ctx, filePath, offset, chunkSize, objectSize)
	if err != nil {
		log.Printf("Error while downloading file: %v", err)
		return
	}

	log.Printf("Downloaded data: %s", data)
}
