package util

import (
	"errors"
	"io"
)

func NewFileReader(source io.Reader) *FileReader {
	return &FileReader{source}
}

type FileReader struct {
	io.Reader
}

func (r *FileReader) Read(p []byte) (int, error) {
	bLen, err := io.ReadAtLeast(r.Reader, p, len(p))
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return bLen, io.EOF
		}
		return bLen, err
	}
	return bLen, nil
}

func (r *FileReader) Close() error {
	if closer, ok := r.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (r *FileReader) Seek(offset int64, whence int) (int64, error) {
	if seeker, ok := r.Reader.(io.Seeker); ok {
		return seeker.Seek(offset, whence)
	}
	return 0, errors.New("seek not supported")
}
