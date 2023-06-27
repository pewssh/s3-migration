package util

import (
	"errors"
	"io"
)

func NewStreamReader(source io.Reader) *StreamReader {
	return &StreamReader{source}
}

type StreamReader struct {
	io.Reader
}

func (r *StreamReader) Read(p []byte) (int, error) {
	bLen, err := io.ReadAtLeast(r.Reader, p, len(p))
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return bLen, io.EOF
		}
		return bLen, err
	}
	return bLen, nil
}

func (r *StreamReader) Close() error {
	if closer, ok := r.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
