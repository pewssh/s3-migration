package util

import (
	"io"
)

type DataChan struct {
	data []byte
	err  error
}

func NewStreamWriter(dataChan chan *DataChan) *StreamWriter {
	return &StreamWriter{dataChan}
}

type StreamWriter struct {
	DataChan chan *DataChan
}

func (sw *StreamWriter) Read(p []byte) (int, error) {
	recieveData, ok := <-sw.DataChan
	if !ok && recieveData == nil {
		return 0, io.EOF
	}
	if recieveData.err != nil {
		return 0, recieveData.err
	}
	if len(recieveData.data) > len(p) {
		return 0, io.ErrShortBuffer
	}
	copy(p, recieveData.data)
	return len(recieveData.data), nil
}

func (sw *StreamWriter) Write(p []byte) (int, error) {
	data := &DataChan{data: p, err: nil}
	sw.DataChan <- data
	return len(p), nil

}
