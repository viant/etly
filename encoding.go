package etly

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
)

const (
	GzipEncoding = "gzip"
)

func getEncodingReader(encoding string, reader io.Reader) (encodingReader io.Reader, err error) {
	switch encoding {
	case GzipEncoding:
		content, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		reader, err = gzip.NewReader(bytes.NewReader(content))
		if err != nil {
			return nil, err
		}

		content, err = ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(content), nil
	case "":
		return reader, nil
	default:
		return nil, fmt.Errorf("Unsupported encoding: %v", encoding)
	}
}

func encodeData(encoding string, data []byte) (io.Reader, error) {
	switch encoding {
	case GzipEncoding:
		buffer := new(bytes.Buffer)
		writer := gzip.NewWriter(buffer)
		_, err := writer.Write(data)
		if err != nil {
			return nil, err
		}

		err = writer.Flush()
		if err != nil {
			return nil, fmt.Errorf("Failed to encode data (flush) %v", err)
		}
		err = writer.Close()
		if err != nil {
			return nil, fmt.Errorf("Failed to encode data (close) %v", err)
		}
		data = buffer.Bytes()
		fallthrough
	case "":
		return bytes.NewReader(data), nil
	default:
		return nil, fmt.Errorf("Unsupported encoding: %v", encoding)
	}
}
