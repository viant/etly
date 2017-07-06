package etly

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/pgzip"
)

const GzipEncoding = "gzip"

type RawUnmarshaler interface {
	RawUnmarshal(input []byte)
}

type RawMarshaler interface {
	RawMarshal() []byte
}

func getEncodingReader(encoding string, reader io.Reader) (encodingReader io.Reader, err error) {
	switch encoding {
	case GzipEncoding:
		reader, err = pgzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		return reader, nil
	case "":
		return reader, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %v", encoding)
	}
}

func encodeData(encoding string, data []byte) ([]byte, error) {
	switch encoding {
	case GzipEncoding:
		buffer := new(bytes.Buffer)
		writer, err := pgzip.NewWriterLevel(buffer, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}
		writer.SetConcurrency(100000, 32)
		_, err = writer.Write(data)
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
		return data, nil
	default:
		return nil, fmt.Errorf("Unsupported encoding: %v", encoding)
	}
}
