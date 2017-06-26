package etly

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync/atomic"
	"time"
)

type TransferObjectRequest struct {
	TaskId    string
	SourceURL string
	Transfer  *Transfer
}

type TransferObjectResponse struct {
	RecordProcessed int
	RecordSkipped   int
	Error           string
	ErrorReason     string
}

type TransferObjectService interface {
	Transfer(request *TransferObjectRequest) *TransferObjectResponse
}

type transferObjectService struct {
	taskRegistry *TaskRegistry
}

func (s *transferObjectService) Transfer(request *TransferObjectRequest) *TransferObjectResponse {
	fmt.Printf("Request.SourceURL: %v\nRequest.SourceName: %v\nRequest.TargetName: %v\nTimestamp: %v,", request.SourceURL, request.Transfer.Source.Name, request.Transfer.Target.Name, time.Now())
	var sourceURL = request.SourceURL
	var transfer = request.Transfer

	_, hasProvider := NewProviderRegistry().registry[transfer.Source.DataType]
	if !hasProvider {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to lookup provider for data type '%v':  %v -> %v", transfer.Source.DataType, transfer.Source.Name, transfer.Target), nil)
	}
	task := NewTransferTaskForId(request.TaskId, transfer)

	s.taskRegistry.Register(task.Task)

	storageService, err := getStorageService(transfer.Source.Resource)
	if err != nil {
		return NewErrorTransferObjectResponse("Failed to get storage service", err)
	}

	source, err := storageService.StorageObject(sourceURL)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to get souce: %v", sourceURL), err)
	}
	reader, err := storageService.Download(source)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to dowload: %v", sourceURL), err)
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to read : %v", sourceURL), err)
	}
	reader = bytes.NewReader(content)
	reader, err = getEncodingReader(transfer.Source.Compression, reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to encoding reader : %v", sourceURL), err)
	}
	content, err = ioutil.ReadAll(reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to readall : %v", sourceURL), err)
	}
	if transfer.Source.DataFormat == "ndjson" {
		var err = s.transferObjectFromNewLineDelimiteredJson(content, transfer, task)
		task.Progress.FileProcessed++
		var response = &TransferObjectResponse{
			RecordProcessed: int(task.Progress.RecordProcessed),
			RecordSkipped:   int(task.Progress.RecordSkipped),
		}
		if err != nil {
			response.Error = err.Error()
		}
		return response
	}
	return NewErrorTransferObjectResponse(fmt.Sprintf("Unsupported source format: %v: %v -> %v", transfer.Source.DataFormat, transfer.Source.Name, transfer.Target), errors.New("Unsupported source format"))
}

func (s *transferObjectService) transferObjectFromNewLineDelimiteredJson(source []byte, transfer *Transfer, task *TransferTask) error {
	provider := NewProviderRegistry().registry[transfer.Source.DataType]
	transformer := NewTransformerRegistry().registry[transfer.Transformer]

	var lines = strings.Split(string(source), "\n")
	predicate, _ := NewFilterRegistry().registry[transfer.Filter]
	var filtered = 0

	var transformed = make([]string, 0)
outer:
	for i, line := range lines {
		if len(line) == 0 {
			continue
		}
		if len(transfer.Source.DataTypeMatch) > 0 {
			for _, sourceData := range transfer.Source.DataTypeMatch {
				if strings.Contains(line, sourceData.MatchingFragment) {
					if sourceData.DataType == "" {
						continue outer
					}
					provider = NewProviderRegistry().registry[sourceData.DataType]
					if provider == nil {
						return fmt.Errorf("Failed to lookup provider for match: %v %v", sourceData.MatchingFragment, sourceData.DataType)
					}
				}
			}
		}
		var source = provider()
		err := decodeJSONTarget(bytes.NewReader([]byte(line)), source)
		if err != nil {
			return fmt.Errorf("Failed to decode json: [%v] %v %v", i, err, line)
		}

		if predicate == nil || predicate.Apply(source) {
			target, err := transformer(source)
			if err != nil {
				return fmt.Errorf("Failed to transform %v", err)
			}
			buf := new(bytes.Buffer)
			err = encodeJSONSource(buf, target)
			if err != nil {
				return err
			}
			tranformedObject := strings.Replace(string(buf.Bytes()), "\n", "", buf.Len())
			transformed = append(transformed, tranformedObject)
			atomic.AddInt32(&task.Progress.RecordProcessed, 1)
		} else {
			atomic.AddInt32(&task.Progress.RecordSkipped, 1)
			filtered++
		}
		task.UpdateElapsed()
	}
	if len(transformed) > 0 {
		reader, err := encodeData(transfer.Target.Compression, []byte(strings.Join(transformed, "\n")))
		if err != nil {
			return err
		}
		storageService, err := getStorageService(transfer.Target.Resource)
		if err != nil {
			return err
		}
		err = storageService.Upload(transfer.Target.Name, reader)
		if err != nil {
			return fmt.Errorf("Failed to upload: %v %v", transfer.Target, err)
		}
	}
	return nil
}

func newtransferObjectService(taskRegistry *TaskRegistry) TransferObjectService {
	return &transferObjectService{
		taskRegistry: taskRegistry,
	}
}

func NewErrorTransferObjectResponse(reason string, err error) *TransferObjectResponse {
	return &TransferObjectResponse{
		ErrorReason: reason,
		Error:       err.Error(),
	}
}
