package etly

import (
	"bytes"
	"fmt"
	"github.com/viant/toolbox"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

type TransferObjectRequest struct {
	TaskID    string
	SourceURL string
	Transfer  *Transfer
}

type TransferObjectResponse struct {
	ProcessedTransfers []*ProcessedTransfer
	RecordProcessed    int
	RecordSkipped      int
	Error              string
}

type ProcessedTransfer struct {
	Transfer        *Transfer
	RecordProcessed int
	RecordSkipped   int
	RecordErrors    int
	Error           string
}

type TransferObjectService interface {
	Transfer(request *TransferObjectRequest) *TransferObjectResponse
}

type PayloadAccessor interface {
	SetPayload(payload string)
}

type transferObjectService struct {
	taskRegistry *TaskRegistry
}

var hostName string

func init() {
	// Extract hostname to propragate up errors
	var err error
	hostName, err = os.Hostname()
	if err != nil {
		panic(err)
	}
}

func (s *transferObjectService) Transfer(request *TransferObjectRequest) *TransferObjectResponse {
	defer func(t time.Time) {
		duration := time.Since(t).Seconds()
		if duration > 240 {
			log.Printf("Request took longer than 4 min:\nFile: %v\nDuration: %v secs\n", request.SourceURL, duration)
		}
	}(time.Now())
	sourceURL := request.SourceURL
	transfer := request.Transfer

	if err := transfer.Validate(); err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("%v", err))
	}
	task := NewTransferTaskForID(request.TaskID, transfer)
	s.taskRegistry.Register(task.Task)

	storageService, err := getStorageService(transfer.Source.Resource)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to get storage service: %v", err))
	}

	source, err := storageService.StorageObject(sourceURL)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to get source: %v %v", sourceURL, err))
	}

	if source.IsContent() && source.FileInfo() != nil && source.FileInfo().Size() == 0 {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Source Object to be transferred is empty. Source Url = %v, %v", sourceURL, err))
	}

	contentReader, err := storageService.Download(source)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to download: %v %v", sourceURL, err))
	}

	defer contentReader.Close()

	var reader io.Reader
	if transfer.Source.Compression != "" {
		reader, err = getEncodingReader(transfer.Source.Compression, contentReader)
		if err != nil {
			return NewErrorTransferObjectResponse(fmt.Sprintf("failed to get encoding reader : %v %v", sourceURL, err))
		}
	} else {
		reader = contentReader
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("failed to ReadAll : %v %v", sourceURL, err))
	}

	if transfer.Source.DataFormat == "ndjson" {
		var processedTransfers, err = s.transferObjectFromNdjson(content, transfer, task)
		task.Progress.FileProcessed++
		var response = &TransferObjectResponse{
			RecordProcessed: int(task.Progress.RecordProcessed),
			RecordSkipped:   int(task.Progress.RecordSkipped),
		}
		response.ProcessedTransfers = processedTransfers
		if err != nil {
			response.Error = fmt.Sprintf("hostname: %s, %v %v", hostName, transfer.Source.Resource.Name, err)
		}

		return response
	}

	return NewErrorTransferObjectResponse(fmt.Sprintf("Unsupported source format: %v: %v -> %v", transfer.Source.DataFormat, transfer.Source.Name, transfer.Target))

}

func expandWorkerVariables(text string, transfer *Transfer, source, target interface{}) (string, error) {
	if transfer.HasRecordLevelVariableExtraction() {
		variables, err := buildVariableWorkerServiceMap(transfer.VariableExtraction, source, target)
		if err != nil {
			return "", err
		}
		text = expandVaiables(text, variables)

	}
	return text, nil
}

type TargetTransformation struct {
	*ProcessedTransfer
	targetRecords []string
}

func getTargetKey(transfer *Transfer, source, target interface{}, state map[string]interface{}) (string, error) {
	if source == nil || target == nil {
		return transfer.Target.Name, nil
	}
	var result = transfer.Target.Name
	if len(state) > 0 {
		for k, v := range state {
			result = strings.Replace(result, k, toolbox.AsString(v), len(result))
		}
	}
	if transfer.HasRecordLevelVariableExtraction() {
		return expandWorkerVariables(result, transfer, source, target)
	}
	return result, nil
}



func (s *transferObjectService) transferObjectFromNdjson(source []byte, transfer *Transfer, task *TransferTask) ([]*ProcessedTransfer, error) {
	dataTypeProvider := NewProviderRegistry().registry[transfer.Source.DataType]
	transformer := NewTransformerRegistry().registry[transfer.Transformer]

	var transformedTargets TargetTransformations = make(map[string]*TargetTransformation)
	lines := bytes.Split(source, []byte("\n"))
	predicate := NewFilterRegistry().registry[transfer.Filter]
	var decodingError = &decodingError{}
	var state = make(map[string]interface{})

outer:
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		if len(transfer.Source.DataTypeMatch) > 0 {
			for _, match := range transfer.Source.DataTypeMatch {
				//if DataType is empty the matching fragment works like exclusion
				if bytes.Contains(line, []byte(match.MatchingFragment)) {
					if match.DataType == "" {
						// Skip to next line
						continue outer
					}
					dataTypeProvider = NewProviderRegistry().registry[match.DataType]
					if dataTypeProvider == nil {
						return nil, fmt.Errorf("failed to lookup provider for match: %v %v", match.MatchingFragment, match.DataType)
					}
					break
				}
			}
		}
		err := transferRecord(state, predicate, dataTypeProvider, line, transformer, transfer, transformedTargets, task, decodingError)
		if err != nil {
			return nil, err
		}
		task.UpdateElapsed()
	}
	result, err := transformedTargets.Upload(transfer)
	if err != nil {
		return nil, err
	}
	return result, decodingError.error
}

func newtransferObjectService(taskRegistry *TaskRegistry) TransferObjectService {
	return &transferObjectService{
		taskRegistry: taskRegistry,
	}
}

func NewErrorTransferObjectResponse(message string) *TransferObjectResponse {
	return &TransferObjectResponse{
		Error: "host:" + hostName + ", " + message,
	}
}
