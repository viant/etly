package etly

import (
	"bytes"
	"fmt"
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

	_, hasProvider := NewProviderRegistry().registry[transfer.Source.DataType]
	if !hasProvider {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to lookup provider for data type '%v':  %v -> %v", transfer.Source.DataType, transfer.Source.Name, transfer.Target))
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

	reader, err := storageService.Download(source)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to download: %v %v", sourceURL, err))
	}

	reader, err = getEncodingReader(transfer.Source.Compression, reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to get encoding reader : %v %v", sourceURL, err))
	}
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to ReadAll : %v %v", sourceURL, err))
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
			response.Error = fmt.Sprintf("hostname: %s, %v %v", hostName, transfer.Source.Resource.Name,  err)
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

func getTargetKey(transfer *Transfer, source, target interface{}) (string, error) {
	if source == nil || target == nil {
		return transfer.Target.Name, nil
	}
	if transfer.HasRecordLevelVariableExtraction() {
		return expandWorkerVariables(transfer.Target.Name, transfer, source, target)
	}
	return transfer.Target.Name, nil
}

func (s *transferObjectService) transferObjectFromNdjson(source []byte, transfer *Transfer, task *TransferTask) ([]*ProcessedTransfer, error) {
	result := make([]*ProcessedTransfer, 0)
	dataTypeProvider := NewProviderRegistry().registry[transfer.Source.DataType]
	transformer := NewTransformerRegistry().registry[transfer.Transformer]
	transformedTargets := make(map[string]*TargetTransformation)
	lines := bytes.Split(source, []byte("\n"))
	predicate := NewFilterRegistry().registry[transfer.Filter]
	filtered := 0
	var decodingError error
	var decodingErrorCount = 0

outer:
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		if len(transfer.Source.DataTypeMatch) > 0 {
			for _, match := range transfer.Source.DataTypeMatch {
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
		source := dataTypeProvider()
		err := decodeJSONTarget(line, source)
		if err != nil {
			decodingErrorCount++
			decodingError = fmt.Errorf("failed to decode json (%v times): %v, %s", decodingErrorCount, err, line)
			if transfer.MaxErrorCounts != nil && decodingErrorCount >= *transfer.MaxErrorCounts {
				return nil, fmt.Errorf("reached max errors %v",decodingError)
			}
			continue
		}

		if predicate == nil || predicate.Apply(source) {
			if payloadAccessor, ok := source.(PayloadAccessor); ok {
				payloadAccessor.SetPayload(string(line))
			}
			target, err := transformer(source)
			if err != nil {
				return nil, fmt.Errorf("failed to transform %v", err)
			}
			buf := new(bytes.Buffer)
			err = encodeJSONSource(buf, target)
			if err != nil {
				return nil, err
			}
			transformedObject := bytes.Replace(buf.Bytes(), []byte("\n"), []byte(""), -1)
			targetKey, err := getTargetKey(transfer, source, target)
			if err != nil {
				return nil, err
			}
			_, found := transformedTargets[targetKey]
			if !found {
				targetTransfer := transfer.Clone()
				targetTransfer.Target.Name = targetKey
				targetTransfer.Meta.Name, err = expandWorkerVariables(targetTransfer.Meta.Name, transfer, source, target)
				if err != nil {
					return nil, err
				}
				transformedTargets[targetKey] = &TargetTransformation{
					targetRecords: make([]string, 0),
					ProcessedTransfer: &ProcessedTransfer{
						Transfer: targetTransfer,
					},
				}
			}
			transformedTargets[targetKey].targetRecords = append(transformedTargets[targetKey].targetRecords, string(transformedObject))
			task.Progress.RecordProcessed++
			transformedTargets[targetKey].RecordProcessed++
			transformedTargets[targetKey].RecordErrors = decodingErrorCount

		} else {
			task.Progress.RecordSkipped++
			filtered++
		}
		task.UpdateElapsed()
	}

	if len(transformedTargets) > 0 {
		for _, transformed := range transformedTargets {
			content := strings.Join(transformed.targetRecords, "\n")
			compressedData, err := encodeData(transfer.Target.Compression, []byte(content))
			if err != nil {
				return nil, err
			}
			storageService, err := getStorageService(transfer.Target.Resource)
			if err != nil {
				return nil, err
			}

			//Disable MD5
			fileName := transformed.ProcessedTransfer.Transfer.Target.Name + "?disableMD5=true"
			err = storageService.Upload(fileName, bytes.NewReader(compressedData))
			if err != nil {
				return nil, fmt.Errorf("failed to upload: %v %v", transfer.Target, err)
			}
			result = append(result, transformed.ProcessedTransfer)
		}
	}
	return result, decodingError
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
