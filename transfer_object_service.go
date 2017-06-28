package etly

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
)

type TransferObjectRequest struct {
	TaskId    string
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
	Transfer *Transfer
	RecordProcessed int
	RecordSkipped   int
	Error           string
}

type TransferObjectService interface {
	Transfer(request *TransferObjectRequest) *TransferObjectResponse
}

type transferObjectService struct {
	taskRegistry *TaskRegistry
}

func (s *transferObjectService) Transfer(request *TransferObjectRequest) *TransferObjectResponse {
	var sourceURL = request.SourceURL
	var transfer = request.Transfer

	_, hasProvider := NewProviderRegistry().registry[transfer.Source.DataType]
	if !hasProvider {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to lookup provider for data type '%v':  %v -> %v", transfer.Source.DataType, transfer.Source.Name, transfer.Target))
	}
	task := NewTransferTaskForId(request.TaskId, transfer)

	s.taskRegistry.Register(task.Task)

	storageService, err := getStorageService(transfer.Source.Resource)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to get storage service: %v", err))
	}

	source, err := storageService.StorageObject(sourceURL)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to get souce: %v %v", sourceURL, err))
	}
	reader, err := storageService.Download(source)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to dowload: %v %v", sourceURL, err))
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to read : %v %v", sourceURL, err))
	}
	reader = bytes.NewReader(content)
	reader, err = getEncodingReader(transfer.Source.Compression, reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to encoding reader : %v %v", sourceURL, err))
	}
	content, err = ioutil.ReadAll(reader)
	if err != nil {
		return NewErrorTransferObjectResponse(fmt.Sprintf("Failed to readall : %v %v", sourceURL, err))
	}
	if transfer.Source.DataFormat == "ndjson" {
		var processedTransfers, err = s.transferObjectFromNewLineDelimiteredJson(content, transfer, task)
		task.Progress.FileProcessed++
		var response = &TransferObjectResponse{
			RecordProcessed: int(task.Progress.RecordProcessed),
			RecordSkipped:   int(task.Progress.RecordSkipped),
		}
		response.ProcessedTransfers = processedTransfers
		if err != nil {
			response.Error = fmt.Sprintf("%v", err)
		}
		return response

	}
	return NewErrorTransferObjectResponse(fmt.Sprintf("Unsupported source format: %v: %v -> %v", transfer.Source.DataFormat, transfer.Source.Name, transfer.Target))

}




func expandWorkerVariables(text string, transfer *Transfer, source, target interface{}) (string, error) {
	if transfer.HasRecordLevelVariableExtraction() {
		variables, err  := buildVariableWorkerServiceMap(transfer.VariableExtraction, source, target)
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
	if transfer.HasRecordLevelVariableExtraction() {
		return expandWorkerVariables(transfer.Target.Name, transfer, source, target)

	}
	return transfer.Target.Name, nil
}



func (s *transferObjectService) transferObjectFromNewLineDelimiteredJson(source []byte, transfer *Transfer, task *TransferTask) ([]*ProcessedTransfer, error) {
	var result = make([]*ProcessedTransfer, 0)
	provider := NewProviderRegistry().registry[transfer.Source.DataType]
	transformer := NewTransformerRegistry().registry[transfer.Transformer]
	var transformedTargets = make(map[string]*TargetTransformation)
	var lines = strings.Split(string(source), "\n")
	predicate, _ := NewFilterRegistry().registry[transfer.Filter]
	var filtered = 0

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
						return nil, fmt.Errorf("Failed to lookup provider for match: %v %v", sourceData.MatchingFragment, sourceData.DataType)
					}
				}
			}
		}
		var source = provider()

		err := decodeJSONTarget(bytes.NewReader([]byte(line)), source)
		if err != nil {
			return nil, fmt.Errorf("Failed to decode json: [%v] %v %v", i, err, line)
		}
		if predicate == nil || predicate.Apply(source) {
			target, err := transformer(source)
			if err != nil {
				return nil, fmt.Errorf("Failed to transform %v", err)
			}
			buf := new(bytes.Buffer)
			err = encodeJSONSource(buf, target)
			if err != nil {
				return nil, err
			}
			transformedObject := strings.Replace(string(buf.Bytes()), "\n", "", buf.Len())



			targetKey, err := getTargetKey(transfer, source,  target)
			if err != nil {
				return nil, err
			}
			_, found := transformedTargets[targetKey]
			if ! found {
				targetTransfer := transfer.Clone()
				targetTransfer.Target.Name = targetKey
				targetTransfer.Meta.Name, err  = expandWorkerVariables(targetTransfer.Meta.Name , transfer, source, target)
				if err != nil {
					return nil, err
				}
				transformedTargets[targetKey] = &TargetTransformation{
					targetRecords:make([]string, 0),
					ProcessedTransfer:&ProcessedTransfer{
						Transfer:targetTransfer,
					},
				}
			}
			transformedTargets[targetKey].targetRecords = append(transformedTargets[targetKey].targetRecords, transformedObject)
			task.Progress.RecordProcessed++
			transformedTargets[targetKey].RecordProcessed++

		} else {
			task.Progress.RecordSkipped++
			filtered++
		}
		task.UpdateElapsed()
	}

	if len(transformedTargets) > 0 {
		for _, transformed := range transformedTargets {
			reader, err := encodeData(transfer.Target.Compression, []byte(strings.Join(transformed.targetRecords, "\n")))
			if err != nil {
				return nil, err
			}
			storageService, err := getStorageService(transfer.Target.Resource)
			if err != nil {
				return nil, err
			}
			err = storageService.Upload(transformed.ProcessedTransfer.Transfer.Target.Name, reader)
			if err != nil {
				return nil, fmt.Errorf("Failed to upload: %v %v", transfer.Target, err)
			}
			result = append(result, transformed.ProcessedTransfer)
		}
	}
	return result, nil
}

func newtransferObjectService(taskRegistry *TaskRegistry) TransferObjectService {
	return &transferObjectService{
		taskRegistry: taskRegistry,
	}
}

func NewErrorTransferObjectResponse(message string) *TransferObjectResponse {
	return &TransferObjectResponse{
		Error: message,
	}
}
