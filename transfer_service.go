package etly

import (
	"bytes"
	"fmt"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

type TransferService struct {
	StorageService storage.Service
	decoderFactory toolbox.DecoderFactory
	encoderFactory toolbox.EncoderFactory
}

func (s *TransferService) appendContentObject(folderUrl string, collection *[]storage.Object) error {
	storageObjects, err := s.StorageService.List(folderUrl)
	if err != nil {
		return err
	}
	for _, objectStorage := range storageObjects {
		if objectStorage.IsFolder() {
			if objectStorage.URL() != folderUrl {
				err = s.appendContentObject(objectStorage.URL(), collection)
				if err != nil {
					return err
				}
			}
		} else {
			*collection = append(*collection, objectStorage)
		}
	}
	return nil
}

func (t *TransferService) Run(task *TransferTask) (err error) {
	task.Status = taskRunningStatus
	defer func() {
		task.Status = taskDoneStatus
		if err != nil {
			task.Error = err.Error()
			task.Status = taskErrorStatus
		}

	}()
	return t.Transfer(task.Transfer, task.Progress)
}

func (s *TransferService) Transfer(transfer *Transfer, progress *TransferProgress) error {
	var transfers, err = s.expandTransfer(transfer)
	if err != nil {
		return err
	}

	for _, expandedTransfer := range transfers {
		_, err := s.transferFromExpandedUrl(expandedTransfer, progress)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *TransferService) encodeSource(writer io.Writer, target interface{}) error {
	return s.encoderFactory.Create(writer).Encode(target)
}

func (s *TransferService) decodeTarget(reader io.Reader, target interface{}) error {
	return s.decoderFactory.Create(reader).Decode(target)
}

func (s *TransferService) loadMeta(URL string) (*Meta, error) {
	exists, err := s.StorageService.Exists(URL)
	if err != nil {
		return nil, err
	}
	if !exists {
		return NewMeta(URL), nil
	}
	storageObject, err := s.StorageService.StorageObject(URL)
	if err != nil {
		return nil, err
	}
	reader, err := s.StorageService.Download(storageObject)
	if err != nil {
		return nil, err
	}
	var result = &Meta{}
	return result, s.decodeTarget(reader, &result)
}

func (s *TransferService) persistMeta(meta *Meta) error {
	buffer := new(bytes.Buffer)
	err := s.encoderFactory.Create(buffer).Encode(meta)
	if err != nil {
		return err
	}
	return s.StorageService.Upload(meta.URL, bytes.NewReader(buffer.Bytes()))
}

func buildVariableMap(variableExtractionRules []*VariableExtraction, source storage.Object) (map[string]string, error) {
	var result = make(map[string]string)
	for _, variableExtraction := range variableExtractionRules {
		var value = ""
		switch variableExtraction.Source {
		case "source":

			compiledExpression, err := regexp.Compile(variableExtraction.RegExpr)
			if err != nil {
				return nil, fmt.Errorf("Failed to build variable - unable to compile expr: %v due to %v", variableExtraction.RegExpr, err)
			}
			if compiledExpression.MatchString(source.URL()) {
				matched := compiledExpression.FindStringSubmatch(source.URL())
				value = matched[0]
			}

			result[variableExtraction.Name] = value

		case "record":
			return nil, fmt.Errorf("Not supporte yet, keep waiting,  source: %v", variableExtraction.Source)

		default:
			return nil, fmt.Errorf("Unsupported source: %v", variableExtraction.Source)

		}
	}
	return result, nil
}

func expandVaiables(text string, variables map[string]string) string {
	for k, v := range variables {
		if strings.Contains(text, k) {
			text = strings.Replace(text, k, v, -1)
		}
	}
	return text
}

func (s *TransferService) expandTransferWithVariableExpression(transfer *Transfer, storeObjects []storage.Object) ([]*StorageObjectTransfer, error) {
	var groupedTransfers = make(map[string]*StorageObjectTransfer)
	for _, storageObject := range storeObjects {
		var variables, err = buildVariableMap(transfer.VariableExtraction, storageObject)
		if err != nil {
			return nil, err
		}
		expandedTarget := expandVaiables(transfer.Target, variables)
		expandedMetaUrl := expandVaiables(transfer.MetaUrl, variables)
		key := expandedTarget + expandedMetaUrl

		storageTransfer, found := groupedTransfers[key]
		if !found {
			storageTransfer = &StorageObjectTransfer{
				Transfer:       transfer.Clone(transfer.Source, expandedTarget, expandedMetaUrl),
				StorageObjects: make([]storage.Object, 0),
			}
		}
		storageTransfer.StorageObjects = append(storageTransfer.StorageObjects, storageObject)

	}
	var result = make([]*StorageObjectTransfer, 0)
	for _, storageTransfer := range groupedTransfers {
		result = append(result, storageTransfer)
	}
	return result, nil
}

func (s *TransferService) transferFromExpandedUrl(transfer *Transfer, progress *TransferProgress) ([]*Meta, error) {

	var candidates = make([]storage.Object, 0)
	err := s.appendContentObject(transfer.Source, &candidates)
	if err != nil {
		return nil, err
	}

	if len(transfer.VariableExtraction) == 0 {
		meta, err := s.transferFromUrl(&StorageObjectTransfer{
			Transfer:       transfer,
			StorageObjects: candidates,
		}, progress)
		if err != nil {
			return nil, err
		}
		return []*Meta{meta}, nil
	}
	var result = make([]*Meta, 0)

	storageTransfers, err := s.expandTransferWithVariableExpression(transfer, candidates)
	if err != nil {
		return nil, err
	}

	for _, storageTransfer := range storageTransfers {
		meta, err := s.transferFromUrl(storageTransfer, progress)
		if err != nil {
			return nil, err
		}
		result = append(result, meta)
	}

	return result, nil

}

func (s *TransferService) transferFromUrl(storageTransfer *StorageObjectTransfer, progress *TransferProgress) (*Meta, error) {
	transfer := storageTransfer.Transfer
	switch transfer.SourceType {
	case "url":

		switch transfer.Target {
		case "url":
			return s.transferFromUrlToUrl(storageTransfer, progress)
		case "datastore":
			return s.transferFromUrlToDatastore(storageTransfer, progress)
		}
	case "datastore":
		return nil, fmt.Errorf("Unsupported yet, keep waiting for soruce type: %v", transfer.SourceType)
	}
	return nil, fmt.Errorf("Unsupported transfer for soruce type: %v", transfer.SourceType)
}

func (s *TransferService) transferFromUrlToDatastore(storageTransfer *StorageObjectTransfer, progress *TransferProgress) (*Meta, error) {
	//TODO big query transfer
	return nil, nil
}

func (s *TransferService) transferFromUrlToUrl(storageTransfer *StorageObjectTransfer, progress *TransferProgress) (*Meta, error) {
	transfer := storageTransfer.Transfer
	candidates := storageTransfer.StorageObjects
	meta, err := s.loadMeta(transfer.MetaUrl)
	if err != nil {
		return nil, err
	}
	var now = time.Now()
	var source = transfer.Source
	var target = transfer.Target
	var metaUrl = transfer.MetaUrl
	//all processed nothing new, the current assumption is that the whole file is process at once.
	for len(meta.Processed) == len(candidates) {
		return nil, nil
	}
	limiter := toolbox.NewBatchLimiter(transfer.MaxParallelTransfers|4, len(candidates))
	var currentTransfers int32 = 0
	for _, candidate := range candidates {

		go func(candidate storage.Object, transferSource *Transfer) {
			limiter.Acquire()
			defer limiter.Done()
			if transfer.SourceExt != "" {
				if !strings.HasSuffix(candidate.URL(), transfer.SourceExt) {
					return
				}
			}
			if _, found := meta.Processed[candidate.URL()]; found {
				return
			}
			if transfer.MaxTransfers > 0 && int(atomic.LoadInt32(&currentTransfers)) > transfer.MaxTransfers {
				return
			}
			targetTransfer := transferSource.Clone(source, target, metaUrl)
			targetTransfer.Target = expandModExpressionIfPresent(transferSource.Target, hash(candidate.URL()))

			if strings.Contains(targetTransfer.Target, "<file>") {
				targetTransfer.Target = strings.Replace(targetTransfer.Target, "<file>", extractFileNameFromUrl(candidate.URL()), 1)
			}

			startTime := time.Now()
			recordProcessed, filtered, e := s.transferObject(candidate, targetTransfer, progress)
			if e != nil {
				logger.Printf("Failed to targetTransfer: %v\n", e)
				err = e
			}

			objectMeta := &ObjectMeta{
				Source:              candidate.URL(),
				Target:              targetTransfer.Target,
				RecordProcessed:     recordProcessed,
				RecordSkipped:       filtered,
				Timestamp:           time.Now(),
				ProcessingTimeInSec: int(time.Now().Unix() - startTime.Unix()),
			}
			atomic.AddInt32(&currentTransfers, 1)
			limiter.Mutex.Lock()
			defer limiter.Mutex.Unlock()
			meta.Processed[candidate.URL()] = objectMeta
		}(candidate, transfer)
	}
	limiter.Wait()
	if err != nil {
		return nil, err
	}
	meta.RecentTransfers = int(currentTransfers)
	meta.ProcessingTimeInSec = int(time.Now().Unix() - now.Unix())
	logger.Printf("Completed: [%v] %v files in %v sec\n", transfer.Name, len(meta.Processed), meta.ProcessingTimeInSec)
	return meta, s.persistMeta(meta)
}

func (s *TransferService) transferObject(source storage.Object, transfer *Transfer, progress *TransferProgress) (int, int, error) {
	_, hasProvider := NewProviderRegistry().registry[transfer.SourceDataType]
	if !hasProvider {
		return 0, 0, fmt.Errorf("Failed to lookup provider for data type '%v':  %v -> %v", transfer.SourceDataType, transfer.Source, transfer.Target)
	}

	_, hasTransformer := NewTransformerRegistry().registry[transfer.Transformer]
	if !hasTransformer {
		return 0, 0, fmt.Errorf("Failed to lookup transformer %v: %v -> %v", transfer.Transformer, transfer.Source, transfer.Target)
	}

	defer func() {
		atomic.AddInt32(&progress.FileProcessed, 1)
	}()

	reader, err := s.StorageService.Download(source)
	if err != nil {
		return 0, 0, err
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return 0, 0, err
	}
	reader = bytes.NewReader(content)
	reader, err = getEncodingReader(transfer.SourceEncoding, reader)
	if err != nil {
		return 0, 0, err
	}
	content, err = ioutil.ReadAll(reader)
	if err != nil {
		return 0, 0, err
	}
	if transfer.SourceFormat == "ndjson" {
		return s.transferObjectFromNewLineDelimiteredJson(content, transfer, progress)
	}
	return 0, 0, fmt.Errorf("Unsupported source format: %v: %v -> %v", transfer.SourceFormat, transfer.Source, transfer.Target)
}

func (s *TransferService) transferObjectFromNewLineDelimiteredJson(source []byte, transfer *Transfer, progress *TransferProgress) (int, int, error) {
	provider := NewProviderRegistry().registry[transfer.SourceDataType]
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
		if len(transfer.SourceDataTypeMatch) > 0 {
			for _, sourceData := range transfer.SourceDataTypeMatch {
				if strings.Contains(line, sourceData.MatchingFragment) {
					if sourceData.DataType == "" {
						continue outer
					}
					provider = NewProviderRegistry().registry[sourceData.DataType]
					if provider == nil {
						return 0, 0, fmt.Errorf("Failed to lookup provider for match: %v %v", sourceData.MatchingFragment, sourceData.DataType)
					}
				}
			}
		}

		var source = provider()
		err := s.decodeTarget(bytes.NewReader([]byte(line)), source)
		if err != nil {
			return 0, 0, fmt.Errorf("Failed to decode json: [%v] %v %v", i, err, line)
		}

		if predicate == nil || predicate.Apply(source) {
			target, err := transformer(source)
			if err != nil {
				return 0, 0, fmt.Errorf("Failed to transform %v", err)
			}
			buf := new(bytes.Buffer)
			err = s.encodeSource(buf, target)
			if err != nil {
				return 0, 0, err
			}
			tranformedObject := strings.Replace(string(buf.Bytes()), "\n", "", buf.Len())
			transformed = append(transformed, tranformedObject)
			atomic.AddInt32(&progress.RecordProcessed, 1)
		} else {
			atomic.AddInt32(&progress.RecordSkipped, 1)
			filtered++
		}
		atomic.StoreInt32(&progress.ElapsedInSec, int32(time.Now().Unix()-progress.startTime.Unix()))
	}
	if len(transformed) > 0 {
		reader, err := encodeData(transfer.TargetEncoding, []byte(strings.Join(transformed, "\n")))
		if err != nil {
			return 0, 0, err
		}
		err = s.StorageService.Upload(transfer.Target, reader)
		if err != nil {
			return 0, 0, fmt.Errorf("Failed to upload: %v %v", transfer.Target, err)
		}
	}
	return len(transformed), filtered, nil
}

func (s *TransferService) expandTransfer(transfer *Transfer) ([]*Transfer, error) {
	var transfers = make(map[string]*Transfer)
	now := time.Now()
	for i := 0; i < transfer.TimeWindow; i++ {
		var timeUnitFactor, err = timeUnitFactor(transfer.TimeWindowUnit)
		if err != nil {
			return nil, err
		}
		var sourceTime time.Time
		var delta = time.Duration(int64(-i) * timeUnitFactor)
		if delta == 0 {
			sourceTime = now
		} else {
			sourceTime = now.Add(delta)
		}
		var source = expandDateExpressionIfPresent(transfer.Source, &sourceTime)
		source = expandCurrentWorkingDirectory(source)
		var target = expandDateExpressionIfPresent(transfer.Target, &sourceTime)
		target = expandCurrentWorkingDirectory(target)
		var metaUrl = expandDateExpressionIfPresent(transfer.MetaUrl, &sourceTime)
		metaUrl = expandCurrentWorkingDirectory(metaUrl)
		transferKey := source + "//" + target + "//" + metaUrl
		candidate, found := transfers[transferKey]
		if !found {
			candidate = transfer.Clone(source, target, metaUrl)
			transfers[transferKey] = candidate
		}
	}
	var result = make([]*Transfer, 0)
	for _, t := range transfers {
		result = append(result, t)
	}
	return result, nil
}

func NewTransferService(storageService storage.Service, decoderFactory toolbox.DecoderFactory, encoderFactory toolbox.EncoderFactory) *TransferService {
	return &TransferService{
		StorageService: storageService,
		decoderFactory: decoderFactory,
		encoderFactory: encoderFactory,
	}
}
