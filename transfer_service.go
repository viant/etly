package etly

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"net/url"
)

const (
	SourceTypeURL       = "url"
	SourceTypeDatastore = "datastore"
)

type transferService struct {
	decoderFactory toolbox.DecoderFactory
	encoderFactory toolbox.EncoderFactory
}

func appendContentObject(storageService storage.Service, folderUrl string, collection *[]storage.Object) error {
	storageObjects, err := storageService.List(folderUrl)
	if err != nil {
		return err
	}
	for _, objectStorage := range storageObjects {
		if objectStorage.IsFolder() {
			if objectStorage.URL() != folderUrl {
				err = appendContentObject(storageService, objectStorage.URL(), collection)
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

func (s *transferService) Run(task *TransferTask) (err error) {
	task.Status = taskRunningStatus
	defer func(error) {
		task.Status = taskDoneStatus
		if err != nil {
			task.Error = err.Error()
			task.Status = taskErrorStatus
		}

	}(err)
	return s.Transfer(task.Transfer, task.Progress)
}

func (s *transferService) Transfer(templateTransfer *Transfer, progress *TransferProgress) error {
	transfers, err := s.getTransferForTimeWindow(templateTransfer)
	if err != nil {
		return err
	}
	switch strings.ToLower(templateTransfer.Source.Type) {
	case SourceTypeURL:
		err = s.transferDataFromURLSources(transfers, progress)
		if err != nil {
			return err
		}
	case SourceTypeDatastore:
		return fmt.Errorf("Unsupported yet source Type %v", templateTransfer.Source.Type)
	default:
		return fmt.Errorf("Unsupported source Type %v", templateTransfer.Source.Type)
	}

	return nil
}

func (s *transferService) transferDataFromURLSources(transfers []*Transfer, progress *TransferProgress) error {
	for _, transfer := range transfers {
		_, err := s.transferDataFromURLSource(transfer, progress)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *transferService) encodeSource(writer io.Writer, target interface{}) error {
	return s.encoderFactory.Create(writer).Encode(target)
}

func (s *transferService) decodeTarget(reader io.Reader, target interface{}) error {
	return s.decoderFactory.Create(reader).Decode(target)
}

func (s *transferService) loadMeta(metaReousrce *Resource) (*Meta, error) {
	storageService, err := getStorageService(metaReousrce)
	if err != nil {
		return nil, err
	}
	exists, err := storageService.Exists(metaReousrce.Name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return NewMeta(metaReousrce.Name), nil
	}
	storageObject, err := storageService.StorageObject(metaReousrce.Name)
	if err != nil {
		return nil, err
	}
	reader, err := storageService.Download(storageObject)
	if err != nil {
		return nil, err
	}
	var result = &Meta{}
	return result, s.decodeTarget(reader, &result)
}

func (s *transferService) persistMeta(meta *Meta, metaResource *Resource) error {
	storageService, err := getStorageService(metaResource)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	err = s.encoderFactory.Create(buffer).Encode(meta)
	if err != nil {
		return err
	}
	return storageService.Upload(meta.URL, bytes.NewReader(buffer.Bytes()))
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
				value = matched[1]
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

func (s *transferService) expandTransferWithVariableExpression(transfer *Transfer, storeObjects []storage.Object) ([]*StorageObjectTransfer, error) {
	var groupedTransfers = make(map[string]*StorageObjectTransfer)
	for _, storageObject := range storeObjects {
		var variables, err = buildVariableMap(transfer.VariableExtraction, storageObject)
		if err != nil {
			return nil, err
		}
		expandedTarget := expandVaiables(transfer.Target.Name, variables)
		expandedMetaUrl := expandVaiables(transfer.Meta.Name, variables)
		key := expandedTarget + expandedMetaUrl

		storageTransfer, found := groupedTransfers[key]
		if !found {
			storageTransfer = &StorageObjectTransfer{
				Transfer:       transfer.New(transfer.Source.Name, expandedTarget, expandedMetaUrl),
				StorageObjects: make([]storage.Object, 0),
			}

			groupedTransfers[key] = storageTransfer
		}

		storageTransfer.StorageObjects = append(storageTransfer.StorageObjects, storageObject)

	}
	var result = make([]*StorageObjectTransfer, 0)
	for _, storageTransfer := range groupedTransfers {
		result = append(result, storageTransfer)
	}
	return result, nil
}

func (s *transferService) transferDataFromURLSource(transfer *Transfer, progress *TransferProgress) ([]*Meta, error) {
	storageService, err := getStorageService(transfer.Source.Resource)
	if err != nil {
		return nil, err
	}
	var candidates = make([]storage.Object, 0)
	err = appendContentObject(storageService, transfer.Source.Name, &candidates)
	if err != nil {
		return nil, err
	}
	if len(transfer.VariableExtraction) == 0 {
		meta, err := s.transferFromURLSource(&StorageObjectTransfer{
			Transfer:       transfer,
			StorageObjects: candidates,
		}, progress)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return []*Meta{}, nil
		}
		return []*Meta{meta}, nil
	}
	var result = make([]*Meta, 0)
	storageTransfers, err := s.expandTransferWithVariableExpression(transfer, candidates)
	if err != nil {
		return nil, err
	}
	logger.Printf("Process %v candidate(s) for JobId:%v\n", len(candidates), transfer.Name)
	for _, storageTransfer := range storageTransfers {
		meta, err := s.transferFromURLSource(storageTransfer, progress)
		if err != nil {
			return nil, err
		}
		if meta != nil {
			result = append(result, meta)
		}
	}

	return result, nil

}

func (s *transferService) filterStorageObjects(storageTransfer *StorageObjectTransfer) error {
	meta, err := s.loadMeta(storageTransfer.Transfer.Meta)
	if err != nil {
		return err
	}
	var filteredObjects = make([]storage.Object, 0)

	var filterRegExp = storageTransfer.Transfer.Source.FilterRegExp
	var filterCompileExpression *regexp.Regexp
	if filterRegExp != "" {
		filterCompileExpression, err = regexp.Compile(storageTransfer.Transfer.Source.FilterRegExp)
		if err != nil {
			return fmt.Errorf("Failed to filter storage object %v %v", filterRegExp, err)
		}
	}
	var elgibleStorageCountSoFar = 0
	var alreadyProcess = 0
	for _, candidate := range storageTransfer.StorageObjects {
		if _, found := meta.Processed[candidate.URL()]; found {
			alreadyProcess++
			continue
		}
		if candidate.IsFolder() {
			continue
		}
		if filterCompileExpression != nil && !filterCompileExpression.MatchString(candidate.URL()) {
			continue
		}
		if storageTransfer.Transfer.MaxTransfers > 0 && elgibleStorageCountSoFar >= storageTransfer.Transfer.MaxTransfers {
			break
		}
		filteredObjects = append(filteredObjects, candidate)
		elgibleStorageCountSoFar++
	}
	storageTransfer.StorageObjects = filteredObjects
	return nil
}

func (s *transferService) transferFromURLSource(storageTransfer *StorageObjectTransfer, progress *TransferProgress) (*Meta, error) {
	err := s.filterStorageObjects(storageTransfer)
	if err != nil {
		return nil, err
	}
	logger.Printf("Process %v files for JobId:%v\n", len(storageTransfer.StorageObjects), storageTransfer.Transfer.Name)
	if len(storageTransfer.StorageObjects) == 0 {
		return nil, nil
	}
	transfer := storageTransfer.Transfer
	switch strings.ToLower(transfer.Target.Type) {
	case SourceTypeURL:
		return s.transferFromUrlToUrl(storageTransfer, progress)
	case SourceTypeDatastore:
		return s.transferFromUrlToDatastore(storageTransfer, progress)
	}
	return nil, fmt.Errorf("Unsupported transfer for target type: %v", transfer.Target.Type)
}

func (s *transferService) transferFromUrlToDatastore(storageTransfer *StorageObjectTransfer, progress *TransferProgress) (*Meta, error) {
	meta, err := s.loadMeta(storageTransfer.Transfer.Meta)
	if err != nil {
		return nil, err
	}
	var startTime = time.Now()

	var target = storageTransfer.Transfer.Target
	parsedUrl, err := url.Parse(target.Name)
	if err != nil {
		return nil, err
	}
	if parsedUrl.Path == "" {
		return nil, fmt.Errorf("Invalid BigQuery target, see the supported form: bg://project/datset.table")
	}

	var resourceFragments = strings.Split(parsedUrl.Path[1:], ".")
	if len(resourceFragments) != 2 {
		return nil, fmt.Errorf("Invalid resource , the supported:  bg://project/datset.table")
	}
	schema, err := SchemaFromFile(target.Schema.Name)
	if err != nil {
		return nil, err
	}
	var URIs = make([]string, 0)
	for _, storageObject := range storageTransfer.StorageObjects {
		URIs = append(URIs, storageObject.URL())
	}

	job := &LoadJob{
		Credential: storageTransfer.Transfer.Target.CredentialFile,
		TableId:    resourceFragments[1],
		DatasetId:  resourceFragments[0],
		ProjectId:  parsedUrl.Host,
		Schema:     schema,
		URIs:       URIs,
	}

	status, jobId, err := NewBigqueryService().Load(job)
	if err != nil {
		return nil, err
	}

	if len(status.Errors) > 0 {

		for i, er := range status.Errors {
			fmt.Printf("ERR %v -> %v", i, er)
		}
		return nil, fmt.Errorf(status.Errors[0].Message)
	}
	message := fmt.Sprintf("Status: %v  with job id: %v", status.State, jobId)
	for _, storageObject := range storageTransfer.StorageObjects {
		URIs = append(URIs, storageObject.URL())
		meta.Processed[storageObject.URL()] = NewObjectMeta(storageTransfer.Transfer.Source.Name, storageObject.URL(), message,
			len(storageTransfer.StorageObjects), 0, &startTime)
	}
	err = s.persistMeta(meta, storageTransfer.Transfer.Meta)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *transferService) transferFromUrlToUrl(storageTransfer *StorageObjectTransfer, progress *TransferProgress) (*Meta, error) {
	transfer := storageTransfer.Transfer
	candidates := storageTransfer.StorageObjects
	meta, err := s.loadMeta(transfer.Meta)
	if err != nil {
		return nil, err
	}
	var now = time.Now()
	var source = transfer.Source.Name
	var target = transfer.Target
	var metaUrl = transfer.Meta.Name
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

			targetTransfer := transferSource.New(source, target.Name, metaUrl)

			targetTransfer.Target.Name = expandModExpressionIfPresent(transferSource.Target.Name, hash(candidate.URL()))
			if strings.Contains(targetTransfer.Target.Name, "<file>") {
				targetTransfer.Target.Name = strings.Replace(targetTransfer.Target.Name, "<file>", extractFileNameFromURL(candidate.URL()), 1)
			}

			startTime := time.Now()
			recordsProcessed, recordSkipped, e := s.transferObject(candidate, targetTransfer, progress)
			if e != nil {
				logger.Printf("Failed to targetTransfer: %v\n", e)
				err = e
			}
			objectMeta := NewObjectMeta(targetTransfer.Source.Name, candidate.URL(), "", recordsProcessed, recordSkipped, &startTime)
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
	return meta, s.persistMeta(meta, storageTransfer.Transfer.Meta)
}

func (s *transferService) transferObject(source storage.Object, transfer *Transfer, progress *TransferProgress) (int, int, error) {
	_, hasProvider := NewProviderRegistry().registry[transfer.Source.DataType]
	if !hasProvider {
		return 0, 0, fmt.Errorf("Failed to lookup provider for data type '%v':  %v -> %v", transfer.Source.DataType, transfer.Source.Name, transfer.Target)
	}

	_, hasTransformer := NewTransformerRegistry().registry[transfer.Transformer]
	if !hasTransformer {
		return 0, 0, fmt.Errorf("Failed to lookup transformer %v: %v -> %v", transfer.Transformer, transfer.Source.Name, transfer.Target)
	}

	defer func() {
		atomic.AddInt32(&progress.FileProcessed, 1)
	}()

	storageService, err := getStorageService(transfer.Source.Resource)
	if err != nil {
		return 0, 0, err
	}

	reader, err := storageService.Download(source)
	if err != nil {
		return 0, 0, err
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return 0, 0, err
	}
	reader = bytes.NewReader(content)
	reader, err = getEncodingReader(transfer.Source.Compression, reader)
	if err != nil {
		return 0, 0, err
	}
	content, err = ioutil.ReadAll(reader)
	if err != nil {
		return 0, 0, err
	}
	if transfer.Source.DataFormat == "ndjson" {
		return s.transferObjectFromNewLineDelimiteredJson(content, transfer, progress)
	}
	return 0, 0, fmt.Errorf("Unsupported source format: %v: %v -> %v", transfer.Source.DataFormat, transfer.Source.Name, transfer.Target)
}

func (s *transferService) transferObjectFromNewLineDelimiteredJson(source []byte, transfer *Transfer, progress *TransferProgress) (int, int, error) {
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
		reader, err := encodeData(transfer.Target.Compression, []byte(strings.Join(transformed, "\n")))
		if err != nil {
			return 0, 0, err
		}
		storageService, err := getStorageService(transfer.Target.Resource)
		if err != nil {
			return 0, 0, err
		}
		err = storageService.Upload(transfer.Target.Name, reader)
		if err != nil {
			return 0, 0, fmt.Errorf("Failed to upload: %v %v", transfer.Target, err)
		}
	}
	return len(transformed), filtered, nil
}

func (s *transferService) getTransferForTimeWindow(transfer *Transfer) ([]*Transfer, error) {
	var transfers = make(map[string]*Transfer)
	now := time.Now()
	for i := 0; i < transfer.TimeWindow.Duration; i++ {
		var timeUnit, err = transfer.TimeWindow.TimeUnit()
		if err != nil {
			return nil, err
		}
		var sourceTime time.Time
		var delta = time.Duration(-i) * timeUnit
		if delta == 0 {
			sourceTime = now
		} else {
			sourceTime = now.Add(delta)
		}
		var source = expandDateExpressionIfPresent(transfer.Source.Name, &sourceTime)
		source = expandCurrentWorkingDirectory(source)
		var target = expandDateExpressionIfPresent(transfer.Target.Name, &sourceTime)
		target = expandCurrentWorkingDirectory(target)
		var metaUrl = expandDateExpressionIfPresent(transfer.Meta.Name, &sourceTime)
		metaUrl = expandCurrentWorkingDirectory(metaUrl)
		transferKey := source + "//" + target + "//" + metaUrl
		candidate, found := transfers[transferKey]
		if !found {
			candidate = transfer.New(source, target, metaUrl)
			transfers[transferKey] = candidate
		}
	}
	var result = make([]*Transfer, 0)
	for _, t := range transfers {
		result = append(result, t)
	}
	return result, nil
}

func newTransferService(
	decoderFactory toolbox.DecoderFactory,
	encoderFactory toolbox.EncoderFactory) *transferService {
	return &transferService{
		decoderFactory: decoderFactory,
		encoderFactory: encoderFactory,
	}
}
