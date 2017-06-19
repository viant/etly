package etly

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const (
	SourceTypeURL       = "url"
	SourceTypeDatastore = "datastore"
)

var regExpCache = make(map[string]*regexp.Regexp)

func compileRegExpr(expr string) (*regexp.Regexp, error) {
	if result, found := regExpCache[expr]; found {
		return result, nil
	}
	compiledExpression, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}
	regExpCache[expr] = compiledExpression
	return compiledExpression, nil
}

type transferService struct {
	transferObjectService TransferObjectService
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
	return s.Transfer(task)
}

func (s *transferService) Transfer(task *TransferTask) error {
	templateTransfer := task.Transfer
	transfers, err := s.getTransferForTimeWindow(templateTransfer)
	if err != nil {
		return err
	}
	switch strings.ToLower(templateTransfer.Source.Type) {
	case SourceTypeURL:
		err = s.transferDataFromURLSources(transfers, task)
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

func (s *transferService) transferDataFromURLSources(transfers []*Transfer, task *TransferTask) error {
	for _, transfer := range transfers {
		_, err := s.transferDataFromURLSource(transfer, task)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *transferService) LoadMeta(metaResource *Resource) (*Meta, error) {
	storageService, err := getStorageService(metaResource)
	if err != nil {
		return nil, err
	}
	exists, err := storageService.Exists(metaResource.Name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return NewMeta(metaResource.Name), nil
	}
	storageObject, err := storageService.StorageObject(metaResource.Name)
	if err != nil {
		return nil, err
	}
	reader, err := storageService.Download(storageObject)
	if err != nil {
		return nil, err
	}
	var result = &Meta{}
	return result, decodeJsonTarget(reader, &result)
}

func (s *transferService) persistMeta(meta *Meta, metaResource *Resource) error {
	storageService, err := getStorageService(metaResource)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	err = encodeJsonSource(buffer, meta)
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

			compiledExpression, err := compileRegExpr(variableExtraction.RegExpr)

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

func (s *transferService) transferDataFromURLSource(transfer *Transfer, task *TransferTask) (result []*Meta, err error) {
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
		}, task)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return []*Meta{}, nil
		}
		return []*Meta{meta}, nil
	}
	result = make([]*Meta, 0)
	storageTransfers, err := s.expandTransferWithVariableExpression(transfer, candidates)
	if err != nil {
		return nil, err
	}
	limiter := toolbox.NewBatchLimiter(transfer.MaxParallelTransfers|4, len(storageTransfers))
	for _, storageTransfer := range storageTransfers {
		go func(storageTransfer *StorageObjectTransfer) {
			limiter.Acquire()
			defer limiter.Done()
			meta, e := s.transferFromURLSource(storageTransfer, task)
			if e != nil {
				err = e
			}
			limiter.Mutex.Lock()
			defer limiter.Mutex.Unlock()
			if meta != nil {
				result = append(result, meta)
			}

		}(storageTransfer)
	}
	limiter.Wait()
	return result, err
}

func (s *transferService) filterStorageObjects(storageTransfer *StorageObjectTransfer) error {
	meta, err := s.LoadMeta(storageTransfer.Transfer.Meta)
	if err != nil {
		return err
	}
	var filteredObjects = make([]storage.Object, 0)

	var filterRegExp = storageTransfer.Transfer.Source.FilterRegExp
	var filterCompileExpression *regexp.Regexp
	if filterRegExp != "" {
		filterCompileExpression, err = compileRegExpr(storageTransfer.Transfer.Source.FilterRegExp)
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

func (s *transferService) transferFromURLSource(storageTransfer *StorageObjectTransfer, task *TransferTask) (*Meta, error) {
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
		return s.transferFromUrlToUrl(storageTransfer, task)
	case SourceTypeDatastore:
		return s.transferFromUrlToDatastore(storageTransfer, task)
	}
	return nil, fmt.Errorf("Unsupported Transfer for target type: %v", transfer.Target.Type)
}

func (s *transferService) transferFromUrlToDatastore(storageTransfer *StorageObjectTransfer, task *TransferTask) (*Meta, error) {
	meta, err := s.LoadMeta(storageTransfer.Transfer.Meta)
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
		atomic.AddInt32(&task.Progress.FileProcessed, 1)
	}
	job := &LoadJob{
		Credential: storageTransfer.Transfer.Target.CredentialFile,
		TableId:    resourceFragments[1],
		DatasetId:  resourceFragments[0],
		ProjectId:  parsedUrl.Host,
		Schema:     schema,
		URIs:       URIs,
	}
	task.UpdateElapsed()
	status, jobId, err := NewBigqueryService().Load(job)
	if err != nil {
		return nil, err
	}
	task.UpdateElapsed()
	if len(status.Errors) > 0 {
		for i, er := range status.Errors {
			fmt.Printf("ERR %v -> %v", i, er)
		}
		return nil, fmt.Errorf(status.Errors[0].Message)
	}
	message := fmt.Sprintf("Status: %v  with job id: %v", status.State, jobId)
	for _, storageObject := range storageTransfer.StorageObjects {
		URIs = append(URIs, storageObject.URL())
		meta.Processed[storageObject.URL()] = NewObjectMeta(storageTransfer.Transfer.Source.Name,
			storageObject.URL(),
			message,
			"",
			len(storageTransfer.StorageObjects),
			0,
			&startTime)
	}
	err = s.persistMeta(meta, storageTransfer.Transfer.Meta)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *transferService) transferFromUrlToUrl(storageTransfer *StorageObjectTransfer, task *TransferTask) (*Meta, error) {
	transfer := storageTransfer.Transfer
	candidates := storageTransfer.StorageObjects
	meta, err := s.LoadMeta(transfer.Meta)
	if err != nil {
		return nil, err
	}
	var now = time.Now()
	var source = transfer.Source.Name
	var target = transfer.Target
	var metaUrl = transfer.Meta.Name
	//all processed nothing new, the current assumption is that the whole file is process at once.
	for len(candidates) == 0 {
		logger.Println("No candidates no process")
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
			recordsProcessed, recordSkipped, e := s.transferObject(candidate, targetTransfer, task)

			var errMessage string
			if e != nil {
				errMessage = e.Error()
				if e != gzip.ErrChecksum {
					logger.Printf("Failed to targetTransfer: %v %T\n", e, e)
					err = e
					return
				}
			}

			objectMeta := NewObjectMeta(targetTransfer.Source.Name,
				candidate.URL(),
				"",
				errMessage,
				recordsProcessed,
				recordSkipped,
				&startTime)
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

func (s *transferService) transferObject(source storage.Object, transfer *Transfer, task *TransferTask) (int, int, error) {
	request := &TransferObjectRequest{
		SourceURL: source.URL(),
		Transfer:  transfer,
		TaskId:    task.Id + "--transferObject",
	}
	var response = s.transferObjectService.Transfer(request)
	var err error
	if response.Error != "" {
		err = errors.New(response.Error)
	}
	atomic.AddInt32(&task.Progress.RecordProcessed, int32(response.RecordProcessed))
	atomic.AddInt32(&task.Progress.RecordSkipped, int32(response.RecordProcessed))
	atomic.AddInt32(&task.Progress.FileProcessed, 1)
	return response.RecordProcessed, response.RecordSkipped, err

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
		source := expandDateExpressionIfPresent(transfer.Source.Name, &sourceTime)
		source = expandCurrentWorkingDirectory(source)
		target := expandDateExpressionIfPresent(transfer.Target.Name, &sourceTime)
		target = expandCurrentWorkingDirectory(target)
		metaUrl := expandDateExpressionIfPresent(transfer.Meta.Name, &sourceTime)
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

func newTransferService(transferObjectService TransferObjectService) *transferService {
	return &transferService{
		transferObjectService: transferObjectService,
	}
}
