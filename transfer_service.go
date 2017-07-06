package etly

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
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
	for i, transfer := range transfers {
		_, err := s.transferDataFromURLSource(i, transfer, task)
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
	return result, decodeJSONTarget(reader, &result)
}

func (s *transferService) persistMeta(resourcedMeta *ResourcedMeta) error {
	storageService, err := getStorageService(resourcedMeta.Resource)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	err = encodeJSONSource(buffer, resourcedMeta.Meta)
	if err != nil {
		return err
	}
	return storageService.Upload(resourcedMeta.Meta.URL, bytes.NewReader(buffer.Bytes()))
}

func (s *transferService) expandTransferWithVariableExpression(transfer *Transfer, storeObjects []storage.Object) ([]*StorageObjectTransfer, error) {
	var groupedTransfers = make(map[string]*StorageObjectTransfer)
	for _, storageObject := range storeObjects {
		var variables, err = buildVariableMasterServiceMap(transfer.VariableExtraction, storageObject)
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

func (s *transferService) transferDataFromURLSource(index int, transfer *Transfer, task *TransferTask) (result []*Meta, err error) {
	storageService, err := getStorageService(transfer.Source.Resource)
	if err != nil {
		return nil, err
	}
	var candidates = make([]storage.Object, 0)
	err = appendContentObject(storageService, transfer.Source.Name, &candidates)
	if err != nil {
		return nil, err
	}

	if !transfer.HasVariableExtraction() {
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
	if transfer.MaxParallelTransfers == 0 {
		transfer.MaxParallelTransfers = 4
	}
	limiter := toolbox.NewBatchLimiter(transfer.MaxParallelTransfers, len(storageTransfers))
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

	storageTransfer.IndexedStorageObjects = make(map[string]storage.Object)

	for _, candidate := range storageTransfer.StorageObjects {
		storageTransfer.IndexedStorageObjects[candidate.URL()] = candidate
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
			continue
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
	return nil, fmt.Errorf("Unsupported ProcessedTransfers for target type: %v", transfer.Target.Type)
}

func (s *transferService) updateMetaStatus(meta *Meta, storageTransfer *StorageObjectTransfer, err error) {
	if err != nil {
		meta.AddError(fmt.Sprintf("%v", err))
	}
	if storageTransfer != nil && storageTransfer.IndexedStorageObjects != nil {
		var sourceStatus = &ProcessingStatus{}

		for source := range storageTransfer.IndexedStorageObjects {
			if value, found := meta.Processed[source]; found {
				sourceStatus.ResourceProcessed++
				sourceStatus.RecordProcessed += int(value.RecordProcessed)
			} else {
				sourceStatus.ResourcePending++
			}
		}
		meta.PutStatus(storageTransfer.Transfer.Source.Name, sourceStatus)
	}
}

func (s *transferService) transferFromUrlToDatastore(storageTransfer *StorageObjectTransfer, task *TransferTask) (m *Meta, err error) {
	meta, e := s.LoadMeta(storageTransfer.Transfer.Meta)
	if e != nil {
		return nil, e
	}
	defer func() {
		s.updateMetaStatus(meta, storageTransfer, err)
		e := s.persistMeta(&ResourcedMeta{meta, storageTransfer.Transfer.Meta})
		if err == nil {
			err = e
		}
	}()
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
		TableID:    resourceFragments[1],
		DatasetID:  resourceFragments[0],
		ProjectID:  parsedUrl.Host,
		Schema:     schema,
		URIs:       URIs,
	}
	task.UpdateElapsed()
	status, jobId, err := NewBigqueryService().Load(job)
	if err != nil {
		return nil, err
	}
	task.UpdateElapsed()

	var buffer bytes.Buffer
	errorURLMap := make(map[string]bool)
	if len(status.Errors) > 0 {
		for _, er := range status.Errors {
			errorURLMap[er.Location] = true
			buffer.WriteString(er.Error())
			buffer.WriteByte('\n')
			// Location can be shown as empty string
			if er.Location == "" {
				continue
			}
			if strings.Contains(er.Message , "Field:") {
				// Log this to meta file so we can skip it next time.
				meta.Processed[er.Location] = NewObjectMeta(storageTransfer.Transfer.Source.Name,
					er.Location,
					"Error loading to GBQ",
					er.Error(),
					0,
					0,
					&startTime)
			}
		}
		return nil, fmt.Errorf("Failed to upload: %v", buffer.String())
	}
	message := fmt.Sprintf("Status: %v  with job id: %v", status.State, jobId)
	for _, storageObject := range storageTransfer.StorageObjects {
		meta.Processed[storageObject.URL()] = NewObjectMeta(storageTransfer.Transfer.Source.Name,
			storageObject.URL(),
			message,
			"",
			len(storageTransfer.StorageObjects),
			0,
			&startTime)
	}

	return meta, err
}

type WorkerProcessedTransferMeta struct {
	ProcessedTransfers []*ProcessedTransfer
	ObjectMeta         *ObjectMeta
}

func (s *transferService) transferFromUrlToUrl(storageTransfer *StorageObjectTransfer, task *TransferTask) (m *Meta, err error) {
	transfer := storageTransfer.Transfer
	candidates := storageTransfer.StorageObjects
	meta, e := s.LoadMeta(transfer.Meta)
	if e != nil {
		return nil, e
	}

	defer func() {
		s.updateMetaStatus(meta, storageTransfer, err)
		e := s.persistMeta(&ResourcedMeta{meta, storageTransfer.Transfer.Meta})
		if err == nil {
			err = e
		}
	}()

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
	workerProcessedTransferMeta := make([]*WorkerProcessedTransferMeta, 0)

	var currentTransfers int32
	for _, candidate := range candidates {
		go func(limiter *toolbox.BatchLimiter, candidate storage.Object, transferSource *Transfer) {
			limiter.Acquire()
			defer limiter.Done()
			targetTransfer := transferSource.New(source, target.Name, metaUrl)

			targetTransfer.Target.Name = expandModExpressionIfPresent(transferSource.Target.Name, hash(candidate.URL()))
			if strings.Contains(targetTransfer.Target.Name, "<file>") {
				targetTransfer.Target.Name = strings.Replace(targetTransfer.Target.Name, "<file>", extractFileNameFromURL(candidate.URL()), 1)
			}

			startTime := time.Now()
			recordsProcessed, recordSkipped, processedTransfers, e := s.transferObject(candidate, targetTransfer, task)
			var errMessage string
			if e != nil {
				errMessage = e.Error()
				if e != gzip.ErrChecksum {
					logger.Printf("Failed to targetTransfer: %v \n", e)
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
			atomic.AddInt32(&task.Progress.RecordProcessed, int32(recordsProcessed))
			atomic.AddInt32(&task.Progress.RecordSkipped, int32(recordSkipped))
			atomic.AddInt32(&task.Progress.FileProcessed, 1)
			atomic.AddInt32(&currentTransfers, 1)
			limiter.Mutex.Lock()
			if len(processedTransfers) > 0 {
				workerProcessedTransferMeta = append(workerProcessedTransferMeta, &WorkerProcessedTransferMeta{
					ProcessedTransfers: processedTransfers,
					ObjectMeta:         objectMeta,
				})
			}
			defer limiter.Mutex.Unlock()
			meta.Processed[candidate.URL()] = objectMeta
		}(limiter, candidate, transfer)
	}

	if len(workerProcessedTransferMeta) > 0 {
		s.updateWorkerProcessedTransferMeta(workerProcessedTransferMeta, storageTransfer)
	}

	limiter.Wait()
	if err != nil {
		return nil, err
	}
	task.UpdateElapsed()
	meta.RecentTransfers = int(currentTransfers)
	meta.ProcessingTimeInSec = int(time.Now().Unix() - now.Unix())
	logger.Printf("Completed: [%v] %v files in %v sec\n", transfer.Name, len(meta.Processed), meta.ProcessingTimeInSec)
	return meta, err

}

func (s *transferService) updateWorkerProcessedTransferMeta(workerProcessedTransferMetas []*WorkerProcessedTransferMeta, storageTransfer *StorageObjectTransfer) {
	var resourcedMetas = make(map[string]*ResourcedMeta)
	for _, workerProcessedTransferMeta := range workerProcessedTransferMetas {
		for _, processedTransfer := range workerProcessedTransferMeta.ProcessedTransfers {
			resourceMeta, found := resourcedMetas[processedTransfer.Transfer.Meta.Name]
			if !found {
				meta, err := s.LoadMeta(processedTransfer.Transfer.Meta)
				if err != nil {
					logger.Printf("Failed to load worker meta: %v ", err)
					continue
				}
				resourceMeta = &ResourcedMeta{
					Meta:     meta,
					Resource: processedTransfer.Transfer.Meta,
				}
				resourcedMetas[processedTransfer.Transfer.Meta.Name] = resourceMeta
			}

			resourceMeta.Meta.Processed[workerProcessedTransferMeta.ObjectMeta.Target] = workerProcessedTransferMeta.ObjectMeta
			resourceMeta.Meta.Status.RecordProcessed += processedTransfer.RecordProcessed
		}
	}

	for _, resourcedMeta := range resourcedMetas {
		err := s.persistMeta(resourcedMeta)
		if err != nil {
			logger.Printf("Failed to persist worker meta: %v", err)
		}
	}
}

func (s *transferService) transferObject(source storage.Object, transfer *Transfer, task *TransferTask) (int, int, []*ProcessedTransfer, error) {
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
	return response.RecordProcessed, response.RecordSkipped, response.ProcessedTransfers, err

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
