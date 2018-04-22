package etly

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/viant/dsc"
	"github.com/viant/etly/pkg/bigquery"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
)

const (
	SourceTypeURL         = "url"
	SourceTypeDatastore   = "datastore"
	defaultMaxAllowedSize = 1024 * 1024 * 64
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
	defer func() {
		if err != nil {
			task.Status = taskErrorStatus
			task.Error = err.Error()
		} else if task.Error != "" {
			task.Status = taskErrorStatus
		} else {
			task.Status = taskDoneStatus
		}
	}()
	err = s.Transfer(task)
	return err
}

func (s *transferService) Transfer(task *TransferTask) error {
	log.Printf("Starting transfer: ID(%s), NAME(%s)", task.Id, task.Transfer.Name)
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
		err = s.transferDataFromDatastoreSources(transfers, task)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unsupported source Type %v", templateTransfer.Source.Type)
	}

	return nil
}

func (s *transferService) transferDataFromDatastoreSources(transfers []*Transfer, task *TransferTask) error {
	for i, transfer := range transfers {
		_, err := s.transferDataFromDatastoreSource(i, transfer, task)
		if err != nil {
			return err
		}
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
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	var result = &Meta{}
	return result, decodeJSONTarget(content, &result)
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

func (s *transferService) transferDataInBatch(recordChannel chan map[string]interface{}, transfer *Transfer, task *TransferTask, fetchedCompleted *int32, threadId int) (completed bool, err error) {
	dataTypeProvider := NewProviderRegistry().registry[transfer.Source.DataType]
	transformer := NewTransformerRegistry().registry[transfer.Transformer]
	var transformedTargets TargetTransformations = make(map[string]*TargetTransformation)
	predicate := NewFilterRegistry().registry[transfer.Filter]
	var decodingError = &decodingError{}
	encoderFactory := toolbox.NewJSONEncoderFactory()
	var maxErrorCount = 0
	transfer.MaxErrorCounts = &maxErrorCount
	var batchCount = 0
	var state = make(map[string]interface{})
	state["$thread"] = threadId
	state["$batchCount"] = batchCount
	var count = len(recordChannel)

	var maxAllowedSize = transfer.Target.MaxAllowedSize
	if maxAllowedSize == 0 {
		maxAllowedSize = defaultMaxAllowedSize
	}
	var processed = 0
outer:
	for {
		select {
		case record := <-recordChannel:
			processed++
			normalizeRecord(record)
			buf := new(bytes.Buffer)
			err = encoderFactory.Create(buf).Encode(record)
			encoded := []byte(buf.String())
			if err != nil {
				break outer
			}
			if err = transferRecord(state, predicate, dataTypeProvider, encoded, transformer, transfer, transformedTargets, task, decodingError); err != nil {
				break outer
			}

			var length = transformedTargets.Length()
			var size = transformedTargets.Size()
			if length > 0 && size > 0 {
				var recordSize = size / length
				if recordSize+size > maxAllowedSize {
					var processed []*ProcessedTransfer
					if processed, err = transformedTargets.Upload(transfer); err != nil {
						break outer
					}
					transformedTargets = make(map[string]*TargetTransformation, 0)
					task.Progress.Update(processed...)
					batchCount++
					state["$batchCount"] = batchCount
					atomic.AddInt32(&task.Progress.BatchCount, 1)
				}
			}
		case <-time.After(time.Second):
			count = len(recordChannel)
			if count == 0 {
				completed = atomic.LoadInt32(fetchedCompleted) == 1
				if completed {
					if count == 0 {
						if transformedTargets.Length() > 0 {

							_, err = transformedTargets.Upload(transfer)
							if err != nil {
								break outer
							}

						}
						atomic.AddInt32(&task.Progress.BatchCount, int32(batchCount))
						break outer
					}
				}
			}
		}
	}
	if err != nil || completed {
		log.Printf("failed to log: %v", err)
		return true, err
	}
	return false, nil
}

func normalizeRecord(record map[string]interface{}) {
	for k, v := range record {
		if v == nil {
			continue
		}
		if toolbox.IsMap(v) {
			var aMap = toolbox.AsMap(v)
			normalizeRecord(aMap)
			record[k] = aMap
			continue
		}

		if toolbox.IsSlice(v) {
			var newSlice = make([]interface{}, 0)
			var aSlice = toolbox.AsSlice(v)
			for _, item := range aSlice {
				if toolbox.IsMap(item) {
					var aMap = toolbox.AsMap(v)
					normalizeRecord(aMap)
					item = aMap
				}
				newSlice = append(newSlice, item)
			}
			record[k] = newSlice
		}
	}
}

func (s *transferService) transferInTheBackground(recordChannel chan map[string]interface{}, transfer *Transfer, task *TransferTask, fetchedCompleted *int32) *sync.WaitGroup {
	var result = &sync.WaitGroup{}

	var completed bool
	var maxParallelTransfers = transfer.MaxParallelTransfers
	if maxParallelTransfers == 0 {
		maxParallelTransfers = 1
	}
	result.Add(maxParallelTransfers)
	for i := 0; i < maxParallelTransfers; i++ {
		go func(routineSeq int) {
			var err error
			defer func() {
				if err != nil {
					log.Printf("transferInTheBackground err: %v", err)
					task.Status = "error"
					task.Error = err.Error()
					atomic.StoreInt32(&task.StatusCode, StatusTaskNotRunning)
				}
				result.Done()
				return
			}()
			for {
				completed, err = s.transferDataInBatch(recordChannel, transfer, task, fetchedCompleted, routineSeq)
				if err != nil || completed {
					return
				}
			}
		}(i)
	}
	return result
}

func (s *transferService) isRunning(task *TransferTask) bool {
	var statusCode = atomic.LoadInt32(&task.StatusCode)
	isRunning := StatusTaskRunning == statusCode
	return isRunning
}

func (s *transferService) transferDataFromDatastoreSource(index int, transfer *Transfer, task *TransferTask) (result []*Meta, err error) {
	var config = transfer.Source.DsConfig
	if config == nil {
		return nil, fmt.Errorf("Dsconfig was nil")
	}
	for k, v := range config.Parameters {
		if value, ok := v.(string); ok {
			config.Parameters[k] = expandCurrentWorkingDirectory(value)
		}
	}
	if err := config.Init(); err != nil {
		return nil, err
	}

	if err := transfer.Validate(); err != nil {
		return nil, err
	}
	manager, err := dsc.NewManagerFactory().Create(config)
	if err != nil {
		return nil, err
	}
	var recordsChannel = make(chan map[string]interface{}, task.Transfer.Source.BatchSize+1)
	var fetchCompleted int32
	atomic.StoreInt32(&task.StatusCode, StatusTaskRunning)

	waitGroup := s.transferInTheBackground(recordsChannel, transfer, task, &fetchCompleted)
	var SQL = transfer.Source.Name
	if !strings.Contains(strings.ToUpper(SQL), "SELECT ") {
		SQL = "SELECT * FROM " + transfer.Source.Name
	}
	counter := 0

	err = manager.ReadAllWithHandler(SQL, []interface{}{}, func(scanner dsc.Scanner) (bool, error) {
		record := make(map[string]interface{})
		atomic.AddInt32(&task.Progress.RecordRead, 1)
		err := scanner.Scan(&record)
		if err != nil {
			return false, fmt.Errorf("failed to scan:%v", err)
		}
		select {
		case recordsChannel <- record:
		case <-time.After(5 * time.Second):
		}
		counter++
		toContinue := s.isRunning(task)
		return toContinue, nil
	})

	atomic.StoreInt32(&fetchCompleted, 1)
	if err != nil {
		return nil, err
	}
	meta := &Meta{
		Status: &ProcessingStatus{
			RecordProcessed:   int(task.Progress.RecordProcessed),
			ResourcePending:   0,
			ResourceProcessed: 1,
		},
	}

	waitGroup.Wait()
	return []*Meta{meta}, nil
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
	if len(candidates) == 0 {
		log.Printf("Finish, no candidate to process for %s\n", transfer.Name)
		return nil, nil
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
	// if transfer.MaxParallelTransfers == 0 {
	// 	transfer.MaxParallelTransfers = 4
	// }
	// limiter := toolbox.NewBatchLimiter(transfer.MaxParallelTransfers, len(storageTransfers))
	var wg sync.WaitGroup
	for _, storageTransfer := range storageTransfers {
		wg.Add(1)
		go func(storageTransfer *StorageObjectTransfer) {
			//limiter.Acquire()
			//defer limiter.Done()
			defer wg.Done()
			meta, e := s.transferFromURLSource(storageTransfer, task)
			if e != nil {
				err = e
			}
			//limiter.Mutex.Lock()
			//defer limiter.Mutex.Unlock()
			if meta != nil {
				meta.lock.Lock()
				result = append(result, meta)
				meta.lock.Unlock()
			}

		}(storageTransfer)
	}
	//limiter.Wait()
	wg.Wait()
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
		if candidate.FileInfo().Size() == 0 {
			//Skipping zero byte files
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
	logger.Printf("Process %v files for JobId:%v * %v * %v\n", len(storageTransfer.StorageObjects), storageTransfer.Transfer.Name, storageTransfer.Transfer.Source.Name, storageTransfer.Transfer.Target.Name)
	if len(storageTransfer.StorageObjects) == 0 {
		return nil, nil
	}
	transfer := storageTransfer.Transfer
	switch strings.ToLower(transfer.Target.Type) {
	case SourceTypeURL:
		return s.transferFromURLToURL(storageTransfer, task)
	case SourceTypeDatastore:
		return s.transferFromURLToDatastore(storageTransfer, task)
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

func (s *transferService) transferFromURLToDatastore(storageTransfer *StorageObjectTransfer, task *TransferTask) (m *Meta, err error) {
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
	schema, err := bigquery.SchemaFromFile(target.Schema.Name)
	if err != nil {
		return nil, err
	}
	var URIs = make([]string, 0)
	for _, storageObject := range storageTransfer.StorageObjects {
		URIs = append(URIs, storageObject.URL())
		atomic.AddInt32(&task.Progress.FileProcessed, 1)
	}
	job := &bigquery.LoadJob{
		Credential: storageTransfer.Transfer.Target.CredentialFile,
		TableID:    resourceFragments[1],
		DatasetID:  resourceFragments[0],
		ProjectID:  parsedUrl.Host,
		Schema:     schema,
		URIs:       URIs,
	}
	task.UpdateElapsed()

	logger.Printf("loading: Table:%v * Dataset:%v * Files:%v\n", job.TableID, job.DatasetID, len(URIs))
	status, jobId, err := bigquery.New().Load(job)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GBQ load job: %v", err)
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
			if strings.Contains(er.Message, "Field:") || strings.Contains(er.Message, "field:") {
				// Log this to meta file so we can skip it next time.
				meta.Processed[er.Location] = NewObjectMeta(storageTransfer.Transfer.Source.Name,
					er.Location,
					"error loading to GBQ",
					er.Error(),
					0,
					0,
					&startTime)
			}
		}
		return nil, fmt.Errorf("failed to perform GBQ load: %v", buffer.String())
	}

	message := fmt.Sprintf("status: %v  with job id: %v", status.State, jobId)
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

func (s *transferService) transferFromURLToURL(storageTransfer *StorageObjectTransfer, task *TransferTask) (m *Meta, err error) {
	transfer := storageTransfer.Transfer
	candidates := storageTransfer.StorageObjects
	meta, e := s.LoadMeta(transfer.Meta)
	if e != nil {
		return nil, e
	}

	defer func() {
		s.updateMetaStatus(meta, storageTransfer, err)
		if e := s.persistMeta(&ResourcedMeta{meta, storageTransfer.Transfer.Meta}); e != nil {
			log.Printf("failed to persist meta status for url:%v, Error:%v", meta.URL, e)
		}
		if err == nil {
			err = e
		}
	}()

	var source = transfer.Source.Name
	var target = transfer.Target
	var metaUrl = transfer.Meta.Name
	//all processed nothing new, the current assumption is that the whole file is process at once.
	for len(candidates) == 0 {
		logger.Println("no candidates no process")
		return nil, nil
	}
	if transfer.MaxParallelTransfers == 0 {
		transfer.MaxParallelTransfers = 4
	}
	limiter := toolbox.NewBatchLimiter(transfer.MaxParallelTransfers, len(candidates))
	var transferMetaLock sync.Mutex
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
				if strings.Contains(errMessage, "failed to decode json") && !strings.Contains(errMessage, "reached max errors") {
					//let this file be processed
					if len(meta.Errors) == 0 {
						meta.Errors = make([]*Error, 0)
					}
					meta.Errors = append(meta.Errors, &Error{
						Error: errMessage,
						Time:  time.Now(),
					})
				} else if e != gzip.ErrChecksum {
					logger.Printf("failed to targetTransfer: %v \n", e)
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
			transferMetaLock.Lock()
			if len(processedTransfers) > 0 {
				workerProcessedTransferMeta = append(workerProcessedTransferMeta, &WorkerProcessedTransferMeta{
					ProcessedTransfers: processedTransfers,
					ObjectMeta:         objectMeta,
				})
			}
			meta.Processed[candidate.URL()] = objectMeta
			transferMetaLock.Unlock()
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
	logger.Printf("Completed: [%v] %v files (processed so far: %v) \n", transfer.Name, len(candidates), len(meta.Processed))
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
		TaskID:    task.Id + "--transferObject",
	}
	logger.Printf("Transfer: %s\n", source.URL())
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
		source = expandEnvironmentVariableIfPresent(source)
		target := expandDateExpressionIfPresent(transfer.Target.Name, &sourceTime)
		target = expandCurrentWorkingDirectory(target)
		target = expandEnvironmentVariableIfPresent(target)
		metaUrl := expandDateExpressionIfPresent(transfer.Meta.Name, &sourceTime)
		metaUrl = expandCurrentWorkingDirectory(metaUrl)
		metaUrl = expandEnvironmentVariableIfPresent(metaUrl)
		transferKey := source + "//" + target + "//" + metaUrl
		candidate, found := transfers[transferKey]
		if !found {
			candidate = transfer.New(source, target, metaUrl)
			transfers[transferKey] = candidate
		}
	}
	var result = make([]*Transfer, 0)
	for _, t := range transfers {
		var sourceName = t.Source.Name
		sourceName = strings.Replace(sourceName, " ", "_", len(sourceName))
		sourceName = strings.Replace(sourceName, "*", "_", len(sourceName))
		sourceName = strings.Replace(sourceName, ",", "_", len(sourceName))
		logger.Printf("Expand to: %s-%s-%s \n", sourceName, t.Target.Name, t.Meta.Name)
		result = append(result, t)
	}
	return result, nil
}

func newTransferService(transferObjectService TransferObjectService) *transferService {
	return &transferService{
		transferObjectService: transferObjectService,
	}
}
