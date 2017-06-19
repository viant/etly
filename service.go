package etly

import (
	"errors"
	"github.com/viant/toolbox"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

var logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)

const MaxStatusTaskCount = 10

type Service struct {
	config           *Config
	isRunning        int32
	stopNotification chan bool
	transferService  *transferService
	taskRegistry     *TaskRegistry
}

func (s *Service) Status() string {
	tasks := s.taskRegistry.GetAll()
	if len(tasks) == 0 {
		return taskOKStatus
	}
	for i, task := range tasks {
		if i > MaxStatusTaskCount {
			break
		}
		if task.Status == taskErrorStatus {
			return taskErrorStatus
		}
	}
	return taskOKStatus
}

func (s *Service) Start() error {
	if !atomic.CompareAndSwapInt32(&s.isRunning, 0, 1) {
		return errors.New("Service has been already started")
	}

	go func() {
		var sleepDuration = time.Second
		for {
			isRunning := atomic.LoadInt32(&s.isRunning)
			if isRunning == 0 {
				break
			}
			select {
			case <-s.stopNotification:
				break
			case <-time.After(sleepDuration):
				err := s.run()
				if err != nil {
					logger.Printf("Failed to run task %v", err)
				}
			}
		}
	}()
	return nil
}

func (s *Service) run() error {
	var result error
	for _, transfer := range s.config.Transfers {
		now := time.Now()
		if transfer.nextRun == nil || transfer.nextRun.Unix() < now.Unix() {
			err := transfer.scheduleNextRun(now)
			if err != nil {
				logger.Printf("Failed to schedule transfer: %v %v", err, transfer)
				result = err
				continue
			}
			task := NewTransferTask(transfer)
			s.taskRegistry.Register(task.Task)
			err = s.transferService.Run(task)
			if err != nil {
				logger.Printf("Failed to transfer: %v %v", err, transfer)
				result = err
			}
		}
	}
	return result
}

func (s *Service) GetTasks(request http.Request, ids ...string) []*Task {
	var result []*Task
	if len(ids) == 0 {
		result = s.taskRegistry.GetAll()
	} else {
		result = s.taskRegistry.GetByIds(ids...)
	}
	request.ParseForm()
	offset := toolbox.AsInt(request.Form.Get("offset"))
	limit := toolbox.AsInt(request.Form.Get("limit"))
	if limit == 0 || limit > len(result) {
		limit = len(result)
	}
	return result[offset:limit]
}

func (s *Service) GetErrors() []*ObjectMeta {
	corruptedFiles := make([]*ObjectMeta, 0)
	for _, transfer := range s.config.Transfers {
		meta, err  := s.transferService.LoadMeta(transfer.Meta)
		if err != nil {
			logger.Printf("Failed to load Meta file: %v %v", transfer, err)
			continue
		}
		for _, processedFile := range meta.Processed {
			if processedFile.Error != "" {
				corruptedFiles = append(corruptedFiles, processedFile)
			}
		}
	}
	return corruptedFiles
}

func (s *Service) Stop() {
	atomic.StoreInt32(&s.isRunning, 0)
	s.stopNotification <- true
}

func NewService(config *Config) (*Service, error) {
	transferService := newTransferService(toolbox.NewJSONDecoderFactory(), toolbox.NewJSONEncoderFactory())
	taskRegistry := NewTaskRegistry()
	var result = &Service{
		config:           config,
		isRunning:        0,
		stopNotification: make(chan bool, 1),
		transferService:  transferService,
		taskRegistry:     taskRegistry,
	}
	return result, nil
}
