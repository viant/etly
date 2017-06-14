package etly

import (
	"errors"
	"fmt"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

var logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)

type Service struct {
	config           *Config
	isRunning        int32
	stopNotification chan bool
	transferService  *TransferService
	taskRegistry     *TaskRegistry
}

func (s *Service) Status() string {
	tasks := s.taskRegistry.GetAll()
	if len(tasks) == 0 {
		return "OK"
	}

	for i, task := range tasks {
		if i > 10 {
			break
		}
		if task.Status == taskErrorStatus {
			return taskErrorStatus
		}
	}
	return "OK"
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
			err := s.Run()
			if err != nil {
				logger.Printf("Failed to run task %v", err)
			}

			select {
			case <-s.stopNotification:
				break
			case <-time.After(sleepDuration):
				err := s.Run()
				if err != nil {
					logger.Printf("Failed to run task %v", err)
				}
			}
		}
	}()
	return nil
}

func (s *Service) Run() error {
	var result error

	for _, transfer := range s.config.Transfers {
		now := time.Now()
		if transfer.nextRun == nil || transfer.nextRun.Unix() < now.Unix() {
			err := transfer.scheduleNextRun(now)
			if err != nil {
				logger.Printf("Failed to scedule transfer: %v %v", err, transfer)
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

func (s *Service) Stop() {
	atomic.StoreInt32(&s.isRunning, 0)
	s.stopNotification <- true
}

func NewService(config *Config) (*Service, error) {
	storageService := storage.NewService()
	transferService := &TransferService{
		storageService,
		toolbox.NewJSONDecoderFactory(),
		toolbox.NewJSONEncoderFactory(),
	}
	taskRegistry := &TaskRegistry{
		History: make([]*Task, 0),
		Active:  make([]*Task, 0),
	}
	var result = &Service{
		config:           config,
		isRunning:        0,
		stopNotification: make(chan bool, 1),
		transferService:  transferService,
		taskRegistry:     taskRegistry,
	}

	if len(config.Storage) > 0 {
		for _, store := range config.Storage {
			if store.Namespace == "" {
				store.Namespace = store.Schema
			}
			provider := NewStorageProvider().Get(store.Namespace)
			if provider == nil {
				return nil, fmt.Errorf("Failed to lookup storage provider for '%v'", store.Namespace)
			}

			service, err := provider(store)
			if err != nil {
				return nil, err
			}
			storageService.Register(store.Schema, service)
		}
	}
	return result, nil
}
