package etly

import (
	"log"
	"sync"
	"time"
)

const taskRunningStatus = "RUNNING"           //Status when a task if picked up for processing but could be empty task with no actual transferring
const taskTransferringStatus = "TRANSFERRING" //Status when a task get actual work for transferring
const taskDoneStatus = "DONE"
const taskErrorStatus = "ERROR"
const taskOKStatus = "OK"

const (
	//StatusTaskNotRunning  represent terminated task
	StatusTaskNotRunning = iota
	//StatusTaskRunning represents active copy task
	StatusTaskRunning
)

//Task represents an ETL status
type Task struct {
	Id         string
	Progress   interface{}
	Status     string
	StatusCode int32
	StartTime  time.Time
	Error      string
	*sync.Mutex
}

func (t *Task) UpdateStatus(s string) {
	t.Lock()
	defer t.Unlock()
	if t.Status == taskTransferringStatus || s == taskTransferringStatus {
		log.Printf("Changing to %v state from %v state for task:%v", s, t.Status, t.Id)
	}
	t.Status = s
}
