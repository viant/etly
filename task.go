package etly

import "time"

const taskRunningStatus = "RUNNING"
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
	StartTime time.Time
	Error      string
}
