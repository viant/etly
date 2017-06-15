package etly

const taskRunningStatus = "RUNNING"
const taskDoneStatus = "DONE"
const taskErrorStatus = "ERROR"
const taskOKStatus = "OK"

//Task represents an ETL task
type Task struct {
	Id       string
	Progress interface{}
	Status   string
	Error    string
}
