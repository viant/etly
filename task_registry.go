package etly

import (
	"log"
	"sync"
)

const MaxHistory = 20

// TaskRegistry contains list of active and finished tasks.
type TaskRegistry struct {
	activeMutex  *sync.Mutex
	historyMutex *sync.Mutex
	Active       []*Task
	History      []*Task
}

// Register a status to TaskRegistry
func (reg *TaskRegistry) Register(task *Task) {
	if task == nil {
		log.Printf("failed to register empty task to registry:%v", reg)
		return
	}
	reg.activeMutex.Lock()
	defer reg.activeMutex.Unlock()
	var tasks = make([]*Task, 0)
	tasks = append(tasks, task)
	for _, t := range reg.Active {
		if t == nil {
			log.Printf("invalid task found in t task registry:%v", reg)
			continue
		}
		if t.Status == taskRunningStatus || t.Status == taskTransferringStatus {
			tasks = append(tasks, t)
		} else {
			reg.Archive(t)
		}
	}
	reg.Active = tasks
}

func (reg *TaskRegistry) Archive(task *Task) {
	reg.historyMutex.Lock()
	defer reg.historyMutex.Unlock()
	var tasks = make([]*Task, 0)
	tasks = append(tasks, task)

	for _, history := range reg.History {
		if len(tasks) > MaxHistory {
			break
		}
		tasks = append(tasks, history)
	}
	reg.History = tasks
}

func findTask(candidates []*Task, mutex sync.Locker, result *[]*Task, requested map[string]bool) {
	mutex.Lock()
	defer mutex.Unlock()
	for _, task := range candidates {
		if requested[task.Id] {
			*result = append(*result, task)
		}
	}
}

func (reg *TaskRegistry) GetByIDs(ids ...string) []*Task {
	var idMap = make(map[string]bool)
	for _, id := range ids {
		idMap[id] = true
	}
	var result = make([]*Task, 0)
	findTask(reg.Active, reg.activeMutex, &result, idMap)
	findTask(reg.History, reg.historyMutex, &result, idMap)
	return result
}

func appendTask(candidates []*Task, mutex sync.Locker, result *[]*Task) {
	mutex.Lock()
	defer mutex.Unlock()
	*result = append(*result, candidates...)
}

func (reg *TaskRegistry) GetAll() []*Task {
	var result = make([]*Task, 0)
	appendTask(reg.Active, reg.activeMutex, &result)
	appendTask(reg.History, reg.historyMutex, &result)
	return result
}

// Get active tasks additionally filterable by status
func (reg *TaskRegistry) GetActive(status string) []*Task {
	if status == "" {
		return reg.Active
	}
	var result = make([]*Task, 0)
	for _, t := range reg.Active {
		if t.Status == status {
			result = append(result, t)
		}
	}
	return result
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		History:      make([]*Task, 0),
		Active:       make([]*Task, 0),
		activeMutex:  &sync.Mutex{},
		historyMutex: &sync.Mutex{},
	}
}
