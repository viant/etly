package etly

import (
	"log"
	"sync"
)

const MaxHistory = 20

// TaskRegistry contains list of active and finished tasks.
type TaskRegistry struct {
	activeMutex   *sync.Mutex
	transferMutex *sync.Mutex
	historyMutex  *sync.Mutex
	Active        []*Task
	Transferring  []*Task
	History       []*Task
}

// Register a status to TaskRegistry
func (reg *TaskRegistry) Register(task *Task) {
	if task == nil {
		log.Printf("failed to register empty task to registry:%v", reg)
		return
	}
	reg.addActive(task)

	//Refresh registry
	reg.refreshRegistries()
}

// Update a registry to contain only tasks for its state. Others are segregated according to new state
func (reg *TaskRegistry) updateRegistry(oldTasks []*Task, m *sync.Mutex, taskState string) []*Task {
	var tasks = make([]*Task, 0)
	m.Lock()
	defer m.Unlock()
	for _, t := range oldTasks {
		if t == nil {
			log.Printf("invalid task found in task registry for state:%v", taskState)
			continue
		}
		if t.Status == taskState {
			tasks = append(tasks, t)
		} else {
			//State has changed. Put to appropriate registry.
			//Managing generically so has cases for all the states and need to check again for taskState to skip it
			switch t.Status {
			case taskRunningStatus:
				if t.Status == taskState {
					continue
				}
				reg.addActive(t)
			case taskTransferringStatus:
				if t.Status == taskState {
					continue
				}
				reg.addTransferring(t)
			case taskErrorStatus:
			case taskDoneStatus:
				if t.Status == taskState {
					continue
				}
				reg.addArchive(t)
			}
		}
	}
	return tasks
}

func (reg *TaskRegistry) addActive(task *Task) {
	reg.activeMutex.Lock()
	defer reg.activeMutex.Unlock()
	if task.Status != taskRunningStatus {
		task.UpdateStatus(taskRunningStatus)
	}
	reg.Active = append(reg.Active, task)
}

func (reg *TaskRegistry) addTransferring(task *Task) {
	reg.transferMutex.Lock()
	defer reg.transferMutex.Unlock()
	reg.Transferring = append(reg.Transferring, task)
}

func (reg *TaskRegistry) addArchive(task *Task) {
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

// Deprecated: Use addArchive method instead
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
	appendTask(reg.Transferring, reg.transferMutex, &result)
	appendTask(reg.History, reg.historyMutex, &result)
	return result
}

func (reg *TaskRegistry) GetByStatus(status string) []*Task {
	var tasks = make([]*Task, 0)

	//Refresh registries
	reg.refreshRegistries()

	switch status {
	case taskRunningStatus:
		appendTask(reg.Active, reg.activeMutex, &tasks)
	case taskTransferringStatus:
		appendTask(reg.Transferring, reg.transferMutex, &tasks)
	case taskErrorStatus: //Error was not maintained previously as a seperate registry. Keeping it as it until there is a need for it
	case taskDoneStatus:
		appendTask(reg.History, reg.historyMutex, &tasks)
	}
	return tasks
}

// Helper to refresh registries
func (reg *TaskRegistry) refreshRegistries() {
	reg.Active = reg.updateRegistry(reg.Active, reg.activeMutex, taskRunningStatus)
	reg.Transferring = reg.updateRegistry(reg.Transferring, reg.transferMutex, taskTransferringStatus)
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		History:       make([]*Task, 0),
		Active:        make([]*Task, 0),
		Transferring:  make([]*Task, 0),
		activeMutex:   &sync.Mutex{},
		transferMutex: &sync.Mutex{},
		historyMutex:  &sync.Mutex{},
	}
}
