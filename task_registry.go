package etly

import "sync"

var maxHistory = 100

type TaskRegistry struct {
	activeMutex  *sync.Mutex
	historyMutex *sync.Mutex
	Active       []*Task
	History      []*Task
}

func (t *TaskRegistry) Register(task *Task) {
	t.activeMutex.Lock()
	defer t.activeMutex.Unlock()
	var tasks = make([]*Task, 0)
	tasks = append(tasks, task)
	for _, active := range t.Active {
		if active.Status != "RUNNING" {
			tasks = append(tasks, active)
		}
		t.Archive(active)
	}
	t.Active = tasks
}

func (t *TaskRegistry) Archive(task *Task) {
	t.historyMutex.Lock()
	defer t.historyMutex.Unlock()
	var tasks = make([]*Task, 0)
	tasks = append(tasks, task)
	for _, history := range t.History {
		if len(tasks) > maxHistory {
			break
		}
		tasks = append(tasks, history)
	}
	t.History = tasks
}

func findTask(candidates []*Task, mutex *sync.Mutex, result *[]*Task, requested map[string]bool) {
	mutex.Lock()
	defer mutex.Unlock()
	for _, task := range candidates {
		if requested[task.Id] {
			*result = append(*result, task)
		}
	}
}

func (t *TaskRegistry) GetByIds(ids ...string) []*Task {
	var idMap = make(map[string]bool)
	for _, id := range ids {
		idMap[id] = true
	}
	var result = make([]*Task, 0)
	findTask(t.Active, t.activeMutex, &result, idMap)
	findTask(t.History, t.historyMutex, &result, idMap)
	return result
}

func appendTask(candidates []*Task, mutex *sync.Mutex, result *[]*Task) {
	mutex.Lock()
	defer mutex.Unlock()
	*result = append(*result, candidates...)
}

func (t *TaskRegistry) GetAll() []*Task {
	var result = make([]*Task, 0)
	appendTask(t.Active, t.activeMutex, &result)
	appendTask(t.History, t.historyMutex, &result)
	return result
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		Active:       make([]*Task, 0),
		History:      make([]*Task, 0),
		historyMutex: &sync.Mutex{},
		activeMutex:  &sync.Mutex{},
	}
}
