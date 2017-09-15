package etly

import (
	"sync"

	"github.com/viant/toolbox"
)

var filterRegistry *FilterRegistry

type FilterRegistry struct {
	lock     sync.Mutex
	registry map[string]toolbox.Predicate
}

func (r *FilterRegistry) Register(name string, predicate toolbox.Predicate) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.registry[name] = predicate
}

func NewFilterRegistry() *FilterRegistry {
	if filterRegistry != nil {
		return filterRegistry
	}
	filterRegistry = &FilterRegistry{
		registry: make(map[string]toolbox.Predicate),
	}
	return filterRegistry
}
